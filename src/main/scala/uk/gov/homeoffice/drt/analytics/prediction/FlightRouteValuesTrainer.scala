package uk.gov.homeoffice.drt.analytics.prediction

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.Timeout
import akka.{Done, NotUsed}
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.slf4j.LoggerFactory
import uk.gov.homeoffice.drt.actor.PredictionModelActor.{ModelUpdate, RegressionModelFromSpark}
import uk.gov.homeoffice.drt.actor.TerminalDateActor.FlightRoute
import uk.gov.homeoffice.drt.analytics.prediction.FlightRouteValuesTrainer.ModelExamplesProvider
import uk.gov.homeoffice.drt.ports.Terminals
import uk.gov.homeoffice.drt.ports.Terminals.Terminal
import uk.gov.homeoffice.drt.prediction.Feature.{OneToMany, Single}
import uk.gov.homeoffice.drt.prediction.{Feature, Persistence}
import uk.gov.homeoffice.drt.time.{SDate, SDateLike}

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.SeqHasAsJava

object FlightRouteValuesTrainer {
  type ModelExamplesProvider[MI] = (Terminal, SDateLike, Int) => Source[(MI, Iterable[(Double, Seq[String])]), NotUsed]
}

case class FlightRouteValuesTrainer(modelName: String,
                                    examplesProvider: ModelExamplesProvider[FlightRoute],
                                    persistence: Persistence[FlightRoute],
                                    baselineValue: Double,
                                    daysOfTrainingData: Int,
                                   ) {
  private val log = LoggerFactory.getLogger(getClass)

  def trainTerminals(terminals: List[Terminal])
                    (implicit system: ActorSystem, ec: ExecutionContext, mat: Materializer, timeout: Timeout): Future[Done] =
    Source(terminals)
      .mapAsync(1) { terminal =>
        train(daysOfTrainingData, 20, terminal).map(r => logStats(terminal, r))
      }
      .runWith(Sink.ignore)

  private def logStats(terminal: Terminal, result: Seq[Option[Double]]): Unit = {
    val total = result.size
    val modelCount = result.count(_.isDefined)
    val threshold = 10
    val improvementsOverThreshold = result.collect { case Some(imp) if imp >= threshold => imp }.size
    log.info(s"Terminal ${terminal.toString}: $total total, $modelCount models, $improvementsOverThreshold >= $threshold% improvement")
  }

  private def train(daysOfData: Int, validationSetPct: Int, terminal: Terminals.Terminal)
                   (implicit system: ActorSystem, ec: ExecutionContext, mat: Materializer, timeout: Timeout): Future[Seq[Option[Double]]] = {
    implicit val session: SparkSession = SparkSession
      .builder
      .appName("DRT Analytics")
      .config("spark.master", "local")
      .getOrCreate()

    val start = SDate.now().addDays(-1)
    val features: List[Feature] = List(OneToMany(List("dayOfTheWeek"), "dow"), OneToMany(List("partOfDay"), "pod"))
    val featureColumnNames: List[String] = features.flatMap {
      case OneToMany(colNames, _) => colNames
      case Single(colName) => List(colName)
    }

    val trainingSetPct = 100 - validationSetPct

    examplesProvider(terminal, start, daysOfData)
      .map {
        case (modelIdentifier, allExamples) =>
          val withIndex: Iterable[((Double, Seq[String]), Int)] = allExamples.zipWithIndex
          val dataFrame = prepareDataFrame(featureColumnNames, withIndex)
          removeOutliers(dataFrame) match {
            case examples if examples.count() <= 5 =>
              persistence.updateModel(modelIdentifier, modelName, None)
              None
            case withoutOutliers =>
              val trainingExamples = (allExamples.size.toDouble * (trainingSetPct.toDouble / 100)).toInt
              val dataSet = DataSet(withoutOutliers, features)
              val lrModel: LinearRegressionModel = dataSet.trainModel("label", trainingSetPct)
              val improvementPct = calculateImprovementPct(dataSet, withIndex, lrModel, validationSetPct, baselineValue)
              val regressionModel = RegressionModelFromSpark(lrModel)
              val modelUpdate = ModelUpdate(regressionModel, dataSet.featuresWithOneToManyValues, trainingExamples, improvementPct, modelName)
              persistence.updateModel(modelIdentifier, modelName, Option(modelUpdate))
              Some(improvementPct)
          }
      }
      .runWith(Sink.seq)
  }

  private def calculateImprovementPct(dataSet: DataSet,
                                      withIndex: Iterable[((Double, Seq[String]), Int)],
                                      model: LinearRegressionModel,
                                      validationSetPct: Int,
                                      baselineValue: Double,
                                     )
                                     (implicit session: SparkSession): Double = {
    val labelsAndPredictions = dataSet
      .predict("label", validationSetPct, model)
      .rdd
      .map { row =>
        val idx = row.getAs[String]("index")
        withIndex.find(_._2.toString == idx).map {
          case ((label, _), _) =>
            val prediction = Math.round(row.getAs[Double]("prediction"))
            (label, prediction.toDouble)
        }.getOrElse((0d, 0d))
      }
    val labelsAndValues = dataSet.df.rdd.map { r =>
      val label = r.getAs[Double]("label")
      (label, baselineValue)
    }
    val predMetrics = new RegressionMetrics(labelsAndPredictions)
    val schMetrics = new RegressionMetrics(labelsAndValues)
    val improvement = schMetrics.rootMeanSquaredError - predMetrics.rootMeanSquaredError
    val pctImprovement = (improvement / schMetrics.rootMeanSquaredError) * 100

    log.info(s"RMSE = ${predMetrics.rootMeanSquaredError.round} Vs ${schMetrics.rootMeanSquaredError.round} -> ${improvement.round} / ${pctImprovement.round}%")

    pctImprovement
  }

  private def prepareDataFrame(columnNames: List[String], valuesZippedWithIndex: Iterable[((Double, Seq[String]), Int)])
                              (implicit session: SparkSession): Dataset[Row] = {

    val labelField = StructField("label", DoubleType, nullable = false)
    val indexField = StructField("index", StringType, nullable = false)

    val fields = columnNames.map(columnName =>
      StructField(columnName, StringType, nullable = false)
    )
    val schema = StructType(labelField +: fields :+ indexField)

    val rows = valuesZippedWithIndex.map {
      case ((labelValue, featureValues), idx) => Row(labelValue +: featureValues :+ idx.toString: _*)
    }.toList.asJava

    session.createDataFrame(rows, schema).sort("label")
  }

  private def removeOutliers(dataFrame: Dataset[Row]): Dataset[Row] = {
    val quantiles = dataFrame.stat.approxQuantile("label", Array(0.25, 0.75), 0.0)
    val q1 = quantiles(0)
    val q3 = quantiles(1)
    val iqr = q3 - q1
    val lowerRange = q1 - 1.5 * iqr
    val upperRange = q3 + 1.5 * iqr
    dataFrame.filter(s"$lowerRange <= label and label <= $upperRange")
  }
}
