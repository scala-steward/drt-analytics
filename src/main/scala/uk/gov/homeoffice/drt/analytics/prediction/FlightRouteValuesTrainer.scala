package uk.gov.homeoffice.drt.analytics.prediction

import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import akka.{Done, NotUsed}
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.slf4j.LoggerFactory
import uk.gov.homeoffice.drt.actor.PredictionModelActor.{ModelUpdate, RegressionModelFromSpark, WithId}
import uk.gov.homeoffice.drt.analytics.prediction.FlightRouteValuesTrainer.ModelExamplesProvider
import uk.gov.homeoffice.drt.ports.Terminals
import uk.gov.homeoffice.drt.ports.Terminals.Terminal
import uk.gov.homeoffice.drt.prediction.Persistence
import uk.gov.homeoffice.drt.prediction.arrival.FeatureColumns.Feature
import uk.gov.homeoffice.drt.time.{SDate, SDateLike}

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.SeqHasAsJava

object FlightRouteValuesTrainer {
  type ModelExamplesProvider[MI] = (Terminal, SDateLike, Int) => Source[(MI, Iterable[(Double, Seq[String], Seq[Double])]), NotUsed]
}

case class FlightRouteValuesTrainer(modelName: String,
                                    features: List[Feature[_]],
                                    examplesProvider: ModelExamplesProvider[WithId],
                                    persistence: Persistence,
                                    baselineValue: Terminal => Double,
                                    daysOfTrainingData: Int,
                                   ) {
  private val log = LoggerFactory.getLogger(getClass)

  implicit val session: SparkSession = SparkSession
    .builder
    .appName("DRT Analytics")
    .config("spark.master", "local")
    .getOrCreate()

  def trainTerminals(terminals: List[Terminal])
                    (implicit ec: ExecutionContext, mat: Materializer): Future[Done] =
    Source(terminals)
      .mapAsync(1) { terminal =>
        log.info(s"Training $modelName for $terminal")
        train(daysOfTrainingData, 20, terminal).map(r => logStats(terminal, r))
          .recover {
            case t =>
              log.error(s"Failed to train $modelName for $terminal", t)
          }
      }
      .runWith(Sink.ignore)

  private def logStats(terminal: Terminal, result: Seq[Option[Double]]): Unit = {
    val total = result.size
    val modelCount = result.count(_.isDefined)
    log.info(s"Terminal ${terminal.toString}: $total total, $modelCount models")

    Seq(10, 20, 30, 40, 50, 60, 70, 80, 90, 100).foreach { threshold =>
      val improvementsOverThreshold = result.collect { case Some(imp) if imp >= threshold => imp }.size
      val pctOverThresholdTotal = (improvementsOverThreshold.toDouble / total.toDouble * 100).toInt
      log.info(s"Terminal ${terminal.toString}: $pctOverThresholdTotal% >= $threshold% improvement")
    }
  }

  private def train(daysOfData: Int, validationSetPct: Int, terminal: Terminals.Terminal)
                   (implicit mat: Materializer, executionContext: ExecutionContext): Future[Seq[Option[Double]]] = {

    val start = SDate.now().addDays(-1)

    val featureColumnNames = features.map(_.label)

    val trainingSetPct = 100 - validationSetPct

    log.info(s"Training $modelName for $terminal with $daysOfData days of data, $trainingSetPct% training, $validationSetPct% validation")

    examplesProvider(terminal, start, daysOfData)
      .mapAsync(1) {
        case (modelIdentifier, allExamples) =>
          val withIndex = allExamples.zipWithIndex
          val dataFrame = prepareDataFrame(featureColumnNames, withIndex)
          removeOutliers(dataFrame) match {
            case examples if examples.count() <= 5 =>
              log.info(s"Insufficient examples for $modelIdentifier after outlier removal")
              persistence
                .updateModel(modelIdentifier, modelName, None)
                .map(_ => None)

            case withoutOutliers =>
              log.info(s"Training $modelName for $modelIdentifier with ${withoutOutliers.count()} out of ${allExamples.size} examples after outlier removal")
              val trainingExamples = (allExamples.size.toDouble * (trainingSetPct.toDouble / 100)).toInt
              val dataSet = DataSet(withoutOutliers, features)
              val lrModel: LinearRegressionModel = dataSet.trainModel("label", trainingSetPct)
              val improvementPct = calculateImprovementPct(dataSet, withIndex, lrModel, validationSetPct, baselineValue(terminal))
              val regressionModel = RegressionModelFromSpark(lrModel)
              val modelUpdate = ModelUpdate(regressionModel, dataSet.featuresWithOneToManyValues, trainingExamples, improvementPct, modelName)
              persistence
                .updateModel(modelIdentifier, modelName, Option(modelUpdate))
                .map(_ => Some(improvementPct))
                .recover {
                  case t =>
                    log.error(s"Failed to update model for $modelIdentifier", t)
                    None
                }
          }
      }
      .runWith(Sink.seq)
  }

  private def calculateImprovementPct(dataSet: DataSet,
                                      withIndex: Iterable[((Double, Seq[String], Seq[Double]), Int)],
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
          case ((label, _, _), _) =>
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

    log.info(s"RMSE = ${predMetrics.rootMeanSquaredError.round} pax Vs ${schMetrics.rootMeanSquaredError.round} pax -> ${improvement.round} pax improvement / ${pctImprovement.round}% improvement")

    pctImprovement
  }

  private def prepareDataFrame(featureColumnNames: Seq[String], valuesZippedWithIndex: Iterable[((Double, Seq[String], Seq[Double]), Int)])
                              (implicit session: SparkSession): Dataset[Row] = {
    val labelField = StructField("label", DoubleType, nullable = false)
    val indexField = StructField("index", StringType, nullable = false)

    val fields = featureColumnNames.map(columnName =>
      StructField(columnName, StringType, nullable = false)
    )
    val schema = StructType(labelField +: fields :+ indexField)

    val rows = valuesZippedWithIndex.map {
      case ((labelValue, oneToManyFeatureValues, singleFeatureValues), idx) =>
        val values = labelValue +: (oneToManyFeatureValues ++ singleFeatureValues) :+ idx.toString
        Row(values: _*)
    }.toList.asJava

    session.createDataFrame(rows, schema).sort("index")
  }

  private def removeOutliers(dataFrame: Dataset[Row]): Dataset[Row] = {
//    dataFrame.stat.approxQuantile("label", Array(0.25, 0.75), 0.0) match {
//      case quantiles if quantiles.length == 2 =>
//        val q1 = quantiles(0)
//        val q3 = quantiles(1)
//        val iqr = q3 - q1
//        val lowerRange = q1 - 1.5 * iqr
//        val upperRange = q3 + 1.5 * iqr
//        dataFrame.filter(s"$lowerRange <= label and label <= $upperRange")
//      case _ => dataFrame
//    }
    dataFrame
  }
}
