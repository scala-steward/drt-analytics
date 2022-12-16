package uk.gov.homeoffice.drt.analytics.prediction

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.Timeout
import akka.{Done, NotUsed}
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.slf4j.LoggerFactory
import uk.gov.homeoffice.drt.analytics.actors.{ExamplesAndUpdates, ModelUpdate}
import uk.gov.homeoffice.drt.analytics.actors.TouchdownPredictionActor.RegressionModelFromSpark
import uk.gov.homeoffice.drt.analytics.time.SDate
import uk.gov.homeoffice.drt.ports.Terminals
import uk.gov.homeoffice.drt.ports.Terminals.Terminal
import uk.gov.homeoffice.drt.prediction.Feature.OneToMany
import uk.gov.homeoffice.drt.prediction.{FeaturesWithOneToManyValues, RegressionModel}

import scala.concurrent.{ExecutionContext, Future}

case class FlightRouteValuesTrainer[MI](examplesAndUpdates: ExamplesAndUpdates[MI]) {
  private val log = LoggerFactory.getLogger(getClass)

  def trainTerminals(terminals: List[Terminal])
                    (implicit system: ActorSystem, ec: ExecutionContext, mat: Materializer, timeout: Timeout): Future[Done] =
    Source(terminals)
      .mapAsync(1) { terminal =>
        train(150, 20, terminal).map(r => logStats(terminal, r))
      }
      .runWith(Sink.ignore)

  def logStats(terminal: Terminal, result: Seq[Option[Double]]): Unit = {
    val total = result.size
    val modelCount = result.count(_.isDefined)
    val threshold = 10
    val improvementsOverThreshold = result.collect { case Some(imp) if imp >= threshold => imp }.size
    log.info(s"Terminal ${terminal.toString}: $total total, $modelCount models, $improvementsOverThreshold >= $threshold% improvement")
  }

  def train(daysOfData: Int, validationSetPct: Int, terminal: Terminals.Terminal)
           (implicit system: ActorSystem, ec: ExecutionContext, mat: Materializer, timeout: Timeout): Future[Seq[Option[Double]]] = {

    implicit val session: SparkSession = SparkSession
      .builder
      .appName("DRT Analytics")
      .config("spark.master", "local")
      .getOrCreate()

    val start = SDate.now().addDays(-1)
    val columnNames = List("label", "dayOfTheWeek", "partOfDay", "index")
    val features = List(OneToMany(List("dayOfTheWeek"), "dow"), OneToMany(List("partOfDay"), "pod"))

    val trainingSetPct = 100 - validationSetPct

    examplesAndUpdates.modelIdWithExamples(terminal, start, daysOfData)
      .map {
        case (modelIdentifier, fullExamples) =>
          val withIndex: Map[(Long, Double), Int] = fullExamples.zipWithIndex
          val dataFrame = prepareDataFrame(columnNames, withIndex)
          removeOutliers(dataFrame) match {
            case examples if examples.count() <= 5 =>
              examplesAndUpdates.updateModel(modelIdentifier, None)

              None
            case withoutOutliers =>
              val trainingExamples = (fullExamples.size.toDouble * (trainingSetPct.toDouble / 100)).toInt
              val dataSet = DataSet(withoutOutliers, features)
              val lrModel: LinearRegressionModel = dataSet.trainModel("label", trainingSetPct)
              val improvementPct = calculateImprovementPct(dataSet, withIndex, lrModel, validationSetPct)
              val regressionModel = RegressionModelFromSpark(lrModel)
              val updateModel = ModelUpdate(regressionModel, dataSet.featuresWithOneToManyValues, trainingExamples, improvementPct)
              examplesAndUpdates.updateModel(modelIdentifier, Option(updateModel))

              Some(improvementPct)
          }
      }
      .runWith(Sink.seq)
  }

  private def calculateImprovementPct(dataSet: DataSet, withIndex: Map[(Long, Double), Int], model: LinearRegressionModel, validationSetPct: Int)
                                     (implicit session: SparkSession): Double = {
    val labelsAndPredictions = dataSet
      .predict("label", validationSetPct, model)
      .rdd
      .map { row =>
        val idx = row.getAs[String]("index")
        withIndex.find(_._2.toString == idx).map {
          case ((_, label), _) =>
            val prediction = Math.round(row.getAs[Double]("prediction"))
            (label.toDouble, prediction.toDouble)
        }.getOrElse((0d, 0d))
      }
    val labelsAndValues = dataSet.df.rdd.map { r =>
      (r.getAs[Double]("label"), 0d)
    }
    val predMetrics = new RegressionMetrics(labelsAndPredictions)
    val schMetrics = new RegressionMetrics(labelsAndValues)
    val improvement = schMetrics.rootMeanSquaredError - predMetrics.rootMeanSquaredError
    val pctImprovement = (improvement / schMetrics.rootMeanSquaredError) * 100

    log.info(s"RMSE = ${predMetrics.rootMeanSquaredError.round} Vs ${schMetrics.rootMeanSquaredError.round} -> ${improvement.round} / ${pctImprovement.round}%")

    pctImprovement
  }

  private def prepareDataFrame(columnNames: List[String], valuesZippedWithIndex: Map[(Long, Double), Int])
                              (implicit session: SparkSession): Dataset[Row] = {
    import session.implicits._

    valuesZippedWithIndex
      .map {
        case ((scheduledMillis, targetValue), idx) =>
          val mornAft = s"${SDate(scheduledMillis).getHours / 12}"
          (targetValue, SDate(scheduledMillis).getDayOfWeek.toString, mornAft, idx.toString)
      }
      .toList.toDF(columnNames: _*)
      .sort("label")
  }

  private def removeOutliers(dataFrame: Dataset[Row]): Dataset[Row] = {
    val quantiles = dataFrame.stat.approxQuantile("label", Array(0.25, 0.75), 0.0)
    val q1 = quantiles(0)
    val q3 = quantiles(1)
    val iqr = q3 - q1
    val lowerRange = q1 - 1.5 * iqr
    val upperRange = q3 + 1.5 * iqr
    dataFrame.filter(s"$lowerRange < label and label < $upperRange")
  }
}
