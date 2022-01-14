package uk.gov.homeoffice.drt.analytics.prediction

import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.pattern.ask
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import akka.util.Timeout
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.slf4j.LoggerFactory
import uk.gov.homeoffice.drt.analytics.actors.{MinutesOffScheduledActor, TouchdownPredictionActor}
import uk.gov.homeoffice.drt.analytics.actors.TouchdownPredictionActor.{RegressionModel, TouchdownModelAndFeatures}
import uk.gov.homeoffice.drt.analytics.prediction.FeatureType.OneToMany
import uk.gov.homeoffice.drt.analytics.time.SDate
import uk.gov.homeoffice.drt.ports.Terminals

import scala.concurrent.{ExecutionContext, Future}

object TouchdownTrainer {
  private val log = LoggerFactory.getLogger(getClass)

  def train(daysOfData: Int, validationSetPct: Int, terminal: Terminals.Terminal)
           (implicit system: ActorSystem, ec: ExecutionContext, session: SparkSession, mat: Materializer, timeout: Timeout): Future[Seq[Option[Double]]] = {
    val start = SDate.now().addDays(-1)
    val columnNames = List("label", "dayOfTheWeek", "partOfDay", "index")
    val features = List(OneToMany(List("dayOfTheWeek"), "dow"), OneToMany(List("partOfDay"), "pod"))

    val trainingSetPct = 100 - validationSetPct

    MinutesOffScheduledActor
      .offScheduledByTerminalFlightNumberOrigin(terminal, start, daysOfData)
      .map {
        case (_, offScheduleExamples) if offScheduleExamples.size <= 5 => None
        case ((terminal, number, origin), offScheduleExamples) =>
          val withIndex: Map[(Long, Int), Int] = addIndex(offScheduleExamples)
          val dataFrame = prepareDataFrame(columnNames, withIndex)
          val withoutOutliers = removeOutliers(dataFrame)

          val trainingExamples = (offScheduleExamples.size.toDouble * (trainingSetPct.toDouble / 100)).toInt

          val dataSet = DataSet(withoutOutliers, features)
          val model: LinearRegressionModel = dataSet.trainModel("label", trainingSetPct)

          val improvementPct = calculateImprovementPct(dataSet, withIndex, model, validationSetPct)

          val modelAndFeatures = TouchdownModelAndFeatures(RegressionModel(model), dataSet.features, trainingExamples, improvementPct.toInt)

          val actor = system.actorOf(Props(new TouchdownPredictionActor(() => SDate.now(), terminal, number, origin)))
          actor.ask(modelAndFeatures).map(_ => actor ! PoisonPill)

          Some(improvementPct)
      }
      .runWith(Sink.seq)
  }

  private def calculateImprovementPct(dataSet: DataSet, withIndex: Map[(Long, Int), Int], model: LinearRegressionModel, validationSetPct: Int)
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
    val labelsAndScheduleds = dataSet.df.rdd.map { r =>
      (r.getAs[Double]("label"), 0d)
    }
    val predMetrics = new RegressionMetrics(labelsAndPredictions)
    val schMetrics = new RegressionMetrics(labelsAndScheduleds)
    val improvement = schMetrics.rootMeanSquaredError - predMetrics.rootMeanSquaredError
    val pctImprovement = (improvement / schMetrics.rootMeanSquaredError) * 100

    log.info(s"RMSE = ${predMetrics.rootMeanSquaredError.round} Vs ${schMetrics.rootMeanSquaredError.round} -> ${improvement.round} / ${pctImprovement.round}%")

    pctImprovement
  }

  private def prepareDataFrame(columnNames: List[String], offScheduledsWithIndex: Map[(Long, Int), Int])
                              (implicit session: SparkSession): Dataset[Row] = {
    import session.implicits._

    offScheduledsWithIndex
      .map {
        case ((scheduled, offScheduled), idx) =>
          val mornAft = s"${SDate(scheduled).hours / 12}"
          (offScheduled.toDouble, SDate(scheduled).dayOfWeek.toString, mornAft, idx.toString)
      }
      .toList.toDF(columnNames: _*)
      .sort("label")
  }

  private def addIndex(offScheduleds: Map[Long, Int]): Map[(Long, Int), Int] = {
    offScheduleds
      .map { case (sch, off) => (sch, off / 60000) }
      .zipWithIndex
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
