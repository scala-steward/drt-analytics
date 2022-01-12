package uk.gov.homeoffice.drt.analytics.prediction

import akka.actor.{ActorSystem, PoisonPill, Props, Terminated}
import akka.pattern.ask
import akka.stream.scaladsl.Sink
import akka.stream.{ActorMaterializer, Materializer}
import akka.util.Timeout
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import uk.gov.homeoffice.drt.analytics.actors.MinutesOffScheduledActor.{ArrivalKey, GetState}
import uk.gov.homeoffice.drt.analytics.actors.TouchdownPredictionActor.{RegressionModel, TouchdownModelAndFeatures}
import uk.gov.homeoffice.drt.analytics.actors.{MinutesOffScheduledActor, TouchdownPredictionActor}
import uk.gov.homeoffice.drt.analytics.prediction.FeatureType.OneToMany
import uk.gov.homeoffice.drt.analytics.time.SDate
import uk.gov.homeoffice.drt.ports.Terminals.{T1, T2}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}


class ArrivalTimeSpec extends AnyWordSpec with Matchers {
  implicit val timeout: Timeout = new Timeout(5.second)

  val context: (ActorSystem => ExecutionContext => Materializer => Any) => Future[Terminated] = (test: ActorSystem => ExecutionContext => Materializer => Any) => {
    implicit val system: ActorSystem = ActorSystem("test")
    implicit val ec: ExecutionContextExecutor = system.dispatcher
    implicit val mat: ActorMaterializer = ActorMaterializer.create(system)

    test(system)(ec)(mat)

    system.terminate()
  }

  "A MinutesOffScheduledActor" ignore {
    "recover some state" in context {
      implicit system =>
        implicit ec =>
          implicit mat =>
            val actor = system.actorOf(Props(new MinutesOffScheduledActor(T2, 2020, 10, 1)))
            val result = Await.result(actor.ask(GetState).mapTo[Map[ArrivalKey, Int]], 1.second)

            result.size should not be 0
    }
    "provide a stream of arrivals across a day range" in context {
      implicit system =>
        implicit ec =>
          implicit mat =>
            val start = SDate(2020, 10, 1, 0, 0)
            val days = 10

            val arrivals = MinutesOffScheduledActor.offScheduledByTerminalFlightNumberOrigin(T2, start, days)

            val result = Await.result(arrivals.runWith(Sink.seq), 5.seconds)

            result.forall(_._2.nonEmpty) should be(true)
    }
  }

  "A MinutesOffScheduledActor" should {
    "provide a map of (terminal, flight-number) -> scheduled -> off-scheduled" in context {

      implicit system =>
        implicit ec =>
          implicit mat =>

            implicit val session: SparkSession = SparkSession
              .builder
              .appName("DRT Analytics")
              .config("spark.master", "local")
              .getOrCreate()

            val start = SDate(2022, 1, 1, 0, 0)
            val days = 150
            val columnNames = List("label", "dayOfTheWeek", "partOfDay", "index")
            val features = List(OneToMany(List("dayOfTheWeek"), "dow"), OneToMany(List("partOfDay"), "pod"))

            val eventualResult = MinutesOffScheduledActor
              .offScheduledByTerminalFlightNumberOrigin(T1, start, days)
              .map {
                case (_, offScheduleds) if offScheduleds.size <= 5 =>
                  println(s"Not enough examples to train on (${offScheduleds.size})")
                  None
                case ((terminal, number, origin), offScheduleds) if offScheduleds.size > 5 =>
                  println(s"\nNext training set")
                  val withIndex: Map[(Long, Int), Int] = addIndex(offScheduleds)
                  val dataFrame = prepareDataFrame(columnNames, withIndex)
                  val withoutOutliers = removeOutliers(dataFrame)

                  val trainingExamples = (offScheduleds.size.toDouble * 0.8).toInt
                  println(s"Training on $trainingExamples examples")

                  val dataSet = DataSet(withoutOutliers, features)
                  val model: LinearRegressionModel = dataSet.trainModel("label", 80)

                  println(s"coefficients: (${model.coefficients.size}): ${model.coefficients}")
                  println(s"oneToManyValues: (${dataSet.features.oneToManyValues.size}): ${dataSet.features.oneToManyValues}")

                  val lrSummary = dataSet.evaluate("label", 80, model)
                  println(s"Summary: RMSE ${lrSummary.rootMeanSquaredError.round}, R2 ${lrSummary.r2}")

                  val improvementPct = calculateImprovementPct(dataSet, withIndex, model, dataSet.features)

                  val actor = system.actorOf(Props(new TouchdownPredictionActor(() => SDate.now(), terminal, number, origin)))
                  val modelAndFeatures = TouchdownModelAndFeatures(RegressionModel(model), dataSet.features, trainingExamples, improvementPct.toInt)
                  actor.ask(modelAndFeatures).map(_ => actor ! PoisonPill)
                  Some(improvementPct)
              }
              .runWith(Sink.seq)
            val result = Await.result(eventualResult, 5.minutes)
            val total = result.size
            val modelCount = result.count(_.isDefined)
            val threshold = 10
            val improvements = result.collect { case Some(imp) if imp >= threshold => imp }.size
            println(s"total: $total, models: $modelCount, $improvements >= $threshold%")
    }
  }

  private def calculateImprovementPct(dataSet: DataSet, withIndex: Map[(Long, Int), Int], model: LinearRegressionModel, features: Features)
                                     (implicit session: SparkSession): Double = {
    val labelsAndPreds = dataSet
      .predict2("label", 20, model)
      .rdd
      .map { row =>
        val idx = row.getAs[String]("index")
        withIndex.find(_._2.toString == idx).map {
          case ((_, label), _) =>
            val prediction = Math.round(row.getAs[Double]("prediction"))
            (label.toDouble, prediction.toDouble)
        }.getOrElse((0d, 0d))
      }
    val labelsAndSch = dataSet.df.rdd.map { r =>
      (r.getAs[Double]("label"), 0d)
    }
    val predMetrics = new RegressionMetrics(labelsAndPreds)
    val schMetrics = new RegressionMetrics(labelsAndSch)
    val improvement = schMetrics.rootMeanSquaredError - predMetrics.rootMeanSquaredError
    val pctImprovement = (improvement / schMetrics.rootMeanSquaredError) * 100

    println(s"RMSE = ${predMetrics.rootMeanSquaredError.round} Vs ${schMetrics.rootMeanSquaredError.round} -> ${improvement.round} / ${pctImprovement.round}%")

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
