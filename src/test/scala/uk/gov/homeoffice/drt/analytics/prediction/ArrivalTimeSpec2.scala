package uk.gov.homeoffice.drt.analytics.prediction

import akka.actor.{ActorSystem, Props, Terminated}
import akka.pattern.ask
import akka.stream.scaladsl.Sink
import akka.stream.{ActorMaterializer, Materializer}
import akka.util.Timeout
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.sql.SparkSession
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import uk.gov.homeoffice.drt.analytics.actors.MinutesOffScheduledActor2
import uk.gov.homeoffice.drt.analytics.actors.MinutesOffScheduledActor2.{ArrivalKey, GetState}
import uk.gov.homeoffice.drt.analytics.prediction.FeatureType.OneToMany
import uk.gov.homeoffice.drt.analytics.time.SDate
import uk.gov.homeoffice.drt.ports.Terminals.T2

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}


class ArrivalTimeSpec2 extends AnyWordSpec with Matchers {
  implicit val timeout: Timeout = new Timeout(5.second)

  val context: (ActorSystem => ExecutionContext => Materializer => Any) => Future[Terminated] = (test: ActorSystem => ExecutionContext => Materializer => Any) => {
    implicit val system: ActorSystem = ActorSystem("test")
    implicit val ec: ExecutionContextExecutor = system.dispatcher
    implicit val mat: ActorMaterializer = ActorMaterializer.create(system)

    test(system)(ec)(mat)

    system.terminate()
  }

  "A MinutesOffScheduledActor2" ignore {
    "recover some state" in context {
      implicit system =>
        implicit ec =>
          implicit mat =>
            val actor = system.actorOf(Props(new MinutesOffScheduledActor2(T2, 2020, 10, 1)))
            val result = Await.result(actor.ask(GetState).mapTo[Map[ArrivalKey, Int]], 1.second)

            result.size should not be (0)
    }
    "provide a stream of arrivals across a day range" in context {
      implicit system =>
        implicit ec =>
          implicit mat =>
            val start = SDate(2020, 10, 1, 0, 0)
            val days = 10

            val arrivals = MinutesOffScheduledActor2.offScheduledByTerminalFlightNumber(T2, start, days)

            val result = Await.result(arrivals.runWith(Sink.seq), 5.seconds)

            result.forall(_._2.nonEmpty) should be(true)
    }
  }

  "A MinutesOffScheduledActor2" should {
    "provide a map of (terminal, flight-number) -> scheduled -> off-scheduled" in context {

      implicit system =>
        implicit ec =>
          implicit mat =>

            implicit val session: SparkSession = SparkSession
              .builder
              .appName("DRT Analytics")
              .config("spark.master", "local")
              .getOrCreate()

            import session.implicits._

            val start = SDate(2020, 10, 1, 0, 0)
            val days = 350
            val columnNames = List("label", "dayOfTheWeek", "hhmm", "index")
            val features = List(OneToMany(List("dayOfTheWeek"), "dow"), OneToMany(List("hhmm"), "hhmm"))

            val eventualResult = MinutesOffScheduledActor2
              .offScheduledByTerminalFlightNumber(T2, start, days)
              .mapConcat {
                case ((terminal, number), offScheduleds) =>
                  offScheduleds
                    .groupBy {
                      case (_, (_, origin, carriers)) => origin
                    }
                    .values
                    .map { y => ((terminal, number), y) }.toList
              }
              .filter(_._2.size > 10)
              .map {
                case ((terminal, number), offScheduleds) =>
                  val carriers = offScheduleds.map(_._2._3).toSet
                  val scheduleds = offScheduleds.keys.map(s => f"${SDate(s).hours}%02d:${SDate(s).minutes}%02d")
                  if (carriers.size > 1) println(s"carriers: $carriers")
                  if (scheduleds.size > 1) println(s"scheduleds: $scheduleds")
                  val withIndex = offScheduleds.map { case (sch, (off, origin, carrier)) => (sch, (off / 60000, origin)) }.zipWithIndex
                  val dataFrame = withIndex
                    .map {
                      case ((scheduled, (offScheduled, _)), idx) =>
                        val hhmm = f"${SDate(scheduled).hours}%02d:${SDate(scheduled).minutes}%02d"
                        (offScheduled.toDouble, SDate(scheduled).dayOfWeek, hhmm, idx.toString)
                    }
                    .toList.toDF(columnNames: _*)
                    .sort("label")

                  val quantiles = dataFrame.stat.approxQuantile("label", Array(0.25, 0.75), 0.0)
                  val q1 = quantiles(0)
                  val q3 = quantiles(1)
                  val iqr = q3 - q1
                  val lowerRange = q1 - 1.5 * iqr
                  val upperRange = q3 + 1.5 * iqr
                  val withoutOutliers = dataFrame.filter(s"$lowerRange < label and label < $upperRange")

                  println(s"\nTraining on ${(offScheduleds.size.toDouble * 0.8).toInt} examples")

                  val dataSet = DataSet(withoutOutliers, columnNames, features)
                  val model: LinearRegressionModel = dataSet.trainModel("label", 80)
                  model.coefficients.toArray
                  new LinearRegressionModel()
                  //val serialised = model.ser
                  val lrSummary = dataSet.evaluate("label", 80, model)
                  println(s"Summary: RMSE ${lrSummary.rootMeanSquaredError.round}, R2 ${lrSummary.r2}")
                  val labelsAndPreds = dataSet
                    .predict("label", 20, model)
                    .rdd
                    .map { row =>
                      val idx = row.getAs[String]("index")
                      withIndex.find(_._2.toString == idx).map {
                        case ((_, (label, origin)), _) =>
                          val prediction = Math.round(row.getAs[Double]("prediction"))
                          (label.toDouble, prediction.toDouble)
                      }.getOrElse((0d, 0d))
                    }
                  val labelsAndSch = dataSet.df
                    .rdd
                    .map { r =>
                      (r.getAs[Double]("label"), 0d)
                    }
                  val predMetrics = new RegressionMetrics(labelsAndPreds)
                  val schMetrics = new RegressionMetrics(labelsAndSch)

                  val improvement = schMetrics.rootMeanSquaredError - predMetrics.rootMeanSquaredError
                  val pctImprovement = (improvement / schMetrics.rootMeanSquaredError) * 100

                  println(s"RMSE = ${predMetrics.rootMeanSquaredError.round} Vs ${schMetrics.rootMeanSquaredError.round} -> ${improvement.round} / ${pctImprovement.round}%")

                  (improvement, pctImprovement)
              }
              .runWith(Sink.seq)
            val allImprovements = Await.result(eventualResult, 5.minutes)
            val improvements = allImprovements.filter(_._1 > 0)
            val goodThreshold = 20
            val goodImprovements = allImprovements.filter(_._2 > goodThreshold)
            val rmseAll = Math.sqrt(allImprovements.map { case (i, _) => i * i }.sum / allImprovements.size)
            val total = allImprovements.size
            val totalImprovements = improvements.size
            val totalGoodImprovements = goodImprovements.size
            val rmsImprovements = Math.sqrt(improvements.filter(_._1 > 0).map { case (i, _) => i * i }.sum / improvements.size)
            val rmsGoodImprovements = Math.sqrt(goodImprovements.filter(_._2 > goodThreshold).map { case (i, _) => i * i }.sum / goodImprovements.size)
            println(s"Total: $total, $totalImprovements improvements, $totalGoodImprovements improvements >= $goodThreshold%")
            println(s"RMSE: all $rmseAll, imps $rmsImprovements, good $rmsGoodImprovements")
            session.close()
    }
  }
}
