package uk.gov.homeoffice.drt.analytics.prediction

import akka.actor.{ActorSystem, Props, Terminated}
import akka.pattern.ask
import akka.stream.scaladsl.Sink
import akka.stream.{ActorMaterializer, Materializer}
import akka.util.Timeout
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.sql.SparkSession
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import uk.gov.homeoffice.drt.analytics.actors.MinutesOffScheduledActor
import uk.gov.homeoffice.drt.analytics.actors.MinutesOffScheduledActor.{ArrivalKey, GetState}
import uk.gov.homeoffice.drt.analytics.prediction.FeatureType.OneToMany
import uk.gov.homeoffice.drt.analytics.time.SDate
import uk.gov.homeoffice.drt.ports.Terminals.T2

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

            result.size should not be (0)
    }
    "provide a stream of arrivals across a day range" in context {
      implicit system =>
        implicit ec =>
          implicit mat =>
            val start = SDate(2020, 10, 1, 0, 0)
            val days = 10

            val arrivals = MinutesOffScheduledActor.offScheduledByTerminalFlightNumber(T2, start, days)

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

            import session.implicits._

            val start = SDate(2020, 10, 1, 0, 0)
            val days = 200
            val columnNames = List("label", "dayOfTheWeek", "index")

            val eventualResult = MinutesOffScheduledActor
              .offScheduledByTerminalFlightNumber(T2, start, days)
              .filter(_._2.size > 10)
              .map {
                case ((terminal, number), offScheduleds) =>
                  val withIndex = offScheduleds.map { case (sch, off) => (sch, off / 60000) }.zipWithIndex
                  val dataFrame = withIndex
                    .map {
                      case ((scheduled, offScheduled), idx) =>
                        (offScheduled.toDouble, SDate(scheduled).dayOfWeek, idx.toString)
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

                  println(s"Training on ${(offScheduleds.size.toDouble * 0.8).toInt} examples")

                  val dataSet = DataSet(withoutOutliers, columnNames, List(OneToMany(List("dayOfTheWeek"), "dow")))
                  val model = dataSet.trainModel("label", 80)
                  val labelsAndPreds = dataSet
                    .predict("label", 20, model)
                    .rdd
                    .map { row =>
                      val idx = row.getAs[String]("index")
                      withIndex.find(_._2.toString == idx).map {
                        case ((_, label), _) =>
                          val prediction = Math.round(row.getAs[Double]("prediction"))
                          println(s"$label -> $prediction")
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

                  println(s"RMSE = ${predMetrics.rootMeanSquaredError} Vs ${schMetrics.rootMeanSquaredError}")

                  improvement
              }
              .runWith(Sink.seq)
            val improvements = Await.result(eventualResult, 5.minutes)
            println(s"RMS avg improvement = ${Math.sqrt(improvements.map(i => i*i).sum / improvements.size)}")
            session.close()
    }
  }
}
