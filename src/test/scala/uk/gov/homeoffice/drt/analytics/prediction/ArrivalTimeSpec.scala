package uk.gov.homeoffice.drt.analytics.prediction

import akka.actor.{ActorSystem, Props, Terminated}
import akka.pattern.ask
import akka.stream.scaladsl.Sink
import akka.stream.{ActorMaterializer, Materializer}
import akka.util.Timeout
import org.apache.spark.sql.SparkSession
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import uk.gov.homeoffice.drt.analytics.actors.MinutesOffScheduledActor
import uk.gov.homeoffice.drt.analytics.actors.MinutesOffScheduledActor.{ArrivalKey, GetState}
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

            val eventualResult = TouchdownTrainer.train(150, 20, T1)
            val result = Await.result(eventualResult, 5.minutes)
            val total = result.size
            val modelCount = result.count(_.isDefined)
            val threshold = 10
            val improvements = result.collect { case Some(imp) if imp >= threshold => imp }.size
            println(s"total: $total, models: $modelCount, $improvements >= $threshold%")
    }
  }
}
