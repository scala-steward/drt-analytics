package uk.gov.homeoffice.drt.analytics.prediction

import akka.actor.{ActorSystem, Props, Terminated}
import akka.pattern.ask
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import akka.util.Timeout
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import uk.gov.homeoffice.drt.actor.TerminalDateActor.{ArrivalKey, GetState}
import uk.gov.homeoffice.drt.analytics.prediction.FlightsMessageValueExtractor.minutesOffSchedule
import uk.gov.homeoffice.drt.analytics.prediction.flights.{FlightRoutesValuesExtractor, FlightValueExtractionActor}
import uk.gov.homeoffice.drt.ports.Terminals.T2
import uk.gov.homeoffice.drt.time.SDate

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}


class MinutesOffScheduledMock extends FlightValueExtractionActor(T2, 2020, 10, 1, minutesOffSchedule) {
  byKey = Map(ArrivalKey(0L, "T2", 1) -> ((2d, Seq("a", "b")), "MMM"))
}

class ArrivalTimeSpec extends AnyWordSpec with Matchers {
  implicit val timeout: Timeout = new Timeout(5.second)

  val context: (ActorSystem => ExecutionContext => Materializer => Any) => Future[Terminated] = (test: ActorSystem => ExecutionContext => Materializer => Any) => {
    implicit val system: ActorSystem = ActorSystem("test")
    implicit val ec: ExecutionContextExecutor = system.dispatcher
    implicit val mat: Materializer = Materializer(system)

    test(system)(ec)(mat)

    system.terminate()
  }

  "A MinutesOffScheduledActor" should {
    "recover some state" in context {
      implicit system =>
        implicit ec =>
          implicit mat =>
            val actor = system.actorOf(Props(new MinutesOffScheduledMock))
            val result = Await.result(actor.ask(GetState).mapTo[Map[ArrivalKey, Int]], 1.second)

            result.size should not be 0
    }

    "provide a stream of arrivals across a day range" in context {
      implicit system =>
        implicit ec =>
          implicit mat =>
            val start = SDate(2020, 10, 1, 0, 0)
            val days = 10

            val arrivals = FlightRoutesValuesExtractor[FlightValueExtractionActor](classOf[FlightValueExtractionActor], minutesOffSchedule).extractedValueByFlightRoute(T2, start, days)

            val result = Await.result(arrivals.runWith(Sink.seq), 5.seconds)

            result.forall(_._2.nonEmpty) should be(true)
    }
  }
}
