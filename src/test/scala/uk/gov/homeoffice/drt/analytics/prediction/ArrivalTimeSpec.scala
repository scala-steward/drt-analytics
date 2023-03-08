package uk.gov.homeoffice.drt.analytics.prediction

import akka.actor.{ActorSystem, Props, Terminated}
import akka.pattern.ask
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import akka.util.Timeout
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import uk.gov.homeoffice.drt.actor.PredictionModelActor.TerminalFlightNumberOrigin
import uk.gov.homeoffice.drt.actor.TerminalDateActor.{ArrivalKey, GetState}
import uk.gov.homeoffice.drt.analytics.prediction.FlightWithSplitsMessageGenerator.generateFlightWithSplitsMessage
import uk.gov.homeoffice.drt.analytics.prediction.FlightsMessageValueExtractor.minutesOffSchedule
import uk.gov.homeoffice.drt.analytics.prediction.flights.{FlightValueExtractionActor, ValuesExtractor}
import uk.gov.homeoffice.drt.ports.Terminals.T2
import uk.gov.homeoffice.drt.protobuf.messages.CrunchState.FlightWithSplitsMessage
import uk.gov.homeoffice.drt.protobuf.messages.FlightsMessage.FlightMessage
import uk.gov.homeoffice.drt.time.{SDate, SDateLike, UtcDate}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}

object FlightWithSplitsMessageGenerator {
  def generateFlightWithSplitsMessage(terminal: String, flightCode: String, origin: String, scheduled: Long): FlightWithSplitsMessage = {
    FlightWithSplitsMessage(flight = Option(
      FlightMessage(
        terminal = Option(terminal),
        iATA = Option(flightCode),
        origin = Option(origin),
        scheduled = Option(scheduled),
        touchdown = Option(scheduled),
      )
    ))
  }

}

class MinutesOffScheduledMock(scheduled: SDateLike) extends FlightValueExtractionActor(T2, UtcDate(2020, 10, 1), minutesOffSchedule, TerminalFlightNumberOrigin.fromMessage) {
  byArrivalKey = Map(
    ArrivalKey(0L, "T2", 1) -> generateFlightWithSplitsMessage("T2", "BA0001", "LHR", scheduled.millisSinceEpoch),
  )
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
    val scheduled = SDate(2020, 10, 1, 12, 5)
    "recover some state" in context {
      implicit system =>
        implicit ec =>
          implicit mat =>
            val actor = system.actorOf(Props(new MinutesOffScheduledMock(scheduled)))
            val result = Await.result(actor.ask(GetState).mapTo[Map[ArrivalKey, Int]], 1.second)

            result.size should not be 0
    }

    "provide a stream of arrivals across a day range" in context {
      implicit system =>
        implicit ec =>
          implicit mat =>
            val start = scheduled.getLocalLastMidnight
            val days = 10

            val arrivals = ValuesExtractor(classOf[FlightValueExtractionActor], minutesOffSchedule, TerminalFlightNumberOrigin.fromMessage)
              .extractValuesByKey(T2, start, days)

            val result = Await.result(arrivals.runWith(Sink.seq), 5.seconds)

            result.forall(_._2.nonEmpty) should be(true)
    }
  }
}
