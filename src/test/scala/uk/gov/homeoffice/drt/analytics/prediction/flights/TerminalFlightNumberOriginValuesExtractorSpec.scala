package uk.gov.homeoffice.drt.analytics.prediction.flights

import akka.actor.{Actor, ActorSystem}
import akka.stream.scaladsl.Sink
import akka.testkit.TestKit
import akka.util.Timeout
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpecLike
import uk.gov.homeoffice.drt.actor.PredictionModelActor.TerminalFlightNumberOrigin
import uk.gov.homeoffice.drt.actor.commands.Commands.GetState
import uk.gov.homeoffice.drt.analytics.actors.TerminalDateActor
import uk.gov.homeoffice.drt.analytics.actors.TerminalDateActor.ArrivalKey
import uk.gov.homeoffice.drt.arrivals.Arrival
import uk.gov.homeoffice.drt.ports.Terminals.{T1, Terminal}
import uk.gov.homeoffice.drt.time.{SDate, UtcDate}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}

class MockFlightsActor(val terminal: Terminal,
                       val date: UtcDate,
                       val extractValues: Arrival => Option[(Double, Seq[String], Seq[Double], String)],
                       val aggregateKey: Arrival => Option[TerminalFlightNumberOrigin],
                       val preProcessing: (UtcDate, Map[ArrivalKey, Arrival]) => Future[Map[ArrivalKey, Arrival]],
                      ) extends Actor with TerminalDateActor[Arrival] {
  override def receive: Receive = {
    case GetState => sender() ! MockFlightsActor.state
  }
}

object MockFlightsActor {
  var state: Map[TerminalFlightNumberOrigin, Iterable[(Double, Seq[String], Seq[Double], String)]] = Map()
}

class TerminalFlightNumberOriginValuesExtractorSpec
  extends TestKit(ActorSystem("TerminalFlightNumberOriginsValuesExtractor"))
    with AnyWordSpecLike with BeforeAndAfterAll {

  implicit val ec: ExecutionContextExecutor = ExecutionContext.global
  implicit val timeout: Timeout = new Timeout(1.second)

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "TerminalFlightNumberOriginsValuesExtractor" should {
    val singleFlight = Map(TerminalFlightNumberOrigin("T1", 1, "JFK") -> List((0d, Seq("1", "0"), Seq(), "")))
    val multiFlights = Map(
      TerminalFlightNumberOrigin("T1", 1, "JFK") -> List((0d, Seq("1", "0"), Seq(), ""), (5d, Seq("2", "1"), Seq(), ""), (2d, Seq("3", "0"), Seq(), "")),
      TerminalFlightNumberOrigin("T2", 5555, "ABC") -> List((1d, Seq("6", "1"), Seq(), "")),
    )

    "return a source of (TerminalFlightNumberOrigin, extracted values) for a single flight on a route" in {
      val extractor = ValuesExtractor(
        classOf[MockFlightsActor],
        (_: Arrival) => Some((0L, Seq("1", "0"), Seq(), "")), TerminalFlightNumberOrigin.fromArrival,
        (_, a) => Future.successful(a)
      )
      MockFlightsActor.state = singleFlight

      val result = Await.result(extractor.extractValuesByKey(T1, SDate("2023-01-01T00:00"), 1).runWith(Sink.seq), 1.second)

      assert(result === Seq((TerminalFlightNumberOrigin("T1", 1, "JFK"), List((0d, List("1", "0"), List(), "")))))
    }

    "return a source of (TerminalFlightNumberOrigin, extracted values) for a multiple flights on a multiple routes" in {
      val extractor = ValuesExtractor(
        classOf[MockFlightsActor],
        (_: Arrival) => Some((0L, Seq("1", "0"), Seq(), "")), TerminalFlightNumberOrigin.fromArrival,
        (_, a) => Future.successful(a)
      )
      MockFlightsActor.state = multiFlights

      val result = Await.result(extractor.extractValuesByKey(T1, SDate("2023-01-01T00:00"), 1).runWith(Sink.seq), 1.second)

      assert(result === Seq((
        TerminalFlightNumberOrigin("T1", 1, "JFK"), List(
        (0d, List("1", "0"), List(), ""),
        (5d, List("2", "1"), List(), ""),
        (2d, List("3", "0"), List(), ""),
      )), (
        TerminalFlightNumberOrigin("T2", 5555, "ABC"), List(
        (1d, List("6", "1"), List(), ""),
      )),
      ))
    }
  }
}
