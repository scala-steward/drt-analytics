package uk.gov.homeoffice.drt.analytics.prediction.flights

import akka.actor.{Actor, ActorSystem}
import akka.stream.scaladsl.Sink
import akka.testkit.TestKit
import akka.util.Timeout
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpecLike
import uk.gov.homeoffice.drt.actor.TerminalDateActor
import uk.gov.homeoffice.drt.actor.TerminalDateActor.{ArrivalKeyWithOrigin, FlightRoute, GetState}
import uk.gov.homeoffice.drt.ports.Terminals.{T1, Terminal}
import uk.gov.homeoffice.drt.protobuf.messages.CrunchState.FlightWithSplitsMessage
import uk.gov.homeoffice.drt.time.{SDate, UtcDate}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor}

class MockFlightsActor(val terminal: Terminal,
                       val date: UtcDate,
                       val extractValues: FlightWithSplitsMessage => Option[(Double, Seq[String])]
                      ) extends Actor with TerminalDateActor {
  override def receive: Receive = {
    case GetState => sender() ! MockFlightsActor.state
  }
}

object MockFlightsActor {
  var state: Map[ArrivalKeyWithOrigin, (Long, Seq[String])] = Map()
}

class FlightRoutesValuesExtractorSpec
  extends TestKit(ActorSystem("FlightRoutesValuesExtractor"))
    with AnyWordSpecLike with BeforeAndAfterAll {

  implicit val ec: ExecutionContextExecutor = ExecutionContext.global
  implicit val timeout: Timeout = new Timeout(1.second)

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "FlightRoutesValuesExtractor" should {
    val singleFlight = Map(ArrivalKeyWithOrigin(0L, "T1", 1, "JFK") -> (0L, Seq("1", "0")))
    val multiFlights = Map(
      ArrivalKeyWithOrigin(0L, "T1", 1, "JFK") -> (0L, Seq("1", "0")),
      ArrivalKeyWithOrigin(100L, "T1", 1, "JFK") -> (5L, Seq("2", "1")),
      ArrivalKeyWithOrigin(200L, "T1", 1, "JFK") -> (2L, Seq("3", "0")),
      ArrivalKeyWithOrigin(0L, "T2", 5555, "ABC") -> (1L, Seq("6", "1")),
    )

    "return a source of (FlightRoute, extracted values) for a single flight on a route" in {
      val extractor = FlightRoutesValuesExtractor(classOf[MockFlightsActor], _ => Some((0L, Seq("1", "0"))))
      MockFlightsActor.state = singleFlight

      val result = Await.result(extractor.extractedValueByFlightRoute(T1, SDate("2023-01-01T00:00"), 1).runWith(Sink.seq), 1.second)

      assert(result === Seq((FlightRoute("T1", 1, "JFK"), List((0d, List("1", "0"))))))
    }

    "return a source of (FlightRoute, extracted values) for a multiple flights on a multiple routes" in {
      val extractor = FlightRoutesValuesExtractor(classOf[MockFlightsActor], _ => Some((0L, Seq("1", "0"))))
      MockFlightsActor.state = multiFlights

      val result = Await.result(extractor.extractedValueByFlightRoute(T1, SDate("2023-01-01T00:00"), 1).runWith(Sink.seq), 1.second)

      assert(result === Seq((
        FlightRoute("T1", 1, "JFK"), List(
        (0d, List("1", "0")),
        (5d, List("2", "1")),
        (2d, List("3", "0")),
      )),(
        FlightRoute("T2", 5555, "ABC"), List(
        (1d, List("6", "1")),
      )),
      ))
    }
  }
}
