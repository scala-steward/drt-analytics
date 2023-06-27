package uk.gov.homeoffice.drt.analytics.prediction.flights

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import uk.gov.homeoffice.drt.actor.TerminalDateActor.ArrivalKey
import uk.gov.homeoffice.drt.arrivals.{ApiFlightWithSplits, Arrival, ArrivalGenerator, Passengers}
import uk.gov.homeoffice.drt.ports.Terminals.{T1, T2}
import uk.gov.homeoffice.drt.ports.UnknownFeedSource
import uk.gov.homeoffice.drt.protobuf.messages.CrunchState.{FlightWithSplitsMessage, FlightsWithSplitsDiffMessage, FlightsWithSplitsMessage}
import uk.gov.homeoffice.drt.protobuf.messages.FlightsMessage.UniqueArrivalMessage
import uk.gov.homeoffice.drt.protobuf.serialisation.FlightMessageConversion.flightWithSplitsToMessage
import uk.gov.homeoffice.drt.time.{SDate, UtcDate}

class FlightActorLikeSpec extends AnyWordSpec with Matchers {
  val scheduled = "2020-01-01T00:00"
  val arrival1: Arrival = ArrivalGenerator.arrival(iata = "BA0001", terminal = T1, schDt = scheduled).copy(
    PassengerSources = Map(UnknownFeedSource -> Passengers(None, None)),
    PcpTime = None,
  )
  val flightWithSplitsMessage1: FlightWithSplitsMessage = flightWithSplitsToMessage(ApiFlightWithSplits(arrival1, Set()))
  val arrival2: Arrival = ArrivalGenerator.arrival(iata = "BA2222", terminal = T2, schDt = scheduled).copy(
    PassengerSources = Map(UnknownFeedSource -> Passengers(None, None)),
    PcpTime = None,
  )
  val flightWithSplitsMessage2: FlightWithSplitsMessage = flightWithSplitsToMessage(ApiFlightWithSplits(arrival2, Set()))

  "processSnapshot" should {
    val actorLike = newMock(None, UtcDate(2023, 6, 22))
    "add flights contained in a FlightsWithSplitsMessage" in {
      actorLike.processSnapshot(FlightsWithSplitsMessage(scala.Seq(flightWithSplitsMessage1)))
      actorLike.byArrivalKey should ===(Map(ArrivalKey(arrival1) -> arrival1))
    }
  }

  "processFlightsWithSplitsMessage" should {
    val actorLike = newMock(None, UtcDate(2023, 6, 22))
    "add a flight from a FlightWithSplitsMessage" in {
      actorLike.processFlightWithSplitsMessage(flightWithSplitsMessage1)
      actorLike.byArrivalKey should ===(Map(ArrivalKey(arrival1) -> arrival1))
    }
  }

  "processRemovalMessage" should {
    val actorLike = newMock(None, UtcDate(2023, 6, 22))
    "remove an arrival contained in the UniqueArrivalMessage" in {
      actorLike.byArrivalKey = Map(ArrivalKey(arrival1) -> arrival1)
      actorLike.processRemovalMessage(uniqueArrivalMessage(arrival1))
      actorLike.byArrivalKey should ===(Map())
    }
  }

  "processFlightsWithSplitsDiffMessage" should {
    val actorLike = newMock(None, UtcDate(2023, 6, 22))
    "add and remove flights contained in a FlightsWithSplitsMessage" in {
      actorLike.byArrivalKey = Map(ArrivalKey(arrival1) -> arrival1)
      val removeArrival1AddArrival2 = FlightsWithSplitsDiffMessage(Option(1L), Seq(uniqueArrivalMessage(arrival1)), scala.Seq(flightWithSplitsMessage2))
      actorLike.processDiffMsg(removeArrival1AddArrival2)
      actorLike.byArrivalKey should ===(Map(ArrivalKey(arrival2) -> arrival2))
    }
  }

  "processFlightsWithSplitsDiffMessage with a recovery point in time set" should {
    val pit = SDate("2023-06-22T12:00")
    val actorLike = newMock(Option(pit.millisSinceEpoch), UtcDate(2023, 6, 22))
    "ignore a FlightsWithSplitsMessage created after the recovery point in time" in {
      actorLike.byArrivalKey = Map(ArrivalKey(arrival1) -> arrival1)
      val createdAtLaterThanRecovery = Option(pit.addMinutes(1).millisSinceEpoch)
      val removeArrival1AddArrival2 = FlightsWithSplitsDiffMessage(
        createdAtLaterThanRecovery,
        Seq(uniqueArrivalMessage(arrival1)),
        scala.Seq(flightWithSplitsMessage2))
      actorLike.processDiffMsg(removeArrival1AddArrival2)
      actorLike.byArrivalKey should ===(Map(ArrivalKey(arrival1) -> arrival1))
    }
  }

  private def newMock(maybePit: Option[Long], dateToUse: UtcDate): FlightActorLike =
    new FlightActorLike {
      override val maybePointInTime: Option[Long] = maybePit
      override val date: UtcDate = dateToUse
    }

  private def uniqueArrivalMessage(arrival: Arrival) =
    UniqueArrivalMessage(
      Option(arrival.VoyageNumber.numeric),
      Option(arrival1.Terminal.toString),
      Option(arrival1.Scheduled),
      Option(arrival1.Origin.toString)
    )
}
