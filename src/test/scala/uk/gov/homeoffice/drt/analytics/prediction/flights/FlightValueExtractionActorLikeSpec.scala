package uk.gov.homeoffice.drt.analytics.prediction.flights

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import uk.gov.homeoffice.drt.actor.PredictionModelActor
import uk.gov.homeoffice.drt.actor.TerminalDateActor.ArrivalKey
import uk.gov.homeoffice.drt.arrivals.{ApiFlightWithSplits, Arrival, ArrivalGenerator, Passengers}
import uk.gov.homeoffice.drt.ports.Terminals.{T1, T2}
import uk.gov.homeoffice.drt.ports.UnknownFeedSource
import uk.gov.homeoffice.drt.protobuf.messages.CrunchState.{FlightWithSplitsMessage, FlightsWithSplitsDiffMessage, FlightsWithSplitsMessage}
import uk.gov.homeoffice.drt.protobuf.messages.FlightsMessage.{FlightMessage, UniqueArrivalMessage}
import uk.gov.homeoffice.drt.protobuf.serialisation.FlightMessageConversion.flightWithSplitsToMessage
import uk.gov.homeoffice.drt.time.SDate


class FlightValueExtractionActorLikeSpec extends AnyWordSpec with Matchers {
  val scheduled = "2020-01-01T00:00"
  val arrival1 = ArrivalGenerator.arrival(iata = "BA0001", terminal = T1, schDt = scheduled).copy(
    PassengerSources = Map(UnknownFeedSource -> Passengers(None, None)),
    PcpTime = None,
  )
  val flightWithSplitsMessage1 = flightWithSplitsToMessage(ApiFlightWithSplits(arrival1, Set()))
  val arrival2 = ArrivalGenerator.arrival(iata = "BA2222", terminal = T2, schDt = scheduled).copy(
    PassengerSources = Map(UnknownFeedSource -> Passengers(None, None)),
    PcpTime = None,
  )
  val flightWithSplitsMessage2 = flightWithSplitsToMessage(ApiFlightWithSplits(arrival2, Set()))

  "processSnapshot" should {
    val actorLike = newMock
    "add flights contained in a FlightsWithSplitsMessage" in {
      actorLike.processSnapshot(FlightsWithSplitsMessage(scala.Seq(flightWithSplitsMessage1)))
      actorLike.byArrivalKey should ===(Map(ArrivalKey(arrival1) -> arrival1))
    }
  }

  "processFlightsWithSplitsMessage" should {
    val actorLike = newMock
    "add a flight from a FlightWithSplitsMessage" in {
      actorLike.processFlightsWithSplitsMessage(flightWithSplitsMessage1)
      actorLike.byArrivalKey should ===(Map(ArrivalKey(arrival1) -> arrival1))
    }
  }

  "processRemovalMessage" should {
    val actorLike = newMock
    "remove an arrival contained in the UniqueArrivalMessage" in {
      actorLike.byArrivalKey = Map(ArrivalKey(arrival1) -> arrival1)
      actorLike.processRemovalMessage(uniqueArrivalMessage(arrival1))
      actorLike.byArrivalKey should ===(Map())
    }
  }

  "processFlightsWithSplitsDiffMessage" should {
    val actorLike = newMock
    "add and remove flights contained in a FlightsWithSplitsMessage" in {
      actorLike.byArrivalKey = Map(ArrivalKey(arrival1) -> arrival1)
      val removeArrival1AddArrival2 = FlightsWithSplitsDiffMessage(None, Seq(uniqueArrivalMessage(arrival1)), scala.Seq(flightWithSplitsMessage2))
      actorLike.processFlightsWithSplitsDiffMessage(removeArrival1AddArrival2)
      actorLike.byArrivalKey should ===(Map(ArrivalKey(arrival2) -> arrival2))
    }
  }

  private def newMock = {
    new FlightValueExtractionActorLike {
      override val extractValues: Arrival => Option[(Double, Seq[String], Seq[Double])] = _ => None
      override val extractKey: Arrival => Option[PredictionModelActor.WithId] = _ => None
    }
  }

  private def uniqueArrivalMessage(arrival: Arrival) =
    UniqueArrivalMessage(
      Option(arrival.VoyageNumber.numeric),
      Option(arrival1.Terminal.toString),
      Option(arrival1.Scheduled),
      Option(arrival1.Origin.toString)
    )
}
