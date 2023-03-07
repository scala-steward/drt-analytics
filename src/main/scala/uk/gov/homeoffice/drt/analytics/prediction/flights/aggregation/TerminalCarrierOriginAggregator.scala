package uk.gov.homeoffice.drt.analytics.prediction.flights.aggregation

import uk.gov.homeoffice.drt.actor.TerminalDateActor.WithId
import uk.gov.homeoffice.drt.arrivals.Arrival
import uk.gov.homeoffice.drt.protobuf.messages.CrunchState.FlightWithSplitsMessage


case class TerminalCarrierOrigin(terminal: String, carrier: String, origin: String) extends WithId {
  val id = s"$terminal-$carrier-$origin"
}

object TerminalCarrierOriginAggregator extends ExamplesAggregator[TerminalCarrierOrigin] {
  private def parseCarrierCode(code: String): Option[String] = code match {
    case Arrival.flightCodeRegex(carrierCode, _, _) => Option(carrierCode)
    case _ => None
  }

  override val aggregateKey: FlightWithSplitsMessage => Option[TerminalCarrierOrigin] = (arrival: FlightWithSplitsMessage) =>
    for {
      flight <- arrival.flight
      terminal <- flight.terminal
      iata <- flight.iATA
      carrierCode <- parseCarrierCode(iata)
      origin <- flight.origin
    } yield {
      TerminalCarrierOrigin(terminal, carrierCode, origin)
    }
}


