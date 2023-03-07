package uk.gov.homeoffice.drt.analytics.prediction.flights.aggregation

import uk.gov.homeoffice.drt.actor.TerminalDateActor.FlightRoute
import uk.gov.homeoffice.drt.arrivals.Arrival
import uk.gov.homeoffice.drt.protobuf.messages.CrunchState.FlightWithSplitsMessage

import scala.util.Try

object RouteAggregator extends ExamplesAggregator[FlightRoute] {
  private def parseFlightNumber(code: String): Option[Int] = code match {
    case Arrival.flightCodeRegex(_, flightNumber, _) => Try(flightNumber.toInt).toOption
    case _ => None
  }

  override val aggregateKey: FlightWithSplitsMessage => Option[FlightRoute] = (arrival: FlightWithSplitsMessage) =>
    for {
      flight <- arrival.flight
      terminal <- flight.terminal
      iata <- flight.iATA
      flightNumber <- parseFlightNumber(iata)
      origin <- flight.origin
    } yield {
      FlightRoute(terminal, flightNumber, origin)
    }
}
