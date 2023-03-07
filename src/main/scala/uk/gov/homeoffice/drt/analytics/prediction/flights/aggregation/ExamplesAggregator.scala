package uk.gov.homeoffice.drt.analytics.prediction.flights.aggregation

import uk.gov.homeoffice.drt.actor.TerminalDateActor.{ArrivalKeyWithOrigin, WithId}
import uk.gov.homeoffice.drt.protobuf.messages.CrunchState.FlightWithSplitsMessage

trait ExamplesAggregator[T <: WithId] {
  def aggregateKey: FlightWithSplitsMessage => Option[T]
}
