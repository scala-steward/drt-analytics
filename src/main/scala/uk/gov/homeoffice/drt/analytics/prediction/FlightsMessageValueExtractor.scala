package uk.gov.homeoffice.drt.analytics.prediction

import uk.gov.homeoffice.drt.protobuf.messages.CrunchState.FlightWithSplitsMessage

object FlightsMessageValueExtractor {
  val offScheduledMinutes: FlightWithSplitsMessage => Option[Double] = (msg: FlightWithSplitsMessage) => for {
    scheduled <- msg.getFlight.scheduled
    touchdown <- msg.getFlight.touchdown
  } yield (touchdown - scheduled).toDouble / 60000

  val touchdownToChoxMinutes: FlightWithSplitsMessage => Option[Double] = (msg: FlightWithSplitsMessage) => for {
    touchdown <- msg.getFlight.touchdown
    actualChox <- msg.getFlight.actualChox
  } yield (touchdown - actualChox).toDouble / 60000
}
