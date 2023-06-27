package uk.gov.homeoffice.drt.analytics.messages

import uk.gov.homeoffice.drt.analytics.SimpleArrival
import uk.gov.homeoffice.drt.protobuf.messages.FlightsMessage.FlightMessage
import uk.gov.homeoffice.drt.protobuf.serialisation.FlightMessageConversion

import scala.util.matching.Regex

object MessageConversion {

  def fromFlightMessage(fm: FlightMessage): SimpleArrival = {
    val flightCodeRegex: Regex = "^([A-Z0-9]{2,3}?)([0-9]{1,4})([A-Z]?)$".r

    val flightNumber = fm.iATA.getOrElse("")

    val (carrierCode: String, voyageNumber: Int) = flightNumber match {
      case flightCodeRegex(cc, vn, _) => (cc, vn.toInt)
      case _ => ("", 0)
    }

    SimpleArrival(
      carrierCode = carrierCode,
      number = voyageNumber,
      scheduled = fm.scheduled.getOrElse(0L),
      terminal = fm.terminal.getOrElse(""),
      origin = fm.origin.getOrElse(""),
      status = fm.status.getOrElse(""),
      passengerSources = FlightMessageConversion.getPassengerSources(fm),
      maxPax = fm.maxPax,
    )
  }
}
