package uk.gov.homeoffice.drt.analytics.messages

import server.protobuf.messages.FlightsMessage.FlightMessage
import uk.gov.homeoffice.drt.analytics.Arrival

import scala.util.matching.Regex

object MessageConversion {

  def fromFlightMessage(fm: FlightMessage): Arrival = {
    val flightCodeRegex: Regex = "^([A-Z0-9]{2,3}?)([0-9]{1,4})([A-Z]?)$".r

    val flightNumber = fm.iATA.getOrElse("")

    val (carrierCode: String, voyageNumber: Int) = flightNumber match {
      case flightCodeRegex(cc, vn, _) => (cc, vn.toInt)
      case _ => ("", 0)
    }

    Arrival(
      carrierCode,
      voyageNumber,
      fm.scheduled.getOrElse(0L),
      fm.terminal.getOrElse(""),
      fm.estimated,
      fm.touchdown,
      fm.estimatedChox,
      fm.actualChox,
      fm.actPax.getOrElse(0),
      fm.tranPax.getOrElse(0))
  }
}
