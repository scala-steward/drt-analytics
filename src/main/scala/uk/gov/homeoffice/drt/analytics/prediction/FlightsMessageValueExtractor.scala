package uk.gov.homeoffice.drt.analytics.prediction

import uk.gov.homeoffice.drt.actor.WalkTimeProvider
import uk.gov.homeoffice.drt.arrivals.Arrival
import uk.gov.homeoffice.drt.ports.Terminals.Terminal
import uk.gov.homeoffice.drt.protobuf.messages.CrunchState.FlightWithSplitsMessage
import uk.gov.homeoffice.drt.protobuf.messages.FlightsMessage.FlightMessage
import uk.gov.homeoffice.drt.time.{SDate, SDateLike}

object FlightsMessageValueExtractor {
  val minutesOffSchedule: FlightWithSplitsMessage => Option[(Double, Seq[String])] = (msg: FlightWithSplitsMessage) => for {
    scheduled <- msg.getFlight.scheduled
    touchdown <- msg.getFlight.touchdown
  } yield {
    val minutes = (touchdown - scheduled).toDouble / 60000
    (minutes, featureValues(scheduled))
  }

  val minutesToChox: FlightWithSplitsMessage => Option[(Double, Seq[String])] = (msg: FlightWithSplitsMessage) => for {
    scheduled <- msg.getFlight.scheduled
    touchdown <- msg.getFlight.touchdown
    actualChox <- msg.getFlight.actualChox
  } yield {
    val minutes = (actualChox - touchdown).toDouble / 60000
    (minutes, featureValues(scheduled))
  }

  def walkTimeMinutes(walkTimeProvider: WalkTimeProvider): FlightWithSplitsMessage => Option[(Double, Seq[String])] = (msg: FlightWithSplitsMessage) => {
    val flight = msg.getFlight
    for {
      terminal <- flight.terminal.map(Terminal(_))
      scheduled <- flight.scheduled
      walkTimeMinutes <- maybeWalkTimeMinutes(walkTimeProvider, flight, terminal)
    }
    yield (walkTimeMinutes.toDouble, featureValues(scheduled))
  }

  private def maybeWalkTimeMinutes(walkTimeProvider: WalkTimeProvider, flight: FlightMessage, terminal: Terminal): Option[Int] =
    flight.gate.getOrElse(flight.stand.getOrElse("")) match {
      case "" =>
        None
      case gateOrStand =>
        walkTimeProvider.walkTimes.get(terminal, gateOrStand)
    }

  private def featureValues(scheduled: Long): Seq[String] = {
    val scheduledSdate = SDate(scheduled)
    val mornAft = morningAfternoon(scheduledSdate)
    val dow = dayOfWeek(scheduledSdate)
    Seq(dow, mornAft)
  }

  private def dayOfWeek(scheduled: SDateLike): String = scheduled.getDayOfWeek.toString

  private def morningAfternoon(scheduled: SDateLike): String = s"${scheduled.getHours / 12}"
}
