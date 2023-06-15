package uk.gov.homeoffice.drt.analytics.prediction.flights

import akka.persistence.{PersistentActor, Recovery, SnapshotOffer, SnapshotSelectionCriteria}
import org.slf4j.LoggerFactory
import uk.gov.homeoffice.drt.actor.TerminalDateActor.{ArrivalKey, GetState}
import uk.gov.homeoffice.drt.arrivals.Arrival
import uk.gov.homeoffice.drt.ports.Terminals.Terminal
import uk.gov.homeoffice.drt.protobuf.messages.CrunchState.{FlightWithSplitsMessage, FlightsWithSplitsDiffMessage, FlightsWithSplitsMessage}
import uk.gov.homeoffice.drt.protobuf.messages.FlightsMessage.UniqueArrivalMessage
import uk.gov.homeoffice.drt.protobuf.serialisation.FlightMessageConversion.flightWithSplitsFromMessage
import uk.gov.homeoffice.drt.time.{SDate, UtcDate}

import scala.util.Try


class FlightsActor(val terminal: Terminal,
                   val date: UtcDate,
                   val maybePointInTime: Option[Long],
                  ) extends PersistentActor {
  private val log = LoggerFactory.getLogger(getClass)

  override def persistenceId: String = f"terminal-flights-${terminal.toString.toLowerCase}-${date.year}-${date.month}%02d-${date.day}%02d"

  var byArrivalKey: Map[ArrivalKey, Arrival] = Map()

  private def parseFlightNumber(code: String): Option[Int] = {
    code match {
      case Arrival.flightCodeRegex(_, flightNumber, _) => Try(flightNumber.toInt).toOption
      case _ => None
    }
  }

  override def recovery: Recovery = maybePointInTime match {
    case None =>
      Recovery(SnapshotSelectionCriteria(Long.MaxValue, maxTimestamp = Long.MaxValue, 0L, 0L))
    case Some(pointInTime) =>
      val criteria = SnapshotSelectionCriteria(maxTimestamp = pointInTime)
      Recovery(fromSnapshot = criteria, replayMax = 250)
  }

  override def receiveRecover: Receive = {
    case SnapshotOffer(_, ss) =>
      ss match {
        case msg: FlightsWithSplitsMessage => msg.flightWithSplits.foreach(processFlightsWithSplitsMessage)
        case unexpected => log.warn(s"Got unexpected snapshot offer message: ${unexpected.getClass}")
      }

    case FlightsWithSplitsDiffMessage(Some(createdAt), removals, updates) =>
      maybePointInTime match {
        case Some(time) if createdAt > time =>
          log.info(s"Skipping replay of FlightsWithSplitsDiffMessage at $createdAt as it's after the point in time ${SDate(time).toISOString}")
        case _ =>
          updates.foreach(processFlightsWithSplitsMessage)
          if (SDate(createdAt) < SDate(date).addHours(28)) {
            if (removals.length < byArrivalKey.size)
              removals.foreach(processRemovalMessage)
          }
      }
  }

  private def processRemovalMessage(r: UniqueArrivalMessage): Unit =
    for {
      scheduled <- r.scheduled
      terminal <- r.terminalName
      flightNumber <- r.number
    } yield {
      byArrivalKey = byArrivalKey - ArrivalKey(scheduled, terminal, flightNumber)
    }

  private def processFlightsWithSplitsMessage(u: FlightWithSplitsMessage): Unit =
    for {
      flightCode <- u.getFlight.iATA
      flightNumber <- parseFlightNumber(flightCode)
      terminal <- u.getFlight.terminal
      scheduled <- u.getFlight.scheduled
    } yield {
      byArrivalKey = byArrivalKey.updated(ArrivalKey(scheduled, terminal, flightNumber), flightWithSplitsFromMessage(u).apiFlight)
    }

  override def receiveCommand: Receive = {
    case GetState =>
      sender() ! byArrivalKey.values.toSeq
  }
}
