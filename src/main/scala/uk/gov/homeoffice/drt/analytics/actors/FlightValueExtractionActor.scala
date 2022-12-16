package uk.gov.homeoffice.drt.analytics.actors

import akka.persistence.{PersistentActor, RecoveryCompleted, SnapshotOffer}
import org.slf4j.LoggerFactory
import uk.gov.homeoffice.drt.analytics.actors.TerminalDateActor.{ArrivalKey, ArrivalKeyWithOrigin, GetState}
import uk.gov.homeoffice.drt.arrivals.Arrival
import uk.gov.homeoffice.drt.ports.Terminals.Terminal
import uk.gov.homeoffice.drt.protobuf.messages.CrunchState.{FlightWithSplitsMessage, FlightsWithSplitsDiffMessage, FlightsWithSplitsMessage}
import uk.gov.homeoffice.drt.protobuf.messages.FlightsMessage.UniqueArrivalMessage

import scala.util.Try


class FlightValueExtractionActor(val terminal: Terminal,
                                 val year: Int,
                                 val month: Int,
                                 val day: Int,
                                 val extractValue: FlightWithSplitsMessage => Option[Double],
                                ) extends TerminalDateActor with PersistentActor {
  private val log = LoggerFactory.getLogger(getClass)

  override def persistenceId: String = f"terminal-flights-${terminal.toString.toLowerCase}-$year-$month%02d-$day%02d"

  var byKey: Map[ArrivalKey, (Double, String)] = Map()
  var byKeyWithOrigin: Map[ArrivalKeyWithOrigin, Double] = Map()

  def parseFlightNumber(code: String): Option[Int] = {
    code match {
      case Arrival.flightCodeRegex(_, flightNumber, _) => Try(flightNumber.toInt).toOption
      case _ => None
    }
  }

  override def receiveRecover: Receive = {
    case SnapshotOffer(_, ss) =>
      ss match {
        case msg: FlightsWithSplitsMessage => msg.flightWithSplits.foreach(processFlightsWithSplitsMessage)
        case unexpected => log.warn(s"Got unexpected snapshot offer message: ${unexpected.getClass}")
      }

    case RecoveryCompleted =>
      byKeyWithOrigin = byKey
        .groupBy {
          case (_, (_, origin)) => origin
        }
        .flatMap {
          case (origin, arrivals) =>
            arrivals.map {
              case (key, (off, _)) => (ArrivalKeyWithOrigin(key.scheduled, key.terminal, key.number, origin), off)
            }
        }

    case FlightsWithSplitsDiffMessage(_, removals, updates) =>
      updates.foreach(processFlightsWithSplitsMessage)
      removals.foreach(processRemovalMessage)
  }

  private def processRemovalMessage(r: UniqueArrivalMessage): Unit =
    for {
      scheduled <- r.scheduled
      terminal <- r.terminalName
      flightNumber <- r.number
    } yield {
      byKey = byKey - ArrivalKey(scheduled, terminal, flightNumber)
    }

  private def processFlightsWithSplitsMessage(u: FlightWithSplitsMessage): Unit =
    for {
      flightCode <- u.getFlight.iATA
      flightNumber <- parseFlightNumber(flightCode)
      terminal <- u.getFlight.terminal
      scheduled <- u.getFlight.scheduled
      origin <- u.getFlight.origin
      extractedValue <- extractValue(u)
    } yield {
      byKey = byKey.updated(ArrivalKey(scheduled, terminal, flightNumber), (extractedValue, origin))
    }

  override def receiveCommand: Receive = {
    case GetState => sender() ! byKeyWithOrigin
  }
}
