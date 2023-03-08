package uk.gov.homeoffice.drt.analytics.prediction.flights

import akka.persistence.{PersistentActor, RecoveryCompleted, SnapshotOffer}
import org.slf4j.LoggerFactory
import uk.gov.homeoffice.drt.actor.PredictionModelActor.WithId
import uk.gov.homeoffice.drt.actor.TerminalDateActor
import uk.gov.homeoffice.drt.actor.TerminalDateActor.{ArrivalKey, GetState}
import uk.gov.homeoffice.drt.arrivals.Arrival
import uk.gov.homeoffice.drt.ports.Terminals.Terminal
import uk.gov.homeoffice.drt.protobuf.messages.CrunchState.{FlightWithSplitsMessage, FlightsWithSplitsDiffMessage, FlightsWithSplitsMessage}
import uk.gov.homeoffice.drt.protobuf.messages.FlightsMessage.UniqueArrivalMessage
import uk.gov.homeoffice.drt.time.UtcDate

import scala.util.Try


class FlightValueExtractionActor(val terminal: Terminal,
                                 val date: UtcDate,
                                 val extractValues: FlightWithSplitsMessage => Option[(Double, Seq[String])],
                                 val extractKey: FlightWithSplitsMessage => Option[WithId],
                                ) extends TerminalDateActor with PersistentActor {
  private val log = LoggerFactory.getLogger(getClass)

  override def persistenceId: String = f"terminal-flights-${terminal.toString.toLowerCase}-${date.year}-${date.month}%02d-${date.day}%02d"

  var byArrivalKey: Map[ArrivalKey, FlightWithSplitsMessage] = Map()
  var valuesWithFeaturesByExtractedKey: Map[WithId, Iterable[(Double, Seq[String])]] = Map()

  private def parseFlightNumber(code: String): Option[Int] = {
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
      valuesWithFeaturesByExtractedKey = byArrivalKey
        .groupBy {
          case (_, msg) => extractKey(msg)
        }
        .collect {
          case (Some(key), flightMessages) =>
            val examples = flightMessages
              .map { case (_, msg) => extractValues(msg) }
              .collect { case Some(value) => value }
            (key, examples)
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
      byArrivalKey = byArrivalKey - ArrivalKey(scheduled, terminal, flightNumber)
    }

  private def processFlightsWithSplitsMessage(u: FlightWithSplitsMessage): Unit =
    for {
      flightCode <- u.getFlight.iATA
      flightNumber <- parseFlightNumber(flightCode)
      terminal <- u.getFlight.terminal
      scheduled <- u.getFlight.scheduled
    } yield {
      byArrivalKey = byArrivalKey.updated(ArrivalKey(scheduled, terminal, flightNumber), u)
    }

  override def receiveCommand: Receive = {
    case GetState => sender() ! valuesWithFeaturesByExtractedKey
  }
}
