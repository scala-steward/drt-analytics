package uk.gov.homeoffice.drt.analytics.actors

import akka.NotUsed
import akka.actor.{Actor, ActorSystem, PoisonPill, Props}
import akka.pattern.ask
import akka.persistence.{PersistentActor, RecoveryCompleted, SnapshotOffer}
import akka.stream.scaladsl.Source
import akka.util.Timeout
import org.slf4j.LoggerFactory
import uk.gov.homeoffice.drt.protobuf.messages.CrunchState.{FlightWithSplitsMessage, FlightsWithSplitsDiffMessage, FlightsWithSplitsMessage}
import uk.gov.homeoffice.drt.protobuf.messages.FlightsMessage.UniqueArrivalMessage
import uk.gov.homeoffice.drt.analytics.actors.MinutesOffScheduledActor.{ArrivalKey, ArrivalKeyWithOrigin, FlightRoute, GetState}
import uk.gov.homeoffice.drt.analytics.time.SDate
import uk.gov.homeoffice.drt.arrivals.Arrival
import uk.gov.homeoffice.drt.ports.Terminals.Terminal

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import scala.util.matching.Regex


trait MinutesOffScheduledActor extends Actor {
  val terminal: Terminal
  val year: Int
  val month: Int
  val day: Int
}

object MinutesOffScheduledActor {
  case object GetState

  case class FlightRoute(terminal: String, number: Int, origin: String)

  case class ArrivalKey(scheduled: Long, terminal: String, number: Int)

  case class ArrivalKeyWithOrigin(scheduled: Long, terminal: String, number: Int, origin: String)
}

class MinutesOffScheduledActorImpl(val terminal: Terminal, val year: Int, val month: Int, val day: Int) extends MinutesOffScheduledActor with PersistentActor {
  private val log = LoggerFactory.getLogger(getClass)

  override def persistenceId: String = f"terminal-flights-${terminal.toString.toLowerCase}-$year-$month%02d-$day%02d"

  var byKey: Map[ArrivalKey, (Int, String)] = Map()
  var byKeyWithOrigin: Map[ArrivalKeyWithOrigin, Int] = Map()

  def parseCarrierAndFlightNumber(code: String): Option[(String, Int)] = {
    code match {
      case Arrival.flightCodeRegex(carrier, flightNumber, _) =>
        Try(flightNumber.toInt).toOption.map(n => (carrier, n))
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
      (carrier, flightNumber) <- parseCarrierAndFlightNumber(flightCode)
      terminal <- u.getFlight.terminal
      scheduled <- u.getFlight.scheduled
      touchdown <- u.getFlight.touchdown
      origin <- u.getFlight.origin
    } yield {
      byKey = byKey.updated(ArrivalKey(scheduled, terminal, flightNumber), ((touchdown - scheduled).toInt, origin))
    }

  override def receiveCommand: Receive = {
    case GetState => sender() ! byKeyWithOrigin
  }
}
