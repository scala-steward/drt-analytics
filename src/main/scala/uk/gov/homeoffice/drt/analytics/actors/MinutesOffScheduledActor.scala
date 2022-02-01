package uk.gov.homeoffice.drt.analytics.actors

import akka.NotUsed
import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.pattern.ask
import akka.persistence.{PersistentActor, RecoveryCompleted, SnapshotOffer}
import akka.stream.scaladsl.Source
import akka.util.Timeout
import org.slf4j.LoggerFactory
import server.protobuf.messages.CrunchState.{FlightWithSplitsMessage, FlightsWithSplitsDiffMessage, FlightsWithSplitsMessage}
import server.protobuf.messages.FlightsMessage.UniqueArrivalMessage
import uk.gov.homeoffice.drt.analytics.actors.MinutesOffScheduledActor.{ArrivalKey, ArrivalKeyWithOrigin, GetState}
import uk.gov.homeoffice.drt.analytics.time.SDate
import uk.gov.homeoffice.drt.ports.Terminals.Terminal

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import scala.util.matching.Regex

object MinutesOffScheduledActor {
  private val log = LoggerFactory.getLogger(getClass)

  case object GetState

  case class ArrivalKey(scheduled: Long, terminal: String, number: Int)

  case class ArrivalKeyWithOrigin(scheduled: Long, terminal: String, number: Int, origin: String)

  def offScheduledByTerminalFlightNumberOrigin(terminal: Terminal, startDate: SDate, numberOfDays: Int)
                                              (implicit system: ActorSystem, ec: ExecutionContext, timeout: Timeout): Source[((String, Int, String), Map[Long, Int]), NotUsed] =
    Source(((-1 * numberOfDays) until 0).toList)
      .mapAsync(1) { day =>
        val date = startDate.addDays(day)
        log.info(s"Grabbing arrivals for ${date.toISODateOnly}")
        arrivalsWithOffScheduledForDate(terminal, date)
      }
      .map(byTerminalFlightNumberAndOrigin)
      .fold(Map[(String, Int, String), Map[Long, Int]]()) {
        case (acc, incoming) => addByTerminalAndFlightNumber(acc, incoming)
      }
      .mapConcat(identity)

  private def addByTerminalAndFlightNumber(acc: Map[(String, Int, String), Map[Long, Int]], incoming: Map[(String, Int, String), Map[Long, Int]]): Map[(String, Int, String), Map[Long, Int]] =
    incoming.foldLeft(acc) {
      case (acc, (key, arrivals)) => acc.updated(key, acc.getOrElse(key, Map()) ++ arrivals)
    }

  private def byTerminalFlightNumberAndOrigin(byArrivalKey: Map[ArrivalKeyWithOrigin, Int]): Map[(String, Int, String), Map[Long, Int]] =
    byArrivalKey
      .groupBy { case (key, _) =>
        (key.terminal, key.number, key.origin)
      }
      .map {
        case ((terminal, number, origin), byArrivalKey) =>
          val scheduledToOffAndOrigin = byArrivalKey.map {
            case (key, values) => (key.scheduled, values)
          }
          ((terminal, number, origin), scheduledToOffAndOrigin)
      }

  private def arrivalsWithOffScheduledForDate(terminal: Terminal, currentDay: SDate)
                                             (implicit system: ActorSystem, ec: ExecutionContext, timeout: Timeout): Future[Map[ArrivalKeyWithOrigin, Int]] = {
    val actor = system.actorOf(Props(new MinutesOffScheduledActor(terminal, currentDay.getFullYear, currentDay.getMonth, currentDay.getDate)))
    actor
      .ask(GetState).mapTo[Map[ArrivalKeyWithOrigin, Int]]
      .map { arrivals =>
        log.info(s"Got data for $terminal / ${currentDay.toISODateOnly}")
        actor ! PoisonPill
        arrivals
      }
  }
}


class MinutesOffScheduledActor(terminal: Terminal, year: Int, month: Int, day: Int) extends PersistentActor {
  private val log = LoggerFactory.getLogger(getClass)

  override def persistenceId: String = f"terminal-flights-${terminal.toString.toLowerCase}-$year-$month%02d-$day%02d"

  var byKey: Map[ArrivalKey, (Int, String)] = Map()
  var byKeyWithOrigin: Map[ArrivalKeyWithOrigin, Int] = Map()

  def parseCarrierAndFlightNumber(code: String): Option[(String, Int)] = {
    val flightCodeRegex: Regex = "^([A-Z0-9]{2,3}?)([0-9]{1,4})([A-Z]*)$".r

    code match {
      case flightCodeRegex(carrier, flightNumber, _) =>
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
