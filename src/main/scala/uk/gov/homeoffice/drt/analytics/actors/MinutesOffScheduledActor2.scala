package uk.gov.homeoffice.drt.analytics.actors

import akka.NotUsed
import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.pattern.ask
import akka.persistence.{PersistentActor, RecoveryCompleted}
import akka.stream.scaladsl.Source
import akka.util.Timeout
import server.protobuf.messages.CrunchState.FlightsWithSplitsDiffMessage
import uk.gov.homeoffice.drt.analytics.actors.MinutesOffScheduledActor2.{ArrivalKey, GetState}
import uk.gov.homeoffice.drt.analytics.time.SDate
import uk.gov.homeoffice.drt.ports.Terminals.Terminal

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import scala.util.matching.Regex

object MinutesOffScheduledActor2 {
  case object GetState

  case class ArrivalKey(scheduled: Long, terminal: String, number: Int)

  def offScheduledByTerminalFlightNumber(terminal: Terminal, startDate: SDate, numberOfDays: Int)
                                        (implicit system: ActorSystem, ec: ExecutionContext, timeout: Timeout): Source[((String, Int), Map[Long, (Int, String, String)]), NotUsed] =
    Source((0 until numberOfDays).toList)
      .mapAsync(1)(day => arrivalsWithOffScheduledForDate(terminal, startDate.addDays(day)))
      .map(byTerminalAndFlightNumber)
      .fold(Map[(String, Int), Map[Long, (Int, String, String)]]()) {
        case (acc, incoming) => addByTerminalAndFlightNumber(acc, incoming)
      }
      .mapConcat(identity)

  private def addByTerminalAndFlightNumber(acc: Map[(String, Int), Map[Long, (Int, String, String)]], incoming: Map[(String, Int), Map[Long, (Int, String, String)]]): Map[(String, Int), Map[Long, (Int, String, String)]] =
    incoming.foldLeft(acc) {
      case (acc, (key, arrivals)) => acc.updated(key, acc.getOrElse(key, Map()) ++ arrivals)
    }

  private def byTerminalAndFlightNumber(byArrivalKey: Map[ArrivalKey, (Int, String, String)]): Map[(String, Int), Map[Long, (Int, String, String)]] =
    byArrivalKey
      .groupBy { case (key, _) =>
        (key.terminal, key.number)
      }
      .map {
        case ((terminal, number), byArrivalKey) =>
          val scheduledToOffAndOrigin = byArrivalKey.map {
            case (key, values) => (key.scheduled, values)
          }
          ((terminal, number), scheduledToOffAndOrigin)
      }

  private def arrivalsWithOffScheduledForDate(terminal: Terminal, currentDay: SDate)
                                             (implicit system: ActorSystem, ec: ExecutionContext, timeout: Timeout): Future[Map[ArrivalKey, (Int, String, String)]] = {
    val actor = system.actorOf(Props(new MinutesOffScheduledActor2(terminal, currentDay.fullYear, currentDay.month, currentDay.date)))
    actor
      .ask(GetState).mapTo[Map[ArrivalKey, (Int, String, String)]]
      .map { arrivals =>
        actor ! PoisonPill
        arrivals
      }
  }
}


class MinutesOffScheduledActor2(terminal: Terminal, year: Int, month: Int, day: Int) extends PersistentActor {
  override def persistenceId: String = f"terminal-flights-${terminal.toString.toLowerCase}-$year-$month%02d-$day%02d"

  var byKey: Map[ArrivalKey, (Int, String, String)] = Map()

  def parseCarrierAndFlightNumber(code: String): Option[(String, Int)] = {
    val flightCodeRegex: Regex = "^([A-Z0-9]{2,3}?)([0-9]{1,4})([A-Z]*)$".r

    code match {
      case flightCodeRegex(carrier, flightNumber, _) =>
        Try(flightNumber.toInt).toOption.map(n => (carrier, n))
      case _ => None
    }
  }

  override def receiveRecover: Receive = {
    case RecoveryCompleted =>
    case FlightsWithSplitsDiffMessage(_, removals, updates) =>
      updates.map { u =>
        for {
          flightCode <- u.getFlight.iATA
          (carrier, flightNumber) <- parseCarrierAndFlightNumber(flightCode)
          terminal <- u.getFlight.terminal
          scheduled <- u.getFlight.scheduled
          touchdown <- u.getFlight.touchdown
          origin <- u.getFlight.origin
        } yield {
          byKey = byKey.updated(ArrivalKey(scheduled, terminal, flightNumber), ((touchdown - scheduled).toInt, origin, carrier))
        }
      }
      removals.map { r =>
        for {
          scheduled <- r.scheduled
          terminal <- r.terminalName
          flightNumber <- r.number
        } yield {
          byKey = byKey - ArrivalKey(scheduled, terminal, flightNumber)
        }
      }
  }

  override def receiveCommand: Receive = {
    case GetState => sender() ! byKey
  }
}
