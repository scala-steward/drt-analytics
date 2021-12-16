package uk.gov.homeoffice.drt.analytics.actors

import akka.NotUsed
import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.pattern.ask
import akka.persistence.{PersistentActor, RecoveryCompleted}
import akka.stream.scaladsl.Source
import akka.util.Timeout
import server.protobuf.messages.CrunchState.FlightsWithSplitsDiffMessage
import uk.gov.homeoffice.drt.analytics.actors.MinutesOffScheduledActor.{ArrivalKey, GetState}
import uk.gov.homeoffice.drt.analytics.time.SDate
import uk.gov.homeoffice.drt.ports.Terminals.Terminal

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import scala.util.matching.Regex


object MinutesOffScheduledActor {
  case object GetState

  case class ArrivalKey(scheduled: Long, terminal: String, number: Int)

  def offScheduledByTerminalFlightNumber(terminal: Terminal, startDate: SDate, numberOfDays: Int)
                                        (implicit system: ActorSystem, ec: ExecutionContext, timeout: Timeout): Source[((String, Int), Map[Long, Int]), NotUsed] =
    Source((0 until numberOfDays).toList)
      .mapAsync(1)(day => arrivalsWithOffScheduledForDate(terminal, startDate.addDays(day)))
      .map(byTerminalAndFlightNumber)
      .fold(Map[(String, Int), Map[Long, Int]]()) {
        case (acc, incoming) => addByTerminalAndFlightNumber(acc, incoming)
      }
      .mapConcat(identity)

  private def addByTerminalAndFlightNumber(acc: Map[(String, Int), Map[Long, Int]], incoming: Map[(String, Int), Map[Long, Int]]): Map[(String, Int), Map[Long, Int]] =
    incoming.foldLeft(acc) {
      case (acc, (key, arrivals)) => acc.updated(key, acc.getOrElse(key, Map()) ++ arrivals)
    }

  private def byTerminalAndFlightNumber(byArrivalKey: Map[ArrivalKey, Int]): Map[(String, Int), Map[Long, Int]] =
    byArrivalKey
      .groupBy { case (key, _) =>
        (key.terminal, key.number)
      }
      .map {
        case (((terminal, number), byArrivalKey)) =>
          val scheduledToOffScheduled = byArrivalKey.map { case (key, offScheduled) => (key.scheduled, offScheduled) }
          ((terminal, number), scheduledToOffScheduled)
      }

  private def arrivalsWithOffScheduledForDate(terminal: Terminal, currentDay: SDate)
                                             (implicit system: ActorSystem, ec: ExecutionContext, timeout: Timeout): Future[Map[ArrivalKey, Int]] = {
    val actor = system.actorOf(Props(new MinutesOffScheduledActor(terminal, currentDay.fullYear, currentDay.month, currentDay.date)))
    actor
      .ask(GetState).mapTo[Map[ArrivalKey, Int]]
      .map { arrivals =>
        actor ! PoisonPill
        arrivals
      }
  }
}


class MinutesOffScheduledActor(terminal: Terminal, year: Int, month: Int, day: Int) extends PersistentActor {
  override def persistenceId: String = f"terminal-flights-${terminal.toString.toLowerCase}-$year-$month%02d-$day%02d"

  var byKey: Map[ArrivalKey, Int] = Map()

  def parseFlightNumber(code: String): Option[Int] = {
    val flightCodeRegex: Regex = "^([A-Z0-9]{2,3}?)([0-9]{1,4})([A-Z]*)$".r

    code match {
      case flightCodeRegex(_, flightNumber, _) =>
        Try(flightNumber.toInt).toOption
      case _ => None
    }
  }

  override def receiveRecover: Receive = {
    case RecoveryCompleted =>
    case FlightsWithSplitsDiffMessage(_, removals, updates) =>
      updates.map { u =>
        for {
          flightCode <- u.getFlight.iATA
          flightNumber <- parseFlightNumber(flightCode)
          terminal <- u.getFlight.terminal
          scheduled <- u.getFlight.scheduled
          touchdown <- u.getFlight.touchdown
        } yield {
          byKey = byKey.updated(ArrivalKey(scheduled, terminal, flightNumber), (touchdown - scheduled).toInt)
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
