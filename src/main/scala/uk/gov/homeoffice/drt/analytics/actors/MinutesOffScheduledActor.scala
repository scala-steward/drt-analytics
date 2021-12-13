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
import uk.gov.homeoffice.drt.ports.Terminals.{T2, Terminal}

import scala.concurrent.ExecutionContext
import scala.util.Try
import scala.util.matching.Regex


object MinutesOffScheduledActor {
  case object GetState

  case class ArrivalKey(scheduled: Long, terminal: String, number: Int)

  def byDaySource(startDate: SDate, numberOfDays: Int)
                 (implicit system: ActorSystem, ec: ExecutionContext, timeout: Timeout): Source[Map[ArrivalKey, Int], NotUsed] =
    Source((0 until numberOfDays).toList).mapAsync(1) { day =>
      val currentDay = startDate.addDays(day)
      val actor = system.actorOf(Props(new MinutesOffScheduledActor(T2, currentDay.fullYear, currentDay.month, currentDay.date)))
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

  // (flight code, terminal) -> scheduled -> minute off scheduled
  var arrivals: Map[(String, String), Map[Long, Int]] = Map()
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
