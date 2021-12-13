package uk.gov.homeoffice.drt.analytics.actors

import akka.persistence.{PersistentActor, RecoveryCompleted}
import server.protobuf.messages.CrunchState.FlightsWithSplitsDiffMessage
import uk.gov.homeoffice.drt.analytics.actors.MinutesOffScheduledActor.{ArrivalKey, GetState}
import uk.gov.homeoffice.drt.ports.Terminals.Terminal

import scala.util.Try
import scala.util.matching.Regex


object MinutesOffScheduledActor {
  case object GetState

  case class ArrivalKey(scheduled: Long, terminal: String, number: Int)
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
