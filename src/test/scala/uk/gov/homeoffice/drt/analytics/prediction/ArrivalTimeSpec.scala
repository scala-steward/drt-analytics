package uk.gov.homeoffice.drt.analytics.prediction

import akka.persistence.{PersistentActor, RecoveryCompleted}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import server.protobuf.messages.FlightsMessage.FlightsWithSplitsDiffMessage
import uk.gov.homeoffice.drt.ports.Terminals.Terminal

import scala.util.matching.Regex

case class ArrivalKey(scheduled: Long, terminal: String, number: Int)

class TerminalDayFlightActor(terminal: Terminal, year: Int, month: Int, day: Int) extends PersistentActor {
  override def persistenceId: String = f"terminal-flights-${terminal.toString.toLowerCase}-$year-$month%02d-$day%02d"

  // (flight code, terminal) -> scheduled -> minute off scheduled
  var arrivals: Map[(String, String), Map[Long, Int]] = Map()
  var byKey: Map[ArrivalKey, Int] = Map()

  def parseFlightNumber(code: String): Int = {
    val flightCodeRegex: Regex = "^([A-Z0-9]{2,3}?)([0-9]{1,4})([A-Z]*)$".r

    code match {
      case flightCodeRegex(operator, flightNumber, suffix) =>
        val number = f"${flightNumber.toInt}%04d"
        f"$operator$number$suffix"
      case _ => code
    }


  }

  override def receiveRecover: Receive = {
    case RecoveryCompleted =>
    case FlightsWithSplitsDiffMessage(_, removals, updates) =>
      updates.map { u =>
        for {
          flightCode <- u.getFlight.iATA
          terminal <- u.getFlight.terminal
          scheduled <- u.getFlight.scheduled
          touchdown <- u.getFlight.touchdown
        } yield {
          byKey = byKey.updated(ArrivalKey(scheduled, terminal, parseFlightNumber()))
          val updatedMinutesOffScheduled = arrivals.getOrElse((flightCode, terminal), Map()).updated(scheduled, touchdown - scheduled)
          arrivals = arrivals.updated((flightCode, terminal), updatedMinutesOffScheduled)
        }
      }
      removals.map { r =>
        for {
          scheduled <- r.scheduled
          terminal <- r.terminalName
          flightNumber <- r.number
        } yield {

        }
      }
  }

  override def receiveCommand: Receive = {

  }
}

class ArrivalTimeSpec extends AnyWordSpec with Matchers {
  "An arrivals actor" should {
    ""
  }
}
