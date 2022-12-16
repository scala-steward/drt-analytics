package uk.gov.homeoffice.drt.analytics.actors

import akka.actor.Actor
import uk.gov.homeoffice.drt.ports.Terminals.Terminal
import uk.gov.homeoffice.drt.protobuf.messages.CrunchState.FlightWithSplitsMessage


trait TerminalDateActor extends Actor {
  val terminal: Terminal
  val year: Int
  val month: Int
  val day: Int
  val extractValue: FlightWithSplitsMessage => Option[Double]
}

object TerminalDateActor {
  case object GetState

  case class FlightRoute(terminal: String, number: Int, origin: String)

  case class ArrivalKey(scheduled: Long, terminal: String, number: Int)

  case class ArrivalKeyWithOrigin(scheduled: Long, terminal: String, number: Int, origin: String)
}


