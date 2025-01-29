package uk.gov.homeoffice.drt.analytics.actors

import akka.actor.Actor
import uk.gov.homeoffice.drt.arrivals.Arrival
import uk.gov.homeoffice.drt.ports.Terminals.Terminal
import uk.gov.homeoffice.drt.time.UtcDate


trait TerminalDateActor[T] extends Actor {
  val terminal: Terminal
  val date: UtcDate
  val extractValues: T => Option[(Double, Seq[String], Seq[Double], String)]
}

object TerminalDateActor {

  case class ArrivalKey(scheduled: Long, terminal: String, number: Int)

  object ArrivalKey {
    def apply(arrival: Arrival): ArrivalKey = ArrivalKey(arrival.Scheduled, arrival.Terminal.toString, arrival.VoyageNumber.numeric)
  }

  case class ArrivalKeyWithOrigin(scheduled: Long, terminal: String, number: Int, origin: String)

  object ArrivalKeyWithOrigin {
    def apply(arrival: Arrival): ArrivalKeyWithOrigin = ArrivalKeyWithOrigin(arrival.Scheduled, arrival.Terminal.toString, arrival.VoyageNumber.numeric, origin = arrival.Origin.toString)
  }
}
