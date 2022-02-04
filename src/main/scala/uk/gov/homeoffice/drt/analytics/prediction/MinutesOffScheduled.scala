package uk.gov.homeoffice.drt.analytics.prediction

import akka.NotUsed
import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.pattern.ask
import akka.stream.scaladsl.Source
import akka.util.Timeout
import org.slf4j.LoggerFactory
import uk.gov.homeoffice.drt.analytics.actors.MinutesOffScheduledActor
import uk.gov.homeoffice.drt.analytics.actors.MinutesOffScheduledActor.{ArrivalKeyWithOrigin, FlightRoute, GetState}
import uk.gov.homeoffice.drt.analytics.time.SDate
import uk.gov.homeoffice.drt.ports.Terminals.Terminal

import scala.concurrent.{ExecutionContext, Future}

case class MinutesOffScheduled[T <: MinutesOffScheduledActor](actorClass: Class[T]) {

  private val log = LoggerFactory.getLogger(getClass)

  def offScheduledByTerminalFlightNumberOrigin(terminal: Terminal, startDate: SDate, numberOfDays: Int)
                                              (implicit system: ActorSystem, ec: ExecutionContext, timeout: Timeout): Source[(FlightRoute, Map[Long, Int]), NotUsed] =
    Source(((-1 * numberOfDays) until 0).toList)
      .mapAsync(1)(day => arrivalsWithOffScheduledForDate(terminal, startDate.addDays(day)))
      .map(byTerminalFlightNumberAndOrigin)
      .fold(Map[FlightRoute, Map[Long, Int]]()) {
        case (acc, incoming) => addByTerminalAndFlightNumber(acc, incoming)
      }
      .mapConcat(identity)

  private def addByTerminalAndFlightNumber(acc: Map[FlightRoute, Map[Long, Int]], incoming: Map[FlightRoute, Map[Long, Int]]): Map[FlightRoute, Map[Long, Int]] =
    incoming.foldLeft(acc) {
      case (acc, (key, arrivals)) => acc.updated(key, acc.getOrElse(key, Map()) ++ arrivals)
    }

  private def byTerminalFlightNumberAndOrigin(byArrivalKey: Map[ArrivalKeyWithOrigin, Int]): Map[FlightRoute, Map[Long, Int]] =
    byArrivalKey
      .groupBy { case (key, _) =>
        (key.terminal, key.number, key.origin)
      }
      .map {
        case ((terminal, number, origin), byArrivalKey) =>
          val scheduledToOffAndOrigin = byArrivalKey.map {
            case (key, values) => (key.scheduled, values)
          }
          (FlightRoute(terminal, number, origin), scheduledToOffAndOrigin)
      }

  private def arrivalsWithOffScheduledForDate(terminal: Terminal, currentDay: SDate)
                                             (implicit system: ActorSystem, ec: ExecutionContext, timeout: Timeout): Future[Map[ArrivalKeyWithOrigin, Int]] = {
    val actor = system.actorOf(Props(actorClass, terminal, currentDay.getFullYear, currentDay.getMonth, currentDay.getDate))
    actor
      .ask(GetState).mapTo[Map[ArrivalKeyWithOrigin, Int]]
      .map { arrivals =>
        log.info(s"Got data for $terminal / ${currentDay.toISODateOnly}")
        actor ! PoisonPill
        arrivals
      }
  }
}
