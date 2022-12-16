package uk.gov.homeoffice.drt.analytics.prediction

import akka.NotUsed
import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.pattern.ask
import akka.stream.scaladsl.Source
import akka.util.Timeout
import org.slf4j.LoggerFactory
import uk.gov.homeoffice.drt.analytics.actors.TerminalDateActor
import uk.gov.homeoffice.drt.analytics.actors.TerminalDateActor.{ArrivalKeyWithOrigin, FlightRoute, GetState}
import uk.gov.homeoffice.drt.analytics.time.SDate
import uk.gov.homeoffice.drt.ports.Terminals.Terminal
import uk.gov.homeoffice.drt.protobuf.messages.CrunchState.FlightWithSplitsMessage

import scala.concurrent.{ExecutionContext, Future}

case class ValuesExtractorForFlightRoutes[T <: TerminalDateActor](actorClass: Class[T], extractValue: FlightWithSplitsMessage => Option[Double]) {

  private val log = LoggerFactory.getLogger(getClass)

  def extractedValueByFlightRoute(terminal: Terminal, startDate: SDate, numberOfDays: Int)
                                 (implicit system: ActorSystem, ec: ExecutionContext, timeout: Timeout): Source[(FlightRoute, Map[Long, Double]), NotUsed] =
    Source(((-1 * numberOfDays) until 0).toList)
      .mapAsync(1)(day => arrivalsWithExtractedValueForDate(terminal, startDate.addDays(day)))
      .map(byTerminalFlightNumberAndOrigin)
      .fold(Map[FlightRoute, Map[Long, Double]]()) {
        case (acc, incoming) => addByTerminalAndFlightNumber(acc, incoming)
      }
      .mapConcat(identity)

  private def addByTerminalAndFlightNumber(acc: Map[FlightRoute, Map[Long, Double]], incoming: Map[FlightRoute, Map[Long, Double]]): Map[FlightRoute, Map[Long, Double]] =
    incoming.foldLeft(acc) {
      case (acc, (key, arrivals)) => acc.updated(key, acc.getOrElse(key, Map()) ++ arrivals)
    }

  private def byTerminalFlightNumberAndOrigin(byArrivalKey: Map[ArrivalKeyWithOrigin, Double]): Map[FlightRoute, Map[Long, Double]] =
    byArrivalKey
      .groupBy { case (key, _) =>
        (key.terminal, key.number, key.origin)
      }
      .map {
        case ((terminal, number, origin), byArrivalKey) =>
          val scheduledToExtractedValue = byArrivalKey.map {
            case (key, extractedValue) => (key.scheduled, extractedValue)
          }
          (FlightRoute(terminal, number, origin), scheduledToExtractedValue)
      }

  private def arrivalsWithExtractedValueForDate(terminal: Terminal, currentDay: SDate)
                                             (implicit system: ActorSystem, ec: ExecutionContext, timeout: Timeout): Future[Map[ArrivalKeyWithOrigin, Double]] = {
    val actor = system.actorOf(Props(actorClass, terminal, currentDay.getFullYear, currentDay.getMonth, currentDay.getDate, extractValue))
    actor
      .ask(GetState).mapTo[Map[ArrivalKeyWithOrigin, Double]]
      .map { arrivals =>
        actor ! PoisonPill
        arrivals
      }
  }
}
