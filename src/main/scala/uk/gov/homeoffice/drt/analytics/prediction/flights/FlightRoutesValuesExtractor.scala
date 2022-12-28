package uk.gov.homeoffice.drt.analytics.prediction.flights

import akka.NotUsed
import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.pattern.ask
import akka.stream.scaladsl.Source
import akka.util.Timeout
import uk.gov.homeoffice.drt.actor.TerminalDateActor
import uk.gov.homeoffice.drt.actor.TerminalDateActor.{ArrivalKeyWithOrigin, FlightRoute, GetState}
import uk.gov.homeoffice.drt.ports.Terminals.Terminal
import uk.gov.homeoffice.drt.protobuf.messages.CrunchState.FlightWithSplitsMessage
import uk.gov.homeoffice.drt.time.SDateLike

import scala.concurrent.{ExecutionContext, Future}

case class FlightRoutesValuesExtractor[T <: TerminalDateActor](actorClass: Class[T],
                                                               extractValues: FlightWithSplitsMessage => Option[(Double, Seq[String])],
                                                              ) {

  def extractedValueByFlightRoute(terminal: Terminal, startDate: SDateLike, numberOfDays: Int)
                                 (implicit system: ActorSystem,
                                  ec: ExecutionContext,
                                  timeout: Timeout
                                 ): Source[(FlightRoute, Iterable[(Double, Seq[String])]), NotUsed] =
    Source(((-1 * numberOfDays) until 0).toList)
      .mapAsync(1)(day => arrivalsWithExtractedValueForDate(terminal, startDate.addDays(day)))
      .map(byTerminalFlightNumberAndOrigin)
      .fold(Map[FlightRoute, Iterable[(Double, Seq[String])]]()) {
        case (acc, incoming) => addByTerminalAndFlightNumber(acc, incoming)
      }
      .mapConcat(identity)

  private def addByTerminalAndFlightNumber(acc: Map[FlightRoute, Iterable[(Double, Seq[String])]],
                                           incoming: Map[FlightRoute, Iterable[(Double, Seq[String])]],
                                          ): Map[FlightRoute, Iterable[(Double, Seq[String])]] =
    incoming.foldLeft(acc) {
      case (acc, (key, arrivals)) => acc.updated(key, acc.getOrElse(key, Map()) ++ arrivals)
    }

  private def byTerminalFlightNumberAndOrigin(byArrivalKey: Map[ArrivalKeyWithOrigin, (Double, Seq[String])]): Map[FlightRoute, Iterable[(Double, Seq[String])]] =
    byArrivalKey
      .groupBy { case (key, _) =>
        (key.terminal, key.number, key.origin)
      }
      .map {
        case ((terminal, number, origin), byArrivalKey) =>
          (FlightRoute(terminal, number, origin), byArrivalKey.values)
      }

  private def arrivalsWithExtractedValueForDate(terminal: Terminal, currentDay: SDateLike)
                                               (implicit system: ActorSystem,
                                                ec: ExecutionContext,
                                                timeout: Timeout
                                               ): Future[Map[ArrivalKeyWithOrigin, (Double, Seq[String])]] = {
    val actor = system.actorOf(Props(actorClass, terminal, currentDay.getFullYear, currentDay.getMonth, currentDay.getDate, extractValues))
    actor
      .ask(GetState).mapTo[Map[ArrivalKeyWithOrigin, (Double, Seq[String])]]
      .map { arrivals =>
        actor ! PoisonPill
        arrivals
      }
  }
}
