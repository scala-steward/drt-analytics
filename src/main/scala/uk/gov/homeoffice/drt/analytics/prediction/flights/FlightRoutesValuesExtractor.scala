package uk.gov.homeoffice.drt.analytics.prediction.flights

import akka.NotUsed
import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.pattern.ask
import akka.stream.scaladsl.Source
import akka.util.Timeout
import org.slf4j.LoggerFactory
import uk.gov.homeoffice.drt.actor.TerminalDateActor
import uk.gov.homeoffice.drt.actor.TerminalDateActor.{ArrivalKeyWithOrigin, FlightRoute, GetState, WithId}
import uk.gov.homeoffice.drt.analytics.prediction.flights.aggregation.ExamplesAggregator
import uk.gov.homeoffice.drt.ports.Terminals.Terminal
import uk.gov.homeoffice.drt.protobuf.messages.CrunchState.FlightWithSplitsMessage
import uk.gov.homeoffice.drt.time.{SDateLike, UtcDate}

import scala.concurrent.{ExecutionContext, Future}


case class FlightValuesExtractor[T <: TerminalDateActor, A <: WithId](actorClass: Class[T],
                                                                      extractValues: FlightWithSplitsMessage => Option[(Double, Seq[String])],
                                                                      extractKey: FlightWithSplitsMessage => Option[A],
                                                                     )
                                                                     (implicit system: ActorSystem,
                                                                      ec: ExecutionContext,
                                                                      timeout: Timeout
                                                                     ) {
  private val log = LoggerFactory.getLogger(getClass)

  val extractedValueByFlightRoute: (Terminal, SDateLike, Int) => Source[(A, Iterable[(Double, Seq[String])]), NotUsed] =
    (terminal, startDate, numberOfDays) => {
      Source(((-1 * numberOfDays) until 0).toList)
        .mapAsync(1)(day => arrivalsWithExtractedValueForDate(terminal, startDate.addDays(day).toUtcDate))
        .fold(Map[A, Iterable[(Double, Seq[String])]]()) {
          case (acc, incoming) => addByTerminalAndFlightNumber(acc, incoming)
        }
        .mapConcat(identity)
    }

  private def addByTerminalAndFlightNumber(acc: Map[A, Iterable[(Double, Seq[String])]],
                                           incoming: Map[A, Iterable[(Double, Seq[String])]],
                                          ): Map[A, Iterable[(Double, Seq[String])]] =
    incoming.foldLeft(acc) {
      case (acc, (key, arrivals)) => acc.updated(key, acc.getOrElse(key, Map()) ++ arrivals)
    }

  private def arrivalsWithExtractedValueForDate(terminal: Terminal, date: UtcDate)
                                               (implicit system: ActorSystem,
                                                ec: ExecutionContext,
                                                timeout: Timeout
                                               ): Future[Map[A, Iterable[(Double, Seq[String])]]] = {
    val actor = system.actorOf(Props(actorClass, terminal, date, extractValues, extractKey))
    actor
      .ask(GetState).mapTo[Map[A, Iterable[(Double, Seq[String])]]]
      .map { arrivals =>
        actor ! PoisonPill
        arrivals
      }
      .recoverWith {
        case t: Throwable =>
          log.error(s"Failed to get arrivals for $terminal $date", t.getMessage)
          actor ! PoisonPill
          Future.failed(t)
      }
  }
}
