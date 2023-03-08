package uk.gov.homeoffice.drt.analytics.prediction.flights

import akka.NotUsed
import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.pattern.ask
import akka.stream.scaladsl.Source
import akka.util.Timeout
import org.slf4j.LoggerFactory
import scalapb.GeneratedMessage
import uk.gov.homeoffice.drt.actor.PredictionModelActor.WithId
import uk.gov.homeoffice.drt.actor.TerminalDateActor
import uk.gov.homeoffice.drt.actor.TerminalDateActor.GetState
import uk.gov.homeoffice.drt.ports.Terminals.Terminal
import uk.gov.homeoffice.drt.time.{SDateLike, UtcDate}

import scala.concurrent.{ExecutionContext, Future}


case class ValuesExtractor[T <: TerminalDateActor, A <: WithId, M <: GeneratedMessage](actorClass: Class[T],
                                                                                       extractValues: M => Option[(Double, Seq[String])],
                                                                                       extractKey: M => Option[A],
                                                                                      )
                                                                                      (implicit system: ActorSystem,
                                                                                       ec: ExecutionContext,
                                                                                       timeout: Timeout
                                                                                      ) {
  private val log = LoggerFactory.getLogger(getClass)

  val extractValuesByKey: (Terminal, SDateLike, Int) => Source[(A, Iterable[(Double, Seq[String])]), NotUsed] =
    (terminal, startDate, numberOfDays) => {
      Source(((-1 * numberOfDays) until 0).toList)
        .mapAsync(1)(day => extractValuesForDate(terminal, startDate.addDays(day).toUtcDate))
        .fold(Map[A, Iterable[(Double, Seq[String])]]())(accumulate)
        .mapConcat(identity)
    }

  private def accumulate(acc: Map[A, Iterable[(Double, Seq[String])]],
                         incoming: Map[A, Iterable[(Double, Seq[String])]],
                        ): Map[A, Iterable[(Double, Seq[String])]] =
    incoming.foldLeft(acc) {
      case (acc, (key, arrivals)) => acc.updated(key, acc.getOrElse(key, Map()) ++ arrivals)
    }

  private def extractValuesForDate(terminal: Terminal, date: UtcDate)
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
