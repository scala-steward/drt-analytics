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
import uk.gov.homeoffice.drt.actor.TerminalDateActor.ArrivalKey
import uk.gov.homeoffice.drt.actor.commands.Commands.GetState
import uk.gov.homeoffice.drt.arrivals.Arrival
import uk.gov.homeoffice.drt.ports.Terminals.Terminal
import uk.gov.homeoffice.drt.time.{SDateLike, UtcDate}

import scala.concurrent.{ExecutionContext, Future}


case class ValuesExtractor[T <: TerminalDateActor[_], M <: GeneratedMessage](actorClass: Class[T],
                                                                             extractValues: _ => Option[(Double, Seq[String], Seq[Double])],
                                                                             extractKey: M => Option[WithId],
                                                                             preProcess: (UtcDate, Map[ArrivalKey, Arrival]) => Future[Map[ArrivalKey, Arrival]],
                                                                            )
                                                                            (implicit system: ActorSystem,
                                                                             ec: ExecutionContext,
                                                                             timeout: Timeout
                                                                            ) {
  private val log = LoggerFactory.getLogger(getClass)

  val extractValuesByKey: (Terminal, SDateLike, Int) => Source[(WithId, Iterable[(Double, Seq[String], Seq[Double])]), NotUsed] =
    (terminal, startDate, numberOfDays) => {
      Source(((-1 * numberOfDays) until 0).toList)
        .mapAsync(1) { day =>
          extractValuesForDate(terminal, startDate.addDays(day).toUtcDate)
        }
        .fold(Map[WithId, Iterable[(Double, Seq[String], Seq[Double])]]())(accumulate)
        .mapConcat(identity)
    }

  private def accumulate(acc: Map[WithId, Iterable[(Double, Seq[String], Seq[Double])]],
                         incoming: Map[WithId, Iterable[(Double, Seq[String], Seq[Double])]],
                        ): Map[WithId, Iterable[(Double, Seq[String], Seq[Double])]] =
    incoming.foldLeft(acc) {
      case (acc, (key, examples)) => acc.updated(key, acc.getOrElse(key, Iterable()) ++ examples)
    }

  private def extractValuesForDate(terminal: Terminal, date: UtcDate)
                                  (implicit system: ActorSystem,
                                   ec: ExecutionContext,
                                   timeout: Timeout
                                  ): Future[Map[WithId, Iterable[(Double, Seq[String], Seq[Double])]]] = {
    val actor = system.actorOf(Props(actorClass, terminal, date, extractValues, extractKey, preProcess))
    actor
      .ask(GetState).mapTo[Map[WithId, Iterable[(Double, Seq[String], Seq[Double])]]]
      .map { featuresAndValuesForDate =>
        actor ! PoisonPill
        featuresAndValuesForDate
      }
      .recoverWith {
        case t: Throwable =>
          log.error(s"Failed to get arrivals for $terminal $date: ${t.getMessage}")
          actor ! PoisonPill
          Future.failed(t)
      }
  }
}
