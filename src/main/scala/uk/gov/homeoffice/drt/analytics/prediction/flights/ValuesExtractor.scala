package uk.gov.homeoffice.drt.analytics.prediction.flights

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source
import org.slf4j.LoggerFactory
import scalapb.GeneratedMessage
import uk.gov.homeoffice.drt.actor.PredictionModelActor.WithId
import uk.gov.homeoffice.drt.analytics.actors.TerminalDateActor
import uk.gov.homeoffice.drt.ports.Terminals.Terminal
import uk.gov.homeoffice.drt.time.{SDateLike, UtcDate}

import scala.concurrent.{ExecutionContext, Future}


case class ValuesExtractor[T <: TerminalDateActor[_], M <: GeneratedMessage](extraction: (UtcDate, Terminal) => Future[Map[WithId, Iterable[(Double, Seq[String], Seq[Double], String)]]])
                                                                            (implicit ec: ExecutionContext) {
  private val log = LoggerFactory.getLogger(getClass)

  val extractValuesByKey: (Terminal, SDateLike, Int) => Source[(WithId, Iterable[(Double, Seq[String], Seq[Double], String)]), NotUsed] =
    (terminal, startDate, numberOfDays) => {
      Source(((-1 * numberOfDays) until 0).toList)
        .mapAsync(1) { day =>
          extractValuesForDate(terminal, startDate.addDays(day).toUtcDate)
        }
        .fold(Map[WithId, Iterable[(Double, Seq[String], Seq[Double], String)]]())(accumulate)
        .mapConcat(identity)
    }

  private def accumulate(acc: Map[WithId, Iterable[(Double, Seq[String], Seq[Double], String)]],
                         incoming: Map[WithId, Iterable[(Double, Seq[String], Seq[Double], String)]],
                        ): Map[WithId, Iterable[(Double, Seq[String], Seq[Double], String)]] =
    incoming.foldLeft(acc) {
      case (acc, (key, examples)) => acc.updated(key, acc.getOrElse(key, Iterable()) ++ examples)
    }

  private def extractValuesForDate(terminal: Terminal, date: UtcDate)
                                  (implicit ec: ExecutionContext): Future[Map[WithId, Iterable[(Double, Seq[String], Seq[Double], String)]]] = {
//    val extraction: (UtcDate, Terminal) => Future[Map[WithId, Iterable[(Double, Seq[String], Seq[Double], String)]]] = ArrivalValueExtraction(arrivalsForDateAndTerminal, extractValues, extractKey, preProcess)
    extraction(date, terminal).map { featuresAndValuesForDate =>
      log.info(s"Extracted ${featuresAndValuesForDate.size} features for $terminal on $date")
      featuresAndValuesForDate
    }

//    val actor = system.actorOf(Props(actorClass, terminal, date, extractValues, extractKey, preProcess))
//    actor
//      .ask(GetState).mapTo[Map[WithId, Iterable[(Double, Seq[String], Seq[Double], String)]]]
//      .map { featuresAndValuesForDate =>
//        actor ! PoisonPill
//        featuresAndValuesForDate
//      }
//      .recoverWith {
//        case t: Throwable =>
//          log.error(s"Failed to get arrivals for $terminal $date: ${t.getMessage}")
//          actor ! PoisonPill
//          Future.failed(t)
//      }
  }

//  private def extractValuesForDate__old(terminal: Terminal, date: UtcDate)
//                                  (implicit system: ActorSystem,
//                                   ec: ExecutionContext,
//                                   timeout: Timeout
//                                  ): Future[Map[WithId, Iterable[(Double, Seq[String], Seq[Double], String)]]] = {
//    val actor = system.actorOf(Props(actorClass, terminal, date, extractValues, extractKey, preProcess))
//    actor
//      .ask(GetState).mapTo[Map[WithId, Iterable[(Double, Seq[String], Seq[Double], String)]]]
//      .map { featuresAndValuesForDate =>
//        actor ! PoisonPill
//        featuresAndValuesForDate
//      }
//      .recoverWith {
//        case t: Throwable =>
//          log.error(s"Failed to get arrivals for $terminal $date: ${t.getMessage}")
//          actor ! PoisonPill
//          Future.failed(t)
//      }
//  }
}
