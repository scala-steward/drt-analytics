package uk.gov.homeoffice.drt.analytics.services

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.util.Timeout
import org.slf4j.LoggerFactory
import uk.gov.homeoffice.drt.analytics.Arrivals
import uk.gov.homeoffice.drt.analytics.actors.{ArrivalsActor, FeedPersistenceIds, GetArrivals}
import uk.gov.homeoffice.drt.arrivals.Arrival
import uk.gov.homeoffice.drt.time.{SDate, UtcDate}

import scala.concurrent.{ExecutionContext, Future}

object ArrivalsHelper {
  private val log = LoggerFactory.getLogger(getClass)

  val noopPreProcess: (UtcDate, Iterable[Arrival]) => Future[Iterable[Arrival]] = (_, a) => Future.successful(a)

  def populateMaxPax()(implicit
                       system: ActorSystem,
                       ec: ExecutionContext,
                       timeout: Timeout): (UtcDate, Iterable[Arrival]) => Future[Iterable[Arrival]] =
    (date, arrivals) => {
      def pctWithMaxPax(arrivals: Iterable[Arrival]): Int = (100 * arrivals.count(_.MaxPax.isDefined).toDouble / arrivals.size).round.toInt

      val pctOk = pctWithMaxPax(arrivals)
      if (pctOk < 80) {
        log.info(s"Only $pctOk% of arrivals have max pax for $date, populating")
        val arrivalsActor = system.actorOf(ArrivalsActor.props(FeedPersistenceIds.forecastBase, SDate(date)))
        arrivalsActor
          .ask(GetArrivals(SDate(date), SDate(date).addDays(1))).mapTo[Arrivals]
          .map { baseArrivals =>
            arrivalsActor ! akka.actor.PoisonPill

            arrivals.map {
              arr =>
                val maybeArrival = baseArrivals.arrivals.get(arr.unique)
                val maybeMaxPax = maybeArrival.flatMap(_.maxPax)
                arr.copy(MaxPax = maybeMaxPax)
            }
          }
          .map{ a =>
            val pctOk = pctWithMaxPax(a)
            log.info(s"Populated max pax for $date, now $pctOk% have max pax")
            a
          }
          .recover {
            case t =>
              log.error(s"Failed to populate max pax for $date", t)
              arrivals
          }
      } else Future.successful(arrivals)
    }
}
