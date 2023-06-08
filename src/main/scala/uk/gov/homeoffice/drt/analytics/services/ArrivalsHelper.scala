package uk.gov.homeoffice.drt.analytics.services

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.util.Timeout
import org.slf4j.LoggerFactory
import uk.gov.homeoffice.drt.actor.TerminalDateActor.ArrivalKey
import uk.gov.homeoffice.drt.analytics.Arrivals
import uk.gov.homeoffice.drt.analytics.actors.{ArrivalsActor, FeedPersistenceIds, GetArrivals}
import uk.gov.homeoffice.drt.arrivals.Arrival
import uk.gov.homeoffice.drt.time.{SDate, UtcDate}

import scala.concurrent.{ExecutionContext, Future}

object ArrivalsHelper {
  private val log = LoggerFactory.getLogger(getClass)

  val noopPreProcess: (UtcDate, Map[ArrivalKey, Arrival]) => Future[Map[ArrivalKey, Arrival]] = (_, a) => Future.successful(a)

  def populateMaxPax()(implicit
                       system: ActorSystem,
                       ec: ExecutionContext,
                       timeout: Timeout): (UtcDate, Map[ArrivalKey, Arrival]) => Future[Map[ArrivalKey, Arrival]] =
    (date, arrivals) => {
      val withMaxPaxPct = (100 * arrivals.values.count(_.MaxPax.isDefined).toDouble / arrivals.size).round
      if (withMaxPaxPct < 80) {
        log.debug(s"Only $withMaxPaxPct% of arrivals have max pax for $date, populating")
        val arrivalsActor = system.actorOf(ArrivalsActor.props(FeedPersistenceIds.forecastBase, date))
        arrivalsActor
          .ask(GetArrivals(SDate(date), SDate(date).addDays(1))).mapTo[Arrivals]
          .map { baseArrivals =>
            arrivalsActor ! akka.actor.PoisonPill

            arrivals.view.mapValues {
              arr =>
                val maybeArrival = baseArrivals.arrivals.get(arr.unique)
                val maybeMaxPax = maybeArrival.flatMap(_.maxPax)
                arr.copy(MaxPax = maybeMaxPax)
            }.toMap
          }
          .recover {
            case t =>
              log.error(s"Failed to populate max pax for $date", t)
              arrivals
          }
      } else Future.successful(arrivals)
    }
}
