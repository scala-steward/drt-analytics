package uk.gov.homeoffice.drt.analytics.passengers

import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.pattern.AskableActorRef
import akka.util.Timeout
import org.joda.time.DateTimeZone
import org.slf4j.{Logger, LoggerFactory}
import uk.gov.homeoffice.drt.analytics.actors.{ArrivalsActor, FeedPersistenceIds, GetArrivals}
import uk.gov.homeoffice.drt.analytics.time.SDate
import uk.gov.homeoffice.drt.analytics.{Arrival, Arrivals, UniqueArrival}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

object DailySummaries {
  val log: Logger = LoggerFactory.getLogger(getClass)

  def arrivalsForSources(sources: List[String], date: SDate, lastDate: SDate)
                        (implicit ec: ExecutionContext,
                         system: ActorSystem): Seq[Future[(String, Arrivals)]] = sources.map { source =>
    val pointInTimeForDate = if (source == FeedPersistenceIds.live) date.addHours(26) else date.addHours(-12)
    val askableActor: AskableActorRef = system.actorOf(Props(classOf[ArrivalsActor], source, pointInTimeForDate))
    val result = askableActor
      .ask(GetArrivals(date, lastDate))(new Timeout(30 seconds))
      .asInstanceOf[Future[Arrivals]]
      .map { ar => (source, ar) }
    result.onComplete(_ => askableActor.ask(PoisonPill)(new Timeout(1 second)))
    result
  }

  def mergeArrivals(arrivalSourceFutures: Seq[Future[(String, Arrivals)]])
                   (implicit ec: ExecutionContext): Future[Map[UniqueArrival, Arrival]] = Future.sequence(arrivalSourceFutures)
    .map { sourcesWithArrivals =>
      log.info(s"Got all feed source responses")
      val baseArrivals = sourcesWithArrivals.toMap.getOrElse(FeedPersistenceIds.forecastBase, Arrivals(Map()))
      val supplementalSources = Seq(FeedPersistenceIds.forecast, FeedPersistenceIds.live)
      supplementalSources.foldLeft(baseArrivals.arrivals) {
        case (arrivalsSoFar, source) =>
          val supplementalArrivals = sourcesWithArrivals.toMap.getOrElse(source, Arrivals(Map())).arrivals
          log.info(s"Applying ${supplementalArrivals.size} $source to ${arrivalsSoFar.size} existing arrivals")
          arrivalsSoFar.mapValues { arrival =>
            supplementalArrivals.get(arrival.uniqueArrival) match {
              case Some(suppArr) if suppArr.actPax > 0 =>
                arrival.copy(actPax = suppArr.actPax, transPax = suppArr.transPax)
              case _ => arrival
            }
          }
      }
    }

  def summary(date: SDate,
              startDate: SDate,
              numberOfDays: Int,
              terminal: String,
              eventualArrivals: Future[Map[UniqueArrival, Arrival]])
             (implicit ec: ExecutionContext): Future[String] = {
    eventualArrivals.map {
      case arrivals =>
        log.info(s"Got all merged arrivals: ${arrivals.size}")
        val arrivalsForTerminal = arrivals.values
          .filter(_.terminal == terminal)
          .filterNot(_.isCancelled)

        arrivalsForTerminal.groupBy(_.origin).toSeq.sortBy(_._1).map {
          case (origin, arrivalsByOrigin) =>

            val paxByDayAndOrigin = arrivalsByOrigin
              .groupBy { a =>
                SDate(a.scheduled, DateTimeZone.forID("Europe/London")).toISODateOnly
              }
              .mapValues {
                _.map(_.actPax).sum
              }

            log.info(s"Got ${paxByDayAndOrigin.size} merged arrivals after filtering")

            val paxByDay = (0 to numberOfDays).map { dayOffset =>
              paxByDayAndOrigin.get(startDate.addDays(dayOffset).toISODateOnly) match {
                case None => "-"
                case Some(pax) => s"$pax"
              }
            }
            s"${date.toISODateOnly},$terminal,$origin,${paxByDay.mkString(",")}"
        }.mkString("\n")
    }
  }
}

