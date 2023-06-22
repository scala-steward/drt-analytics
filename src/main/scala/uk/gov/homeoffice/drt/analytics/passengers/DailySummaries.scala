package uk.gov.homeoffice.drt.analytics.passengers

import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.pattern.ask
import akka.util.Timeout
import org.joda.time.DateTimeZone
import org.slf4j.{Logger, LoggerFactory}
import uk.gov.homeoffice.drt.analytics.actors.{ArrivalsActor, FeedPersistenceIds, GetArrivals}
import uk.gov.homeoffice.drt.analytics.{Arrivals, DailyPaxCountsOnDay, SimpleArrival}
import uk.gov.homeoffice.drt.arrivals.UniqueArrival
import uk.gov.homeoffice.drt.time.{SDate, SDateLike}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

object DailySummaries {
  val log: Logger = LoggerFactory.getLogger(getClass)

  def arrivalsForSources(sourcePersistenceIds: List[String],
                         date: SDateLike,
                         lastDate: SDateLike,
                         actorProps: (String, SDateLike) => Props)
                        (implicit ec: ExecutionContext,
                         system: ActorSystem): Seq[Future[(String, Arrivals)]] = sourcePersistenceIds
    .map { source =>
      val pointInTimeForDate = if (source == FeedPersistenceIds.live) date.addHours(26) else date.addHours(-12)
      val actor = system.actorOf(actorProps(source, pointInTimeForDate))
      val result = actor
        .ask(GetArrivals(date, lastDate))(new Timeout(30.seconds))
        .asInstanceOf[Future[Arrivals]]
        .map { ar => (source, ar) }
      result.onComplete(_ => actor.ask(PoisonPill)(new Timeout(1.second)))
      result
    }

  def mergeArrivals(arrivalSourceFutures: Seq[Future[(String, Arrivals)]])
                   (implicit ec: ExecutionContext): Future[Map[UniqueArrival, SimpleArrival]] = Future.sequence(arrivalSourceFutures)
    .map { sourcesWithArrivals =>
      log.info(s"Got all feed source responses")
      val baseArrivals = sourcesWithArrivals.toMap.getOrElse(FeedPersistenceIds.forecastBase, Arrivals(Map()))
      val supplementalSources = Seq(FeedPersistenceIds.forecast, FeedPersistenceIds.live)
      supplementalSources.foldLeft(baseArrivals.arrivals) {
        case (arrivalsSoFar, source) =>
          val supplementalArrivals = sourcesWithArrivals.toMap.getOrElse(source, Arrivals(Map())).arrivals
          log.info(s"Applying ${supplementalArrivals.size} $source to ${arrivalsSoFar.size} existing arrivals")
          arrivalsSoFar.view.mapValues { arrival =>
            supplementalArrivals.get(arrival.uniqueArrival) match {
              case Some(suppArr) if suppArr.bestPaxEstimate.getPcpPax.getOrElse(0) > 0 =>
                arrival.copy(passengerSources = suppArr.passengerSources)
              case _ => arrival
            }
          }.toMap
      }
    }

  def dailyPaxCountsForDayByOrigin(date: SDateLike,
                                   startDate: SDateLike,
                                   numberOfDays: Int,
                                   terminal: String,
                                   eventualArrivals: Future[Map[UniqueArrival, SimpleArrival]])
                                  (implicit ec: ExecutionContext): Future[Map[String, DailyPaxCountsOnDay]] = {
    eventualArrivals.map { arrivals =>
      log.info(s"Got all merged arrivals: ${arrivals.size}")
      val arrivalsForTerminal = arrivals.values
        .filter(_.terminal == terminal)
        .filterNot(_.isCancelled)

      arrivalsForTerminal.groupBy(_.origin).map {
        case (origin, arrivalsByOrigin) =>
          val paxByDayAndOrigin = arrivalsByOrigin
            .groupBy { a =>
              SDate(a.scheduled, DateTimeZone.forID("Europe/London")).toISODateOnly
            }
            .mapValues {
              _.map(a => a.bestPaxEstimate.getPcpPax.getOrElse(0)).sum
            }

          val paxByDay = (0 to numberOfDays)
            .map { dayOffset =>
              val day = startDate.addDays(dayOffset)
              paxByDayAndOrigin.get(day.toISODateOnly).map { p => (day.millisSinceEpoch, p) }
            }
            .collect { case Some(dayAndPax) => dayAndPax }
            .toMap

          (origin, DailyPaxCountsOnDay(date.millisSinceEpoch, paxByDay))
      }
    }
  }

  def dailyOriginCountsToCsv(date: SDateLike,
                             startDate: SDateLike,
                             numberOfDays: Int,
                             terminal: String,
                             eventualOriginDailyPaxCounts: Future[Map[String, DailyPaxCountsOnDay]])
                            (implicit ec: ExecutionContext): Future[String] = eventualOriginDailyPaxCounts
    .map { arrivals =>
      arrivals.map {
        case (origin, pc) =>
          val paxByDay = (0 until numberOfDays).map { dayOffset =>
            pc.dailyPax.get(startDate.addDays(dayOffset).millisSinceEpoch).map(_.toString).getOrElse("-")
          }
          s"${date.toISODateOnly},$terminal,$origin,${paxByDay.mkString(",")}"
      }.mkString("\n")
    }

  def dailyPaxCountsForDayAndTerminalByOrigin(terminal: String,
                                              startDate: SDateLike,
                                              numberOfDays: Int,
                                              sourcesInOrder: List[String],
                                              dayOffset: Int)
                                             (implicit ec: ExecutionContext,
                                              system: ActorSystem): Future[Map[String, DailyPaxCountsOnDay]] = {
    val viewDate = startDate.addDays(dayOffset)
    val lastDate = startDate.addDays(numberOfDays)
    val eventualsBySource = arrivalsForSources(sourcesInOrder, viewDate, lastDate, ArrivalsActor.props)
    val eventualMergedArrivals = mergeArrivals(eventualsBySource)
    dailyPaxCountsForDayByOrigin(viewDate, startDate, numberOfDays, terminal, eventualMergedArrivals)
  }

  def toCsv(terminal: String,
            startDate: SDateLike,
            numberOfDays: Int,
            sourcesInOrder: List[String],
            dayOffset: Int)(implicit ec: ExecutionContext, system: ActorSystem): Future[String] = {
    val viewDate = startDate.addDays(dayOffset)
    val eventualCountsByOrigin = dailyPaxCountsForDayAndTerminalByOrigin(terminal, startDate, numberOfDays, sourcesInOrder, dayOffset)

    dailyOriginCountsToCsv(viewDate, startDate, numberOfDays, terminal, eventualCountsByOrigin)
      .recoverWith { case t =>
        log.error(s"Failed to get summaries", t)
        Future("")
      }
  }

}
