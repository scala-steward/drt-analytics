package uk.gov.homeoffice.drt.analytics

import akka.NotUsed
import akka.actor.ActorSystem
import akka.pattern.AskableActorRef
import akka.stream.scaladsl.Source
import akka.util.Timeout
import org.joda.time.DateTimeZone
import org.slf4j.{Logger, LoggerFactory}
import uk.gov.homeoffice.drt.analytics.actors.FeedPersistenceIds
import uk.gov.homeoffice.drt.analytics.passengers.DailySummaries
import uk.gov.homeoffice.drt.analytics.time.SDate

import scala.concurrent.{ExecutionContext, Future}

object PaxDeltas {
  val log: Logger = LoggerFactory.getLogger(getClass)
  val sourcesInOrder = List(FeedPersistenceIds.forecastBase, FeedPersistenceIds.forecast, FeedPersistenceIds.live)

  def maybeAverageDelta(maybeDeltas: Seq[Option[Int]]): Option[Int] = {
    val total = maybeDeltas.collect { case Some(diff) => diff }.sum.toDouble
    maybeDeltas.count(_.isDefined) match {
      case 0 => None
      case daysWithNumbers => Option((total / daysWithNumbers).round.toInt)
    }
  }

  def maybeDeltas(dailyPaxNosByDay: Map[(Long, Long), Int],
                  numberOfDays: Int,
                  now: () => SDate): Seq[Option[Int]] = {
    val startDay = now().addDays(-1).getLocalLastMidnight

    (0 until numberOfDays).map { dayOffset =>
      val day = startDay.addDays(-1 * dayOffset)
      val dayBefore = day.addDays(-1)
      val maybeActualPax = dailyPaxNosByDay.get((day.millisSinceEpoch, day.millisSinceEpoch))
      val maybeForecastPax = dailyPaxNosByDay.get((dayBefore.millisSinceEpoch, day.millisSinceEpoch))
      for {
        actual <- maybeActualPax
        forecast <- maybeForecastPax
      } yield forecast - actual
    }
  }

  def updateDailyPassengersByOriginAndDay(terminal: String,
                                          startDate: SDate,
                                          numberOfDays: Int,
                                          passengersActor: AskableActorRef)
                                         (implicit timeout: Timeout,
                                          ec: ExecutionContext,
                                          system: ActorSystem): Source[Option[(String, SDate)], NotUsed] =
    Source(0 to numberOfDays)
      .mapAsync(1) { dayOffset =>
        DailySummaries.dailyPaxCountsForDayAndTerminalByOrigin(terminal, startDate, numberOfDays, sourcesInOrder, dayOffset)
      }
      .mapConcat(identity)
      .mapAsync(1) {
        case (origin, dailyPaxCountsOnDay) if dailyPaxCountsOnDay.dailyPax.isEmpty =>
          log.info(s"No daily nos available for $origin on ${dailyPaxCountsOnDay.day.toISOString}")
          Future.successful(None)

        case (origin, dailyPaxCountsOnDay) =>
          passengersActor.ask(OriginTerminalDailyPaxCountsOnDay(origin, terminal, dailyPaxCountsOnDay))
            .map(_ => Option((origin, dailyPaxCountsOnDay.day)))
            .recover {
              case t =>
                log.error(s"Did not receive ack for $origin on ${dailyPaxCountsOnDay.day.toISOString}", t)
                None
            }
      }

  def dailyPassengersByOriginAndDayCsv(terminal: String,
                                       startDate: SDate,
                                       numberOfDays: Int)
                                      (implicit ec: ExecutionContext, system: ActorSystem): Source[String, NotUsed] = {
    val header = csvHeader(startDate, numberOfDays)

    Source(0 to numberOfDays)
      .mapAsync(1) { dayOffset =>
        DailySummaries.toCsv(terminal, startDate, numberOfDays, sourcesInOrder, dayOffset).map { dataRow =>
          if (dayOffset == 0) header + "\n" + dataRow else dataRow
        }
      }
  }

  private def csvHeader(startDate: SDate, numberOfDays: Int): String =
    "Date,Terminal,Origin," + (0 to numberOfDays).map { offset => startDate.addDays(offset).toISODateOnly }.mkString(",")

  def startDate(numDays: Int): SDate = {
    val today = SDate(localNow.getFullYear, localNow.getMonth, localNow.getDate, 0, 0)
    today.addDays(-1 * (numDays - 1))
  }

  def localNow: SDate = {
    SDate.now(DateTimeZone.forID("Europe/London"))
  }
}
