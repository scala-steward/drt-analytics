package uk.gov.homeoffice.drt.analytics

import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.pattern.AskableActorRef
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.Timeout
import org.joda.time.DateTimeZone
import org.slf4j.{Logger, LoggerFactory}
import uk.gov.homeoffice.drt.analytics.actors.{ArrivalsActor, FeedPersistenceIds}
import uk.gov.homeoffice.drt.analytics.time.SDate

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.language.postfixOps
import scala.util.{Failure, Success}

object AnalyticsApp extends App {
  val log: Logger = LoggerFactory.getLogger(getClass)
  implicit val system: ActorSystem = ActorSystem("DrtAnalytics")
  implicit val ec: ExecutionContextExecutor = ExecutionContext.global
  implicit val mat: ActorMaterializer = ActorMaterializer()

  val terminal = "T5"

  val sourcesInOrder = List(FeedPersistenceIds.forecastBase, FeedPersistenceIds.forecast, FeedPersistenceIds.live)

  val startDate = SDate("2020-02-15", DateTimeZone.forID("Europe/London"))
  val numberOfDays = 25

  val eventualSummaries = Source(0 to numberOfDays).mapAsync(1) { dayOffset =>
    val viewDate = startDate.addDays(dayOffset)
    val lastDate = startDate.addDays(numberOfDays)
    val eventualsBySource = DailySummaries.arrivalsForSources(sourcesInOrder, viewDate, lastDate)
    val eventualMergedArrivals = DailySummaries.mergeArrivals(eventualsBySource)

    DailySummaries.summary(viewDate, startDate, numberOfDays, terminal, eventualMergedArrivals)
  }.runWith(Sink.seq)

  eventualSummaries.onComplete {
    case Failure(t) =>
      log.error("Failed to get summaries", t)
    case Success(summaries) =>
      println("Date,Terminal," + (0 to numberOfDays).map { offset => startDate.addDays(offset).toISODateOnly}.mkString(",") )
      println(summaries.mkString("\n"))
  }
}

object DailySummaries {
  val log: Logger = LoggerFactory.getLogger(getClass)

  def arrivalsForSources(sources: List[String], date: SDate, lastDate: SDate)
                        (implicit ec: ExecutionContext, system: ActorSystem): Seq[Future[(String, Arrivals)]] = sources.map { source =>
    val pointInTimeForDate = if (source == FeedPersistenceIds.live) date.addHours(26) else date.addHours(-12)
    val askableActor: AskableActorRef = system.actorOf(Props(classOf[ArrivalsActor], source, pointInTimeForDate))
    val result = askableActor
      .ask(GetArrivals(date, lastDate))(new Timeout(5 seconds))
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


  def summary(date: SDate, startDate: SDate, numberOfDays: Int, terminal: String, eventualArrivals: Future[Map[UniqueArrival, Arrival]])
             (implicit ec: ExecutionContext): Future[String] = {
    eventualArrivals.map {
      case arrivals =>
        log.info(s"Got all merged arrivals: ${arrivals.size}")
        val paxByDayAndTerminal = arrivals.values
          .filter(_.terminal == terminal)
          .groupBy { a =>
            val sch = SDate(a.scheduled, DateTimeZone.forID("Europe/London"))
            (sch.toISODateOnly, a.terminal)
          }
          .mapValues {
            _.map(a => a.actPax - a.transPax).sum
          }
          .toSeq
          .sortBy { case ((day, terminal), _) => s"$day-$terminal" }
        log.info(s"Got ${paxByDayAndTerminal.size} merged arrivals after filtering")

        val daySummaries = paxByDayAndTerminal.map {
          case ((date, _), pax) => (date, pax)
        }.toMap
        val paxByDay = (0 to numberOfDays).map { dayOffset =>
          daySummaries.get(startDate.addDays(dayOffset).toISODateOnly) match {
            case None => "-"
            case Some(pax) => s"$pax"
          }
        }
        s"${date.toISODateOnly},$terminal,${paxByDay.mkString(",")}"
    }
  }
}

case class GetArrivals(firstDay: SDate, lastDay: SDate)
