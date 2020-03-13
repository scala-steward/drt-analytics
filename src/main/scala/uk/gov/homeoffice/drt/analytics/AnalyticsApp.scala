package uk.gov.homeoffice.drt.analytics

import akka.NotUsed
import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.common.{CsvEntityStreamingSupport, EntityStreamingSupport}
import akka.http.scaladsl.marshalling.{Marshaller, Marshalling}
import akka.http.scaladsl.model.ContentTypes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.directives.MethodDirectives.get
import akka.http.scaladsl.server.directives.RouteDirectives.complete
import akka.pattern.AskableActorRef
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.util.{ByteString, Timeout}
import org.joda.time.DateTimeZone
import org.slf4j.{Logger, LoggerFactory}
import uk.gov.homeoffice.drt.analytics.actors.{ArrivalsActor, FeedPersistenceIds}
import uk.gov.homeoffice.drt.analytics.time.SDate

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

object AnalyticsApp extends App {
  val log: Logger = LoggerFactory.getLogger(getClass)
  implicit val system: ActorSystem = ActorSystem("DrtAnalytics")
  implicit val ec: ExecutionContextExecutor = ExecutionContext.global
  implicit val mat: ActorMaterializer = ActorMaterializer()

  val serverBinding: Future[Http.ServerBinding] = Http().bindAndHandle(Routes.routes, "localhost", 8081)

  serverBinding.onComplete {
    case Success(bound) =>
      log.info(s"Server online at http://${bound.localAddress.getHostString}:${bound.localAddress.getPort}/")
    case Failure(e) =>
      log.error(s"Server could not start!", e)
      system.terminate()
  }
  Await.result(system.whenTerminated, Duration.Inf)
}

object Routes {
  val log: Logger = LoggerFactory.getLogger(getClass)
  implicit val system: ActorSystem = ActorSystem("DrtAnalytics")
  implicit val ec: ExecutionContextExecutor = ExecutionContext.global
  implicit val mat: ActorMaterializer = ActorMaterializer()

  implicit val stringAsCsv = Marshaller.strict[String, ByteString] { t =>
    Marshalling.WithFixedContentType(ContentTypes.`text/csv(UTF-8)`, () => {
      ByteString(t)
    })
  }
  implicit val csvStreamingSupport: CsvEntityStreamingSupport = EntityStreamingSupport.csv()

  lazy val routes: Route = pathPrefix("daily-pax" / Segments(2)) {
    case List(terminal, numberOfDays) =>
      Try(numberOfDays.toInt) match {
        case Failure(t) =>
          complete("Number of days must be an integer")
        case Success(numDays) =>
          val now = SDate.now(DateTimeZone.forID("Europe/London"))
          val today = SDate(now.fullYear, now.month, now.date, 0, 0)
          val startDate = today.addDays(-1 * (numDays - 1))
          get {
            complete(dailyPassengersCsv(terminal.toUpperCase, startDate, numDays - 1))
          }
      }
    case _ =>
      complete("Hmm")
  }

  private def dailyPassengersCsv(terminal: String, startDate: SDate, numberOfDays: Int): Source[String, NotUsed] = {
    val sourcesInOrder = List(FeedPersistenceIds.forecastBase, FeedPersistenceIds.forecast, FeedPersistenceIds.live)

    val header = "Date,Terminal," + (0 to numberOfDays).map { offset => startDate.addDays(offset).toISODateOnly }.mkString(",")

    val eventualSummaries: Source[String, NotUsed] = Source(0 to numberOfDays)
      .mapAsync(1) { dayOffset =>
        val viewDate = startDate.addDays(dayOffset)
        val lastDate = startDate.addDays(numberOfDays)
        val eventualsBySource = DailySummaries.arrivalsForSources(sourcesInOrder, viewDate, lastDate)
        val eventualMergedArrivals = DailySummaries.mergeArrivals(eventualsBySource)

        DailySummaries.summary(viewDate, startDate, numberOfDays, terminal, eventualMergedArrivals).map { row =>
          if (dayOffset == 0) List(header, row) else List(row)
        }
      }
      .mapConcat(identity)

    eventualSummaries
  }
}

object DailySummaries {
  val log: Logger = LoggerFactory.getLogger(getClass)

  def arrivalsForSources(sources: List[String], date: SDate, lastDate: SDate)
                        (implicit ec: ExecutionContext,
                         system: ActorSystem): Seq[Future[(String, Arrivals)]] = sources.map { source =>
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


  def summary(date: SDate,
              startDate: SDate,
              numberOfDays: Int,
              terminal: String,
              eventualArrivals: Future[Map[UniqueArrival, Arrival]])
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
