package uk.gov.homeoffice.drt.analytics

import akka.NotUsed
import akka.actor.{ActorSystem, PoisonPill}
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
import uk.gov.homeoffice.drt.analytics.actors.{Ack, FeedPersistenceIds, OriginTerminalPassengersActor}
import uk.gov.homeoffice.drt.analytics.passengers.DailySummaries
import uk.gov.homeoffice.drt.analytics.time.SDate

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

object Routes {
  val log: Logger = LoggerFactory.getLogger(getClass)
  implicit val system: ActorSystem = ActorSystem("DrtAnalytics")
  implicit val ec: ExecutionContextExecutor = ExecutionContext.global
  implicit val mat: ActorMaterializer = ActorMaterializer()
  implicit val timeout: Timeout = new Timeout(5 seconds)

  implicit val stringAsCsv: Marshaller[String, ByteString] = Marshaller.strict[String, ByteString] { t =>
    Marshalling.WithFixedContentType(ContentTypes.`text/csv(UTF-8)`, () => {
      ByteString(t)
    })
  }
  implicit val csvStreamingSupport: CsvEntityStreamingSupport = EntityStreamingSupport.csv()

  lazy val routes: Route = concat(
    pathPrefix("daily-pax" / Segments(2)) {
      case List(terminal, numberOfDays) =>
        Try(numberOfDays.toInt) match {
          case Failure(t) =>
            complete("Number of days must be an integer")
          case Success(numDays) =>
            val now = SDate.now(DateTimeZone.forID("Europe/London"))
            val today = SDate(now.fullYear, now.month, now.date, 0, 0)
            val startDate = today.addDays(-1 * (numDays - 1))
            get {
              complete(dailyPassengersByOriginAndDayCsv(terminal.toUpperCase, startDate, numDays - 1))
            }
        }
      case _ => complete("Hmm")
    },
    pathPrefix("update-daily-pax" / Segments(2)) {
      case List(terminal, numberOfDays) =>
        Try(numberOfDays.toInt) match {
          case Failure(t) =>
            complete("Number of days must be an integer")
          case Success(numDays) =>
            val now = SDate.now(DateTimeZone.forID("Europe/London"))
            val today = SDate(now.fullYear, now.month, now.date, 0, 0)
            val startDate = today.addDays(-1 * (numDays - 1))
            get {
              complete(updateDailyPassengersByOriginAndDay(terminal.toUpperCase, startDate, numDays - 1))
            }
        }
      case _ => complete("Hmm")
    })

  val sourcesInOrder = List(FeedPersistenceIds.forecastBase, FeedPersistenceIds.forecast, FeedPersistenceIds.live)

  private def dailyPassengersByOriginAndDayCsv(terminal: String,
                                               startDate: SDate,
                                               numberOfDays: Int): Source[String, NotUsed] = {
    val header = "Date,Terminal,Origin," + (0 to numberOfDays).map { offset => startDate.addDays(offset).toISODateOnly }.mkString(",")

    val eventualSummaries: Source[String, NotUsed] = Source(0 to numberOfDays)
      .mapAsync(1) { dayOffset =>
        DailySummaries.toCsv(terminal, startDate, numberOfDays, sourcesInOrder, header, dayOffset)
      }
      .mapConcat(identity)

    eventualSummaries
  }

  private def updateDailyPassengersByOriginAndDay(terminal: String,
                                                  startDate: SDate,
                                                  numberOfDays: Int)
                                                 (implicit timeout: Timeout): Source[String, NotUsed] =
    Source(0 to numberOfDays)
      .mapAsync(1) { dayOffset =>
        DailySummaries.dailyPaxCountsForDayAndTerminalByOrigin(terminal, startDate, numberOfDays, sourcesInOrder, dayOffset)
      }
      .mapConcat(identity)
      .mapAsync(1) {
        case (origin, dailyPaxCountsOnDay) if dailyPaxCountsOnDay.dailyPax.isEmpty =>
          Future(s"No daily nos available for ${origin} on ${dailyPaxCountsOnDay.day.toISOString}")
        case (origin, dailyPaxCountsOnDay)  =>
          val askableActor: AskableActorRef = system.actorOf(OriginTerminalPassengersActor.props(origin, terminal))
          val eventualAck = askableActor.ask(dailyPaxCountsOnDay).map {
            case x => s" ${x}: Daily pax counts persisted for $origin on ${dailyPaxCountsOnDay.day.toISOString}"
          }
          eventualAck
            .recoverWith {
              case t =>
                log.error("Did not receive ack", t)
                Future("Daily pax counts might not have been persisted. Didn't receive an ack")
            }
            .onComplete(_ => askableActor.ask(PoisonPill))

          eventualAck
      }
}
