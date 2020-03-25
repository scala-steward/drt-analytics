package uk.gov.homeoffice.drt.analytics

import akka.NotUsed
import akka.actor.{ActorSystem, Props}
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
import uk.gov.homeoffice.drt.analytics.actors.{FeedPersistenceIds, PassengersActor}
import uk.gov.homeoffice.drt.analytics.passengers.DailySummaries
import uk.gov.homeoffice.drt.analytics.time.SDate

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.language.postfixOps
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

  val passengersActor: AskableActorRef = system.actorOf(Props(new PassengersActor()))

  lazy val routes: Route = concat(
    pathPrefix("daily-pax" / Segments(2)) {
      case List(terminal, numberOfDays) =>
        Try(numberOfDays.toInt) match {
          case Failure(_) =>
            complete("Number of days must be an integer")
          case Success(numDays) =>
            get {
              complete(dailyPassengersByOriginAndDayCsv(terminal.toUpperCase, startDate(numDays), numDays - 1))
            }
        }
      case _ => complete("Hmm")
    },
    pathPrefix("update-daily-pax" / Segments(2)) {
      case List(terminal, numberOfDays) =>
        Try(numberOfDays.toInt) match {
          case Failure(_) =>
            complete("Number of days must be an integer")
          case Success(numDays) =>
            get {
              complete(updateDailyPassengersByOriginAndDay(terminal.toUpperCase, startDate(numDays), numDays - 1, passengersActor))
            }
        }
      case _ => complete("Hmm")
    })

  private def startDate(numDays: Int) = {
    val today = SDate(localNow.fullYear, localNow.month, localNow.date, 0, 0)
    today.addDays(-1 * (numDays - 1))
  }

  private def localNow = {
    SDate.now(DateTimeZone.forID("Europe/London"))
  }

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
                                                  numberOfDays: Int,
                                                  passengersActor: AskableActorRef)
                                                 (implicit timeout: Timeout): Source[String, NotUsed] =
    Source(0 to numberOfDays)
      .mapAsync(1) { dayOffset =>
        DailySummaries.dailyPaxCountsForDayAndTerminalByOrigin(terminal, startDate, numberOfDays, sourcesInOrder, dayOffset)
      }
      .mapConcat(identity)
      .mapAsync(1) {
        case (origin, dailyPaxCountsOnDay) if dailyPaxCountsOnDay.dailyPax.isEmpty =>
          Future(s"No daily nos available for $origin on ${dailyPaxCountsOnDay.day.toISOString}")
        case (origin, dailyPaxCountsOnDay) =>
          val eventualAck = passengersActor.ask(OriginTerminalDailyPaxCountsOnDay(origin, terminal, dailyPaxCountsOnDay))
            .map(_ => s"Daily pax counts persisted for $origin on ${dailyPaxCountsOnDay.day.toISOString}")

          eventualAck
            .recoverWith {
              case t =>
                log.error("Did not receive ack", t)
                Future("Daily pax counts might not have been persisted. Didn't receive an ack")
            }

          eventualAck
      }
}
