package uk.gov.homeoffice.drt.analytics

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.common.{CsvEntityStreamingSupport, EntityStreamingSupport}
import akka.http.scaladsl.marshalling.{Marshaller, Marshalling}
import akka.http.scaladsl.model.ContentTypes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.directives.MethodDirectives.get
import akka.http.scaladsl.server.directives.RouteDirectives.complete
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.util.ByteString
import org.joda.time.DateTimeZone
import org.slf4j.{Logger, LoggerFactory}
import uk.gov.homeoffice.drt.analytics.actors.FeedPersistenceIds
import uk.gov.homeoffice.drt.analytics.passengers.DailySummaries
import uk.gov.homeoffice.drt.analytics.time.SDate

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success, Try}

object Routes {
  val log: Logger = LoggerFactory.getLogger(getClass)
  implicit val system: ActorSystem = ActorSystem("DrtAnalytics")
  implicit val ec: ExecutionContextExecutor = ExecutionContext.global
  implicit val mat: ActorMaterializer = ActorMaterializer()

  implicit val stringAsCsv: Marshaller[String, ByteString] = Marshaller.strict[String, ByteString] { t =>
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
    case _ => complete("Hmm")
  }

  private def dailyPassengersCsv(terminal: String, startDate: SDate, numberOfDays: Int): Source[String, NotUsed] = {
    val sourcesInOrder = List(FeedPersistenceIds.forecastBase, FeedPersistenceIds.forecast, FeedPersistenceIds.live)

    val header = "Date,Terminal,Origin," + (0 to numberOfDays).map { offset => startDate.addDays(offset).toISODateOnly }.mkString(",")

    val eventualSummaries: Source[String, NotUsed] = Source(0 to numberOfDays)
      .mapAsync(1) { dayOffset =>
        val viewDate = startDate.addDays(dayOffset)
        val lastDate = startDate.addDays(numberOfDays)
        val eventualsBySource = DailySummaries.arrivalsForSources(sourcesInOrder, viewDate, lastDate)
        val eventualMergedArrivals = DailySummaries.mergeArrivals(eventualsBySource)

        DailySummaries.summary(viewDate, startDate, numberOfDays, terminal, eventualMergedArrivals)
          .map { row =>
            if (dayOffset == 0) List(header, row) else List(row)
          }
          .recoverWith { case t =>
            log.error(s"Failed to get summaries", t)
            Future(List())
          }
      }
      .mapConcat(identity)

    eventualSummaries
  }
}
