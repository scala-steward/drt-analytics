package uk.gov.homeoffice.drt.analytics

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
import akka.util.{ByteString, Timeout}
import org.slf4j.{Logger, LoggerFactory}
import uk.gov.homeoffice.drt.analytics.actors.PassengersActor
import uk.gov.homeoffice.drt.analytics.time.SDate

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
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

  val passengersActor: AskableActorRef = system.actorOf(Props(new PassengersActor(() => SDate.now(), 30)))

  def routes: Route = concat(
    pathPrefix("daily-pax" / Segments(2)) {
      case List(terminal, numberOfDays) =>
        Try(numberOfDays.toInt) match {
          case Failure(_) =>
            complete("Number of days must be an integer")
          case Success(numDays) =>
            get {
              complete(PaxDeltas.dailyPassengersByOriginAndDayCsv(terminal.toUpperCase, PaxDeltas.startDate(numDays), numDays - 1))
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
              complete(PaxDeltas.updateDailyPassengersByOriginAndDay(terminal.toUpperCase, PaxDeltas.startDate(numDays), numDays - 1, passengersActor))
            }
        }
      case _ => complete("Hmm")
    })

}
