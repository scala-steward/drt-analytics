package uk.gov.homeoffice.drt.analytics

import akka.actor.{ActorSystem, Terminated}
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.slf4j.{Logger, LoggerFactory}
import uk.gov.homeoffice.drt.analytics.Routes.passengersActor
import uk.gov.homeoffice.drt.ports.PortCode
import uk.gov.homeoffice.drt.ports.config.AirportConfigs

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.language.postfixOps
import scala.util.{Failure, Success}

object AnalyticsApp extends App {
  val log: Logger = LoggerFactory.getLogger(getClass)
  val config = ConfigFactory.load()

  implicit val system: ActorSystem = ActorSystem("DrtAnalytics")
  implicit val ec: ExecutionContextExecutor = ExecutionContext.global
  implicit val mat: ActorMaterializer = ActorMaterializer()
  implicit val timeout: Timeout = new Timeout(5 seconds)

  val isInteractive = config.getBoolean("interactive-mode")
  val portCode = config.getString("portcode")
  val daysToLookBack = config.getInt("days-to-look-back")

  if (isInteractive) startInteractiveMode else runNonInteractiveMode

  private def runNonInteractiveMode: Any = {
    log.info(s"Starting in non-interactive mode")

    AirportConfigs.confByPort.get(PortCode(portCode.toUpperCase)) match {
      case None =>
        log.error(s"Invalid port code '$portCode''")
        system.terminate()
        System.exit(0)

      case Some(portConfig) =>
        val eventualUpdates = Source(portConfig.terminals.toList)
          .flatMapConcat { terminal =>
            PaxDeltas.updateDailyPassengersByOriginAndDay(terminal.toString.toUpperCase, PaxDeltas.startDate(daysToLookBack), daysToLookBack - 1, passengersActor)
          }
          .runWith(Sink.seq)
          .map(_.foreach(println))
        Await.ready(eventualUpdates, 5 minutes)
        system.terminate()
        System.exit(0)
    }
  }

  private def startInteractiveMode: Terminated = {
    log.info(s"Starting in interactive mode")

    val serverBinding: Future[Http.ServerBinding] = Http().bindAndHandle(Routes.routes, "0.0.0.0", 8081)

    serverBinding.onComplete {
      case Success(bound) =>
        log.info(s"Server online at http://${bound.localAddress.getHostString}:${bound.localAddress.getPort}/")
      case Failure(e) =>
        log.error(s"Server could not start!", e)
        system.terminate()
    }
    Await.result(system.whenTerminated, Duration.Inf)
  }
}
