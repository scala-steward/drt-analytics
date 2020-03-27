package uk.gov.homeoffice.drt.analytics

import akka.actor.{ActorSystem, Terminated}
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.slf4j.{Logger, LoggerFactory}
import uk.gov.homeoffice.drt.analytics.Routes.passengersActor

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

    Ports.terminals.get(portCode) match {
      case None =>
        println(s"Invalid port code '$portCode''")
      case Some(terminals) =>
        Source(terminals)
          .flatMapConcat { terminal =>
            PaxDeltas.updateDailyPassengersByOriginAndDay(terminal.toUpperCase, PaxDeltas.startDate(daysToLookBack), daysToLookBack - 1, passengersActor)
          }
          .runWith(Sink.seq)
          .map(_.foreach(println))
          .onComplete(_ => system.terminate().onComplete(_ => System.exit(0)))
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

object Ports {
  val terminals = Map(
    "bfs" -> List("t1"),
    "bhd" -> List("t1"),
    "bhx" -> List("t1", "t2"),
    "brs" -> List("t1"),
    "edi" -> List("t1"),
    "ema" -> List("a1", "a2"),
    "gla" -> List("t1"),
    "lcy" -> List("t1"),
    "lgw" -> List("n", "s"),
    "lhr" -> List("t2", "t3", "t4", "t5"),
    "lpl" -> List("t1"),
    "ltn" -> List("t1"),
    "man" -> List("t1", "t2", "t3"),
    "ncl" -> List("t1"),
    "stn" -> List("t1")
    )
}
