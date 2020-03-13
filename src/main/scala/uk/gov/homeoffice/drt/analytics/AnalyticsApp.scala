package uk.gov.homeoffice.drt.analytics

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.language.postfixOps
import scala.util.{Failure, Success}

object AnalyticsApp extends App {
  val log: Logger = LoggerFactory.getLogger(getClass)
  implicit val system: ActorSystem = ActorSystem("DrtAnalytics")
  implicit val ec: ExecutionContextExecutor = ExecutionContext.global
  implicit val mat: ActorMaterializer = ActorMaterializer()

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
