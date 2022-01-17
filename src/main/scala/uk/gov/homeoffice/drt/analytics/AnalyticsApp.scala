package uk.gov.homeoffice.drt.analytics

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.slf4j.{Logger, LoggerFactory}
import uk.gov.homeoffice.drt.analytics.prediction.TouchdownTrainer
import uk.gov.homeoffice.drt.analytics.services.PassengerCounts
import uk.gov.homeoffice.drt.ports.PortCode
import uk.gov.homeoffice.drt.ports.config.AirportConfigs

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor}
import scala.language.postfixOps

object AnalyticsApp extends App {
  private val log: Logger = LoggerFactory.getLogger(getClass)
  val config = ConfigFactory.load()

  implicit val system: ActorSystem = ActorSystem("DrtAnalytics")
  implicit val ec: ExecutionContextExecutor = ExecutionContext.global
  implicit val mat: ActorMaterializer = ActorMaterializer()
  implicit val timeout: Timeout = new Timeout(5 seconds)

  val portCode = PortCode(config.getString("port-code").toUpperCase)
  val daysToLookBack = config.getInt("days-to-look-back")

  AirportConfigs.confByPort.get(portCode) match {
    case None =>
      log.error(s"Invalid port code '$portCode''")
      system.terminate()
      System.exit(0)

    case Some(portConfig) =>
      val eventualUpdates = config.getString("job").toLowerCase match {
        case "update-pax-counts" => PassengerCounts(portConfig, daysToLookBack)
        case "update-touchdown-models" => TouchdownTrainer(portConfig)
      }
      Await.ready(eventualUpdates, 30 minutes)
      system.terminate()
      System.exit(0)
  }
}

