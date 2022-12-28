package uk.gov.homeoffice.drt.analytics

import akka.Done
import akka.actor.ActorSystem
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.slf4j.{Logger, LoggerFactory}
import uk.gov.homeoffice.drt.analytics.prediction.flights.FlightExamplesAndUpdates
import uk.gov.homeoffice.drt.analytics.prediction.{FlightRouteValuesTrainer, FlightsMessageValueExtractor}
import uk.gov.homeoffice.drt.analytics.services.PassengerCounts
import uk.gov.homeoffice.drt.ports.PortCode
import uk.gov.homeoffice.drt.ports.config.AirportConfigs
import uk.gov.homeoffice.drt.prediction.{ToChoxModelAndFeatures, TouchdownModelAndFeatures}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.language.postfixOps

object AnalyticsApp extends App {
  private val log: Logger = LoggerFactory.getLogger(getClass)
  val config = ConfigFactory.load()

  implicit val system: ActorSystem = ActorSystem("DrtAnalytics")
  implicit val ec: ExecutionContextExecutor = ExecutionContext.global
  implicit val timeout: Timeout = new Timeout(5 seconds)

  val portCode = PortCode(config.getString("port-code").toUpperCase)
  val daysToLookBack = config.getInt("days-to-look-back")

  AirportConfigs.confByPort.get(portCode) match {
    case None =>
      log.error(s"Invalid port code '$portCode''")
      system.terminate()
      System.exit(0)

    case Some(portConfig) =>
      log.info(s"Looking for job ${config.getString("options.job-name")}")
      val eventualUpdates = config.getString("options.job-name").toLowerCase match {
        case "update-pax-counts" =>
          PassengerCounts.updateForPort(portConfig, daysToLookBack)

        case "update-touchdown-models" =>
          val examplesAndUpdates = FlightExamplesAndUpdates(FlightsMessageValueExtractor.minutesOffSchedule, TouchdownModelAndFeatures.targetName)
          FlightRouteValuesTrainer(TouchdownModelAndFeatures.targetName, examplesAndUpdates.modelIdWithExamples, examplesAndUpdates.updateModel, 0d)
            .trainTerminals(portConfig.terminals.toList)

        case "update-chox-models" =>
          val examplesAndUpdates = FlightExamplesAndUpdates(FlightsMessageValueExtractor.minutesToChox, ToChoxModelAndFeatures.targetName)
          val baselineTimeToChox = portConfig.timeToChoxMillis / 60000
          println(s"baseline time to chox: $baselineTimeToChox")
          FlightRouteValuesTrainer(ToChoxModelAndFeatures.targetName, examplesAndUpdates.modelIdWithExamples, examplesAndUpdates.updateModel, baselineTimeToChox)
            .trainTerminals(portConfig.terminals.toList)

        case unknown =>
          log.error(s"Unknown job name '$unknown'")
          Future.successful(Done)
      }

      Await.ready(eventualUpdates, 30 minutes)
      System.exit(0)
  }
}

