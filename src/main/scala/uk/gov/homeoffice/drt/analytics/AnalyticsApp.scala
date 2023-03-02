package uk.gov.homeoffice.drt.analytics

import akka.Done
import akka.actor.ActorSystem
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.slf4j.{Logger, LoggerFactory}
import uk.gov.homeoffice.drt.actor.WalkTimeProvider
import uk.gov.homeoffice.drt.analytics.prediction.FlightRouteValuesTrainer
import uk.gov.homeoffice.drt.analytics.prediction.FlightsMessageValueExtractor.{minutesOffSchedule, minutesToChox, walkTimeMinutes}
import uk.gov.homeoffice.drt.analytics.prediction.flights.{FlightRoutesValuesExtractor, FlightValueExtractionActor}
import uk.gov.homeoffice.drt.analytics.services.PassengerCounts
import uk.gov.homeoffice.drt.ports.PortCode
import uk.gov.homeoffice.drt.ports.Terminals.{T1, Terminal}
import uk.gov.homeoffice.drt.ports.config.AirportConfigs
import uk.gov.homeoffice.drt.prediction.arrival.{OffScheduleModelAndFeatures, ToChoxModelAndFeatures, WalkTimeModelAndFeatures}
import uk.gov.homeoffice.drt.prediction.persistence.Flight
import uk.gov.homeoffice.drt.protobuf.messages.CrunchState.FlightWithSplitsMessage

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
  val daysOfTrainingData = config.getInt("options.training.days-of-data")

  println(s"Training on $daysOfTrainingData days of data")

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

        case "update-off-schedule-models" =>
          trainModels(OffScheduleModelAndFeatures.targetName, portConfig.terminals, minutesOffSchedule, baselineValue = 0d)

        case "update-to-chox-models" =>
          val baselineTimeToChox = portConfig.timeToChoxMillis / 60000
          trainModels(ToChoxModelAndFeatures.targetName, portConfig.terminals, minutesToChox, baselineTimeToChox)

        case "update-walk-time-models" =>
          /** fix!! - hard coded terminal */
          val baselineWalkTime = portConfig.defaultWalkTimeMillis(T1) / 60000
          println(s"Baseline walk time is $baselineWalkTime minutes")
          val provider = WalkTimeProvider(config.getString("options.walk-time-file-path"))
          log.info(s"Loaded ${provider.walkTimes.size} walk times from ${config.getString("options.walk-time-file-path")}")
          trainModels(WalkTimeModelAndFeatures.targetName, portConfig.terminals, walkTimeMinutes(provider), baselineWalkTime)

        case unknown =>
          log.error(s"Unknown job name '$unknown'")
          Future.successful(Done)
      }

      Await.ready(eventualUpdates, 30 minutes)
      System.exit(0)
  }

  private def trainModels(modelName: String,
                          terminals: Iterable[Terminal],
                          featuresFromMessage: FlightWithSplitsMessage => Option[(Double, Seq[String])],
                          baselineValue: Double): Future[Done] = {
    val examplesProvider = FlightRoutesValuesExtractor(classOf[FlightValueExtractionActor], featuresFromMessage)
      .extractedValueByFlightRoute
    val persistence = Flight()

    FlightRouteValuesTrainer(modelName, examplesProvider, persistence, baselineValue, daysOfTrainingData)
      .trainTerminals(terminals.toList)
  }
}

