package uk.gov.homeoffice.drt.analytics

import akka.Done
import akka.actor.ActorSystem
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.slf4j.{Logger, LoggerFactory}
import uk.gov.homeoffice.drt.analytics.prediction.flights.{FlightValueExtractionActor, ValuesExtractor}
import uk.gov.homeoffice.drt.analytics.prediction.modeldefinitions.{OffScheduleModelDefinition, ToChoxModelDefinition, WalkTimeModelDefinition}
import uk.gov.homeoffice.drt.analytics.prediction.{FlightRouteValuesTrainer, ModelDefinition}
import uk.gov.homeoffice.drt.analytics.services.PassengerCounts
import uk.gov.homeoffice.drt.ports.PortCode
import uk.gov.homeoffice.drt.ports.Terminals.Terminal
import uk.gov.homeoffice.drt.ports.config.AirportConfigs
import uk.gov.homeoffice.drt.prediction.persistence.Flight
import uk.gov.homeoffice.drt.time.{SDate, SDateLike}

import java.nio.file.{Files, Paths}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.language.postfixOps

object AnalyticsApp extends App {
  private val log: Logger = LoggerFactory.getLogger(getClass)
  val config = ConfigFactory.load()

  implicit val system: ActorSystem = ActorSystem("DrtAnalytics")
  implicit val ec: ExecutionContextExecutor = ExecutionContext.global
  implicit val timeout: Timeout = new Timeout(5 seconds)
  implicit val sdateProvider: Long => SDateLike = (ts: Long) => SDate(ts)

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
          trainModels(OffScheduleModelDefinition, portConfig.terminals)

        case "update-to-chox-models" =>
          val baselineTimeToChox = portConfig.timeToChoxMillis / 60000
          trainModels(ToChoxModelDefinition(baselineTimeToChox), portConfig.terminals)

        case "update-walk-time-models" =>
          val gatesPath = config.getString("options.gates-walk-time-file-path")
          val maybeGatesFile = Option(gatesPath).filter(fileExists)
          val standsPath = config.getString("options.stands-walk-time-file-path")
          val maybeStandsFile = Option(standsPath).filter(fileExists)

          log.info(s"Loading walk times from ${maybeGatesFile.toList ++ maybeStandsFile.toList}")
          trainModels(WalkTimeModelDefinition(maybeGatesFile, maybeStandsFile, portConfig.defaultWalkTimeMillis), portConfig.terminals)

        case unknown =>
          log.error(s"Unknown job name '$unknown'")
          Future.successful(Done)
      }

      Await.ready(eventualUpdates, 30 minutes)
      System.exit(0)
  }

  private def fileExists(path: String): Boolean = Files.exists(Paths.get(path))

  private def trainModels[T](modDef: ModelDefinition[T, Terminal], terminals: Iterable[Terminal]): Future[Done] = {
    val examplesProvider = ValuesExtractor(classOf[FlightValueExtractionActor], modDef.targetValueAndFeatures, modDef.aggregateValue).extractValuesByKey
    val persistence = Flight()

    FlightRouteValuesTrainer(modDef.modelName, modDef.features, examplesProvider, persistence, modDef.baselineValue, daysOfTrainingData)
      .trainTerminals(terminals.toList)
  }
}
