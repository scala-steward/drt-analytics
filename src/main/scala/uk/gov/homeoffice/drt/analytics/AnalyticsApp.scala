package uk.gov.homeoffice.drt.analytics

import akka.Done
import akka.actor.ActorSystem
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.slf4j.{Logger, LoggerFactory}
import software.amazon.awssdk.services.s3.S3AsyncClient
import uk.gov.homeoffice.drt.actor.TerminalDateActor.ArrivalKey
import uk.gov.homeoffice.drt.analytics.prediction.flights.{FlightValueExtractionActor, ValuesExtractor}
import uk.gov.homeoffice.drt.analytics.prediction.modeldefinitions._
import uk.gov.homeoffice.drt.analytics.prediction.{FlightRouteValuesTrainer, ModelDefinition}
import uk.gov.homeoffice.drt.analytics.s3.Utils
import uk.gov.homeoffice.drt.analytics.services.ArrivalsHelper.{noopPreProcess, populateMaxPax}
import uk.gov.homeoffice.drt.analytics.services.{ModelAccuracy, PaxModelStats}
import uk.gov.homeoffice.drt.arrivals.Arrival
import uk.gov.homeoffice.drt.ports.PortCode
import uk.gov.homeoffice.drt.ports.Terminals.Terminal
import uk.gov.homeoffice.drt.ports.config.AirportConfigs
import uk.gov.homeoffice.drt.prediction.ModelAndFeatures
import uk.gov.homeoffice.drt.prediction.arrival.{ArrivalModelAndFeatures, PaxCapModelAndFeatures}
import uk.gov.homeoffice.drt.prediction.persistence.Flight
import uk.gov.homeoffice.drt.time.{SDate, SDateLike, UtcDate}

import java.nio.file.{Files, Paths}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.language.postfixOps

object AnalyticsApp extends App {
  private val log: Logger = LoggerFactory.getLogger(getClass)
  val config = ConfigFactory.load()

  implicit val system: ActorSystem = ActorSystem("DrtAnalytics")
  implicit val ec: ExecutionContextExecutor = ExecutionContext.global
  implicit val timeout: Timeout = new Timeout(60 seconds)
  implicit val sdateProvider: Long => SDateLike = (ts: Long) => SDate(ts)

  private val portCode = PortCode(config.getString("port-code").toUpperCase)
  private val daysOfTrainingData = config.getInt("options.training.days-of-data")
  implicit val s3AsyncClient: S3AsyncClient = Utils.s3AsyncClient(config.getString("aws.access-key-id"), config.getString("aws.secret-access-key"))
  private val bucketName = config.getString("aws.s3.bucket")

  AirportConfigs.confByPort.get(portCode) match {
    case None =>
      log.error(s"Invalid port code '$portCode'")
      system.terminate()
      System.exit(0)

    case Some(portConfig) =>
      log.info(s"Looking for job ${config.getString("options.job-name")}")
      val eventualUpdates = config.getString("options.job-name").toLowerCase match {
        case "update-off-schedule-models" =>
          trainModels(OffScheduleModelDefinition, portConfig.terminals, noopPreProcess)

        case "update-to-chox-models" =>
          val baselineTimeToChox = portConfig.timeToChoxMillis / 60000
          trainModels(ToChoxModelDefinition(baselineTimeToChox), portConfig.terminals, noopPreProcess)

        case "update-walk-time-models" =>
          val gatesPath = config.getString("options.gates-walk-time-file-path")
          val standsPath = config.getString("options.stands-walk-time-file-path")

          log.info(s"Looking for walk time files $gatesPath and $standsPath")

          val maybeGatesFile = Option(gatesPath).filter(fileExists)
          val maybeStandsFile = Option(standsPath).filter(fileExists)

          log.info(s"Loading walk times from ${maybeGatesFile.toList ++ maybeStandsFile.toList}")
          trainModels(WalkTimeModelDefinition(maybeGatesFile, maybeStandsFile, portConfig.defaultWalkTimeMillis), portConfig.terminals, noopPreProcess)

        case "update-pax-cap-models" =>
          trainModels(PaxCapModelDefinition, portConfig.terminals, populateMaxPax()).flatMap { _ =>
            ModelAccuracy.analyse(daysOfTrainingData, portCode.iata, portConfig.terminals, paxCapModelCollector, bucketName)
          }

        case "dump-daily-pax-cap" =>
          ModelAccuracy.analyse(daysOfTrainingData, portCode.iata, portConfig.terminals, paxCapModelCollector, bucketName)

        case unknown =>
          log.error(s"Unknown job name '$unknown'")
          Future.successful(Done)
      }

      Await.ready(eventualUpdates, 60 minutes)
      System.exit(0)
  }

  private def paxCapModelCollector: Iterable[ModelAndFeatures] => Iterable[ArrivalModelAndFeatures] = _.collect {
    case m: PaxCapModelAndFeatures => m
  }

  private def fileExists(path: String): Boolean = path.nonEmpty && Files.exists(Paths.get(path))

  private def trainModels[T](modDef: ModelDefinition[T, Terminal],
                             terminals: Iterable[Terminal],
                             preProcess: (UtcDate, Map[ArrivalKey, Arrival]) => Future[Map[ArrivalKey, Arrival]]
                            ): Future[Done] = {
    val examplesProvider = ValuesExtractor(
      classOf[FlightValueExtractionActor],
      modDef.targetValueAndFeatures,
      modDef.aggregateValue,
      preProcess
    ).extractValuesByKey
    val persistence = Flight()

    FlightRouteValuesTrainer(modDef.modelName, modDef.features, examplesProvider, persistence, modDef.baselineValue, daysOfTrainingData)
      .trainTerminals(terminals.toList)
  }
}
