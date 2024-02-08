package uk.gov.homeoffice.drt.analytics.services

import akka.Done
import akka.actor.ActorSystem
import akka.util.Timeout
import com.typesafe.config.Config
import org.slf4j.{Logger, LoggerFactory}
import uk.gov.homeoffice.drt.actor.TerminalDateActor.ArrivalKey
import uk.gov.homeoffice.drt.analytics.prediction.dump.{ModelPredictionsDump, NoOpDump, PaxPredictionDump}
import uk.gov.homeoffice.drt.analytics.prediction.flights.{ArrivalsProvider, FlightValueExtractionActor, ValuesExtractor}
import uk.gov.homeoffice.drt.analytics.prediction.modeldefinitions.{OffScheduleModelDefinition, PaxCapModelDefinition, ToChoxModelDefinition, WalkTimeModelDefinition}
import uk.gov.homeoffice.drt.analytics.prediction.{FlightRouteValuesTrainer, ModelDefinition}
import uk.gov.homeoffice.drt.analytics.services.ArrivalsHelper.{noopPreProcess, populateMaxPax}
import uk.gov.homeoffice.drt.arrivals.Arrival
import uk.gov.homeoffice.drt.ports.Terminals.Terminal
import uk.gov.homeoffice.drt.ports.{AirportConfig, PortCode}
import uk.gov.homeoffice.drt.prediction.ModelPersistence
import uk.gov.homeoffice.drt.time.UtcDate

import java.nio.file.{Files, Paths}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

case class JobExecutor(config: Config,
                       portCode: PortCode, maybeWritePredictions: Try[(String, String) => Future[Done]],
                       persistence: ModelPersistence,
                      )
                      (implicit ec: ExecutionContext, timeout: Timeout, system: ActorSystem) {
  private val log: Logger = LoggerFactory.getLogger(getClass)

  private val daysOfTrainingData = config.getInt("options.training.days-of-data")

  def executeJob(portConfig: AirportConfig, jobName: String): Future[Done] = {
    jobName match {
      case "update-pax-counts" =>
        val daysToLookBack = config.getInt("days-to-look-back")
        PassengerCounts.updateForPort(portConfig, daysToLookBack)

      case "update-off-schedule-models" =>
        trainModels(OffScheduleModelDefinition, portCode.iata, portConfig.terminals, noopPreProcess, 0.1, 0.9, NoOpDump)

      case "update-to-chox-models" =>
        val baselineTimeToChox = portConfig.timeToChoxMillis / 60000
        trainModels(ToChoxModelDefinition(baselineTimeToChox), portCode.iata, portConfig.terminals, noopPreProcess, 0.1, 0.9, NoOpDump)

      case "update-walk-time-models" =>
        val gatesPath = config.getString("options.gates-walk-time-file-path")
        val standsPath = config.getString("options.stands-walk-time-file-path")

        log.info(s"Looking for walk time files $gatesPath and $standsPath")

        val maybeGatesFile = Option(gatesPath).filter(fileExists)
        val maybeStandsFile = Option(standsPath).filter(fileExists)

        log.info(s"Loading walk times from ${maybeGatesFile.toList ++ maybeStandsFile.toList}")
        val modelDef = WalkTimeModelDefinition(maybeGatesFile, maybeStandsFile, portConfig.defaultWalkTimeMillis)
        trainModels(modelDef, portCode.iata, portConfig.terminals, noopPreProcess, 0.1, 0.9, NoOpDump)

      case "update-pax-cap-models" =>
        val writePredictions = maybeWritePredictions.getOrElse((_: String, _: String) => Future.successful(Done))
        val paxPredictionsDumper = PaxPredictionDump(ArrivalsProvider().arrivals, writePredictions)
        trainModels(PaxCapModelDefinition, portCode.iata, portConfig.terminals, populateMaxPax(), 0d, 1d, paxPredictionsDumper)

      case unknown =>
        log.error(s"Unknown job name '$unknown'")
        Future.successful(Done)
    }
  }

  private def fileExists(path: String): Boolean = path.nonEmpty && Files.exists(Paths.get(path))

  private def trainModels[T](modDef: ModelDefinition[T, Terminal],
                             portCode: String,
                             terminals: Iterable[Terminal],
                             preProcess: (UtcDate, Iterable[Arrival]) => Future[Iterable[Arrival]],
                             lowerQuantile: Double,
                             upperQuantile: Double,
                             dumpStats: ModelPredictionsDump,
                            ): Future[Done] = {
    val examplesProvider = ValuesExtractor(
      classOf[FlightValueExtractionActor],
      modDef.targetValueAndFeatures,
      modDef.aggregateValue,
      preProcess
    ).extractValuesByKey

    val trainer = FlightRouteValuesTrainer(
      modelName = modDef.modelName,
      features = modDef.features,
      examplesProvider = examplesProvider,
      baselineValue = modDef.baselineValue,
      daysOfTrainingData = daysOfTrainingData,
      lowerQuantile = lowerQuantile,
      upperQuantile = upperQuantile,
      persistence = persistence,
      dumper = dumpStats,
    )

    trainer
      .trainTerminals(portCode, terminals.toList)
      .map { d =>
        trainer.session.stop()
        d
      }
  }


}
