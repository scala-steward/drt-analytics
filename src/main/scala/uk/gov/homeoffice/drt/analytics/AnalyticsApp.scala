package uk.gov.homeoffice.drt.analytics

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.util.Timeout
import akka.{Done, NotUsed}
import com.typesafe.config.ConfigFactory
import org.slf4j.{Logger, LoggerFactory}
import uk.gov.homeoffice.drt.actor.PredictionModelActor.{TerminalCarrierOrigin, TerminalFlightNumberOrigin, WithId}
import uk.gov.homeoffice.drt.actor.WalkTimeProvider
import uk.gov.homeoffice.drt.analytics.prediction.FlightRouteValuesTrainer
import uk.gov.homeoffice.drt.analytics.prediction.FlightsMessageValueExtractor.{minutesOffSchedule, minutesToChox, walkTimeMinutes}
import uk.gov.homeoffice.drt.analytics.prediction.flights.{FlightValueExtractionActor, ValuesExtractor}
import uk.gov.homeoffice.drt.analytics.services.PassengerCounts
import uk.gov.homeoffice.drt.ports.PortCode
import uk.gov.homeoffice.drt.ports.Terminals.Terminal
import uk.gov.homeoffice.drt.ports.config.AirportConfigs
import uk.gov.homeoffice.drt.prediction.arrival.{OffScheduleModelAndFeatures, ToChoxModelAndFeatures, WalkTimeModelAndFeatures}
import uk.gov.homeoffice.drt.prediction.persistence.Flight
import uk.gov.homeoffice.drt.protobuf.messages.CrunchState.FlightWithSplitsMessage
import uk.gov.homeoffice.drt.time.SDateLike

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
          trainModels(
            OffScheduleModelAndFeatures.targetName,
            portConfig.terminals,
            minutesOffSchedule,
            TerminalFlightNumberOrigin.fromMessage,
            baselineValue = _ => 0d)

        case "update-to-chox-models" =>
          val baselineTimeToChox = portConfig.timeToChoxMillis / 60000
          trainModels(
            ToChoxModelAndFeatures.targetName,
            portConfig.terminals,
            minutesToChox,
            TerminalFlightNumberOrigin.fromMessage,
            _ => baselineTimeToChox)

        case "update-walk-time-models" =>
          val baselineWalkTimeSeconds: Terminal => Double = (t: Terminal) => portConfig.defaultWalkTimeMillis.get(t).map(_.toDouble / 1000).getOrElse(0)
          val gatesPath = config.getString("options.gates-walk-time-file-path")
          val maybeGatesFile = if (gatesPath.nonEmpty) Option(gatesPath) else None
          val standsPath = config.getString("options.stands-walk-time-file-path")
          val maybeStandsFile = if (standsPath.nonEmpty) Option(standsPath) else None
          val provider = WalkTimeProvider(maybeGatesFile, maybeStandsFile)

          log.info(s"Loaded walk times from $maybeGatesFile and $maybeStandsFile")
          trainModels(
            WalkTimeModelAndFeatures.targetName,
            portConfig.terminals,
            walkTimeMinutes(provider),
            TerminalCarrierOrigin.fromMessage,
            baselineWalkTimeSeconds)

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
                          keyFromMessage: FlightWithSplitsMessage => Option[WithId],
                          baselineValue: Terminal => Double): Future[Done] = {
    val examplesProvider: (Terminal, SDateLike, Int) => Source[(WithId, Iterable[(Double, Seq[String])]), NotUsed] =
      ValuesExtractor(classOf[FlightValueExtractionActor], featuresFromMessage, keyFromMessage).extractValuesByKey
    val persistence = Flight()

    FlightRouteValuesTrainer(modelName, examplesProvider, persistence, baselineValue, daysOfTrainingData)
      .trainTerminals(terminals.toList)
  }
}

