package uk.gov.homeoffice.drt.analytics

import akka.Done
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.slf4j.{Logger, LoggerFactory}
import uk.gov.homeoffice.drt.actor.PredictionModelActor
import uk.gov.homeoffice.drt.analytics.prediction.flights.{FlightValueExtractionActor, ValuesExtractor}
import uk.gov.homeoffice.drt.analytics.prediction.modeldefinitions._
import uk.gov.homeoffice.drt.analytics.prediction.{FlightRouteValuesTrainer, ModelDefinition}
import uk.gov.homeoffice.drt.analytics.services.PassengerCounts
import uk.gov.homeoffice.drt.arrivals.Arrival
import uk.gov.homeoffice.drt.ports.PortCode
import uk.gov.homeoffice.drt.ports.Terminals.Terminal
import uk.gov.homeoffice.drt.ports.config.AirportConfigs
import uk.gov.homeoffice.drt.prediction.ModelAndFeatures
import uk.gov.homeoffice.drt.prediction.arrival.{ArrivalModelAndFeatures, PaxCapModelAndFeatures, PaxModelAndFeatures}
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

  AirportConfigs.confByPort.get(portCode) match {
    case None =>
      log.error(s"Invalid port code '$portCode'")
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
          val standsPath = config.getString("options.stands-walk-time-file-path")

          log.info(s"Looking for walk time files $gatesPath and $standsPath")

          val maybeGatesFile = Option(gatesPath).filter(fileExists)
          val maybeStandsFile = Option(standsPath).filter(fileExists)

          log.info(s"Loading walk times from ${maybeGatesFile.toList ++ maybeStandsFile.toList}")
          trainModels(WalkTimeModelDefinition(maybeGatesFile, maybeStandsFile, portConfig.defaultWalkTimeMillis), portConfig.terminals)

        case "update-pax-models" =>
          trainModels(PaxModelDefinition, portConfig.terminals).flatMap { _ =>
            dumpDailyPax(150, portConfig.terminals, PaxModelStats, paxModelCollector)
          }

        case "update-pax-cap-models" =>
          trainModels(PaxCapModelDefinition, portConfig.terminals).flatMap { _ =>
            dumpDailyPax(150, portConfig.terminals, PaxCapModelStats, paxCapModelCollector)
          }

        case "dump-daily-pax" =>
          dumpDailyPax(daysOfTrainingData, portConfig.terminals, PaxModelStats, paxModelCollector)

        case "dump-daily-pax-cap" =>
          dumpDailyPax(daysOfTrainingData, portConfig.terminals, PaxCapModelStats, paxCapModelCollector)

        case unknown =>
          log.error(s"Unknown job name '$unknown'")
          Future.successful(Done)
      }

      Await.ready(eventualUpdates, 30 minutes)
      System.exit(0)
  }

  def paxModelCollector: Iterable[ModelAndFeatures] => Iterable[ArrivalModelAndFeatures] = _.collect {
    case m: PaxModelAndFeatures => m
  }
  def paxCapModelCollector: Iterable[ModelAndFeatures] => Iterable[ArrivalModelAndFeatures] = _.collect {
    case m: PaxCapModelAndFeatures => m
  }

  private def fileExists(path: String): Boolean = path.nonEmpty && Files.exists(Paths.get(path))

  private def trainModels[T](modDef: ModelDefinition[T, Terminal], terminals: Iterable[Terminal]): Future[Done] = {
    val examplesProvider = ValuesExtractor(classOf[FlightValueExtractionActor], modDef.targetValueAndFeatures, modDef.aggregateValue).extractValuesByKey
    val persistence = Flight()

    FlightRouteValuesTrainer(modDef.modelName, modDef.features, examplesProvider, persistence, modDef.baselineValue, daysOfTrainingData)
      .trainTerminals(terminals.toList)
  }

  private def dumpDailyPax(days: Int, terminals: Iterable[Terminal], stats: PaxModelStatsLike, collector: Iterable[ModelAndFeatures] => Iterable[ArrivalModelAndFeatures]): Future[Done] = {
    val startDate = SDate.now().addDays(-days)
    val persistence = Flight()

    println(s"Date,Holiday,Terminal,Actual Pax,Flights,Pax per flight,Predicted Pax,Pred per flight,Diff %,% Cap")

    Source(terminals.toList)
      .mapAsync(1) { terminal =>
        val terminalId = PredictionModelActor.Terminal(terminal.toString)
        persistence.getModels(terminalId).map(models => (terminal, models))
      }
      .map { case (terminal, models) =>
        (terminal, collector(models.models.values))
      }
      .collect {
        case (terminal, model) => (terminal, model.head)
      }
      .mapAsync(1) { case (terminal, model) =>
        Source((0 to days).toList)
          .mapAsync(1) { daysAgo =>
            val date = startDate.addDays(daysAgo).toUtcDate
            val predFn: Arrival => Int = stats.predictionForArrival(model)

            stats.arrivalsForDate(date, terminal).map(_.filter(!_.Origin.isDomesticOrCta)).map {
              arrivals =>
                val predPax = stats.sumPredPaxForDate(arrivals, predFn)
                val actPax = stats.sumActPaxForDate(arrivals)
                val flightsCount = arrivals.length
                (predPax, actPax, flightsCount)
            }
          }
          .collect { case (predPax, actPax, flightsCount) if flightsCount > 0 =>
            (predPax, actPax)
          }
          .runWith(Sink.seq)
          .map { stats =>
            (terminal, stats)
          }
      }
      .runForeach { case (terminal, results) =>
        val diffs = results.map { case (predPax, actPax) =>
          predPax - actPax
        }
        val minDiff = results.minBy { case (predPax, actPax) =>
          predPax - actPax
        }
        val maxDiff = results.maxBy { case (predPax, actPax) =>
          predPax - actPax
        }
        val min = (minDiff._1 - minDiff._2).toDouble / minDiff._2 * 100
        val max = (maxDiff._1 - maxDiff._2).toDouble / maxDiff._2 * 100
        val rmse = Math.sqrt(diffs.map(d => d * d).sum / diffs.length)
        val meanPax = results.map(_._2).sum / results.length
        val rmsePercent = rmse / meanPax * 100
        log.info(f"Terminal $terminal: Mean daily pax: $meanPax, RMSE: $rmse%.2f ($rmsePercent%.1f%%), min: $min%.1f%%, max: $max%.1f%%")
      }
  }
}
