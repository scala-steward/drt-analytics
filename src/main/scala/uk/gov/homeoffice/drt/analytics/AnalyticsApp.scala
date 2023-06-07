package uk.gov.homeoffice.drt.analytics

import akka.Done
import akka.actor.ActorSystem
import akka.pattern.ask
import akka.stream.scaladsl.{Sink, Source}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.slf4j.{Logger, LoggerFactory}
import uk.gov.homeoffice.drt.actor.PredictionModelActor
import uk.gov.homeoffice.drt.actor.TerminalDateActor.ArrivalKey
import uk.gov.homeoffice.drt.analytics.actors.{ArrivalsActor, FeedPersistenceIds, GetArrivals}
import uk.gov.homeoffice.drt.analytics.prediction.flights.{FlightValueExtractionActor, ValuesExtractor}
import uk.gov.homeoffice.drt.analytics.prediction.modeldefinitions._
import uk.gov.homeoffice.drt.analytics.prediction.{FlightRouteValuesTrainer, ModelDefinition}
//import uk.gov.homeoffice.drt.analytics.services.PassengerCounts
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

  val portCode = PortCode(config.getString("port-code").toUpperCase)
  val daysOfTrainingData = config.getInt("options.training.days-of-data")

  val noopPreProcess: (UtcDate, Map[ArrivalKey, Arrival]) => Future[Map[ArrivalKey, Arrival]] = (_, a) => Future.successful(a)

  val populateMaxPax: (UtcDate, Map[ArrivalKey, Arrival]) => Future[Map[ArrivalKey, Arrival]] = (date, arrivals) => {
    val withMaxPaxPct = (100 * arrivals.values.count(_.MaxPax.isDefined).toDouble / arrivals.size).round
    if (withMaxPaxPct < 80) {
      log.info(s"Only $withMaxPaxPct% of arrivals have max pax for $date, populating")
      val arrivalsActor = system.actorOf(ArrivalsActor.props(FeedPersistenceIds.forecastBase, date))
      arrivalsActor
        .ask(GetArrivals(SDate(date), SDate(date).addDays(1))).mapTo[Arrivals]
        .map { baseArrivals =>
          arrivalsActor ! akka.actor.PoisonPill

          arrivals.view.mapValues {
            arr =>
              val maybeArrival = baseArrivals.arrivals.get(arr.unique)
              val maybeMaxPax = maybeArrival.flatMap(_.maxPax)
              arr.copy(MaxPax = maybeMaxPax)
          }.toMap
        }
        .recover {
          case t =>
            log.error(s"Failed to populate max pax for $date", t)
            arrivals
        }
    } else Future.successful(arrivals)
  }

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
          trainModels(PaxCapModelDefinition, portConfig.terminals, populateMaxPax).flatMap { _ =>
            dumpDailyPax(daysOfTrainingData, portConfig.terminals, PaxCapModelStats, paxCapModelCollector)
          }

        case "dump-daily-pax-cap" =>
          dumpDailyPax(daysOfTrainingData, portConfig.terminals, PaxCapModelStats, paxCapModelCollector)

        case unknown =>
          log.error(s"Unknown job name '$unknown'")
          Future.successful(Done)
      }

      Await.ready(eventualUpdates, 30 minutes)
      System.exit(0)
  }

  def paxCapModelCollector: Iterable[ModelAndFeatures] => Iterable[ArrivalModelAndFeatures] = _.collect {
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

  private def dumpDailyPax(days: Int, terminals: Iterable[Terminal], stats: PaxModelStatsLike, collector: Iterable[ModelAndFeatures] => Iterable[ArrivalModelAndFeatures]): Future[Done] = {
    val startDate = SDate.now().addDays(-days)
    val persistence = Flight()

//    import java.io._
//    val pw = new PrintWriter(new File(s"${portCode.iata.toLowerCase}-pax-cap.csv"))
//    pw.println(s"Date,Ternimal,Actual pax,Pred pax,Flights,Actual per flight,Predicted per flight,Actual % cap,Pred % cap,Diff")

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

            stats.arrivalsForDate(date, terminal, populateMaxPax).map(_.filter(!_.Origin.isDomesticOrCta)).map {
              arrivals =>
                val predPax = stats.sumPredPaxForDate(arrivals, predFn)
                val actPax = stats.sumActPaxForDate(arrivals)
                val predPctCap = stats.sumPredPctCapForDate(arrivals, predFn)
                val actPctCap = stats.sumActPctCapForDate(arrivals)
                val flightsCount = arrivals.length
                (date, predPax, actPax, flightsCount, predPctCap, actPctCap)
            }
          }
          .collect { case (date, predPax, actPax, flightsCount, predPctCap, actPctCap) if flightsCount > 0 =>
            val diff = (predPax - actPax).toDouble / actPax * 100
            val actPaxPerFlight = actPax.toDouble / flightsCount
            val predPaxPerFlight = predPax.toDouble / flightsCount
//            pw.println(f"${date.toISOString},$terminal,$actPax,$predPax,$flightsCount,$actPaxPerFlight%.2f,$predPaxPerFlight%.2f,$actPctCap%.2f,$predPctCap%.2f,$diff%.2f")
            log.info(s"Done $date")
            (predPax, actPax)
          }
          .runWith(Sink.seq)
          .map { stats =>
//            pw.close()
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
