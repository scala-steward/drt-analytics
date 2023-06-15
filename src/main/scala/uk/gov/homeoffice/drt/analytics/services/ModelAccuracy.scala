package uk.gov.homeoffice.drt.analytics.services

import akka.Done
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import akka.util.Timeout
import org.slf4j.{Logger, LoggerFactory}
import software.amazon.awssdk.services.s3.S3AsyncClient
import uk.gov.homeoffice.drt.actor.PredictionModelActor
import uk.gov.homeoffice.drt.analytics.prediction.modeldefinitions.PaxModelStatsLike
import uk.gov.homeoffice.drt.analytics.s3.Utils
import uk.gov.homeoffice.drt.analytics.services.ArrivalsHelper.populateMaxPax
import uk.gov.homeoffice.drt.arrivals.Arrival
import uk.gov.homeoffice.drt.ports.{ApiFeedSource, LiveFeedSource}
import uk.gov.homeoffice.drt.ports.Terminals.Terminal
import uk.gov.homeoffice.drt.prediction.ModelAndFeatures
import uk.gov.homeoffice.drt.prediction.arrival.ArrivalModelAndFeatures
import uk.gov.homeoffice.drt.prediction.persistence.Flight
import uk.gov.homeoffice.drt.time.{SDate, SDateLike, UtcDate}

import scala.concurrent.{ExecutionContext, Future}

object ModelAccuracy {
  private val log: Logger = LoggerFactory.getLogger(getClass)

  def analyse(days: Int,
              port: String,
              terminals: Iterable[Terminal],
              statsHelper: PaxModelStatsLike,
              collector: Iterable[ModelAndFeatures] => Iterable[ArrivalModelAndFeatures],
              bucketName: String,
             )
             (implicit
              system: ActorSystem,
              executionContext: ExecutionContext,
              timeout: Timeout,
              s3Client: S3AsyncClient
             ): Future[Done] = {
    val startDate = SDate.now().addDays(-days)
    val persistence = Flight()

    val csvHeader = s"Date,Terminal,Actual pax,Pred pax,Flights,Actual per flight,Predicted per flight,Actual % cap,Pred % cap,Pred diff %,Fcst pax, Fcst % cap, Fcst diff %"

    Source(terminals.toList)
      .mapAsync(1) { terminal =>
        val terminalId = PredictionModelActor.Terminal(terminal.toString)
        persistence.getModels(terminalId).map(models => (terminal, models))
      }
      .map { case (terminal, models) =>
        val modelsAndFeatures = collector(models.models.values)
        (terminal, modelsAndFeatures)
      }
      .collect {
        case (terminal, models) =>
          val model = models.head
          (terminal, model)
      }
      .mapAsync(1) { case (terminal, model) =>
        Source((0 to days).toList)
          .mapAsync(1) { daysAgo =>
            statsForDate(statsHelper, startDate, terminal, model, daysAgo)
          }
          .collect { case (date, predPax, actPax, fcstPax, flightsCount, predPctCap, actPctCap, fcstPctCap) if flightsCount > 0 =>
            val predDiff = (predPax - actPax).toDouble / actPax * 100
            val fcstDiff = (fcstPax - actPax).toDouble / actPax * 100
            val actPaxPerFlight = actPax.toDouble / flightsCount
            val predPaxPerFlight = predPax.toDouble / flightsCount
            val csvRow = f"${date.toISOString},$terminal,$actPax,$predPax,$flightsCount,$actPaxPerFlight%.2f,$predPaxPerFlight%.2f,$actPctCap%.2f,$predPctCap%.2f,$predDiff%.2f,$fcstPax,$fcstPctCap%.2f,$fcstDiff%.2f"
            (predPax, actPax, csvRow)
          }
          .runWith(Sink.seq)
          .map { stats =>
            (terminal, stats)
          }
      }
      .mapAsync(1) {
        case (terminal, results) =>
          val csvContent = (csvHeader :: results.map(_._3).toList).mkString("\n")
          Utils.writeToBucket(bucketName, s"analytics/passenger-forecast/$port-$terminal.csv", csvContent)
            .map(_ => (terminal, results))
      }
      .runForeach { case (terminal, results) => logStats(terminal, results) }
  }

  private def statsForDate(stats: PaxModelStatsLike,
                           startDate: SDateLike,
                           terminal: Terminal,
                           model: ArrivalModelAndFeatures,
                           daysAgo: Int,
                          )(
                            implicit ec: ExecutionContext,
                            system: ActorSystem,
                            timeout: Timeout
                          ): Future[(UtcDate, Int, Int, Int, Int, Double, Double, Double)] = {
    val date = startDate.addDays(daysAgo).toUtcDate
    val predFn: Arrival => Int = stats.predictionForArrival(model)

    stats.arrivalsForDate(date, terminal, populateMaxPax, expectedFeeds = List(ApiFeedSource, LiveFeedSource)).map(_.filter(!_.Origin.isDomesticOrCta)).flatMap {
      arrivals =>
        stats.arrivalsForDate(date, terminal, populateMaxPax, Option(7), List()).map(_.filter(!_.Origin.isDomesticOrCta)).map {
          fArrivals =>
            val predPax = stats.sumPredPaxForDate(arrivals, predFn)
            val actPax = stats.sumActPaxForDate(arrivals)
            val fcstPax = stats.sumActPaxForDate(fArrivals)
            val predPctCap = stats.sumPredPctCapForDate(arrivals, predFn)
            val actPctCap = stats.sumActPctCapForDate(arrivals)
            val fcstPctCap = stats.sumActPctCapForDate(fArrivals)
            val flightsCount = arrivals.length
            (date, predPax, actPax, fcstPax, flightsCount, predPctCap, actPctCap, fcstPctCap)
        }
    }
  }

  private def logStats(terminal: Terminal, results: Seq[(Int, Int, String)]): Unit = {
    val diffs = results.map { case (predPax, actPax, _) =>
      predPax - actPax
    }
    val minDiff = results.minBy { case (predPax, actPax, _) =>
      predPax - actPax
    }
    val maxDiff = results.maxBy { case (predPax, actPax, _) =>
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
