package uk.gov.homeoffice.drt.analytics.services

import akka.Done
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import akka.util.Timeout
import org.slf4j.{Logger, LoggerFactory}
import software.amazon.awssdk.services.s3.S3AsyncClient
import uk.gov.homeoffice.drt.actor.PredictionModelActor
import uk.gov.homeoffice.drt.analytics.s3.Utils
import uk.gov.homeoffice.drt.analytics.services.ArrivalsHelper.populateMaxPax
import uk.gov.homeoffice.drt.arrivals.Arrival
import uk.gov.homeoffice.drt.ports.{ApiFeedSource, LiveFeedSource, MlFeedSource}
import uk.gov.homeoffice.drt.ports.Terminals.Terminal
import uk.gov.homeoffice.drt.prediction.ModelAndFeatures
import uk.gov.homeoffice.drt.prediction.arrival.{ArrivalModelAndFeatures, PaxCapModelAndFeatures}
import uk.gov.homeoffice.drt.prediction.persistence.Flight
import uk.gov.homeoffice.drt.time.{SDate, SDateLike, UtcDate}

import java.io.{File, FileWriter}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object ModelAccuracy {
  private val log: Logger = LoggerFactory.getLogger(getClass)

  def analyse(days: Int,
              port: String,
              terminals: Iterable[Terminal],
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
    val statsStartDate = startDate.addDays((days * 0.8).toInt)
    val persistence = Flight()

    val csvHeader = s"Date,Terminal,Actual pax,Pred pax,Flights,Actual per flight,Predicted per flight,Actual % cap,Pred % cap,Pred diff %,Fcst pax, Fcst % cap, Fcst diff %"

    Source(terminals.toList)
      .mapAsync(1) { terminal =>
        val terminalId = PredictionModelActor.Terminal(terminal.toString)
        persistence.getModels(Seq(PaxCapModelAndFeatures.targetName))(terminalId).map(models => (terminal, models))
      }
      .map { case (terminal, models) =>
        val modelsAndFeatures = collector(models.models.values)
        (terminal, modelsAndFeatures.headOption)
      }
      .collect {
        case (terminal, Some(model)) => (terminal, model)
      }
      .mapAsync(1) { case (terminal, model) =>
        Source((0 until days).toList)
          .mapAsync(1)(day => statsForDate(startDate, terminal, model, day))
          .collect { case (date, predPax, actPax, fcstPax, flightsCount, predPctCap, actPctCap, fcstPctCap) if flightsCount > 0 =>
            val predDiff = (predPax - actPax).toDouble / actPax * 100
            val fcstDiff = (fcstPax - actPax).toDouble / actPax * 100
            val actPaxPerFlight = actPax.toDouble / flightsCount
            val predPaxPerFlight = predPax.toDouble / flightsCount
            val csvRow = f"${date.toISOString},$terminal,$actPax,$predPax,$flightsCount,$actPaxPerFlight%.2f,$predPaxPerFlight%.2f,$actPctCap%.2f,$predPctCap%.2f,$predDiff%.2f,$fcstPax,$fcstPctCap%.2f,$fcstDiff%.2f"
            val maybeStats = if (SDate(date) >= statsStartDate) Option((predPax, fcstPax, actPax)) else None
            (maybeStats, csvRow)
          }
          .runWith(Sink.seq)
          .map { stats =>
            (terminal, stats)
          }
      }
      .mapAsync(1) {
        case (terminal, results) =>
          val csvContent = (csvHeader :: results.map(_._2).toList).mkString("\n")
          val fileWriter = new FileWriter(new File(s"/tmp/pax-forecast-${port}-${terminal}.csv"))
          fileWriter.write(csvContent)
          fileWriter.close()
          Utils.writeToBucket(s3Client, bucketName)(s"analytics/passenger-forecast/$port-$terminal.csv", csvContent)
            .map(_ => (terminal, results))
            .recover {
              case t: Throwable =>
                log.error(s"Failed to write to bucket: ${t.getMessage}")
                (terminal, results)
            }
      }
      .runForeach { case (terminal, results) => logStats(terminal, results) }
  }

  private def statsForDate(startDate: SDateLike,
                           terminal: Terminal,
                           model: ArrivalModelAndFeatures,
                           day: Int,
                          )(
                            implicit ec: ExecutionContext,
                            system: ActorSystem,
                            timeout: Timeout
                          ): Future[(UtcDate, Int, Int, Int, Int, Double, Double, Double)] = {
    val date = startDate.addDays(day).toUtcDate
    val predFn: Arrival => Int = arrival =>
      model
        .updatePrediction(arrival, 0, Option(100), SDate.now())
        .PassengerSources.get(MlFeedSource).flatMap(_.actual)
        .getOrElse {
          log.warn(s"Failed to get prediction for $arrival. Using 175")
          175
        }

    PaxModelStats.arrivalsForDate(date, terminal, populateMaxPax, expectedFeeds = List(ApiFeedSource, LiveFeedSource))
      .map(_.filter(!_.Origin.isDomesticOrCta))
      .flatMap {
        arrivals =>
          PaxModelStats.arrivalsForDate(date, terminal, populateMaxPax, Option(7), List())
            .map(_.filter(!_.Origin.isDomesticOrCta))
            .map {
              fArrivals =>
                val liveUniques = arrivals.map(_.unique)
                val fUniques = fArrivals.map(_.unique)
                val liveArrivals = arrivals.filter(a => fUniques.contains(a.unique))
                val forecastArrivals = fArrivals.filter(a => liveUniques.contains(a.unique))
                if (liveArrivals.length != forecastArrivals.length) {
                  log.error(s"Got ${liveArrivals.length} liveArrivals and ${forecastArrivals.length} fcst arrivals for $date. Skipping")
                  None
                } else {
                  val predPax = PaxModelStats.sumPredPaxForDate(liveArrivals, predFn)
                  val actPax = PaxModelStats.sumActPaxForDate(liveArrivals)
                  val fcstPax = PaxModelStats.sumActPaxForDate(forecastArrivals)
                  val predPctCap = PaxModelStats.sumPredPctCapForDate(liveArrivals, predFn)
                  val actPctCap = PaxModelStats.sumActPctCapForDate(liveArrivals)
                  val fcstPctCap = PaxModelStats.sumActPctCapForDate(forecastArrivals)
                  val flightsCount = liveArrivals.length
                  Option((date, predPax, actPax, fcstPax, flightsCount, predPctCap, actPctCap, fcstPctCap))
                }
            }
            .collect {
              case Some(stats) => stats
            }
      }
  }

  private def logStats(terminal: Terminal, results: Seq[(Option[(Int, Int, Int)], String)]): Unit = {
    val (minP: Double, maxP: Double, meanPaxP: Int, rmsePercentP: Double) = getStats(results.collect { case (Some((predPax, _, actPax)), _) =>
      (predPax, actPax)
    })
    val (minF: Double, maxF: Double, _: Int, rmsePercentF: Double) = getStats(results.collect { case (Some((_, fcstPax, actPax)), _) =>
      (fcstPax, actPax)
    })
    log.info(f"Accuracy: Terminal $terminal: Mean pax: $meanPaxP, RMSE: $rmsePercentP%.1f%% vs $rmsePercentF%.1f%%, min: $minP%.1f%% vs $minF%.1f%%, max: $maxP%.1f%% vs $maxF%.1f%%")
  }

  private def getStats(results: Seq[(Int, Int)]): (Double, Double, Int, Double) = {
    val diffs = results.map { case (guessPax, actPax) =>
      guessPax - actPax
    }
    val minDiff = results.minBy { case (guessPax, actPax) =>
      guessPax - actPax
    }
    val maxDiff = results.maxBy { case (guessPax, actPax) =>
      guessPax - actPax
    }
    val min = (minDiff._1 - minDiff._2).toDouble / minDiff._2 * 100
    val max = (maxDiff._1 - maxDiff._2).toDouble / maxDiff._2 * 100
    val rmse = Try(Math.sqrt(diffs.map(d => (d * d).toLong).sum / diffs.length)) match {
      case Success(r) => r
      case Failure(t) =>
        log.error(s"Failed to calculate RMSE", t)
        0.0
    }
    val meanPax = Try(results.map(_._2).sum / results.length) match {
      case Success(r) => r
      case Failure(t) =>
        log.error(s"Failed to calculate mean pax", t)
        0
    }
    val rmsePercent = Try(rmse / meanPax * 100) match {
      case Success(r) => r
      case Failure(t) =>
        log.error(s"Failed to calculate RMSE percent: $rmse, $meanPax", t)
        0.0
    }
    (min, max, meanPax, rmsePercent)
  }
}
