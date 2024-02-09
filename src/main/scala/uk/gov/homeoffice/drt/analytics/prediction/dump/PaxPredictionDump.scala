package uk.gov.homeoffice.drt.analytics.prediction.dump

import akka.Done
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTimeZone
import org.slf4j.LoggerFactory
import uk.gov.homeoffice.drt.analytics.prediction.DataSet
import uk.gov.homeoffice.drt.arrivals.Arrival
import uk.gov.homeoffice.drt.ports.Terminals.Terminal
import uk.gov.homeoffice.drt.ports._
import uk.gov.homeoffice.drt.time.{LocalDate, SDate}

import scala.concurrent.{ExecutionContext, Future}

case class PaxPredictionDump(arrivalsForDate: (Terminal, LocalDate) => Future[Seq[Arrival]],
                             predictionWriters: Iterable[(String, String) => Future[Done]],
                            )
                            (implicit ec: ExecutionContext, mat: Materializer) extends ModelPredictionsDump {
  private val log = LoggerFactory.getLogger(getClass)

  override def dumpDailyStats(dataSet: DataSet,
                              withIndex: Iterable[(Double, Seq[String], Seq[Double], String)],
                              model: LinearRegressionModel,
                              port: String,
                              terminal: String,
                             )
                             (implicit sparkSession: SparkSession): Future[Done] = {
    log.info(s"Dumping daily stats for $terminal")
    val csvHeader = Seq(
      "Date",
      "Terminal",
      "Actual pax",
      "Pred pax",
      "Flights",
      "Actual per flight",
      "Predicted per flight",
      "Actual % cap",
      "Pred % cap",
      "Pred diff %",
      "Fcst pax",
      "Fcst % cap",
      "Fcst diff %").mkString(",")

    val predictions = predictionsWithLabels(dataSet, withIndex, model)

    capacityStats(predictions, Terminal(terminal), arrivalsForDate)
      .flatMap { stats =>
        log.info(s"Got ${stats.size} days of stats for $terminal")
        val csvContent = stats
          .foldLeft(csvHeader + "\n") {
            case (acc, (date, actPax, predPax, fcstPax, actCap, predCap, fcstCapPct, flightCount)) =>
              val predDiff = (predPax - actPax).toDouble / actPax * 100
              val fcstDiff = (fcstPax - actPax).toDouble / actPax * 100
              val actPaxPerFlight = actPax.toDouble / flightCount
              val predPaxPerFlight = predPax.toDouble / flightCount
              acc + f"${date.toISOString},$terminal,$actPax,$predPax,$flightCount,$actPaxPerFlight%.2f,$predPaxPerFlight%.2f,$actCap%.2f,$predCap%.2f,$predDiff%.2f,$fcstPax,$fcstCapPct%.2f,$fcstDiff\n"
          }
        val fileName = s"$port-$terminal.csv"

        Future
          .sequence(predictionWriters.map(_(fileName, csvContent)))
          .map(_ => Done)
          .recoverWith {
            case t =>
              log.error(s"Failed to dump stats for $terminal", t)
              Future.successful(Done)
          }
      }
  }

  private def predictionsWithLabels(dataSet: DataSet,
                                    withIndex: Iterable[(Double, Seq[String], Seq[Double], String)],
                                    model: LinearRegressionModel,
                                   )
                                   (implicit sparkSession: SparkSession): Map[LocalDate, Array[(Double, Double, String)]] =
    dataSet
      .predict("label", 0, model)
      .rdd
      .map { row =>
        val idx = row.getAs[String]("index")
        withIndex
          .find { case (_, _, _, key) => key == idx }
          .map {
            case (label, _, _, key) =>
              val prediction = Math.round(row.getAs[Double]("prediction"))
              val scheduled = key.split("-")(2).toLong
              val localDate = SDate(scheduled, DateTimeZone.forID("Europe/London")).toLocalDate
              (label, prediction.toDouble, localDate, key)
          }
      }
      .collect()
      .collect {
        case Some(data) => data
      }
      .groupBy {
        case (_, _, date, _) => date
      }
      .view.mapValues {
        _.map {
          case (label, predCap, _, key) => (label, predCap, key)
        }
      }
      .toMap

  private def capacityStats(predictionsWithLabels: Map[LocalDate, Array[(Double, Double, String)]],
                            terminal: Terminal,
                            arrivalsForDate: (Terminal, LocalDate) => Future[Seq[Arrival]],
                           ): Future[Seq[(LocalDate, Int, Int, Int, Double, Double, Double, Int)]] =
    Source(predictionsWithLabels)
      .mapAsync(1) {
        case (date, paxCounts) =>
          val actualCapOrig = paxCounts.map(_._1).sum
          val flightCount = paxCounts.length

          arrivalsForDate(terminal, date)
            .recoverWith {
              case t =>
                log.error(s"Failed to get arrivals for $date", t)
                Future.successful(Seq())
            }
            .map { arrivals =>
              val (actPax, predPax, fcstPax, actCapPct, predCapPct, fcstCapPct) = statsForArrivals(paxCounts, arrivals)

              if (Math.abs(actualCapOrig - actCapPct) > actualCapOrig * 0.1) {
                log.warn(s"Actual cap changed by more than 10% from $actualCapOrig to $actCapPct for $date")
              }

              (date, actPax, predPax, fcstPax, actCapPct / flightCount, predCapPct / flightCount, fcstCapPct / flightCount, flightCount)
            }
      }
      .runWith(Sink.seq)
      .recover {
        case t =>
          log.error(s"Failed to get capacity stats for $terminal", t)
          Seq()
      }
      .map(_.sortBy(_._1))

  private def statsForArrivals(paxCounts: Array[(Double, Double, String)], arrivals: Seq[Arrival]): (Int, Int, Int, Double, Double, Double) = {
    val maybeArrivalStats: Array[(Int, Int, Int, Double, Double, Double)] = for {
      (_, predCap, keyStr) <- paxCounts
      arrival <- arrivals.find { a =>
        a.unique.stringValue == keyStr && !a.isCancelled
      }
      maxPax <- arrival.MaxPax.filter(_ > 0)
      actualPax <- arrival.bestPcpPaxEstimate(Seq(LiveFeedSource, ApiFeedSource))
    } yield {
      val predPax = predCap * maxPax / 100
      val actualCapPct = 100 * actualPax.toDouble / maxPax match {
        case cap if cap > 100 => 100
        case cap => cap
      }
      val forecastPax = arrival.bestPaxEstimate(Seq(ForecastFeedSource, HistoricApiFeedSource, AclFeedSource)).passengers.getPcpPax.getOrElse(0)
      val forecastCapPct = 100 * forecastPax.toDouble / maxPax
      (actualPax, predPax.round.toInt, forecastPax, actualCapPct, predCap, forecastCapPct)
    }

    val (actPax, predPax, fcstPax, actCapPct, predCapPct, fcstCapPct) = maybeArrivalStats
      .foldLeft((0, 0, 0, 0d, 0d, 0d)) {
        case ((actPax, predPax, fcstPax, actCap, predCap, fcstCapPct), (actPax1, predPax1, fcstPax1, actCapPct1, predCapPct1, fcstCapPct1)) =>
          (actPax + actPax1, predPax + predPax1, fcstPax + fcstPax1, actCap + actCapPct1, predCap + predCapPct1, fcstCapPct + fcstCapPct1)
      }
    (actPax, predPax, fcstPax, actCapPct, predCapPct, fcstCapPct)
  }
}
