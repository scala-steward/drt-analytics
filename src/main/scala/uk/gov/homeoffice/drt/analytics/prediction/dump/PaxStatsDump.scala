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

import java.io.{File, FileWriter}
import scala.concurrent.{ExecutionContext, Future}

case class PaxStatsDump(arrivalsForDate: (Terminal, LocalDate) => Future[Seq[Arrival]],
                        dumpStats: (String, String) => Future[Done])
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

    capacityStats(dataSet, withIndex, model, Terminal(terminal), arrivalsForDate)
      .flatMap { stats =>
        log.info(s"Got ${stats.size} days of stats for $terminal")
        val fileWriter = new FileWriter(new File(s"/tmp/pax-forecast-$port-$terminal.csv"))
        val csvContent = stats
          .foldLeft(csvHeader + "\n") {
            case (acc, (date, actPax, predPax, fcstPax, actCap, predCap, fcstCapPct, flightCount)) =>
              val predDiff = (predPax - actPax).toDouble / actPax * 100
              val fcstDiff = (fcstPax - actPax).toDouble / actPax * 100
              val actPaxPerFlight = actPax.toDouble / flightCount
              val predPaxPerFlight = predPax.toDouble / flightCount
              acc + f"${date.toISOString},$terminal,$actPax,$predPax,$flightCount,$actPaxPerFlight%.2f,$predPaxPerFlight%.2f,$actCap%.2f,$predCap%.2f,$predDiff%.2f,$fcstPax,$fcstCapPct%.2f,$fcstDiff\n"
          }
        fileWriter.write(csvContent)
        fileWriter.close()
        dumpStats(s"analytics/passenger-forecast/$port-$terminal.csv", csvContent)
      }
  }

  private def capacityStats(dataSet: DataSet,
                            withIndex: Iterable[(Double, Seq[String], Seq[Double], String)],
                            model: LinearRegressionModel,
                            terminal: Terminal,
                            arrivalsForDate: (Terminal, LocalDate) => Future[Seq[Arrival]],
                           )
                           (implicit sparkSession: SparkSession): Future[Seq[(LocalDate, Int, Int, Int, Double, Double, Double, Int)]] = {
    val dailyPredictions = dataSet
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
              (prediction.toDouble, label, localDate, key)
          }
      }
      .collect()
      .collect {
        case Some(data) => data
      }
      .groupBy {
        case (_, _, date, _) => date
      }

    Source(dailyPredictions)
      .mapAsync(1) {
        case (date, paxCounts) =>
          val actualCapOrig = paxCounts.map(_._2).sum
          val flightCount = paxCounts.length

          arrivalsForDate(terminal, date)
            .recoverWith {
              case t =>
                log.error(s"Failed to get arrivals for $date", t)
                Future.successful(Seq())
            }
            .map { arrivals =>
              val maybeArrivalStats: Array[Option[(Int, Int, Int, Double, Double, Double)]] = for {
                (predCap, _, _, keyStr) <- paxCounts
                arrival <- arrivals.find(_.unique.stringValue == keyStr)
              } yield {
                arrival.MaxPax.filter(_ > 0).map { maxPax =>
                  val predPax = predCap * maxPax / 100
                  val actualPax = arrival.bestPaxEstimate(Seq(LiveFeedSource, ApiFeedSource)).passengers.getPcpPax.getOrElse(0)
                  val actualCapPct = 100 * actualPax.toDouble / maxPax
                  val forecastPax = arrival.bestPaxEstimate(Seq(ForecastFeedSource, HistoricApiFeedSource, AclFeedSource)).passengers.getPcpPax.getOrElse(0)
                  val forecastCapPct = 100 * forecastPax.toDouble / maxPax
                  (actualPax, predPax.round.toInt, forecastPax, actualCapPct, predCap, forecastCapPct)
                }
              }

              val (actPax, predPax, fcstPax, actCapPct, predCapPct, fcstCapPct) = maybeArrivalStats
                .collect {
                  case Some((actPax, predPax, fcstPax, actCapPct, predCapPct, fcstCapPct)) =>
                    (actPax, predPax, fcstPax, actCapPct, predCapPct, fcstCapPct)
                }
                .foldLeft((0, 0, 0, 0d, 0d, 0d)) {
                  case ((actPax, predPax, fcstPax, actCap, predCap, fcstCapPct), (actPax1, predPax1, fcstPax1, actCapPct1, predCapPct1, fcstCapPct1)) =>
                    (actPax + actPax1, predPax + predPax1, fcstPax + fcstPax1, actCap + actCapPct1, predCap + predCapPct1, fcstCapPct + fcstCapPct1)
                }

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
  }
}
