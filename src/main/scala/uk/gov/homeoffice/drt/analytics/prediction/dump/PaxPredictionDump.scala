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
        val (csvContent, predDiffs, fcstDiffs) = stats
          .foldLeft((csvHeader + "\n", Seq.empty[Int], Seq.empty[Int])) {
            case ((accRow, accPredDiffs, accFcstDiffs), (date, actPax, predPax, fcstPax, actCap, predCap, fcstCapPct, flightCount, predPaxDiffs, fcstPaxDiffs)) =>
              val predDiff = (predPax - actPax).toDouble / actPax * 100
              val fcstDiff = (fcstPax - actPax).toDouble / actPax * 100
              val actPaxPerFlight = actPax.toDouble / flightCount
              val predPaxPerFlight = predPax.toDouble / flightCount
              val row = f"${date.toISOString},$terminal,$actPax,$predPax,$flightCount,$actPaxPerFlight%.2f,$predPaxPerFlight%.2f,$actCap%.2f,$predCap%.2f,$predDiff%.2f,$fcstPax,$fcstCapPct%.2f,$fcstDiff\n"
              (accRow + row, accPredDiffs ++ predPaxDiffs, accFcstDiffs ++ fcstPaxDiffs)
          }
        val (maePred, rmsePred, medianPred) = calcStats(predDiffs)
        val (maeFcst, rmseFcst, medianFcst) = calcStats(fcstDiffs)
        val statistics =
          f"""MAE prediction error,$maePred%.2f
             |MAE forecast error,$maeFcst%.2f
             |RMSE prediction error,$rmsePred%.2f
             |RMSE forecast error,$rmseFcst%.2f
             |Median prediction error,$medianPred%.2f
             |Median forecast error,$medianFcst%.2f
             |""".stripMargin
        val contentWithErrorStats = csvContent + statistics
        val fileName = s"$port-$terminal.csv"

        Future
          .sequence(predictionWriters.map(_(fileName, contentWithErrorStats)))
          .map(_ => Done)
          .recoverWith {
            case t =>
              log.error(s"Failed to dump stats for $terminal", t)
              Future.successful(Done)
          }
      }
  }

  private def calcStats(seq: Seq[Int]): (Double, Double, Double) = {
    val count = seq.size
    val mae = seq.map(Math.abs).sum.toDouble / count
    val rmse = Math.sqrt(seq.map(score => score * score).sum / count)
    val median = getMedian[Int, Double](seq)
    (mae, rmse, median)
  }

  private def getMedian[T: Ordering, F]
  (seq: Seq[T])
  (implicit conv: T => F, f: Fractional[F]): F = {
    val sortedSeq = seq.sorted
    if (seq.size % 2 == 1) sortedSeq(sortedSeq.size / 2) else {
      val (up, down) = sortedSeq.splitAt(seq.size / 2)
      import f._
      (conv(up.last) + conv(down.head)) / fromInt(2)
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
                           ): Future[Seq[(LocalDate, Int, Int, Int, Double, Double, Double, Int, Seq[Int], Seq[Int])]] =
    Source(predictionsWithLabels)
      .mapAsync(1) {
        case (date, paxCountsForDate) =>
          val actualCapOrig = paxCountsForDate.map(_._1).sum
          val flightCount = paxCountsForDate.length

          arrivalsForDate(terminal, date)
            .recoverWith {
              case t =>
                log.error(s"Failed to get arrivals for $date", t)
                Future.successful(Seq())
            }
            .map { arrivals =>
              val (actPax, predPax, fcstPax, actCapPct, predCapPct, fcstCapPct, predPaxDiffs, fcstPaxDiffs) = statsForArrivals(paxCountsForDate, arrivals)

              if (Math.abs(actualCapOrig - actCapPct) > actualCapOrig * 0.1) {
                log.warn(s"Actual cap changed by more than 10% from $actualCapOrig to $actCapPct for $date")
              }

              (date, actPax, predPax, fcstPax, actCapPct / flightCount, predCapPct / flightCount, fcstCapPct / flightCount, flightCount, predPaxDiffs, fcstPaxDiffs)
            }
      }
      .runWith(Sink.seq)
      .recover {
        case t =>
          log.error(s"Failed to get capacity stats for $terminal", t)
          Seq()
      }
      .map(_.sortBy(_._1))

  private def statsForArrivals(paxCounts: Array[(Double, Double, String)], arrivals: Seq[Arrival]): (Int, Int, Int, Double, Double, Double, Seq[Int], Seq[Int]) = {
    val maybeArrivalStats: Array[(Int, Int, Int, Double, Double, Double, Int, Int)] = for {
      (_, predCap, keyStr) <- paxCounts
      arrival <- arrivals.find { a =>
        a.unique.stringValue == keyStr && !a.isCancelled
      }
      maxPax <- arrival.MaxPax.filter(_ > 0)
      actualPax <- arrival.bestPcpPaxEstimate(Seq(LiveFeedSource, ApiFeedSource))
    } yield {
      val predPax = (predCap * maxPax / 100).round.toInt
      val actualCapPct = 100 * actualPax.toDouble / maxPax match {
        case cap if cap > 100 => 100
        case cap => cap
      }
      val forecastPax = arrival.bestPaxEstimate(Seq(ForecastFeedSource, HistoricApiFeedSource, AclFeedSource)).passengers.getPcpPax.getOrElse(0)
      val forecastCapPct = 100 * forecastPax.toDouble / maxPax
      val predPaxDiff = predPax - actualPax
      val forecastPaxDiff = forecastPax - actualPax
      (actualPax, predPax, forecastPax, actualCapPct, predCap, forecastCapPct, predPaxDiff, forecastPaxDiff)
    }

    val (actPax, predPax, fcstPax, actCapPct, predCapPct, fcstCapPct, predPaxDiffs, fcstPaxDiffs) = maybeArrivalStats
      .foldLeft((0, 0, 0, 0d, 0d, 0d, Seq.empty[Int], Seq.empty[Int])) {
        case ((actPax, predPax, fcstPax, actCap, predCap, fcstCapPct, predPaxDiffs, fcstPaxDiffs), (actPax1, predPax1, fcstPax1, actCapPct1, predCapPct1, fcstCapPct1, predPaxDiffs1, fcstPaxDiffs1)) =>
          (actPax + actPax1, predPax + predPax1, fcstPax + fcstPax1, actCap + actCapPct1, predCap + predCapPct1, fcstCapPct + fcstCapPct1, predPaxDiffs :+ predPaxDiffs1, fcstPaxDiffs :+ fcstPaxDiffs1)
      }
    (actPax, predPax, fcstPax, actCapPct, predCapPct, fcstCapPct, predPaxDiffs, fcstPaxDiffs)
  }
}
