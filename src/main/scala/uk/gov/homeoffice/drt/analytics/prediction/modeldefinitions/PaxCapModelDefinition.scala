package uk.gov.homeoffice.drt.analytics.prediction.modeldefinitions

import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.pattern.ask
import akka.util.Timeout
import org.slf4j.LoggerFactory
import uk.gov.homeoffice.drt.actor.PredictionModelActor.{WithId, Terminal => TerminalId}
import uk.gov.homeoffice.drt.actor.TerminalDateActor.{ArrivalKey, GetState}
import uk.gov.homeoffice.drt.analytics.prediction.ModelDefinition
import uk.gov.homeoffice.drt.analytics.prediction.flights.FlightsActor
import uk.gov.homeoffice.drt.arrivals.{Arrival, Passengers}
import uk.gov.homeoffice.drt.ports.Terminals.Terminal
import uk.gov.homeoffice.drt.ports.{ApiFeedSource, LiveFeedSource}
import uk.gov.homeoffice.drt.prediction.arrival.ArrivalFeatureValuesExtractor.percentCapacity
import uk.gov.homeoffice.drt.prediction.arrival.FeatureColumns._
import uk.gov.homeoffice.drt.prediction.arrival.{ArrivalModelAndFeatures, PaxCapModelAndFeatures}
import uk.gov.homeoffice.drt.time.{LocalDate, SDate, SDateLike, UtcDate}

import scala.concurrent.{ExecutionContext, Future}

object PaxCapModelDefinition extends ModelDefinition[Arrival, Terminal] {
  implicit val sdateTs: Long => SDateLike = (ts: Long) => SDate(ts)
  implicit val sdateLocal: LocalDate => SDateLike = (ts: LocalDate) => SDate(ts)

  override val modelName: String = PaxCapModelAndFeatures.targetName

  override val features: List[Feature[Arrival]] = List(
    ChristmasDay(),
    Term1a(),
    OctoberHalfTerm(),
    Term1b(),
    ChristmasHoliday(),
    Term2a(),
    SpringHalfTerm(),
    Term2b(),
    EasterHoliday(),
    Term3a(),
    SummerHalfTerm(),
    Term3b(),
    PreSummerHoliday(),
    SummerHoliday(),
    DayOfWeek(),
    Carrier,
    Origin,
    FlightNumber,
    PrePandemicRecovery(SDate("2022-06-01T00:00:00Z")),
  )
  override val aggregateValue: Arrival => Option[WithId] = TerminalId.fromArrival
  override val targetValueAndFeatures: Arrival => Option[(Double, Seq[String], Seq[Double])] = percentCapacity(features)
  override val baselineValue: Terminal => Double = (_: Terminal) => 0d
}

object PaxCapModelStats extends PaxModelStatsLike {
  def predictionForArrival(model: ArrivalModelAndFeatures)(arrival: Arrival): Int = {
    val maybePax = for {
      pctFull <- model.prediction(arrival)
      maxPax <- arrival.MaxPax
    } yield {
      ((pctFull.toDouble / 100) * maxPax).toInt
    }

    maybePax.getOrElse({
      val fallback = arrival.MaxPax.filter(_ > 0).map(_ * 0.8).getOrElse(175d).toInt
      log.debug(s"Using fallback prediction of $fallback for ${arrival.flightCode} @ ${SDate(arrival.Scheduled).toISOString} with ${arrival.MaxPax} capacity")
      fallback
    })
  }
}

trait PaxModelStatsLike {
  protected val log = LoggerFactory.getLogger(getClass)

  def sumActPaxForDate(arrivals: Seq[Arrival]): Int =
    arrivals.map(_.bestPcpPaxEstimate.getOrElse(0)).sum

  def sumPredPaxForDate(arrivals: Seq[Arrival], predPax: Arrival => Int): Int =
    arrivals.map(predPax).sum

  def sumPredPctCapForDate(arrivals: Seq[Arrival], predPax: Arrival => Int): Double = {
    val total = arrivals.map { a =>
      a.MaxPax match {
        case Some(maxPax) if maxPax > 0 => 100d * predPax(a) / maxPax
        case _ => 100d * predPax(a) / 175
      }
    }.sum
    total / arrivals.size
  }

  def sumActPctCapForDate(arrivals: Seq[Arrival]): Double = {
    val total = arrivals.map { a =>
      (a.bestPcpPaxEstimate, a.MaxPax) match {
        case (Some(actPax), Some(maxPax)) if maxPax > 0 => 100d * actPax / maxPax
        case _ => 80
      }
    }.sum
    total / arrivals.size
  }

  def arrivalsForDate(date: UtcDate,
                      terminal: Terminal,
                      populateMaxPax: (UtcDate, Map[ArrivalKey, Arrival]) => Future[Map[ArrivalKey, Arrival]],
                      maybeForecastAheadDays: Option[Int] = None
                     )
                     (implicit system: ActorSystem, ec: ExecutionContext, timeout: Timeout): Future[Seq[Arrival]] = {
    val maybePointInTime = maybeForecastAheadDays.map(d => SDate(date).addDays(d).millisSinceEpoch)
    val actor = system.actorOf(Props(classOf[FlightsActor], terminal, date, maybePointInTime))
    actor
      .ask(GetState).mapTo[Seq[Arrival]]
      .flatMap { arrivals =>
        actor ! PoisonPill
        val filtered = arrivals
          .filterNot(a => a.Origin.isDomesticOrCta)
          .filter { arrival =>
            arrival.PassengerSources.exists {
              case (source, Passengers(maybePax, _)) => List(ApiFeedSource, LiveFeedSource).contains(source) && maybePax.isDefined
            }
          }
        val filteredMap = filtered.map(a => (ArrivalKey(a.Scheduled, a.Terminal.toString, a.VoyageNumber.numeric), a))
        populateMaxPax(date, filteredMap.toMap).map(_.values.toSeq)
      }
      .recover {
        case t =>
          log.error(s"Error getting arrivals for $date", t)
          actor ! PoisonPill
          throw t
      }
  }

  def predictionForArrival(model: ArrivalModelAndFeatures)(arrival: Arrival): Int
}
