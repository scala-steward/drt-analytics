package uk.gov.homeoffice.drt.analytics.prediction.modeldefinitions

import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.pattern.ask
import akka.util.Timeout
import org.slf4j.LoggerFactory
import uk.gov.homeoffice.drt.actor.PredictionModelActor.{WithId, Terminal => TerminalId}
import uk.gov.homeoffice.drt.actor.TerminalDateActor.GetState
import uk.gov.homeoffice.drt.analytics.prediction.ModelDefinition
import uk.gov.homeoffice.drt.analytics.prediction.flights.FlightsActor
import uk.gov.homeoffice.drt.arrivals.{Arrival, Passengers}
import uk.gov.homeoffice.drt.ports.Terminals.Terminal
import uk.gov.homeoffice.drt.ports.{ApiFeedSource, LiveFeedSource}
import uk.gov.homeoffice.drt.prediction.arrival.ArrivalFeatureValuesExtractor.passengerCount
import uk.gov.homeoffice.drt.prediction.arrival.FeatureColumns._
import uk.gov.homeoffice.drt.prediction.arrival.{ArrivalModelAndFeatures, PaxModelAndFeatures}
import uk.gov.homeoffice.drt.time.{LocalDate, SDate, SDateLike, UtcDate}

import scala.concurrent.{ExecutionContext, Future}

object PaxModelDefinition extends ModelDefinition[Arrival, Terminal] {
  implicit val sdateTs: Long => SDateLike = (ts: Long) => SDate(ts)
  implicit val sdateLocal: LocalDate => SDateLike = (ts: LocalDate) => SDate(ts)

  override val modelName: String = PaxModelAndFeatures.targetName

  override val features: List[Feature[Arrival]] = List(
    ChristmasDay(),
    OctoberHalfTerm(),
    ChristmasHoliday(),
    SpringHalfTerm(),
    EasterHoliday(),
    SummerHalfTerm(),
    SummerHoliday(),
    DayOfWeek(),
    Carrier,
    Origin,
    FlightNumber,
  )
  override val aggregateValue: Arrival => Option[WithId] = TerminalId.fromArrival
  override val targetValueAndFeatures: Arrival => Option[(Double, Seq[String], Seq[Double])] = passengerCount(features)
  override val baselineValue: Terminal => Double = (_: Terminal) => 0d
}

trait PaxModelStatsLike {
  protected val log = LoggerFactory.getLogger(getClass)

  def sumActPaxForDate(arrivals: Seq[Arrival]): Int =
    arrivals.map(_.bestPcpPaxEstimate.getOrElse(0)).sum

  def sumPredPaxForDate(arrivals: Seq[Arrival], predPax: Arrival => Int): Int =
    arrivals.map { a =>
      val prediction = predPax(a)
      val cap = (a.bestPcpPaxEstimate, a.MaxPax) match {
        case (Some(actPax), Some(maxPax)) if maxPax > 0 =>
          val pct = 100d * actPax / maxPax
          f"$pct%.1f%%"
        case _ => "n/a"
      }
      val diff = a.bestPcpPaxEstimate match {
        case Some(actPax) if actPax > 0 =>
          val pct = 100 * (prediction - actPax).toDouble / actPax
          f"$pct%.1f%%"
        case _ => "n/a"
      }
//      println(s"${a.Terminal},${a.flightCodeString},${SDate(a.Scheduled).toISOString},${a.bestPcpPaxEstimate.getOrElse("n/a")},$prediction,$diff,$cap")
      prediction
    }.sum

  def arrivalsForDate(date: UtcDate, terminal: Terminal)
                     (implicit system: ActorSystem, ec: ExecutionContext, timeout: Timeout): Future[Seq[Arrival]] = {
    val actor = system.actorOf(Props(classOf[FlightsActor], terminal, date))
    actor
      .ask(GetState).mapTo[Seq[Arrival]]
      .map { arrivals =>
        actor ! PoisonPill
        arrivals.filter { arrival =>
          arrival.PassengerSources.exists {
            case (source, Passengers(maybePax, _)) => List(ApiFeedSource, LiveFeedSource).contains(source) && maybePax.isDefined
          }
        }
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

object PaxModelStats extends PaxModelStatsLike {
  def predictionForArrival(model: ArrivalModelAndFeatures)(arrival: Arrival): Int =
    model.prediction(arrival)
      .getOrElse({
        val fallback = arrival.MaxPax.filter(_ > 0).map(_ * 0.8).getOrElse(175d).toInt
        log.warn(s"Using fallback prediction of $fallback for ${arrival.flightCode} @ ${SDate(arrival.Scheduled).toISOString} with ${arrival.MaxPax} capacity")
        fallback
      })
}
