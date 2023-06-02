package uk.gov.homeoffice.drt.analytics.prediction.modeldefinitions

import uk.gov.homeoffice.drt.actor.PredictionModelActor.{WithId, Terminal => TerminalId}
import uk.gov.homeoffice.drt.analytics.prediction.ModelDefinition
import uk.gov.homeoffice.drt.arrivals.Arrival
import uk.gov.homeoffice.drt.ports.Terminals.Terminal
import uk.gov.homeoffice.drt.prediction.arrival.ArrivalFeatureValuesExtractor.percentCapacity
import uk.gov.homeoffice.drt.prediction.arrival.FeatureColumns._
import uk.gov.homeoffice.drt.prediction.arrival.{ArrivalModelAndFeatures, PaxCapModelAndFeatures}
import uk.gov.homeoffice.drt.time.{LocalDate, SDate, SDateLike}

object PaxCapModelDefinition extends ModelDefinition[Arrival, Terminal] {
  implicit val sdateTs: Long => SDateLike = (ts: Long) => SDate(ts)
  implicit val sdateLocal: LocalDate => SDateLike = (ts: LocalDate) => SDate(ts)

  override val modelName: String = PaxCapModelAndFeatures.targetName

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
      log.warn(s"Using fallback prediction of $fallback for ${arrival.flightCode} @ ${SDate(arrival.Scheduled).toISOString} with ${arrival.MaxPax} capacity")
      fallback
    })
  }
}
