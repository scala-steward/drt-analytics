package uk.gov.homeoffice.drt.analytics.prediction.modeldefinitions

import uk.gov.homeoffice.drt.actor.PredictionModelActor.{TerminalOrigin, WithId}
import uk.gov.homeoffice.drt.analytics.prediction.ModelDefinition
import uk.gov.homeoffice.drt.arrivals.Arrival
import uk.gov.homeoffice.drt.ports.Terminals.Terminal
import uk.gov.homeoffice.drt.prediction.{FeaturesWithOneToManyValues, RegressionModel}
import uk.gov.homeoffice.drt.prediction.arrival.ArrivalFeatureValuesExtractor.minutesOffSchedule
import uk.gov.homeoffice.drt.prediction.arrival.FeatureColumns._
import uk.gov.homeoffice.drt.prediction.arrival.{ArrivalModelAndFeatures, OffScheduleModelAndFeatures}
import uk.gov.homeoffice.drt.time.{SDate, SDateLike}

object DailyPaxModelAndFeatures {
  val targetName: String = "daily-pax"
}

case class DailyPaxModelAndFeatures(model: RegressionModel,
                                    features: FeaturesWithOneToManyValues,
                                    examplesTrainedOn: Int,
                                    improvementPct: Double,
                                   ) extends ArrivalModelAndFeatures {
  override val targetName: String = DailyPaxModelAndFeatures.targetName
}

object DailyPaxModelDefinition extends ModelDefinition[Arrival, Terminal] {
  implicit val sdateProvider: Long => SDateLike = (ts: Long) => SDate(ts)

  override val modelName: String = DailyPaxModelAndFeatures.targetName
  override val features: List[Feature[Arrival]] = List(
    DayOfWeek(),
    PartOfDay(),
    Carrier,
    FlightNumber,
  )
  override val aggregateValue: Arrival => Option[WithId] = TerminalOrigin.fromArrival
  override val targetValueAndFeatures: Arrival => Option[(Double, Seq[String], Seq[Double])] = minutesOffSchedule(features)
  override val baselineValue: Terminal => Double = (_: Terminal) => 0d
}
