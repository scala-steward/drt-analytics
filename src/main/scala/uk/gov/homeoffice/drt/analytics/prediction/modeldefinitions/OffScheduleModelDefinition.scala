package uk.gov.homeoffice.drt.analytics.prediction.modeldefinitions

import uk.gov.homeoffice.drt.actor.PredictionModelActor.{TerminalOrigin, WithId}
import uk.gov.homeoffice.drt.analytics.prediction.ModelDefinition
import uk.gov.homeoffice.drt.arrivals.Arrival
import uk.gov.homeoffice.drt.ports.Terminals.Terminal
import uk.gov.homeoffice.drt.prediction.arrival.ArrivalFeatureValuesExtractor.minutesOffSchedule
import uk.gov.homeoffice.drt.prediction.arrival.FeatureColumns.{Carrier, DayOfWeek, Feature, FlightNumber, PartOfDay}
import uk.gov.homeoffice.drt.prediction.arrival.OffScheduleModelAndFeatures
import uk.gov.homeoffice.drt.time.{SDate, SDateLike}

object OffScheduleModelDefinition extends ModelDefinition[Arrival, Terminal] {
  implicit val sdateProvider: Long => SDateLike = (ts: Long) => SDate(ts)

  override val modelName: String = OffScheduleModelAndFeatures.targetName
  override val features: List[Feature[Arrival]] = List(
    DayOfWeek(),
    PartOfDay(),
    Carrier,
    FlightNumber,
  )
  override val aggregateValue: Arrival => Option[WithId] = TerminalOrigin.fromArrival
  override val targetValueAndFeatures: Arrival => Option[(Double, Seq[String], Seq[Double], String)] = minutesOffSchedule(features)
  override val baselineValue: Terminal => Double = (_: Terminal) => 0d
}
