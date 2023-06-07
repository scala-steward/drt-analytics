package uk.gov.homeoffice.drt.analytics.prediction.modeldefinitions

import uk.gov.homeoffice.drt.actor.PredictionModelActor.{TerminalCarrier, WithId}
import uk.gov.homeoffice.drt.analytics.prediction.ModelDefinition
import uk.gov.homeoffice.drt.arrivals.Arrival
import uk.gov.homeoffice.drt.ports.Terminals.Terminal
import uk.gov.homeoffice.drt.prediction.arrival.ArrivalFeatureValuesExtractor.minutesToChox
import uk.gov.homeoffice.drt.prediction.arrival.FeatureColumns.{DayOfWeek, Feature, FlightNumber, Origin, PartOfDay}
import uk.gov.homeoffice.drt.prediction.arrival.ToChoxModelAndFeatures
import uk.gov.homeoffice.drt.time.{SDate, SDateLike}

case class ToChoxModelDefinition(defaultTimeToChox: Long) extends ModelDefinition[Arrival, Terminal] {
  implicit val sdateProvider: Long => SDateLike = (ts: Long) => SDate(ts)

  override val modelName: String = ToChoxModelAndFeatures.targetName
  override val features: List[Feature[Arrival]] = List(
    DayOfWeek(),
    PartOfDay(),
    Origin,
    FlightNumber,
  )
  override val aggregateValue: Arrival => Option[WithId] = TerminalCarrier.fromArrival
  override val targetValueAndFeatures: Arrival => Option[(Double, Seq[String], Seq[Double])] = minutesToChox(features)
  override val baselineValue: Terminal => Double = (_: Terminal) => defaultTimeToChox.toDouble
}
