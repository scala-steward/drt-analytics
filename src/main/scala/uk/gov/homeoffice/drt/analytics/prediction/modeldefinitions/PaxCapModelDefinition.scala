package uk.gov.homeoffice.drt.analytics.prediction.modeldefinitions

import uk.gov.homeoffice.drt.actor.PredictionModelActor.{WithId, Terminal => TerminalId}
import uk.gov.homeoffice.drt.analytics.prediction.ModelDefinition
import uk.gov.homeoffice.drt.arrivals.Arrival
import uk.gov.homeoffice.drt.ports.Terminals.Terminal
import uk.gov.homeoffice.drt.prediction.arrival.ArrivalFeatureValuesExtractor.percentCapacity
import uk.gov.homeoffice.drt.prediction.arrival.PaxCapModelAndFeaturesV2
import uk.gov.homeoffice.drt.prediction.arrival.features.Feature
import uk.gov.homeoffice.drt.prediction.arrival.features.FeatureColumnsV2._
import uk.gov.homeoffice.drt.time.{LocalDate, SDate, SDateLike}

object PaxCapModelDefinition extends ModelDefinition[Arrival, Terminal] {
  implicit val sdateTs: Long => SDateLike = (ts: Long) => SDate(ts)
  implicit val sdateLocal: LocalDate => SDateLike = (ts: LocalDate) => SDate(ts)

  override val modelName: String = PaxCapModelAndFeaturesV2.targetName

  override val featuresVersion = PaxCapModelAndFeaturesV2.featuresVersion

  override val features: List[Feature[Arrival]] = List(
    Term1a(),
    OctoberHalfTerm(),
    Term1b(),
    ChristmasHoliday(),
    ChristmasDay(),
    Term2a(),
    SpringHalfTerm(),
    Term2b(),
    EasterHoliday(),
    Term3a(),
    SummerHalfTerm(),
    Term3b(),
    SummerHolidayScotland(),
    SummerHoliday(),
    DayOfWeek(),
    Carrier,
    Origin,
    FlightNumber,
  )
  override val aggregateValue: Arrival => Option[WithId] = TerminalId.fromArrival
  override val targetValueAndFeatures: Arrival => Option[(Double, Seq[String], Seq[Double], String)] = percentCapacity(features)
  override val baselineValue: Terminal => Double = (_: Terminal) => 0d
}

