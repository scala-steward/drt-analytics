package uk.gov.homeoffice.drt.analytics.prediction.modeldefinitions

import uk.gov.homeoffice.drt.actor.PredictionModelActor.{WithId, Terminal => TerminalId}
import uk.gov.homeoffice.drt.analytics.prediction.ModelDefinition
import uk.gov.homeoffice.drt.arrivals.Arrival
import uk.gov.homeoffice.drt.ports.Terminals.Terminal
import uk.gov.homeoffice.drt.prediction.arrival.ArrivalFeatureValuesExtractor.percentCapacity
import uk.gov.homeoffice.drt.prediction.arrival.FeatureColumns._
import uk.gov.homeoffice.drt.prediction.arrival.PaxCapModelAndFeatures
import uk.gov.homeoffice.drt.time.{LocalDate, SDate, SDateLike}

object PaxCapModelDefinition extends ModelDefinition[Arrival, Terminal] {
  implicit val sdateTs: Long => SDateLike = (ts: Long) => SDate(ts)
  implicit val sdateLocal: LocalDate => SDateLike = (ts: LocalDate) => SDate(ts)

  override val modelName: String = PaxCapModelAndFeatures.targetName

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
  override val targetValueAndFeatures: Arrival => Option[(Double, Seq[String], Seq[Double], String)] = {
    val featuresAreUnique = features.map(_.prefix).groupBy(identity).map(_._2.size).forall(_ == 1)
    assert(featuresAreUnique, () => s"Features must have unique prefixes: ${features.map(_.prefix)}")
    percentCapacity(features)
  }
  override val baselineValue: Terminal => Double = (_: Terminal) => 0d
}

