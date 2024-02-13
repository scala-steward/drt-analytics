package uk.gov.homeoffice.drt.analytics.prediction.modeldefinitions

import uk.gov.homeoffice.drt.actor.PredictionModelActor.{TerminalCarrier, WithId}
import uk.gov.homeoffice.drt.actor.WalkTimeProvider
import uk.gov.homeoffice.drt.analytics.prediction.ModelDefinition
import uk.gov.homeoffice.drt.arrivals.Arrival
import uk.gov.homeoffice.drt.ports.Terminals.Terminal
import uk.gov.homeoffice.drt.prediction.arrival.ArrivalFeatureValuesExtractor.walkTimeMinutes
import uk.gov.homeoffice.drt.prediction.arrival.WalkTimeModelAndFeatures
import uk.gov.homeoffice.drt.prediction.arrival.features.Feature
import uk.gov.homeoffice.drt.prediction.arrival.features.FeatureColumnsV1._
import uk.gov.homeoffice.drt.time.{SDate, SDateLike}


case class WalkTimeModelDefinition(maybeGatesPath: Option[String],
                                   maybeStandsPath: Option[String],
                                   defaultWalkTimeMillis: Map[Terminal, Long],
                                  ) extends ModelDefinition[Arrival, Terminal] {
  implicit val sdateProvider: Long => SDateLike = (ts: Long) => SDate(ts)

  private val provider = WalkTimeProvider(maybeGatesPath, maybeStandsPath)

  override val modelName: String = WalkTimeModelAndFeatures.targetName
  override val featuresVersion = WalkTimeModelAndFeatures.featuresVersion
  override val features: List[Feature[Arrival]] = List(
    DayOfWeek(),
    PartOfDay(),
    Origin,
    FlightNumber,
  )
  override val aggregateValue: Arrival => Option[WithId] = TerminalCarrier.fromArrival
  override val targetValueAndFeatures: Arrival => Option[(Double, Seq[String], Seq[Double], String)] = walkTimeMinutes(provider)(features)
  override val baselineValue: Terminal => Double = (t: Terminal) => defaultWalkTimeMillis.get(t).map(_.toDouble / 1000).getOrElse(0)
}
