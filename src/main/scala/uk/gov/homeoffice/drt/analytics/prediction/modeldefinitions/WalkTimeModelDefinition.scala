package uk.gov.homeoffice.drt.analytics.prediction.modeldefinitions

import uk.gov.homeoffice.drt.actor.PredictionModelActor.{TerminalCarrier, WithId}
import uk.gov.homeoffice.drt.actor.WalkTimeProvider
import uk.gov.homeoffice.drt.analytics.prediction.ModelDefinition
import uk.gov.homeoffice.drt.arrivals.Arrival
import uk.gov.homeoffice.drt.ports.Terminals.Terminal
import uk.gov.homeoffice.drt.prediction.Feature
import uk.gov.homeoffice.drt.prediction.Feature.OneToMany
import uk.gov.homeoffice.drt.prediction.arrival.ArrivalFeatureValuesExtractor.walkTimeMinutes
import uk.gov.homeoffice.drt.prediction.arrival.FeatureColumns.{DayOfWeek, FlightNumber, Origin, PartOfDay}
import uk.gov.homeoffice.drt.prediction.arrival.WalkTimeModelAndFeatures
import uk.gov.homeoffice.drt.time.{SDate, SDateLike}


case class WalkTimeModelDefinition(maybeGatesPath: Option[String],
                                   maybeStandsPath: Option[String],
                                   defaultWalkTimeMillis: Map[Terminal, Long],
                                  ) extends ModelDefinition[Arrival, Terminal] {
  implicit val sdateProvider: Long => SDateLike = (ts: Long) => SDate(ts)

  private val provider = WalkTimeProvider(maybeGatesPath, maybeStandsPath)

  override val modelName: String = WalkTimeModelAndFeatures.targetName
  override val features: List[Feature] = List(
    OneToMany(List(DayOfWeek()), "dow"),
    OneToMany(List(PartOfDay()), "pod"),
    OneToMany(List(Origin), "ori"),
    OneToMany(List(FlightNumber), "fln"),
  )
  override val aggregateValue: Arrival => Option[WithId] = TerminalCarrier.fromArrival
  override val targetValueAndFeatures: Arrival => Option[(Double, Seq[String], Seq[Double])] = walkTimeMinutes(provider)(features)
  override val baselineValue: Terminal => Double = (t: Terminal) => defaultWalkTimeMillis.get(t).map(_.toDouble / 1000).getOrElse(0)
}

//Terminal/Carrier + day of week + part of day + origin
//Terminal T2: 34 total, 31 models
//91% >= 10% improvement
//91% >= 20% improvement
//91% >= 30% improvement
//91% >= 40% improvement
//91% >= 50% improvement
//82% >= 60% improvement
//70% >= 70% improvement
//52% >= 80% improvement
//38% >= 90% improvement
//14% >= 100% improvement

//Terminal/Carrier + day of week + part of day + origin + flight number
//Terminal T2: 34 total, 31 models
//91% >= 10% improvement
//91% >= 20% improvement
//91% >= 30% improvement
//91% >= 40% improvement
//91% >= 50% improvement
//88% >= 60% improvement
//73% >= 70% improvement
//58% >= 80% improvement
//38% >= 90% improvement
//14% >= 100% improvement

// Terminal/Origin + day of week + part of day + carrier + flight number
//Terminal T2: 72 total, 60 models
//83% >= 10% improvement
//83% >= 20% improvement
//81% >= 30% improvement
//81% >= 40% improvement
//81% >= 50% improvement
//80% >= 60% improvement
//70% >= 70% improvement
//51% >= 80% improvement
//26% >= 90% improvement
//9% >= 100% improvement

