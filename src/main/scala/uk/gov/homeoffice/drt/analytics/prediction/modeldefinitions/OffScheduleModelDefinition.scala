package uk.gov.homeoffice.drt.analytics.prediction.modeldefinitions

import uk.gov.homeoffice.drt.actor.PredictionModelActor.WithId
import uk.gov.homeoffice.drt.analytics.prediction.FlightsMessageValueExtractor.minutesOffSchedule
import uk.gov.homeoffice.drt.analytics.prediction.ModelDefinition
import uk.gov.homeoffice.drt.arrivals.Arrival
import uk.gov.homeoffice.drt.ports.Terminals.Terminal
import uk.gov.homeoffice.drt.prediction.Feature
import uk.gov.homeoffice.drt.prediction.Feature.OneToMany
import uk.gov.homeoffice.drt.prediction.arrival.FeatureColumns.{Carrier, DayOfWeek, FlightNumber, PartOfDay}
import uk.gov.homeoffice.drt.prediction.arrival.OffScheduleModelAndFeatures
import uk.gov.homeoffice.drt.time.{SDate, SDateLike}

object OffScheduleModelDefinition extends ModelDefinition[Arrival, Terminal] {
  implicit val sdateProvider: Long => SDateLike = (ts: Long) => SDate(ts)

  override val modelName: String = OffScheduleModelAndFeatures.targetName
  override val features: List[Feature] = List(
    OneToMany(List(DayOfWeek()), "dow"),
    OneToMany(List(PartOfDay()), "pod"),
    OneToMany(List(Carrier), "car"),
    OneToMany(List(FlightNumber), "fln"),
  )
  override val aggregateValue: Arrival => Option[WithId] = TerminalOrigin.fromArrival
  override val targetValueAndFeatures: Arrival => Option[(Double, Seq[String], Seq[Double])] = minutesOffSchedule(features)
  override val baselineValue: Terminal => Double = (_: Terminal) => 0d
}

// Terminal/FlightNumber/Origin + day of week + part of day
//Terminal T2: 250 total, 200 models","context":"default"}
//31% >= 10% improvement","context":"default"}
//23% >= 20% improvement","context":"default"}
//18% >= 30% improvement","context":"default"}
//12% >= 40% improvement","context":"default"}
//7% >= 50% improvement","context":"default"}
//5% >= 60% improvement","context":"default"}
//4% >= 70% improvement","context":"default"}
//0% >= 80% improvement","context":"default"}
//0% >= 90% improvement","context":"default"}
//0% >= 100% improvement","context":"default"}

// Terminal/Carrier + day of week + part of day + origin + flight number
//Terminal T2: 34 total, 33 models","context":"default"}
//23% >= 10% improvement","context":"default"}
//17% >= 20% improvement","context":"default"}
//11% >= 30% improvement","context":"default"}
//8% >= 40% improvement","context":"default"}
//2% >= 50% improvement","context":"default"}
//2% >= 60% improvement","context":"default"}
//2% >= 70% improvement","context":"default"}
//0% >= 80% improvement","context":"default"}
//0% >= 90% improvement","context":"default"}
//0% >= 100% improvement","context":"default"}

// Terminal/Origin + day of week + part of day + carrier + flight number
//Terminal T2: 72 total, 64 models","context":"default"}
//34% >= 10% improvement","context":"default"}
//23% >= 20% improvement","context":"default"}
//18% >= 30% improvement","context":"default"}
//12% >= 40% improvement","context":"default"}
//5% >= 50% improvement","context":"default"}
//5% >= 60% improvement","context":"default"}
//5% >= 70% improvement","context":"default"}
//0% >= 80% improvement","context":"default"}
//0% >= 90% improvement","context":"default"}
//0% >= 100% improvement","context":"default"}
