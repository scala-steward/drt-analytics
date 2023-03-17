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

// Terminal/FlightNumber/Origin + day of week + part of day
//"Terminal T2: 250 total, 201 models","context":"default"}
//"66% >= 10% improvement","context":"default"}
//"44% >= 20% improvement","context":"default"}
//"26% >= 30% improvement","context":"default"}
//"15% >= 40% improvement","context":"default"}
//"8% >= 50% improvement","context":"default"}
//"6% >= 60% improvement","context":"default"}
//"5% >= 70% improvement","context":"default"}
//"4% >= 80% improvement","context":"default"}
//"4% >= 90% improvement","context":"default"}
//"4% >= 100% improvement","context":"default"}

// Terminal/Carrier + day of week + part of day + origin
//Terminal T2: 34 total, 33 models","context":"default"}
//88% >= 10% improvement","context":"default"}
//73% >= 20% improvement","context":"default"}
//47% >= 30% improvement","context":"default"}
//29% >= 40% improvement","context":"default"}
//17% >= 50% improvement","context":"default"}
//8% >= 60% improvement","context":"default"}
//5% >= 70% improvement","context":"default"}
//2% >= 80% improvement","context":"default"}
//2% >= 90% improvement","context":"default"}
//2% >= 100% improvement","context":"default"}

// Terminal/Carrier + day of week + part of day + origin + flight number
//Terminal T2: 34 total, 33 models","context":"default"}
//88% >= 10% improvement","context":"default"}
//70% >= 20% improvement","context":"default"}
//47% >= 30% improvement","context":"default"}
//29% >= 40% improvement","context":"default"}
//20% >= 50% improvement","context":"default"}
//8% >= 60% improvement","context":"default"}
//5% >= 70% improvement","context":"default"}
//2% >= 80% improvement","context":"default"}
//2% >= 90% improvement","context":"default"}
//2% >= 100% improvement","context":"default"}
