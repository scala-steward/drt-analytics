package uk.gov.homeoffice.drt.analytics.prediction.modeldefinitions

import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.pattern.ask
import akka.util.Timeout
import uk.gov.homeoffice.drt.actor.PredictionModelActor.{Models, TerminalOrigin, WithId}
import uk.gov.homeoffice.drt.actor.TerminalDateActor.GetState
import uk.gov.homeoffice.drt.analytics.BankHolidays
import uk.gov.homeoffice.drt.analytics.actors.ArrivalsActor
import uk.gov.homeoffice.drt.analytics.prediction.ModelDefinition
import uk.gov.homeoffice.drt.analytics.prediction.flights.FlightsActor
import uk.gov.homeoffice.drt.arrivals.Arrival
import uk.gov.homeoffice.drt.ports.Terminals.Terminal
import uk.gov.homeoffice.drt.prediction.arrival.ArrivalFeatureValuesExtractor.minutesOffSchedule
import uk.gov.homeoffice.drt.prediction.arrival.FeatureColumns._
import uk.gov.homeoffice.drt.prediction.arrival.{OffScheduleModelAndFeatures, PaxModelAndFeatures}
import uk.gov.homeoffice.drt.time.{SDate, SDateLike, UtcDate}

import scala.concurrent.{ExecutionContext, Future}

object PaxModelDefinition extends ModelDefinition[Arrival, Terminal] {
  implicit val sdateProvider: Long => SDateLike = (ts: Long) => SDate(ts)

  override val modelName: String = PaxModelAndFeatures.targetName

  private val isBankHoliday: Long => Boolean = ts => BankHolidays.isHolidayWeekend(SDate(ts).toLocalDate)

  override val features: List[Feature[Arrival]] = List(
    BankHolidayWeekend(isBankHoliday),
    MonthOfYear(ts => SDate(ts).getMonth),
    DayOfWeek(),
    PartOfDay(),
    Carrier,
    FlightNumber,
  )
  override val aggregateValue: Arrival => Option[WithId] = TerminalOrigin.fromArrival
  override val targetValueAndFeatures: Arrival => Option[(Double, Seq[String], Seq[Double])] = minutesOffSchedule(features)
  override val baselineValue: Terminal => Double = (_: Terminal) => 0d
}

object PaxModelStats {
  def sumActPaxForDate(arrivals: => Future[Seq[Arrival]])
                      (implicit ec: ExecutionContext): Future[Int] =
    arrivals.map(_.map(_.bestPcpPaxEstimate.getOrElse(0)).sum)

  def sumPredPaxForDate(arrivals: => Future[Seq[Arrival]], predPax: Arrival => Future[Int])
                       (implicit ec: ExecutionContext): Future[Int] =
    arrivals.flatMap {
      b => Future.sequence(b.map(a => predPax(a))).map(_.sum)
    }

  def arrivalsForDate(date: UtcDate, terminal: Terminal)
                     (implicit system: ActorSystem, ec: ExecutionContext, timeout: Timeout): Future[Seq[Arrival]] = {
    val actor = system.actorOf(Props(classOf[FlightsActor], terminal, date))
    actor
      .ask(GetState).mapTo[Seq[Arrival]]
      .map { arrivals =>
        actor ! PoisonPill
        arrivals
      }
  }

  def predictionForArrival(arrivalId: Arrival => Option[WithId], models: WithId => Future[Models])(arrival: Arrival)
                          (implicit ec: ExecutionContext) = {
    models(arrivalId(arrival).get).map { models =>
      models.models
        .get(PaxModelDefinition.modelName)
        .collect {
          case m: PaxModelAndFeatures => m.prediction(arrival)
        }
        .flatten
        .getOrElse(0)
    }
  }
}
