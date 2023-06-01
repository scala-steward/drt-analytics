package uk.gov.homeoffice.drt.analytics.prediction.modeldefinitions

import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.pattern.ask
import akka.util.Timeout
import org.slf4j.LoggerFactory
import uk.gov.homeoffice.drt.actor.PredictionModelActor.{WithId, Terminal => TerminalId}
import uk.gov.homeoffice.drt.actor.TerminalDateActor.GetState
import uk.gov.homeoffice.drt.analytics.BankHolidays
import uk.gov.homeoffice.drt.analytics.prediction.ModelDefinition
import uk.gov.homeoffice.drt.analytics.prediction.flights.FlightsActor
import uk.gov.homeoffice.drt.arrivals.Arrival
import uk.gov.homeoffice.drt.ports.Terminals.Terminal
import uk.gov.homeoffice.drt.prediction.arrival.ArrivalFeatureValuesExtractor.passengerCount
import uk.gov.homeoffice.drt.prediction.arrival.FeatureColumns._
import uk.gov.homeoffice.drt.prediction.arrival.PaxModelAndFeatures
import uk.gov.homeoffice.drt.time.MilliTimes.oneDayMillis
import uk.gov.homeoffice.drt.time.{LocalDate, SDate, SDateLike, UtcDate}

import scala.concurrent.{ExecutionContext, Future}

object PaxModelDefinition extends ModelDefinition[Arrival, Terminal] {
  implicit val sdateTs: Long => SDateLike = (ts: Long) => SDate(ts)
  implicit val sdateLocal: LocalDate => SDateLike = (ts: LocalDate) => SDate(ts)

  override val modelName: String = PaxModelAndFeatures.targetName

  private val isBankHoliday: Long => Boolean = ts => BankHolidays.isHolidayWeekend(SDate(ts).toLocalDate)

//  implicit val elapsedDays = (ts: Long) => ((SDate(ts).millisSinceEpoch - SDate("2022-04-01").millisSinceEpoch) / oneDayMillis).toInt

  override val features: List[Feature[Arrival]] = List(
    //    BankHolidayWeekend(isBankHoliday),
    Year(),
    MonthOfYear(),
    //    DayOfMonth(),
    ChristmasDay(),
    SummerHalfTerm(),
    SummerHoliday(),
    OctoberHalfTerm(),
    ChristmasHoliday(),
    EasterHoliday(),
    //    Since6MonthsAgo(() => SDate.now()),
    //    WeekendDay(),
    DayOfWeek(),
    //    PartOfDay(),
    Carrier,
    Origin,
    FlightNumber,
    //    ElapsedDays()
  )
  override val aggregateValue: Arrival => Option[WithId] = TerminalId.fromArrival
  override val targetValueAndFeatures: Arrival => Option[(Double, Seq[String], Seq[Double])] = passengerCount(features)
  override val baselineValue: Terminal => Double = (_: Terminal) => 0d
}

object PaxModelStats {
  private val log = LoggerFactory.getLogger(getClass)

  def sumActPaxForDate(arrivals: Seq[Arrival])
                      (implicit ec: ExecutionContext): Int =
    arrivals.map(_.bestPcpPaxEstimate.getOrElse(0)).sum

  def sumPredPaxForDate(arrivals: Seq[Arrival], predPax: Arrival => Future[Int])
                       (implicit ec: ExecutionContext): Future[Int] =
    Future
      .sequence(arrivals.map(a => predPax(a)))
      .map(_.sum)

  def arrivalsForDate(date: UtcDate, terminal: Terminal)
                     (implicit system: ActorSystem, ec: ExecutionContext, timeout: Timeout): Future[Seq[Arrival]] = {
    val actor = system.actorOf(Props(classOf[FlightsActor], terminal, date))
    actor
      .ask(GetState).mapTo[Seq[Arrival]]
      .map { arrivals =>
        actor ! PoisonPill
        arrivals.filter(_.bestPcpPaxEstimate.isDefined)
      }
      .recover {
        case t =>
          log.error(s"Error getting arrivals for $date", t)
          actor ! PoisonPill
          throw t
      }
  }

  def predictionForArrival(model: PaxModelAndFeatures)(arrival: Arrival)
                          (implicit ec: ExecutionContext): Future[Int] =
    Future.successful(model.prediction(arrival)
      .getOrElse({
        val fallback = arrival.MaxPax.map(_ * 0.8).getOrElse(220d)
        log.warn(s"Using fallback prediction of $fallback for $arrival")
        fallback.toInt
      }))
}
