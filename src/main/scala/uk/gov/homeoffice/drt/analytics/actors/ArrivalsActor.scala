package uk.gov.homeoffice.drt.analytics.actors

import akka.actor.Props
import akka.persistence._
import org.joda.time.DateTimeZone
import org.slf4j.{Logger, LoggerFactory}
import server.protobuf.messages.FlightsMessage.{FeedStatusMessage, FlightStateSnapshotMessage, FlightsDiffMessage}
import uk.gov.homeoffice.drt.analytics.messages.MessageConversion
import uk.gov.homeoffice.drt.analytics.time.SDate
import uk.gov.homeoffice.drt.analytics.{Arrival, Arrivals, UniqueArrival}

import scala.collection.mutable

case class GetArrivals(firstDay: SDate, lastDay: SDate)

object ArrivalsActor {
  def props: (String, SDate) => Props = (persistenceId: String, pointInTime: SDate) =>
    Props(new ArrivalsActor(persistenceId, pointInTime))
}

class ArrivalsActor(val persistenceId: String, pointInTime: SDate) extends PersistentActor {
  val log: Logger = LoggerFactory.getLogger(getClass)

  var arrivals: mutable.Map[UniqueArrival, Arrival] = mutable.Map()

  override def receiveRecover: Receive = {
    case SnapshotOffer(md, FlightStateSnapshotMessage(flightMessages, _)) =>
      println(s"Got SnapshotOffer from ${SDate(md.timestamp).toISOString}")
      arrivals ++= flightMessages
        .map(MessageConversion.fromFlightMessage)
        .map(a => (a.uniqueArrival, a))

    case FlightsDiffMessage(_, removals, updates, _) =>
      arrivals --= removals.map(m => UniqueArrival(m.number.getOrElse(0), m.terminalName.getOrElse(""), m.scheduled.getOrElse(0L)))
      arrivals ++= updates.map(MessageConversion.fromFlightMessage).map(a => (a.uniqueArrival, a))

    case _: FeedStatusMessage =>

    case RecoveryCompleted =>
      log.info(s"Recovery completed")

    case u =>
      log.info(s"Got unexpected recovery msg: $u")
  }

  override def receiveCommand: Receive = {
    case GetArrivals(start, end) =>
      log.info("Got request for arrivals")
      sender() ! Arrivals(Map() ++ arrivals.filter { case (_, a) =>
        val arrivalDate = SDate(a.scheduled, DateTimeZone.forID("Europe/London")).toISODateOnly
        start.toISODateOnly <= arrivalDate && arrivalDate <= end.toISODateOnly
      })
    case u =>
      log.info(s"Got unexpected command: $u")
  }

  override def recovery: Recovery = {
    val criteria = SnapshotSelectionCriteria(maxTimestamp = pointInTime.millisSinceEpoch)
    val recovery = Recovery(fromSnapshot = criteria, replayMax = 500)

    log.info(s"Recovery: $recovery $persistenceId ${pointInTime.toISOString}")
    recovery
  }
}

object FeedPersistenceIds {
  val forecastBase = "actors.ForecastBaseArrivalsActor-forecast-base"
  val forecast = "actors.ForecastPortArrivalsActor-forecast-port"
  val liveBase = "actors.LiveBaseArrivalsActor-live-base"
  val live = "actors.LiveArrivalsActor-live"
}
