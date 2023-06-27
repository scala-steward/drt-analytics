package uk.gov.homeoffice.drt.analytics.prediction.flights

import akka.persistence.{PersistentActor, Recovery, SnapshotOffer, SnapshotSelectionCriteria}
import org.slf4j.LoggerFactory
import uk.gov.homeoffice.drt.actor.TerminalDateActor.{ArrivalKey, GetState}
import uk.gov.homeoffice.drt.analytics.prediction.flights.FlightMessageConversions.arrivalKeyFromMessage
import uk.gov.homeoffice.drt.arrivals.Arrival
import uk.gov.homeoffice.drt.ports.Terminals.Terminal
import uk.gov.homeoffice.drt.protobuf.messages.CrunchState.{FlightWithSplitsMessage, FlightsWithSplitsDiffMessage, FlightsWithSplitsMessage}
import uk.gov.homeoffice.drt.protobuf.messages.FlightsMessage.UniqueArrivalMessage
import uk.gov.homeoffice.drt.protobuf.serialisation.FlightMessageConversion.flightWithSplitsFromMessage
import uk.gov.homeoffice.drt.time.{SDate, UtcDate}


trait FlightActorLike {
  private val log = LoggerFactory.getLogger(getClass)

  var byArrivalKey: Map[ArrivalKey, Arrival] = Map()
  val maybePointInTime: Option[Long]
  val date: UtcDate

  def processSnapshot(ss: Any): Unit = ss match {
    case msg: FlightsWithSplitsMessage => msg.flightWithSplits.foreach(processFlightWithSplitsMessage)
    case unexpected => log.warn(s"Got unexpected snapshot offer message: ${unexpected.getClass}")
  }

  def processFlightWithSplitsMessage(u: FlightWithSplitsMessage): Unit = {
    val arrival = flightWithSplitsFromMessage(u).apiFlight
    val key = ArrivalKey(arrival)
    byArrivalKey = byArrivalKey.updated(key, arrival)
  }

  def processDiffMsg(msg: FlightsWithSplitsDiffMessage): Unit = {
    (maybePointInTime, msg.createdAt) match {
      case (Some(time), Some(createdAt)) if createdAt > time =>
      case (_, Some(createdAt)) =>
        msg.updates.foreach(processFlightWithSplitsMessage)
        if (SDate(createdAt) < SDate(date).addHours(28)) {
          if (msg.removals.length < byArrivalKey.size)
            msg.removals.foreach(processRemovalMessage)
        }
      case _ =>
    }
  }

  def processRemovalMessage(r: UniqueArrivalMessage): Unit =
    arrivalKeyFromMessage(r).foreach(r => byArrivalKey = byArrivalKey - r)
}

class FlightsActor(val terminal: Terminal,
                   val date: UtcDate,
                   val maybePointInTime: Option[Long],
                  ) extends PersistentActor with FlightActorLike {

  override def persistenceId: String = f"terminal-flights-${terminal.toString.toLowerCase}-${date.year}-${date.month}%02d-${date.day}%02d"

  override def recovery: Recovery = maybePointInTime match {
    case None =>
      Recovery(SnapshotSelectionCriteria(Long.MaxValue, maxTimestamp = Long.MaxValue, 0L, 0L))
    case Some(pointInTime) =>
      val criteria = SnapshotSelectionCriteria(maxTimestamp = pointInTime)
      Recovery(fromSnapshot = criteria, replayMax = 250)
  }

  override def receiveRecover: Receive = {
    case SnapshotOffer(_, ss) =>
      processSnapshot(ss)

    case msg: FlightsWithSplitsDiffMessage =>
      processDiffMsg(msg)
  }

  override def receiveCommand: Receive = {
    case GetState =>
      sender() ! byArrivalKey.values.toSeq
  }
}
