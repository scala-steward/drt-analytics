package uk.gov.homeoffice.drt.analytics.prediction.flights

import akka.persistence.{PersistentActor, Recovery, SnapshotOffer, SnapshotSelectionCriteria}
import org.slf4j.LoggerFactory
import uk.gov.homeoffice.drt.actor.TerminalDateActor.ArrivalKey
import uk.gov.homeoffice.drt.actor.commands.Commands.GetState
import uk.gov.homeoffice.drt.analytics.prediction.flights.FlightMessageConversions.arrivalKeyFromMessage
import uk.gov.homeoffice.drt.arrivals.{ApiFlightWithSplits, Arrival, FlightsWithSplits, Updatable}
import uk.gov.homeoffice.drt.ports.Terminals.Terminal
import uk.gov.homeoffice.drt.protobuf.messages.CrunchState.{FlightWithSplitsMessage, FlightsWithSplitsDiffMessage, FlightsWithSplitsMessage, SplitsForArrivalsMessage}
import uk.gov.homeoffice.drt.protobuf.messages.FlightsMessage.{FlightsDiffMessage, UniqueArrivalMessage}
import uk.gov.homeoffice.drt.protobuf.serialisation.FlightMessageConversion.{flightMessageToApiFlight, flightWithSplitsFromMessage, splitsForArrivalsFromMessage}
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

  def deserialiseFwsMsg(u: FlightWithSplitsMessage): Arrival =
    flightWithSplitsFromMessage(u).apiFlight

  def processUpdatesAndRemovals[A <: Updatable[A]](createdAt: Long,
                                                updates: Iterable[A],
                                                removals: Iterable[UniqueArrivalMessage],
                                                deserialiseUpdate: A => Arrival,
                                  ): Unit = {
    (maybePointInTime, createdAt) match {
      case (Some(time), createdAt) if createdAt > time =>
      case (_, createdAt) =>
        updates
          .map(deserialiseUpdate)
          .foreach { arrival =>
            val updatedArrival = byArrivalKey.get(ArrivalKey(arrival)).map(_.update(arrival)).getOrElse(arrival)
            byArrivalKey = byArrivalKey.updated(ArrivalKey(arrival), updatedArrival)
          }

        if (SDate(createdAt) < SDate(date).addHours(28)) {
          if (removals.size < byArrivalKey.size)
            removals.foreach(processRemovalMessage)
        }
      case _ =>
    }
  }

  def processSplitsDiff(msg: SplitsForArrivalsMessage): Unit = {
    (maybePointInTime, msg.createdAt) match {
      case (Some(time), Some(createdAt)) if createdAt > time =>
      case _ =>
        val nowMillis = SDate.now().millisSinceEpoch
        val flightsWithSplits = FlightsWithSplits(byArrivalKey.values.map(a => ApiFlightWithSplits(a, Set(), None)))
        splitsForArrivalsFromMessage(msg).applyTo(flightsWithSplits, nowMillis, List()) match {
          case (FlightsWithSplits(flights), _) =>
            flights.values.foreach(f => byArrivalKey = byArrivalKey.updated(ArrivalKey(f.apiFlight), f.apiFlight))
        }
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

    case FlightsWithSplitsDiffMessage(Some(createdAt), removals, updates) =>
      processUpdatesAndRemovals(createdAt, updates, removals, deserialiseFwsMsg)

    case FlightsDiffMessage(Some(createdAt), removals, updates, _) =>
      processUpdatesAndRemovals(createdAt, updates, removals, flightMessageToApiFlight)

    case msg: SplitsForArrivalsMessage =>
      processSplitsDiff(msg)
  }

  override def receiveCommand: Receive = {
    case GetState =>
      sender() ! byArrivalKey.values.toSeq
  }
}
