package uk.gov.homeoffice.drt.analytics.actors

import akka.actor.ActorRef
import akka.persistence._
import org.slf4j.{Logger, LoggerFactory}
import scalapb.GeneratedMessage
import server.protobuf.messages.PaxMessage.{OriginTerminalPaxCountsMessage, OriginTerminalPaxCountsMessages, PaxCountMessage}
import uk.gov.homeoffice.drt.analytics.OriginTerminalDailyPaxCountsOnDay
import uk.gov.homeoffice.drt.analytics.actors.PassengersActor.relevantPaxCounts
import uk.gov.homeoffice.drt.analytics.time.SDate

case class PointInTimeOriginTerminalDay(pointInTime: Long, origin: String, terminal: String, day: Long)

case class OriginAndTerminal(origin: String, terminal: String)

case object ClearState

case object Ack

object PassengersActor {
  def relevantPaxCounts(numDaysInAverage: Int, now: () => SDate)(paxCountMessages: Seq[PaxCountMessage]): Seq[PaxCountMessage] = {
    val cutoff = now().getLocalLastMidnight.addDays(-1 * numDaysInAverage).millisSinceEpoch
    paxCountMessages.filter(msg => msg.getDay >= cutoff)
  }
}

class PassengersActor(val now: () => SDate, daysToRetain: Int) extends RecoveryActorLike {
  override val persistenceId = s"daily-pax"

  override val recoveryStartMillis: Long = now().millisSinceEpoch

  override val maybeSnapshotInterval: Option[Int] = Option(250)

  val filterRelevantPaxCounts: Seq[PaxCountMessage] => Seq[PaxCountMessage] = relevantPaxCounts(daysToRetain, now)

  override def processRecoveryMessage: PartialFunction[Any, Unit] = {
    case OriginTerminalPaxCountsMessage(Some(origin), Some(terminal), countMessages) =>
      applyPaxCountMessages(origin, terminal, countMessages)

    case _: OriginTerminalPaxCountsMessage =>
      log.warn(s"Ignoring OriginTerminalPaxCountsMessage with missing origin and/or terminal")

    case RecoveryCompleted =>

    case u =>
      log.info(s"Got unexpected recovery msg: $u")
  }

  private def applyPaxCountMessages(origin: String, terminal: String, countMessages: Seq[PaxCountMessage]): Unit = {
    val relevantPaxCounts = filterRelevantPaxCounts(countMessages)
    val updatesForOriginTerminal = messagesToUpdates(relevantPaxCounts)
    val originAndTerminal = OriginAndTerminal(origin, terminal)
    val updatedOriginTerminal = originTerminalPaxNosState.getOrElse(originAndTerminal, Map()) ++ updatesForOriginTerminal
    updateState(origin, terminal, updatedOriginTerminal)
  }

  override def processSnapshotMessage: PartialFunction[Any, Unit] = {
    case OriginTerminalPaxCountsMessages(messages) => messages.map { message =>
      applyPaxCountMessages(message.getOrigin, message.getTerminal, message.counts)
    }
  }

  override def stateToMessage: GeneratedMessage = OriginTerminalPaxCountsMessages(
    originTerminalPaxNosState.map {
      case (OriginAndTerminal(origin, terminal), stuff) =>
        OriginTerminalPaxCountsMessage(Option(origin), Option(terminal), updatesToMessages(stuff.map {
          case ((a, b), c) => (a, b, c)
        }))
    }.toSeq
  )

  val log: Logger = LoggerFactory.getLogger(persistenceId)

  var originTerminalPaxNosState: Map[OriginAndTerminal, Map[(Long, Long), Int]] = Map()

  override def receiveCommand: Receive = {
    case OriginTerminalDailyPaxCountsOnDay(_, _, paxNosForDay) if paxNosForDay.dailyPax.isEmpty =>
      log.info(s"Received empty DailyPaxCountsOnDay")
      sender() ! Ack

    case originTerminalPaxNosForDay: OriginTerminalDailyPaxCountsOnDay =>
      log.debug(s"Received OriginTerminalDailyPaxCountsOnDay with ${originTerminalPaxNosForDay.counts.dailyPax.size} updates")
      persistDiffAndUpdateState(originTerminalPaxNosForDay, sender())

    case oAndT: OriginAndTerminal =>
      sender() ! originTerminalPaxNosState.get(oAndT)

    case ClearState => originTerminalPaxNosState = Map()

    case SaveSnapshotSuccess(md) =>
      log.info(s"Save snapshot success: $md")

    case SaveSnapshotFailure(md, cause) =>
      log.error(s"Save snapshot failure: $md", cause)

    case u =>
      log.info(s"Got unexpected command: $u")
  }

  private def persistDiffAndUpdateState(originTerminalPaxNosForDay: OriginTerminalDailyPaxCountsOnDay,
                                        replyTo: ActorRef): Unit = {
    val origin = originTerminalPaxNosForDay.origin
    val terminal = originTerminalPaxNosForDay.terminal
    val existingForOriginTerminal = originTerminalPaxNosState.getOrElse(OriginAndTerminal(origin, terminal), Map())
    val (updateForState, diff) = originTerminalPaxNosForDay.applyAndGetDiff(existingForOriginTerminal)

    if (diff.nonEmpty) {
      val message = OriginTerminalPaxCountsMessage(Option(origin), Option(terminal), updatesToMessages(diff))

      persistAndMaybeSnapshot(message, Option((replyTo, Ack)))
      updateState(origin, terminal, updateForState)
    } else {
      log.debug(s"Empty diff. Nothing to persist")
      replyTo ! Ack
    }
  }

  private def updateState(origin: String, terminal: String, updateForState: Map[(Long, Long), Int]): Unit = {
    originTerminalPaxNosState = originTerminalPaxNosState.updated(OriginAndTerminal(origin, terminal), updateForState)
  }

  private def updatesToMessages(updates: Iterable[(Long, Long, Int)]): Seq[PaxCountMessage] = updates.map {
    case (pointInTime, day, paxCount) => PaxCountMessage(Option(pointInTime), Option(day), Option(paxCount))
  }.toSeq

  private def messagesToUpdates(updates: Seq[PaxCountMessage]): Map[(Long, Long), Int] = updates
    .collect {
      case PaxCountMessage(Some(pit), Some(day), Some(paxCount)) => ((pit, day), paxCount)
    }
    .toMap
}
