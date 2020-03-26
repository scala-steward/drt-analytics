package uk.gov.homeoffice.drt.analytics.actors

import akka.actor.ActorRef
import akka.persistence._
import org.slf4j.{Logger, LoggerFactory}
import server.protobuf.messages.PaxMessage.{OriginTerminalPaxCountsMessage, PaxCountMessage}
import uk.gov.homeoffice.drt.analytics.OriginTerminalDailyPaxCountsOnDay

case class PointInTimeOriginTerminalDay(pointInTime: Long, origin: String, terminal: String, day: Long)

case class OriginAndTerminal(origin: String, terminal: String)

case object ClearState

case object Ack

class PassengersActor extends PersistentActor {
  override val persistenceId = s"daily-pax"

  val log: Logger = LoggerFactory.getLogger(persistenceId)

  var originTerminalPaxNosState: Map[OriginAndTerminal, Map[(Long, Long), Int]] = Map()

  override def receiveRecover: Receive = {
    case OriginTerminalPaxCountsMessage(Some(origin), Some(terminal), countMessages) =>
      log.info(s"Got a OriginTerminalPaxCountsMessage with ${countMessages.size} counts. Applying")
      val updatesForOriginTerminal = messagesToUpdates(countMessages)
      val originAndTerminal = OriginAndTerminal(origin, terminal)
      val updatedOriginTerminal = originTerminalPaxNosState.getOrElse(originAndTerminal, Map()) ++ updatesForOriginTerminal
      originTerminalPaxNosState = originTerminalPaxNosState.updated(originAndTerminal, updatedOriginTerminal)

    case _: OriginTerminalPaxCountsMessage =>
      log.warn(s"Ignoring OriginTerminalPaxCountsMessage with missing origin and/or terminal")

    case RecoveryCompleted =>
      log.info(s"Recovery completed")

    case u =>
      log.info(s"Got unexpected recovery msg: $u")
  }

  override def receiveCommand: Receive = {
    case OriginTerminalDailyPaxCountsOnDay(_, _, paxNosForDay) if paxNosForDay.dailyPax.isEmpty =>
      log.info(s"Received empty DailyPaxCountsOnDay")
      sender() ! Ack

    case originTerminalPaxNosForDay: OriginTerminalDailyPaxCountsOnDay =>
      log.info(s"Received OriginTerminalDailyPaxCountsOnDay with ${originTerminalPaxNosForDay.counts.dailyPax.size} updates")
      persistDiffAndUpdateState(originTerminalPaxNosForDay, sender())

    case oAndT: OriginAndTerminal =>
      println(s"state: $originTerminalPaxNosState")
      sender() ! originTerminalPaxNosState.get(oAndT)

    case ClearState => originTerminalPaxNosState = Map()

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

      persist(message) { toPersist =>
        context.system.eventStream.publish(toPersist)
        originTerminalPaxNosState = originTerminalPaxNosState.updated(OriginAndTerminal(origin, terminal), updateForState)
        replyTo ! Ack
      }
    } else {
      log.info(s"Empty diff. Nothing to persist")
      replyTo ! Ack
    }
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
