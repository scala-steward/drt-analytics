package uk.gov.homeoffice.drt.analytics.actors

import akka.actor.ActorRef
import akka.persistence._
import org.slf4j.{Logger, LoggerFactory}
import server.protobuf.messages.PaxMessage.{PaxCountMessage, PaxCountsMessage}
import uk.gov.homeoffice.drt.analytics.time.SDate
import uk.gov.homeoffice.drt.analytics.{DailyPaxCountsOnDay, PaxDeltas}


case class GetAverageDelta(numberOfDays: Int)

case object Ack

class OriginTerminalPassengersActor(origin: String, terminal: String) extends PersistentActor {
  val log: Logger = LoggerFactory.getLogger(getClass)

  override val persistenceId = s"daily-origin-terminal-pax-$origin-$terminal"

  var paxNosState: Map[(Long, Long), Int] = Map()

  override def receiveRecover: Receive = {
    case SnapshotOffer(md, PaxCountsMessage(countMessages)) =>
      println(s"Got SnapshotOffer from ${SDate(md.timestamp).toISOString}")
      paxNosState = messagesToUpdates(countMessages).map { case (pit, day, count) => ((pit, day), count) }.toMap

    case PaxCountsMessage(countMessages) =>
      log.info(s"Got a paxCountsMessage with ${countMessages.size} counts. Applying")
      paxNosState = DailyPaxCountsOnDay.applyDiffToExisting(messagesToUpdates(countMessages), paxNosState)

    case RecoveryCompleted =>
      log.info(s"Recovery completed")

    case u =>
      log.info(s"Got unexpected recovery msg: $u")
  }

  override def receiveCommand: Receive = {
    case paxNosForDay: DailyPaxCountsOnDay =>
      log.info(s"Received DailyPaxCountsOnDay with ${paxNosForDay.dailyPax.size} updates")
      persistDiffAndUpdateState(paxNosForDay, sender())

    case GetAverageDelta(numberOfDays: Int) =>
      log.info(s"Received request for $numberOfDays days average delta")
      sendAverageDelta(numberOfDays, sender())

    case u =>
      log.info(s"Got unexpected command: $u")
  }

  private def sendAverageDelta(numberOfDays: Int, replyTo: ActorRef): Unit = {
    val maybeDeltas = PaxDeltas.maybeDeltas(paxNosState, numberOfDays, () => SDate.now())
    val maybeAverageDelta = PaxDeltas.maybeAverageDelta(maybeDeltas)
    replyTo ! maybeAverageDelta
  }

  private def persistDiffAndUpdateState(paxNosForDay: DailyPaxCountsOnDay, replyTo: ActorRef): Unit = {
    val (newState, diff) = paxNosForDay.applyAndGetDiff(paxNosState)

    persist(diff) { updates =>
      PaxCountsMessage(updatesToMessages(updates))
      paxNosState = newState
      replyTo ! Ack
    }
  }

  private def updatesToMessages(updates: Iterable[(Long, Long, Int)]): Seq[PaxCountMessage] = updates.map {
    case (pointInTime, day, paxCount) => PaxCountMessage(Option(pointInTime), Option(day), Option(paxCount))
  }.toSeq

  private def messagesToUpdates(updates: Seq[PaxCountMessage]): Seq[(Long, Long, Int)] = updates.collect {
    case PaxCountMessage(Some(pit), Some(day), Some(paxCount)) => (pit, day, paxCount)
  }
}
