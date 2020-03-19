package uk.gov.homeoffice.drt.analytics.actors

import akka.persistence._
import org.slf4j.{Logger, LoggerFactory}
import server.protobuf.messages.PaxMessage.{PaxCountMessage, PaxCountsMessage}
import uk.gov.homeoffice.drt.analytics.time.SDate


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
      val (newState, diff) = paxNosForDay.applyAndGetDiff(paxNosState)
      paxNosState = newState
      persist(diff) { updates => PaxCountsMessage(updatesToMessages(updates)) }

    case GetAverageDelta(numberOfDays: Int) =>


    case u =>
      log.info(s"Got unexpected command: $u")
  }

  private def updatesToMessages(updates: Iterable[(Long, Long, Int)]): Seq[PaxCountMessage] = updates.map {
    case (pointInTime, day, paxCount) => PaxCountMessage(Option(pointInTime), Option(day), Option(paxCount))
  }.toSeq

  private def messagesToUpdates(updates: Seq[PaxCountMessage]): Seq[(Long, Long, Int)] = updates.map {
    case PaxCountMessage(Some(pit), Some(day), Some(paxCount)) => (pit, day, paxCount)
  }
}

case class DailyPaxCountsOnDay(dayMillis: Long, dailyPax: Map[Long, Int]) {
  val log: Logger = LoggerFactory.getLogger(getClass)

  import DailyPaxCountsOnDay._

  val day: SDate = SDate(dayMillis)

  def diffFromExisting(existingPaxNos: Map[(Long, Long), Int]): Iterable[(Long, Long, Int)] = dailyPax.map {
    case (incomingDayMillis, incomingPax) =>
      val key = (day.millisSinceEpoch, incomingDayMillis)
      val incomingDay = SDate(incomingDayMillis)

      existingPaxNos.get(key) match {
        case None =>
          log.info(s"New day of pax ($incomingPax) for ${incomingDay.toISOString} on ${day.toISOString}")
          Some((day.millisSinceEpoch, incomingDayMillis, incomingPax))
        case Some(existingPax) if existingPax != incomingPax =>
          log.debug(s"Change in pax ($existingPax -> $incomingPax) for ${incomingDay.toISOString} on ${day.toISOString}")
          Some((day.millisSinceEpoch, incomingDayMillis, incomingPax))
        case Some(existingPax) =>
          log.debug(s"No change in pax ($existingPax) for ${incomingDay.toISOString} on ${day.toISOString}")
          None
      }
  }.collect { case Some(diff) => diff }

  def applyToExisting(existingCounts: Map[(Long, Long), Int]): Map[(Long, Long), Int] =
    applyDiffToExisting(diffFromExisting(existingCounts), existingCounts)

  def applyAndGetDiff(existingCounts: Map[(Long, Long), Int]): (Map[(Long, Long), Int], Iterable[(Long, Long, Int)]) =
    (applyDiffToExisting(diffFromExisting(existingCounts), existingCounts), diffFromExisting(existingCounts))
}

object DailyPaxCountsOnDay {
  def applyDiffToExisting(diff: Iterable[(Long, Long, Int)],
                          existing: Map[(Long, Long), Int]): Map[(Long, Long), Int] = diff.foldLeft(existing) {
    case (stateSoFar, (pit, day, paxCount)) => stateSoFar.updated((pit, day), paxCount)
  }
}

case class GetAverageDelta(numberOfDays: Int)
