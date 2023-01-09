package uk.gov.homeoffice.drt.analytics.services

import akka.Done
import akka.actor.{ActorSystem, Props}
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.Timeout
import org.slf4j.LoggerFactory
import uk.gov.homeoffice.drt.analytics.PaxDeltas
import uk.gov.homeoffice.drt.analytics.actors.PassengersActor
import uk.gov.homeoffice.drt.ports.AirportConfig
import uk.gov.homeoffice.drt.time.SDate

import scala.concurrent.{ExecutionContext, Future}


object PassengerCounts {
  private val log = LoggerFactory.getLogger(getClass)

  def updateForPort(config: AirportConfig, daysToLookBack: Int)
                   (implicit system: ActorSystem, ec: ExecutionContext, mat: Materializer, timeout: Timeout): Future[Done] = {
    val passengersActor = system.actorOf(Props(new PassengersActor(() => SDate.now(), 30)))
    Source(config.terminals.toList)
      .flatMapConcat { terminal =>
        PaxDeltas.updateDailyPassengersByOriginAndDay(terminal.toString.toUpperCase, PaxDeltas.startDate(daysToLookBack), daysToLookBack - 1, passengersActor)
      }
      .filter(_.isDefined)
      .map {
        case Some((origin, date)) => log.debug(s"Daily pax counts persisted for $origin on ${date.toISOString}")
      }
      .runWith(Sink.ignore)
  }
}
