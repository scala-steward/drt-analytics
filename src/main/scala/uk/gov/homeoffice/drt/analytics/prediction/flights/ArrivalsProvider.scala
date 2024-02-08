package uk.gov.homeoffice.drt.analytics.prediction.flights

import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.pattern.ask
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.Timeout
import org.slf4j.LoggerFactory
import uk.gov.homeoffice.drt.actor.commands.Commands.GetState
import uk.gov.homeoffice.drt.analytics.services.ArrivalsHelper.populateMaxPax
import uk.gov.homeoffice.drt.arrivals.Arrival
import uk.gov.homeoffice.drt.ports.Terminals.Terminal
import uk.gov.homeoffice.drt.time.{LocalDate, SDate, UtcDate}

import scala.concurrent.{ExecutionContext, Future}

case class ArrivalsProvider()
                           (implicit ec: ExecutionContext, timeout: Timeout, mat: Materializer, system: ActorSystem) {
  private val log = LoggerFactory.getLogger(getClass)

  val arrivals: (Terminal, LocalDate) => Future[Seq[Arrival]] = (terminal, localDate) => {
    val sdate = SDate(localDate)
    val utcDates = Set(sdate.toUtcDate, sdate.addDays(1).addMinutes(-1).toUtcDate)
    val preProcess = populateMaxPax()
    Source(utcDates.toList)
      .mapAsync(1) { utcDate =>
        val actor = system.actorOf(Props(new FlightsActor(terminal, utcDate, None)))
        actor
          .ask(GetState)
          .mapTo[Seq[Arrival]].map { arrivals =>
            actor ! PoisonPill
            arrivals
              .filter { a =>
                SDate(a.Scheduled).toLocalDate == localDate && !a.Origin.isDomesticOrCta
              }
              .map(a => (a.unique, a)).toMap.values.toSeq
          }
          .flatMap(preProcess(utcDate, _))
          .recoverWith {
            case t =>
              log.error(s"Failed to get state for $terminal on $utcDate", t)
              Future.successful(Seq.empty[Arrival])
          }
      }
      .runWith(Sink.seq)
      .map(_.flatten)
      .recoverWith {
        case t =>
          log.error(s"Failed to get state for $terminal on $localDate", t)
          Future.successful(Seq.empty[Arrival])
      }
  }
}
