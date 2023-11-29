package uk.gov.homeoffice.drt.analytics.services

import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.pattern.ask
import akka.util.Timeout
import org.slf4j.{Logger, LoggerFactory}
import uk.gov.homeoffice.drt.actor.TerminalDateActor.ArrivalKey
import uk.gov.homeoffice.drt.actor.commands.Commands.GetState
import uk.gov.homeoffice.drt.analytics.prediction.flights.FlightsActor
import uk.gov.homeoffice.drt.arrivals.{Arrival, Passengers}
import uk.gov.homeoffice.drt.ports.Terminals.Terminal
import uk.gov.homeoffice.drt.ports.{ApiFeedSource, LiveFeedSource}
import uk.gov.homeoffice.drt.prediction.arrival.ArrivalModelAndFeatures
import uk.gov.homeoffice.drt.time.{SDate, UtcDate}

import scala.concurrent.{ExecutionContext, Future}

object PaxModelStats {
  protected val log: Logger = LoggerFactory.getLogger(getClass)

  private val paxSourceOrderPreference = List(LiveFeedSource, ApiFeedSource)

  def sumActPaxForDate(arrivals: Seq[Arrival]): Int = {
    arrivals.map(_.bestPcpPaxEstimate(paxSourceOrderPreference).getOrElse(0)).sum
  }

  def sumPredPaxForDate(arrivals: Seq[Arrival], predPax: Arrival => Int): Int =
    arrivals.map(predPax).sum

  def sumPredPctCapForDate(arrivals: Seq[Arrival], predPax: Arrival => Int): Double = {
    val total = arrivals.map { a =>
      a.MaxPax match {
        case Some(maxPax) if maxPax > 0 => 100d * predPax(a) / maxPax
        case _ => 100d * predPax(a) / 175
      }
    }.sum
    total / arrivals.size
  }

  def sumActPctCapForDate(arrivals: Seq[Arrival]): Double = {
    val total = arrivals.map { a =>
      (a.bestPcpPaxEstimate(paxSourceOrderPreference), a.MaxPax) match {
        case (Some(actPax), Some(maxPax)) if maxPax > 0 => 100d * actPax / maxPax
        case _ => 80
      }
    }.sum
    total / arrivals.size
  }

  def arrivalsForDate(date: UtcDate,
                      terminal: Terminal,
                      populateMaxPax: (UtcDate, Map[ArrivalKey, Arrival]) => Future[Map[ArrivalKey, Arrival]],
                      maybeForecastAheadDays: Option[Int] = None,
                      expectedFeeds: List[Any]
                     )
                     (implicit
                      system: ActorSystem,
                      ec: ExecutionContext,
                      timeout: Timeout
                     ): Future[Seq[Arrival]] = {
    val maybePointInTime = maybeForecastAheadDays.map(d => SDate(date).addDays(-d).millisSinceEpoch)
    val actor = system.actorOf(Props(new FlightsActor(terminal, date, maybePointInTime)))
    actor
      .ask(GetState).mapTo[Seq[Arrival]]
      .flatMap { arrivals =>
        actor ! PoisonPill
        val filtered = arrivals
          .filterNot(a => a.Origin.isDomesticOrCta)
          .filter { arrival =>
            if (expectedFeeds.nonEmpty)
              arrival.PassengerSources.exists {
                case (source, Passengers(maybePax, _)) => expectedFeeds.contains(source) && maybePax.isDefined
              }
            else true
          }
        val filteredMap = filtered.map(a => (ArrivalKey(a.Scheduled, a.Terminal.toString, a.VoyageNumber.numeric), a))
        populateMaxPax(date, filteredMap.toMap).map(_.values.toSeq)
      }
      .recover {
        case t =>
          log.error(s"Error getting arrivals for $date", t)
          actor ! PoisonPill
          throw t
      }
  }

  def predictionForArrival(model: ArrivalModelAndFeatures)(arrival: Arrival): Int = {
    val maybePax = for {
      pctFull <- model.prediction(arrival)
      maxPax <- arrival.MaxPax
    } yield {
      ((pctFull.toDouble / 100) * maxPax).toInt
    }

    maybePax.getOrElse({
      val fallback = arrival.MaxPax.filter(_ > 0).map(_ * 0.8).getOrElse(175d).toInt
      log.debug(s"Using fallback prediction of $fallback for ${arrival.flightCode} @ ${SDate(arrival.Scheduled).toISOString} with ${arrival.MaxPax} capacity")
      fallback
    })
  }
}
