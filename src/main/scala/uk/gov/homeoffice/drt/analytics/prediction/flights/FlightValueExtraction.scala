package uk.gov.homeoffice.drt.analytics.prediction.flights

import uk.gov.homeoffice.drt.actor.PredictionModelActor.WithId
import uk.gov.homeoffice.drt.analytics.actors.TerminalDateActor.ArrivalKey
import uk.gov.homeoffice.drt.arrivals.Arrival
import uk.gov.homeoffice.drt.ports.Terminals.Terminal
import uk.gov.homeoffice.drt.protobuf.messages.FlightsMessage.UniqueArrivalMessage
import uk.gov.homeoffice.drt.time.UtcDate

import scala.concurrent.{ExecutionContext, Future}

object FlightMessageConversions {
  def arrivalKeyFromMessage(r: UniqueArrivalMessage): Option[ArrivalKey] =
    for {
      scheduled <- r.scheduled
      terminal <- r.terminalName
      flightNumber <- r.number
    } yield {
      ArrivalKey(scheduled, terminal, flightNumber)
    }

}

object ArrivalValueExtraction {
  def apply(arrivalsForDateAndTerminal: (UtcDate, Terminal) => Future[Seq[Arrival]],
            extractValues: Arrival => Option[(Double, Seq[String], Seq[Double], String)],
            extractKey: Arrival => Option[WithId],
            preProcessing: (UtcDate, Iterable[Arrival]) => Future[Iterable[Arrival]],
           )
           (implicit ec: ExecutionContext): (UtcDate, Terminal) => Future[Map[WithId, Iterable[(Double, Seq[String], Seq[Double], String)]]] =
    (date, terminal) => {
      arrivalsForDateAndTerminal(date, terminal)
        .flatMap { arrivals =>
          preProcessing(date, arrivals)
            .map { arrivals =>
              val byArrivalKey = arrivals
                .filterNot(_.Origin.isDomesticOrCta)
                .map(a => ArrivalKey(a) -> a).toMap
              extractions(byArrivalKey, extractValues, extractKey)
            }
        }
    }

  def extractions(byArrivalKeyProcessed: Map[ArrivalKey, Arrival],
                  extractValues: Arrival => Option[(Double, Seq[String], Seq[Double], String)],
                  extractKey: Arrival => Option[WithId]
                 ): Map[WithId, Iterable[(Double, Seq[String], Seq[Double], String)]] =
    byArrivalKeyProcessed
      .groupBy {
        case (_, arrival) => extractKey(arrival)
      }
      .collect {
        case (Some(key), arrivals) =>
          val examples = arrivals.values
            .map(a => (a.unique, a))
            .map { case (_, arrival) => extractValues(arrival) }
            .collect { case Some(value) => value }
          (key, examples)
      }

}
