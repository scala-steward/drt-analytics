package uk.gov.homeoffice.drt.analytics

import uk.gov.homeoffice.drt.arrivals.{Passengers, PaxSource, UniqueArrival}
import uk.gov.homeoffice.drt.ports.{AclFeedSource, ApiFeedSource, FeedSource, ForecastFeedSource, HistoricApiFeedSource, LiveFeedSource, UnknownFeedSource}

case class Arrivals(arrivals: Map[UniqueArrival, SimpleArrival])

case class SimpleArrival(carrierCode: String,
                         number: Int,
                         scheduled: Long,
                         terminal: String,
                         origin: String,
                         status: String,
                         passengerSources: Map[FeedSource, Passengers],
                         maxPax: Option[Int],
                        ) {
  def uniqueArrival: UniqueArrival = UniqueArrival(number, terminal, scheduled, origin)

  def isCancelled: Boolean = status match {
    case st if st.toLowerCase.contains("cancelled") => true
    case st if st.toLowerCase.contains("canceled") => true
    case st if st.toLowerCase.contains("deleted") => true
    case _ => false
  }

  def bestPaxEstimate: PaxSource = {
    val preferredSources: List[FeedSource] = List(
      LiveFeedSource,
      ApiFeedSource,
      ForecastFeedSource,
      HistoricApiFeedSource,
      AclFeedSource,
    )

    preferredSources
      .find { case source => passengerSources.get(source).exists(_.actual.isDefined) }
      .flatMap { case source => passengerSources.get(source).map(PaxSource(source, _)) }
      .getOrElse(PaxSource(UnknownFeedSource, Passengers(None, None)))
  }
}
