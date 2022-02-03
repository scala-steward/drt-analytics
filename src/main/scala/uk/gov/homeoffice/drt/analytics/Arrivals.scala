package uk.gov.homeoffice.drt.analytics

import uk.gov.homeoffice.drt.arrivals.UniqueArrival

case class Arrivals(arrivals: Map[UniqueArrival, SimpleArrival])

case class SimpleArrival(carrierCode: String,
                         number: Int,
                         scheduled: Long,
                         terminal: String,
                         origin: String,
                         status: String,
                         actPax: Int,
                         transPax: Int
                  ) {
  def uniqueArrival: UniqueArrival = UniqueArrival(number, terminal, scheduled, origin)

  def isCancelled: Boolean = status match {
    case st if st.toLowerCase.contains("cancelled") => true
    case st if st.toLowerCase.contains("canceled") => true
    case st if st.toLowerCase.contains("deleted") => true
    case _ => false
  }
}
