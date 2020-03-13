package uk.gov.homeoffice.drt.analytics

case class Arrivals(arrivals: Map[UniqueArrival, Arrival])

case class UniqueArrival(number: Int, terminal: String, scheduled: Long)

case class Arrival(carrierCode: String,
                   number: Int,
                   scheduled: Long,
                   terminal: String,
                   status: String,
                   maybeEstimated: Option[Long],
                   maybeTouchdown: Option[Long],
                   maybeEstimatedChox: Option[Long],
                   maybeActualChox: Option[Long],
                   actPax: Int,
                   transPax: Int
                  ) {

  def uniqueArrival: UniqueArrival = UniqueArrival(number, terminal, scheduled)

  def bestTime: Long = maybeActualChox.getOrElse(
    maybeEstimatedChox.getOrElse(
      maybeTouchdown.getOrElse(
        maybeEstimated.getOrElse(scheduled))))

  def pcpStart: Long = {
    val tenMinutesToPcp = 60 * 1000 * 10
    bestTime + tenMinutesToPcp
  }

  def isCancelled: Boolean = status match {
    case st if st.toLowerCase.contains("cancelled") => true
    case st if st.toLowerCase.contains("canceled") => true
    case st if st.toLowerCase.contains("deleted") => true
    case _ => false
  }
}
