package uk.gov.homeoffice.drt.analytics

case class Arrivals(arrivals: Map[UniqueArrival, Arrival])

case class UniqueArrival(number: Int, terminal: String, scheduled: Long)

case class Arrival(carrierCode: String,
                   number: Int,
                   scheduled: Long,
                   terminal: String,
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
}
