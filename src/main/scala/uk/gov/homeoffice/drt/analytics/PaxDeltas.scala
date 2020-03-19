package uk.gov.homeoffice.drt.analytics

import uk.gov.homeoffice.drt.analytics.time.SDate

object PaxDeltas {
  def maybeAverageDelta(maybeDeltas: Seq[Option[Int]]): Option[Int] = {
    val total = maybeDeltas.collect { case Some(diff) => diff }.sum.toDouble
    maybeDeltas.count(_.isDefined) match {
      case 0 => None
      case daysWithNumbers => Option((total / daysWithNumbers).round.toInt)
    }
  }

  def maybeDeltas(dailyPaxNosByDay: Map[(Long, Long), Int],
                  numberOfDays: Int,
                  now: () => SDate): Seq[Option[Int]] = {
    val startDay = now().addDays(-1).getLocalLastMidnight

    (0 until numberOfDays).map { dayOffset =>
      val day = startDay.addDays(-1 * dayOffset)
      val dayBefore = day.addDays(-1)
      val maybeActualPax = dailyPaxNosByDay.get((day.millisSinceEpoch, day.millisSinceEpoch))
      val maybeForecastPax = dailyPaxNosByDay.get((dayBefore.millisSinceEpoch, day.millisSinceEpoch))
      for {
        actual <- maybeActualPax
        forecast <- maybeForecastPax
      } yield forecast - actual
    }
  }
}
