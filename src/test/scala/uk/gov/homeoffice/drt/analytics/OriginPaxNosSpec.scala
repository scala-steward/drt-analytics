package uk.gov.homeoffice.drt.analytics

import org.specs2.mutable.Specification
import uk.gov.homeoffice.drt.analytics.actors.DailyPaxCountsOnDay
import uk.gov.homeoffice.drt.analytics.time.SDate

class OriginPaxNosSpec extends Specification {
  private val day20200314: Long = SDate("2020-03-14").millisSinceEpoch
  private val day20200315: Long = SDate("2020-03-15").millisSinceEpoch
  private val day20200316: Long = SDate("2020-03-16").millisSinceEpoch
  private val day20200317: Long = SDate("2020-03-17").millisSinceEpoch
  private val day20200318: Long = SDate("2020-03-18").millisSinceEpoch
  private val day20200319: Long = SDate("2020-03-19").millisSinceEpoch

  val paxNos: Map[(Long, Long), Int] = Map(
    (day20200314, day20200315) -> 348, (day20200314, day20200316) -> 348, (day20200314, day20200317) -> 174, (day20200314, day20200318) -> 174, (day20200314, day20200319) -> 711,
    (day20200315, day20200315) -> 363, (day20200315, day20200316) -> 348, (day20200315, day20200317) -> 174, (day20200315, day20200318) -> 174, (day20200315, day20200319) -> 711,
    (day20200316, day20200316) -> 316, (day20200316, day20200317) -> 174, (day20200316, day20200318) -> 174, (day20200316, day20200319) -> 711,
    (day20200317, day20200317) -> 181, (day20200317, day20200318) -> 174, (day20200317, day20200319) -> 711,
    (day20200318, day20200318) -> 182, (day20200318, day20200319) -> 711,
    (day20200319, day20200319) -> 537)

  "Given some forecast/actual daily pax nos by day" >> {

    "When I ask for the diffs for the past 4 days" >> {
      val numberOfDays = 4
      val diffs = paxDiffs(paxNos, numberOfDays, () => SDate("2020-03-19T09:00"))

      "Then I should get differences between the actuals and the forecasts" >> {
        val expected = Seq(Option(-8), Option(-7), Option(32), Option(-15))

        diffs === expected
      }
    }

    "When I ask for the average for those past 4 days" >> {
      val diffs = Seq(Option(-8), Option(-7), Option(32), Option(-15))
      "Then I should see the average of the 4 differences" >> {
        val averageDiff = takeAverage(diffs)
        val expected = Option((Seq(-8, -7, 32, -15).sum.toDouble / 4).round.toInt)

        averageDiff === expected
      }
    }

    "When I ask for the average for 4 days where only 2 of the days have data" >> {
      val diffs = Seq(Option(10), None, None, Option(20))
      "Then I should see the average as the 2 days added and divided by 2" >> {
        val averageDiff = takeAverage(diffs)
        val expected = Option((Seq(10, 20).sum.toDouble / 2).round.toInt)

        averageDiff === expected
      }
    }

    "When I ask for the average for 4 days where none of the days have data" >> {
      val diffs = Seq(None, None, None, None)
      "Then I should get a None" >> {
        val averageDiff = takeAverage(diffs)
        val expected = None

        averageDiff === expected
      }
    }
  }

  "Given a DailyPaxCountsOnDay" >> {
    val date = SDate("2020-03-19T00:00")
    val counts = Map(date.millisSinceEpoch -> 5)
    val dailyPaxCountsOnDay = DailyPaxCountsOnDay(date.millisSinceEpoch, counts)

    "When I ask for the diff with an empty set of counts" >> {
      val emptyExisting = Map[(Long, Long), Int]()
      val diffs = dailyPaxCountsOnDay.diffFromExisting(emptyExisting)

      "Then I should get the counts from the daily pax counts" >> {
        diffs === dailyPaxCountsOnDay.dailyPax.map { case (d, c) => (dailyPaxCountsOnDay.dayMillis, d, c) }
      }
    }

    "When I ask for the diff with an existing set of pax counts which contains the same counts" >> {
      val existing = Map((date.millisSinceEpoch, date.millisSinceEpoch) -> 5)
      val diffs = dailyPaxCountsOnDay.diffFromExisting(existing)

      "Then I should get an empty set of diffs" >> {
        diffs === Iterable()
      }
    }

    "When I ask for the diff with an existing set of pax counts which contains the same day, but different counts" >> {
      val differentCount = 10
      val existing = Map((date.millisSinceEpoch, date.millisSinceEpoch) -> differentCount)
      val diffs = dailyPaxCountsOnDay.diffFromExisting(existing)

      "Then I should get the counts from the daily pax counts" >> {
        diffs === dailyPaxCountsOnDay.dailyPax.map { case (d, c) => (dailyPaxCountsOnDay.dayMillis, d, c) }
      }
    }

    "When I ask for the diff with an existing set of pax counts which contains a different day" >> {
      val existing = Map((date.addDays(1).millisSinceEpoch, date.addDays(1).millisSinceEpoch) -> 10)
      val diffs = dailyPaxCountsOnDay.diffFromExisting(existing)

      "Then I should get the counts from the daily pax counts" >> {
        diffs === dailyPaxCountsOnDay.dailyPax.map { case (d, c) => (dailyPaxCountsOnDay.dayMillis, d, c) }
      }
    }

    "When I apply it to an empty existing set" >> {
      val emptyExisting = Map[(Long, Long), Int]()
      val newSet = dailyPaxCountsOnDay.applyToExisting(emptyExisting)
      "I should get a new set with the counts from the daily counts" >> {
        newSet === dailyPaxCountsOnDay.dailyPax.map { case (d, c) => ((dailyPaxCountsOnDay.dayMillis, d), c) }
      }
    }

    "When I apply it to an existing set containing the same data" >> {
      val existing = Map[(Long, Long), Int]((date.millisSinceEpoch, date.millisSinceEpoch) -> 5)
      val newSet = dailyPaxCountsOnDay.applyToExisting(existing)
      "I should get a new set unchanged from the existing set" >> {
        newSet === existing
      }
    }

    "When I apply it to an existing set containing the same date but a different count" >> {
      val differentCount = 10
      val existing = Map[(Long, Long), Int]((date.millisSinceEpoch, date.millisSinceEpoch) -> differentCount)
      val newSet = dailyPaxCountsOnDay.applyToExisting(existing)
      "I should get a new set with the updated count" >> {
        newSet === dailyPaxCountsOnDay.dailyPax.map { case (d, c) => ((dailyPaxCountsOnDay.dayMillis, d), c) }
      }
    }

    "When I apply it to an existing set containing different dates" >> {
      val existing = Map[(Long, Long), Int]((date.addDays(1).millisSinceEpoch, date.addDays(1).millisSinceEpoch) -> 5)
      val newSet = dailyPaxCountsOnDay.applyToExisting(existing)
      "I should get a new set with both sets of dates" >> {
        newSet === dailyPaxCountsOnDay.dailyPax.map { case (d, c) => ((dailyPaxCountsOnDay.dayMillis, d), c) } ++ existing
      }
    }
  }

  private def takeAverage(diffs: Seq[Option[Int]]): Option[Int] = {
    val total = diffs.collect { case Some(diff) => diff }.sum.toDouble
    diffs.count(_.isDefined) match {
      case 0 => None
      case daysWithNumbers => Option((total / daysWithNumbers).round.toInt)
    }
  }

  private def paxDiffs(dailyPaxNosByDay: Map[(Long, Long), Int],
                       numberOfDays: Int,
                       now: () => SDate): IndexedSeq[Any] = {
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
