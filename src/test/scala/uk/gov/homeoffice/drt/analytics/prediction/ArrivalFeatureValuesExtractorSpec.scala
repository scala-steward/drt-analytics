package uk.gov.homeoffice.drt.analytics.prediction

import org.scalatest.wordspec.AnyWordSpec
import uk.gov.homeoffice.drt.arrivals.ArrivalGenerator
import uk.gov.homeoffice.drt.prediction.Feature.OneToMany
import uk.gov.homeoffice.drt.prediction.arrival.ArrivalFeatureValuesExtractor
import uk.gov.homeoffice.drt.prediction.arrival.FeatureColumns.{DayOfWeek, PartOfDay}
import uk.gov.homeoffice.drt.time.{SDate, SDateLike}

class ArrivalFeatureValuesExtractorSpec extends AnyWordSpec {
  implicit val sdateProvider: Long => SDateLike = (ts: Long) => SDate(ts)
  val features: Seq[OneToMany] = Seq(OneToMany(List(DayOfWeek()), ""), OneToMany(List(PartOfDay()), ""))

  val scheduled: SDateLike = SDate("2023-01-01T00:00")
  val scheduledDt: String = scheduled.toISOString
  val scheduledPlus10: String = scheduled.addMinutes(10).toISOString
  val scheduledPlus15: String = scheduled.addMinutes(15).toISOString

  "minutesOffSchedule" should {
    "give the different between scheduled and touchdown, with the day of the week and morning or afternoon flag" in {
      val arrival = ArrivalGenerator.arrival(schDt = scheduledDt, actDt = scheduledPlus10)
      println(s"arrival scheduled: ${SDate(arrival.Scheduled).toISOString()}, actual: ${arrival.Actual}")
      val result = ArrivalFeatureValuesExtractor.minutesOffSchedule(features)(arrival)
      assert(result == Option((10d, Seq("7", "0"), Seq())))
    }
    "give None when there is no touchdown time" in {
      val arrival = ArrivalGenerator.arrival(schDt = scheduledDt)
      val result = ArrivalFeatureValuesExtractor.minutesOffSchedule(features)(arrival)
      assert(result.isEmpty)
    }
  }
  "minutesToChox" should {

    "give the different between chox and touchdown, with the day of the week and morning or afternoon flag" in {
      val arrival = ArrivalGenerator.arrival(schDt = scheduledDt, actDt = scheduledDt, actChoxDt = scheduledPlus15)
      val result = ArrivalFeatureValuesExtractor.minutesToChox(features)(arrival)
      assert(result == Option((15d, Seq("7", "0"), Seq())))
    }
    "give None when there is no touchdown time" in {
      val arrival = ArrivalGenerator.arrival(schDt = scheduledDt, actChoxDt = scheduledPlus15)
      val result = ArrivalFeatureValuesExtractor.minutesToChox(features)(arrival)
      assert(result.isEmpty)
    }
    "give None when there is no actualChox time" in {
      val arrival = ArrivalGenerator.arrival(schDt = scheduledDt, actDt = scheduledPlus15)
      val result = ArrivalFeatureValuesExtractor.minutesToChox(features)(arrival)
      assert(result.isEmpty)
    }
  }
}
