package uk.gov.homeoffice.drt.analytics.prediction

import org.scalatest.wordspec.AnyWordSpec
import uk.gov.homeoffice.drt.protobuf.messages.CrunchState.FlightWithSplitsMessage
import uk.gov.homeoffice.drt.protobuf.messages.FlightsMessage.FlightMessage
import uk.gov.homeoffice.drt.time.SDate

class FlightsMessageValueExtractorSpec extends AnyWordSpec {
  "minutesOffSchedule" should {
    val scheduled = SDate("2023-01-01T00:00")
    "give the different between scheduled and touchdown, with the day of the week and morning or afternoon flag" in {
      val fws = FlightWithSplitsMessage(
        flight = Option(FlightMessage(
          scheduled = Option(scheduled.millisSinceEpoch),
          touchdown = Option(scheduled.addMinutes(10).millisSinceEpoch),
        )),
      )
      val result = FlightsMessageValueExtractor.minutesOffSchedule(fws)
      assert(result == Option((10d, Seq("7", "0"))))
    }
    "give None when there is no touchdown time" in {
      val fws = FlightWithSplitsMessage(
        flight = Option(FlightMessage(
          scheduled = Option(scheduled.millisSinceEpoch),
          touchdown = None,
        )),
      )
      val result = FlightsMessageValueExtractor.minutesOffSchedule(fws)
      assert(result.isEmpty)
    }
  }
  "minutesToChox" should {
    val scheduled = SDate("2023-01-01T00:00")
    "give the different between chox and touchdown, with the day of the week and morning or afternoon flag" in {
      val fws = FlightWithSplitsMessage(
        flight = Option(FlightMessage(
          scheduled = Option(scheduled.millisSinceEpoch),
          touchdown = Option(scheduled.millisSinceEpoch),
          actualChox = Option(scheduled.addMinutes(15).millisSinceEpoch),
        )),
      )
      val result = FlightsMessageValueExtractor.minutesToChox(fws)
      assert(result == Option((15d, Seq("7", "0"))))
    }
    "give None when there is no touchdown time" in {
      val fws = FlightWithSplitsMessage(
        flight = Option(FlightMessage(
          scheduled = Option(scheduled.millisSinceEpoch),
          touchdown = None,
          actualChox = Option(scheduled.addMinutes(15).millisSinceEpoch),
        )),
      )
      val result = FlightsMessageValueExtractor.minutesToChox(fws)
      assert(result.isEmpty)
    }
    "give None when there is no actualChox time" in {
      val fws = FlightWithSplitsMessage(
        flight = Option(FlightMessage(
          scheduled = Option(scheduled.millisSinceEpoch),
          touchdown = Option(scheduled.addMinutes(15).millisSinceEpoch),
          actualChox = None,
        )),
      )
      val result = FlightsMessageValueExtractor.minutesToChox(fws)
      assert(result.isEmpty)
    }
  }
}
