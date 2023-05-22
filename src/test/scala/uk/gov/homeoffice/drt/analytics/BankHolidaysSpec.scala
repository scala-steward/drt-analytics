package uk.gov.homeoffice.drt.analytics

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import uk.gov.homeoffice.drt.time.{LocalDate, SDate, SDateLike}


class BankHolidaysSpec extends AnyWordSpec with Matchers {

  "isHolidayOrHolidayWeekend" should {
    "return true for a Monday holiday" in {
      BankHolidays.isHolidayOrHolidayWeekend(LocalDate(2020, 8, 31)) shouldBe true
    }

    "return true for a Tuesday holiday" in {
      BankHolidays.isHolidayOrHolidayWeekend(LocalDate(2021, 12, 28)) shouldBe true
    }

    "return true for a Friday holiday" in {
      BankHolidays.isHolidayOrHolidayWeekend(LocalDate(2020, 4, 10)) shouldBe true
    }

    "return true for a Thursday holiday" in {
      BankHolidays.isHolidayOrHolidayWeekend(LocalDate(2022, 6, 2)) shouldBe true
    }

    "return true for a Saturday on a holiday weekend" in {
      BankHolidays.isHolidayOrHolidayWeekend(LocalDate(2020, 8, 29)) shouldBe true
    }

    "return true for a Sunday on a holiday weekend" in {
      BankHolidays.isHolidayOrHolidayWeekend(LocalDate(2020, 8, 30)) shouldBe true
    }

    "return true for a Friday before a holiday weekend" in {
      BankHolidays.isHolidayOrHolidayWeekend(LocalDate(2020, 8, 28)) shouldBe true
    }

    "return false for a Thursday before a non-holiday friday" in {
      BankHolidays.isHolidayOrHolidayWeekend(LocalDate(2020, 8, 27)) shouldBe false
    }

    "return false for a Wednesday before a non-holiday friday" in {
      BankHolidays.isHolidayOrHolidayWeekend(LocalDate(2020, 8, 26)) shouldBe false
    }

    "return true for a Thursday before a holiday friday" in {
      BankHolidays.isHolidayOrHolidayWeekend(LocalDate(2020, 4, 9)) shouldBe true
    }

    "return true for a Wednesday before a holiday Thursday" in {
      BankHolidays.isHolidayOrHolidayWeekend(LocalDate(2022, 6, 1)) shouldBe true
    }

    "return false for a Wednesday before a holiday friday" in {
      BankHolidays.isHolidayOrHolidayWeekend(LocalDate(2020, 4, 8)) shouldBe false
    }
  }
}
