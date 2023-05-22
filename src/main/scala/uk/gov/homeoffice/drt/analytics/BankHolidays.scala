package uk.gov.homeoffice.drt.analytics

import uk.gov.homeoffice.drt.time.{LocalDate, SDate, SDateLike}

object BankHolidays {
  val dates: Seq[LocalDate] = dates2020 ++ dates2021 ++ dates2022 ++ dates2023 ++ dates2024 ++ dates2025

  def isHolidayOrHolidayWeekend(date: LocalDate): Boolean = {
    isHolidayWeekend(date) ||
      isHolidayWeekend(SDate(date).addDays(1).toLocalDate)
  }

  def isHolidayWeekend(date: LocalDate): Boolean = {
    val sdate = SDate(date)

    if (dates.contains(date)) {
      true
    } else {
      if (isSaturday(sdate)) {
        val friday = sdate.addDays(-1).toLocalDate
        val monday = sdate.addDays(2).toLocalDate
        dates.contains(friday) || dates.contains(monday)
      } else if (isSunday(sdate)) {
        val friday = sdate.addDays(-2).toLocalDate
        val monday = sdate.addDays(1).toLocalDate
        dates.contains(friday) || dates.contains(monday)
      } else {
        false
      }
    }
  }

  private def isSaturday(sdate: SDateLike) = sdate.getDayOfWeek == 6

  private def isSunday(sdate: SDateLike) = sdate.getDayOfWeek == 7

  def dates2020: Seq[LocalDate] = Seq(
    LocalDate(2020, 1, 1),
    LocalDate(2020, 4, 10),
    LocalDate(2020, 4, 13),
    LocalDate(2020, 5, 8),
    LocalDate(2020, 5, 25),
    LocalDate(2020, 8, 31),
    LocalDate(2020, 12, 25),
    LocalDate(2020, 12, 28),
  )

  def dates2021: Seq[LocalDate] = Seq(
    LocalDate(2021, 1, 1),
    LocalDate(2021, 4, 2),
    LocalDate(2021, 4, 5),
    LocalDate(2021, 5, 3),
    LocalDate(2021, 5, 31),
    LocalDate(2021, 8, 30),
    LocalDate(2021, 12, 27),
    LocalDate(2021, 12, 28),
  )

  def dates2022: Seq[LocalDate] = Seq(
    LocalDate(2022, 1, 3),
    LocalDate(2022, 4, 15),
    LocalDate(2022, 4, 18),
    LocalDate(2022, 5, 2),
    LocalDate(2022, 6, 2),
    LocalDate(2022, 6, 3),
    LocalDate(2022, 8, 29),
    LocalDate(2022, 9, 19),
    LocalDate(2022, 12, 26),
    LocalDate(2022, 12, 27),
  )

  def dates2023: Seq[LocalDate] = Seq(
    LocalDate(2023, 1, 2),
    LocalDate(2023, 4, 7),
    LocalDate(2023, 4, 10),
    LocalDate(2023, 5, 1),
    LocalDate(2023, 5, 8),
    LocalDate(2023, 5, 29),
    LocalDate(2023, 8, 28),
    LocalDate(2023, 9, 19),
    LocalDate(2023, 12, 25),
    LocalDate(2023, 12, 26),
  )

  def dates2024: Seq[LocalDate] = Seq(
    LocalDate(2024, 1, 2),
    LocalDate(2024, 3, 29),
    LocalDate(2024, 4, 1),
    LocalDate(2024, 5, 6),
    LocalDate(2024, 5, 27),
    LocalDate(2024, 8, 26),
    LocalDate(2024, 12, 25),
    LocalDate(2024, 12, 26),
  )

  def dates2025: Seq[LocalDate] = Seq(
    LocalDate(2025, 1, 2),
    LocalDate(2025, 4, 18),
    LocalDate(2025, 4, 21),
    LocalDate(2025, 5, 5),
    LocalDate(2025, 5, 26),
    LocalDate(2025, 8, 25),
    LocalDate(2025, 12, 25),
    LocalDate(2025, 12, 26),
  )
}
