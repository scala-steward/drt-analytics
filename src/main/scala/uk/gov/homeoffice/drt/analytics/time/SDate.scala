package uk.gov.homeoffice.drt.analytics.time

import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.ISODateTimeFormat
import org.slf4j.{Logger, LoggerFactory}
import uk.gov.homeoffice.drt.time.{LocalDate, SDateLike, UtcDate}

import scala.util.Try

case class SDate(dateTime: DateTime) extends SDateLike {
  import uk.gov.homeoffice.drt.analytics.time.SDate.implicits._

  val europeLondon: DateTimeZone = DateTimeZone.forID("Europe/London")
  val utc: DateTimeZone = DateTimeZone.forID("UTC")

  override def getDayOfWeek: Int = dateTime.getDayOfWeek

  override def getFullYear: Int = dateTime.getYear

  override def getMonth: Int = dateTime.getMonthOfYear

  override def getDate: Int = dateTime.getDayOfMonth

  override def getHours: Int = dateTime.getHourOfDay

  override def getMinutes: Int = dateTime.getMinuteOfHour

  override def getSeconds: Int = dateTime.getSecondOfMinute

  override def addDays(daysToAdd: Int): SDate = dateTime.plusDays(daysToAdd)

  override def addMonths(monthsToAdd: Int): SDate = dateTime.plusMonths(monthsToAdd)

  override def addHours(hoursToAdd: Int): SDate = dateTime.plusHours(hoursToAdd)

  override def addMinutes(mins: Int): SDate = dateTime.plusMinutes(mins)

  override def addMillis(millisToAdd: Int): SDate = dateTime.plusMillis(millisToAdd)

  override def millisSinceEpoch: Long = dateTime.getMillis

  override def toISOString: String = SDate.jodaSDateToIsoString(dateTime)

  override def getZone: String = dateTime.getZone.getID

  override def getTimeZoneOffsetMillis: Long = dateTime.getZone.getOffset(millisSinceEpoch)

  override def getLocalLastMidnight: SDate = {
    val localNow = SDate(dateTime, europeLondon)
    SDate(localNow.toIsoMidnight, europeLondon)
  }

  private lazy val toLocal: SDateLike = SDate(dateTime, europeLondon)

  override def toLocalDateTimeString(): String = {
    f"${toLocal.getFullYear()}-${toLocal.getMonth()}%02d-${toLocal.getDate()}%02d ${toLocal.getHours()}%02d:${toLocal.getMinutes()}%02d"
  }

  override def toLocalDate: LocalDate = LocalDate(toLocal.getFullYear(), toLocal.getMonth(), toLocal.getDate())

  override def toUtcDate: UtcDate = {
    val utcLastMidnight = getUtcLastMidnight
    UtcDate(utcLastMidnight.getFullYear(), utcLastMidnight.getMonth(), utcLastMidnight.getDate())
  }

  override def startOfTheMonth(): SDateLike = SDate(dateTime.getFullYear(), dateTime.getMonth(), 1, 0, 0, europeLondon)

  override def getUtcLastMidnight: SDateLike = {
    val utcNow = SDate(dateTime, utc)
    SDate(utcNow.toIsoMidnight, utc)
  }

  override def getLocalNextMidnight: SDateLike = {
    val nextDay = getLocalLastMidnight.addDays(1)
    SDate(nextDay.toIsoMidnight, europeLondon)
  }
}

object SDate {
  val log: Logger = LoggerFactory.getLogger(getClass)

  object implicits {
    implicit def jodaToSDate(dateTime: DateTime): SDate = SDate(dateTime)
  }

  def jodaSDateToIsoString(dateTime: SDate): String = {
    val fmt = ISODateTimeFormat.dateTimeNoMillis()
    val dt = dateTime.asInstanceOf[SDate].dateTime
    fmt.print(dt)
  }

  def apply(dateTime: String): SDate = SDate(new DateTime(dateTime, DateTimeZone.UTC))

  def apply(dateTime: String, timeZone: DateTimeZone): SDate = SDate(new DateTime(dateTime, timeZone))

  def apply(dateTime: SDate, timeZone: DateTimeZone): SDate = SDate(new DateTime(dateTime.millisSinceEpoch, timeZone))

  def apply(millis: Long): SDate = SDate(new DateTime(millis, DateTimeZone.UTC))

  def apply(millis: Long, timeZone: DateTimeZone): SDate = SDate(new DateTime(millis, timeZone))

  def now(): SDate = SDate(new DateTime(DateTimeZone.UTC))

  def now(dtz: DateTimeZone): SDate = SDate(new DateTime(dtz))

  def apply(y: Int,
            m: Int,
            d: Int,
            h: Int,
            mm: Int): SDate = implicits.jodaToSDate(new DateTime(y, m, d, h, mm, DateTimeZone.UTC))

  def apply(y: Int,
            m: Int,
            d: Int,
            h: Int,
            mm: Int,
            dateTimeZone: DateTimeZone): SDate = implicits.jodaToSDate(new DateTime(y, m, d, h, mm, dateTimeZone))

  def tryParseString(dateTime: String): Try[SDate] = Try(apply(dateTime))
}
