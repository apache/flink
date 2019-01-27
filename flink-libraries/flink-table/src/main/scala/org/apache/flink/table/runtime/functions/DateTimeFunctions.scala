/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.table.runtime.functions

import java.math.{RoundingMode, BigDecimal => JBigDecimal}
import java.sql.{Time, Timestamp}
import java.text.{ParseException, SimpleDateFormat}
import java.time.temporal.ChronoUnit
import java.time.{Instant, ZonedDateTime}
import java.util.{Date, TimeZone}

import org.apache.calcite.avatica.util.{DateTimeUtils, TimeUnit, TimeUnitRange}
import org.apache.calcite.avatica.util.DateTimeUtils._
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.flink.table.api.TableException
import org.apache.flink.table.dataformat.Decimal
import org.slf4j.LoggerFactory

class DateTimeFunctions {}

object DateTimeFunctions {
  private val LOG = LoggerFactory.getLogger(DateTimeFunctions.getClass)

  /** The julian date of the epoch, 1970-01-01. */
  val EPOCH_JULIAN = 2440588

  val MILLIS_PER_HOUR: Int = 3600 * 1000
  val MILLIS_PER_DAY: Int = 24 * 3600 * 1000

  val FORMATS = Array("yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd HH:mm:ss.S",
    "yyyy-MM-dd HH:mm:ss.SS",
    "yyyy-MM-dd HH:mm:ss.SSS"
  )

  private val TIMEZONE_CACHE = new ThreadLocalCache[String, TimeZone](64) {
    protected override def getNewInstance(tz: String): TimeZone = {
      TimeZone.getTimeZone(tz)
    }
  }

  // (format, zoneID)
  private val FORMATTER_TIMEZONE_CACHE =
    new ThreadLocalCache[(String, TimeZone), SimpleDateFormat](64) {
      protected override def getNewInstance(t: (String, TimeZone)): SimpleDateFormat = {
        val sdf = new SimpleDateFormat(t._1)
        sdf.setTimeZone(t._2)
        sdf
      }
    }

  def dateFormat(ts: Long, formatString: String, tz: TimeZone): String = {
    try {
      val toFormatter = FORMATTER_TIMEZONE_CACHE.get((formatString, tz))
      val datetime = new Date(ts)
      toFormatter.format(datetime)
    }
    catch {
      case e: ParseException =>
        LOG.error(
          "Exception when formatting: " + ts, e)
        null
      case e: IllegalArgumentException =>
        LOG.error(
          "Exception when parse format string: " + formatString, e)
        null
    }
  }

  def dateFormat(dateText: String, fromFormat: String, toFormat: String, tz: TimeZone): String = {
    try {
      val fromFormatter = FORMATTER_TIMEZONE_CACHE.get((fromFormat, tz))
      val toFormatter = FORMATTER_TIMEZONE_CACHE.get((toFormat,tz))
      toFormatter.format(fromFormatter.parse(dateText))
    }
    catch {
      case e: ParseException =>
        LOG.error(
          "Exception when formatting: " +
            dateText + ", from: " + fromFormat + ", to: " + toFormat, e)
        null
      case e: IllegalArgumentException =>
        LOG.error(
          "Exception when parse format string: " +
            dateText + ", from: " + fromFormat + ", to: " + toFormat, e)
        null
    }
  }

  def dateFormat(dateText: String, toFormat: String, tz: TimeZone): String =
    dateFormat(dateText, "yyyy-MM-dd HH:mm:ss", toFormat, tz)

  def toDate(v: Int): Int = v

  def toTimestamp(v: Long): Long = v

  // Runtime timestamp unit is milliseconds, so keep sync with it.
  def toTimestamp(v: Double): java.lang.Long = {
    if (v == null) {
      null
    } else {
      v.longValue()
    }
  }

  def toTimestamp(v: Decimal): java.lang.Long = {
    if (v == null) {
      null
    } else {
      Decimal.castToLong(v)
    }
  }

  /**
    * Returns the epoch days(days since 1970-01-01
    * @param dateStr
    * @param fromFormat
    * @return
    */
  def toDate(dateStr: String, fromFormat: String): Int = {
    // It is OK to use UTC, we just want get the epoch days
    // TODO  use offset, better performance
    val ts      = parseToTimeMillis(dateStr, fromFormat, TimeZone.getTimeZone("UTC"))
    val zoneId = java.time.ZoneId.of("UTC")
    val instant = Instant.ofEpochMilli(ts)
    val zdt     = ZonedDateTime.ofInstant(instant, zoneId)
    ymdToUnixDate(zdt.getYear, zdt.getMonthValue, zdt.getDayOfMonth)
  }

  def toTimestamp(dateText: String, tz: TimeZone): java.lang.Long = {
    // default format "yyyy-MM-dd HH:mm:ss[.SSSSSS]"
    val option = toTimestamp(dateText, "yyyy-MM-dd HH:mm:ss", tz)
    option match {
      case null => null
      case _ => option + getMillis(dateText)
    }
  }

  def toTimestamp(dateText: String, fromFormat: String, tz: TimeZone): java.lang.Long = {
    val fromFormatter = FORMATTER_TIMEZONE_CACHE.get((fromFormat, tz))
    try {
      fromFormatter.parse(dateText).getTime
    } catch {
      case _: ParseException => null
    }
  }

  /**
    *
    * @param dateText
    * @param fromFormat
    * @param tzString
    * @return
    * TODO:  refine this. to generate timezone code for it, don't store timezone in HashMap.
    */
  def toTimestampTz(dateText: String, fromFormat: String, tzString: String): Long = {
    val tz = TIMEZONE_CACHE.get(tzString)
    val fromFormatter = FORMATTER_TIMEZONE_CACHE.get((fromFormat, tz))
    fromFormatter.parse(dateText).getTime
  }

  def toTimestampTz(dateText: String, tzString: String): Long = {
    val tz = TIMEZONE_CACHE.get(tzString)
    val fromFormatter = FORMATTER_TIMEZONE_CACHE.get(("yyyy-MM-dd HH:mm:ss", tz))
    fromFormatter.parse(dateText).getTime
  }

  def dateFormatTz(ts: Long, formatString: String, tzString: String): String = {
    val tz = TIMEZONE_CACHE.get(tzString)
    dateFormat(ts, formatString, tz)
  }

  def dateFormatTz(ts: Long,  tzString: String): String = {
    val tz = TIMEZONE_CACHE.get(tzString)
    dateFormat(ts, "yyyy-MM-dd HH:mm:ss", tz)
  }

  def convertTz(dateText: String, format: String, tzFrom: String, tzTo: String): String = {
    dateFormatTz(toTimestampTz(dateText, format, tzFrom), tzTo)
  }

  def convertTz(dateText: String, tzFrom: String, tzTo: String): String = {
    dateFormatTz(toTimestampTz(dateText, "yyyy-MM-dd HH:mm:ss", tzFrom), tzTo)
  }

  def fromTimestamp(ts: Long): Long = {
    ts
  }

  def extractYearMonth(range: TimeUnitRange, v: Int): Int = {
    range match {
      case TimeUnitRange.YEAR => v / 12
      case TimeUnitRange.MONTH => v % 12
      case TimeUnitRange.QUARTER => (v % 12 + 2) / 3
      case _ => throw new AssertionError(range)
    }
  }

  def extractFromDate(range: TimeUnitRange, ts: Long): Long = {
    // for INTERVAL_MILLIS
    //TODO
    convertExtract(range, ts, SqlTypeName.DATE, TimeZone.getTimeZone("UTC"))
  }

  def unixTimeExtract(range: TimeUnitRange, ts: Int): Long = {
    DateTimeUtils.unixTimeExtract(range, ts)
  }

  def extractFromTimestamp(range: TimeUnitRange, ts: Long, tz: TimeZone): Long = {
    convertExtract(range, ts, SqlTypeName.TIMESTAMP, tz)
  }

  /**
    * refer to Calcite-1.13 [[org.apache.calcite.sql2rel.StandardConvertletTable#convertExtract]]
    * and [[org.apache.flink.table.expressions.Extract#convertExtract]]
    */
  private def convertExtract(range: TimeUnitRange,
                             ts: Long,
                             sqlTypeName: SqlTypeName,
                             tz: TimeZone): Long = {
    val unit = range.startUnit
    val offset = tz.getOffset(ts)
    val utcTz = ts + offset

    unit match {
      case TimeUnit.MILLENNIUM | TimeUnit.CENTURY | TimeUnit.YEAR | TimeUnit.QUARTER |
           TimeUnit.MONTH | TimeUnit.DAY | TimeUnit.DOW | TimeUnit.DOY | TimeUnit.WEEK =>
        sqlTypeName match {
          case SqlTypeName.TIMESTAMP =>
            val d = divide(utcTz, TimeUnit.DAY.multiplier)
            return unixDateExtract(range, d)
          // fall through
          case SqlTypeName.DATE =>
            return divide(utcTz, TimeUnit.DAY.multiplier)
          case _ =>
            // TODO support it
            throw new TableException(s"$sqlTypeName is unsupported now.")
        }
      case TimeUnit.DECADE =>
        // TODO support it
        throw new TableException("DECADE is unsupported now.")
      case TimeUnit.EPOCH =>
        // TODO support it
        throw new TableException("EPOCH is unsupported now.")
      case _ => // do nothing
    }

    var res = mod(utcTz, getFactor(unit))
    if (unit eq TimeUnit.QUARTER) {
      res = res - 1
    }
    res = divide(res, unit.multiplier)
    if (unit eq TimeUnit.QUARTER) {
      res = res + 1
    }
    res
  }

  /**
    * refer to Calcite-1.13 [[org.apache.calcite.sql2rel.StandardConvertletTable#divide]]
    */
  private def divide(res: Long, value: JBigDecimal): Long = {
    if (value == JBigDecimal.ONE) {
      res
    } else if (value.compareTo(JBigDecimal.ONE) < 0 && value.signum == 1) {
      val reciprocal = JBigDecimal.ONE.divide(value, RoundingMode.UNNECESSARY)
      reciprocal.multiply(JBigDecimal.valueOf(res)).longValue()
    } else {
      res / value.longValue()
    }
  }

  /**
    * refer to Calcite-1.13 [[org.apache.calcite.sql2rel.StandardConvertletTable#mod]]
    */
  private def mod(res: Long, value: JBigDecimal): Long = {
    if (value == JBigDecimal.ONE) {
      res
    } else {
      res % value.longValue()
    }
  }

  /**
    * refer to Calcite-1.13 [[org.apache.calcite.sql2rel.StandardConvertletTable#getFactor]]
    */
  private def getFactor(unit: TimeUnit): JBigDecimal = unit match {
    case TimeUnit.DAY => JBigDecimal.ONE
    case TimeUnit.HOUR => TimeUnit.DAY.multiplier
    case TimeUnit.MINUTE => TimeUnit.HOUR.multiplier
    case TimeUnit.SECOND => TimeUnit.MINUTE.multiplier
    case TimeUnit.YEAR => JBigDecimal.ONE
    case TimeUnit.MONTH => TimeUnit.YEAR.multiplier
    case TimeUnit.QUARTER => TimeUnit.YEAR.multiplier
    case TimeUnit.YEAR | TimeUnit.DECADE |
         TimeUnit.CENTURY | TimeUnit.MILLENNIUM => JBigDecimal.ONE
    case _ => throw new IllegalArgumentException("Invalid start unit.")
  }

  /**
    * This version works like java.sql.Timestamp, but Timestamp uses the
    * default TimeZone. Here, we use the specified time zone.
    *
    * @param dt  datetime string format is: yyyy-MM-dd HH:mm:ss[.SSSSSS]
    * @return the millis since 1970-01-01 00:00:00 UTC
    */
  def parseToTimeMillis(dt: String, tz: TimeZone): Long = {
    val fmt = if (dt.length <= 10) {
      "yyyy-MM-dd"
    } else {
      "yyyy-MM-dd HH:mm:ss"
    }
    parseToTimeMillis(dt, fmt, tz) + getMillis(dt)
  }

  def parseToTimeMillis(dt: String, fmt: String, tz: TimeZone): Long = {
    val millis = try {
      val sdp = FORMATTER_TIMEZONE_CACHE.get((fmt, tz))
      val d = sdp.parse(dt)
      d.getTime
    } catch {
      case _: Exception =>
        LOG.error("Exception when parse date string in unixTimestamp:" + dt + "," + fmt)
        Long.MinValue
    }
    millis
  }

  private def getMillis(dt: String): Int = {
    val millis = dt.length match {
      case 19 => 0   // "1999-12-31 12:34:56"

      // "1999-12-31 12:34:56.7"
      case 21 => dt.substring(20).toInt * 100

      // "1999-12-31 12:34:56.78"
      case 22 => dt.substring(20).toInt * 10

      // "1999-12-31 12:34:56.123" ~ "1999-12-31 12:34:56.123456"
      case len if len >= 23 && len <= 26 => dt.substring(20, 23).toInt

      case _ => 0
    }
    millis
  }

  /**
    * Timestamp to string
    * Helper for CAST(timestamp as varchar(3))
    *
    * to replace: DateTimeUtils.unixTimestampToString(long timestamp, int precision)
    */
  def timestampToStringPrecision(ts: Long, precision: Int, tz: TimeZone): String = {
    val fmt = if (precision <= 3 && precision >= 0) {
      FORMATS(precision)
    }
    else {
      FORMATS(3)
    }
    dateFormat(ts, fmt, tz)
  }


  def timestampFloor(unit: TimeUnitRange, ts: Long, tz: TimeZone): Long = {
    // assume that we are at UTC timezone, just for algorithm performance
    val offset = tz.getOffset(ts)
    val utcTs = ts + offset

    unit match {
      case TimeUnitRange.HOUR =>
        floor(utcTs, MILLIS_PER_HOUR) - offset
      case TimeUnitRange.DAY =>
        floor(utcTs, MILLIS_PER_DAY) - offset
      case TimeUnitRange.MONTH | TimeUnitRange.YEAR | TimeUnitRange.QUARTER =>
        val days = (utcTs / MILLIS_PER_DAY + EPOCH_JULIAN).asInstanceOf[Int]
        julianDateFloor(unit, days, floor = true) * MILLIS_PER_DAY - offset
      case _ =>
        // for MINUTE and SECONDS etc...,
        // it is more effective to use arithmetic Method
        throw new AssertionError(unit)
    }
  }

  /**
    * Keep the algorithm consistent with DateTime.julianDateFloor, but here
    * we take time zone into account.
    *
    * @param unit
    * @param ts
    * @param tz
    * @return
    */
  def timestampCeil(unit: TimeUnitRange, ts: Long, tz: TimeZone): Long = {
    // assume that we are at UTC timezone, just for algorithm performance
    val offset = tz.getOffset(ts)
    val utcTs = ts + offset

    unit match {
      case TimeUnitRange.HOUR =>
        ceil(utcTs, MILLIS_PER_HOUR) - offset
      case TimeUnitRange.DAY =>
        ceil(utcTs, MILLIS_PER_DAY) - offset
      case TimeUnitRange.MONTH | TimeUnitRange.YEAR | TimeUnitRange.QUARTER =>
        val days = (utcTs / MILLIS_PER_DAY + EPOCH_JULIAN).asInstanceOf[Int]
        julianDateFloor(unit, days, floor = false) * MILLIS_PER_DAY - offset
      case _ =>
        // for MINUTE and SECONDS etc...,
        // it is more effective to use arithmetic Method
        throw new AssertionError(unit)
    }
  }

  private def floor(a: Long, b: Long): Long = {
    val r = a % b
    if (r < 0) {
      a - r - b
    } else {
      a - r
    }
  }

  private def ceil(a: Long, b: Long): Long = {
    val r = a % b
    if (r > 0) {
      a - r + b
    } else {
      a - r
    }
  }

  private def julianDateFloor(range: TimeUnitRange, julian: Int, floor: Boolean): Long = {
    // Algorithm the book "Astronomical Algorithms" by Jean Meeus, 1998
    var b = 0
    var c = 0
    if (julian > 2299160) {
      val a = julian + 32044
      b = (4 * a + 3) / 146097
      c = a - b * 146097 / 4
    }
    else {
      b = 0
      c = julian + 32082
    }
    val d = (4 * c + 3) / 1461
    val e = c - (1461 * d) / 4
    val m = (5 * e + 2) / 153
    val day = e - (153 * m + 2) / 5 + 1
    var month = m + 3 - 12 * (m / 10)
    var quarter = (month + 2) / 3
    var year = b * 100 + d - 4800 + (m / 10)
    range match {
      case TimeUnitRange.YEAR =>
        if (!floor && (month > 1 || day > 1)) year += 1
        ymdToUnixDate(year, 1, 1)
      case TimeUnitRange.MONTH =>
        if (!floor && day > 1) month += 1
        ymdToUnixDate(year, month, 1)
      case TimeUnitRange.QUARTER =>
        if (!floor && (month > 1 || day > 1)) quarter += 1
        ymdToUnixDate(year, quarter * 3 - 2, 1)
      case _ =>
        throw new AssertionError(range)
    }
  }

  /**
    * Returns current timestamp(count by seconds).
    *
    * @return current timestamp.
    */
  def now: Long = {
    val date = new Date
    date.getTime / 1000
  }

  /**
    * Returns current timestamp(count by seconds) with offset.
    *
    * @param offset value(count by seconds).
    * @return current timestamp with offset.
    */
  def now(offset: Long): Long = {
    val date: Date = new Date
    date.getTime / 1000 + offset
  }

  def unixTimestamp: Long = (new Date).getTime / 1000

  def unixTimestamp(dateString: String, dateFormat: String, tz: TimeZone): Long = {
    val ts = parseToTimeMillis(dateString, dateFormat, tz)
    if (ts == Long.MinValue) {
      Long.MinValue
    } else {
      ts / 1000
    }
  }

  def unixTimestamp(dateString: String, tz: TimeZone): Long =
    unixTimestamp(dateString, "yyyy-MM-dd HH:mm:ss", tz)

  /**
    * Convert Timestamp to bigint as seconds.
    *
    * @param t Input timestamp
    * @return seconds
    */
  def unixTimestamp(t: Long): Long = new Timestamp(t).getTime / 1000


  /**
    * Convert unix timestamp to datetime string.
    * If accept any null arguments, return null.
    *
    * @param unixtime unix timestamp.
    * @param format   datetime string format.
    * @return datetime string.
    */
  def fromUnixtime(unixtime: Long, format: String, tz: TimeZone): String = {
    if (unixtime == null || format == null) {
      return null
    }
    try {
      val formatter = FORMATTER_TIMEZONE_CACHE.get((format, tz))
      val date = new Date(unixtime * 1000)
      formatter.format(date);
    } catch {
      case e: ParseException =>
        LOG.error("exception when formatting string: " + unixtime, e)
        null
      case e: IllegalArgumentException =>
        LOG.error("exception when parse format string: " + format, e)
        null
    }
  }

  /**
    * Convert unix timestamp to datetime string
    * with format yyyy-MM-dd HH:mm:ss
    * If accept any null arguments, return null.
    *
    * @param unixtime unix timestamp.
    * @return datetime string.
    */
  def fromUnixtime(unixtime: Long, tz: TimeZone): String = {
    fromUnixtime(unixtime, "yyyy-MM-dd HH:mm:ss", tz)
  }

  /**
    * Convert unix timestamp to datetime string
    * with format yyyy-MM-dd HH:mm:ss
    * If accept any null arguments, return null.
    *
    * @param unixtime unix timestamp.
    * @return datetime string.
    */
  def fromUnixtime(unixtime: Double, tz: TimeZone): String = {
    if (unixtime == null) {
      return null
    }
    fromUnixtime(unixtime.longValue(), "yyyy-MM-dd HH:mm:ss", tz)
  }

  /**
    * Convert unix timestamp to datetime string
    * with format yyyy-MM-dd HH:mm:ss
    * If accept any null arguments, return null.
    *
    * @param unixtime unix timestamp.
    * @return datetime string.
    */
  def fromUnixtime(unixtime: Decimal, tz: TimeZone): String = {
    if (unixtime == null) {
      return null
    }
    fromUnixtime(Decimal.castToLong(unixtime), "yyyy-MM-dd HH:mm:ss", tz)
  }


  /**
    * NOTE:
    * (1). JDK relies on the operating system clock for time.
    * Each operating system has its own method of handling date changes such as
    * leap seconds(e.g. OS will slow down the  clock to accommodate for this).
    * (2). DST(Daylight Saving Time) is a legal issue, governments changed it
    * over time. Some days are NOT exactly 24 hours long, it could be 23/25 hours
    * long on the first or last day of daylight saving time.
    * JDK can handle DST correctly.
    * TODO:
    *       carefully written algorithm can improve the performance
    */
  def dateDiff(t1: Long, t2: Long, tz: TimeZone): Int = {
    val zoneId = tz.toZoneId
    val ld1 = Instant.ofEpochMilli(t1).atZone(zoneId).toLocalDate
    val ld2 = Instant.ofEpochMilli(t2).atZone(zoneId).toLocalDate
    ChronoUnit.DAYS.between(ld2, ld1).asInstanceOf[Int]
  }

  def dateDiff(t1: String, t2: Long, tz: TimeZone): Int = {
    val zoneId = tz.toZoneId
    val lt1 = DateTimeFunctions.parseToTimeMillis(t1, tz)
    val ld1 = Instant.ofEpochMilli(lt1).atZone(zoneId).toLocalDate
    val ld2 = Instant.ofEpochMilli(t2).atZone(zoneId).toLocalDate
    ChronoUnit.DAYS.between(ld2, ld1).asInstanceOf[Int]
  }

  def dateDiff(t1: Long, t2: String, tz: TimeZone): Int = {
    val zoneId = tz.toZoneId
    val lt2 = DateTimeFunctions.parseToTimeMillis(t2, tz)
    val ld1 = Instant.ofEpochMilli(t1).atZone(zoneId).toLocalDate
    val ld2 = Instant.ofEpochMilli(lt2).atZone(zoneId).toLocalDate
    ChronoUnit.DAYS.between(ld2, ld1).asInstanceOf[Int]
  }

  def dateDiff(t1: String, t2: String, tz: TimeZone): Int = {
    val zoneId = tz.toZoneId
    val lt1 = DateTimeFunctions.parseToTimeMillis(t1, tz)
    val lt2 = DateTimeFunctions.parseToTimeMillis(t2, tz)
    val ld1 = Instant.ofEpochMilli(lt1).atZone(zoneId).toLocalDate
    val ld2 = Instant.ofEpochMilli(lt2).atZone(zoneId).toLocalDate

    ChronoUnit.DAYS.between(ld2, ld1).asInstanceOf[Int]
  }


  /**
    * Do subtraction on date string
    * If accept any null arguments, return null.
    *
    * @param dateStr formatted date string.
    *                    support format: any string start with yyyy-MM-dd
    * @param days        days count you want to subtract.
    * @return datetime string.
    */
  def dateSub(dateStr: String, days: Int, tz: TimeZone): String = {
    if (dateStr == null) {
      return null
    }
    val ts = parseToTimeMillis(dateStr, tz)
    if (ts == Long.MinValue) return null

    // any parser error, return null. same as the old version
    if (ts == Long.MinValue) {
      return null
    }

    val zoneId  = tz.toZoneId
    val instant = Instant.ofEpochMilli(ts)
    val zdt     = ZonedDateTime.ofInstant(instant, zoneId)

    dateFormat(zdt.minusDays(days).toInstant.toEpochMilli, "yyyy-MM-dd", tz)
  }

  /**
    * Do subtraction on timestamp
    * If accept any null arguments, return null.
    *
    * @param ts    the timestamp.
    * @param days days count you want to subtract.
    * @return datetime string.
    */
  def dateSub(ts: Long, days: Int, tz: TimeZone): String = {
    val zoneId  = tz.toZoneId
    val instant = Instant.ofEpochMilli(ts)
    val zdt     = ZonedDateTime.ofInstant(instant, zoneId)
    dateFormat(zdt.minusDays(days).toInstant.toEpochMilli, "yyyy-MM-dd", tz)
  }

  /**
    * Do addition on date string
    * If accept any null arguments, return null.
    *
    * @param dateStr formatted date string.
    *                    support format: any string start with yyyy-MM-dd
    * @param days        days count you want to add.
    * @return datetime string.
    */
  def dateAdd(dateStr: String, days: Int, tz: TimeZone): String = {
    if (dateStr == null) {
      return null
    }
    val ts      = parseToTimeMillis(dateStr, tz)
    if (ts == Long.MinValue) return null
    val zoneId  = tz.toZoneId
    val instant = Instant.ofEpochMilli(ts)
    val zdt     = ZonedDateTime.ofInstant(instant, zoneId)
    dateFormat(zdt.plusDays(days).toInstant.toEpochMilli, "yyyy-MM-dd", tz)
  }

  /**
    * Do addition on timestamp
    * If accept any null arguments, return null.
    *
    * @param ts    the timestamp.
    * @param days days count you want to add.
    * @return datetime string.
    */
  def dateAdd(ts: Long, days: Int, tz: TimeZone): String = {
    val zoneId  = tz.toZoneId
    val instant = Instant.ofEpochMilli(ts)
    val zdt     = ZonedDateTime.ofInstant(instant, zoneId)
    dateFormat(zdt.plusDays(days).toInstant.toEpochMilli, "yyyy-MM-dd", tz)
  }

  def internalToDate(v: Int, tz: TimeZone): java.sql.Date = {
    // Note that, in this case, can't handle Daylight Saving Time
    val t = v * DateTimeUtils.MILLIS_PER_DAY
    val offset = tz.getOffset(t)

    // the local instant
    new java.sql.Date(t - offset)
  }

  def internalToTime(v: Int, tz: TimeZone): java.sql.Time =  {
    // Note that, in this case, can't handle Daylight Saving Time
    val offset = tz.getOffset(v)
    new java.sql.Time(v - offset)
  }


  def timeToInternal(v: Time, tz: TimeZone): Int =  {
    val offset = tz.getOffset(v.getTime)
    val ts = v.getTime + offset
    (ts % DateTimeUtils.MILLIS_PER_DAY).toInt
  }

  def dateToInternal(v: Date, tz: TimeZone): Int = {
    val offset = tz.getOffset(v.getTime)
    val ts = v.getTime + offset
    (ts / DateTimeUtils.MILLIS_PER_DAY).toInt
  }


  // Helper functions
  private def ymdToUnixDate(year: Int, month: Int, day: Int) = {
    val julian = ymdToJulian(year, month, day)
    julian - EPOCH_JULIAN
  }

  private def ymdToJulian(year: Int, month: Int, day: Int): Int = {
    val a = (14 - month) / 12
    val y = year + 4800 - a
    val m = month + 12 * a - 3
    var j = day + (153 * m + 2) / 5 + 365 * y + y / 4 - y / 100 + y / 400 - 32045
    if (j < 2299161) j = day + (153 * m + 2) / 5 + 365 * y + y / 4 - 32083
    j
  }
}
