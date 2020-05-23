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

package org.apache.flink.table.planner.expressions

import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api._
import org.apache.flink.table.expressions.TimeIntervalUnit
import org.apache.flink.table.planner.expressions.utils.ExpressionTestBase
import org.apache.flink.table.planner.utils.DateTimeTestUtil
import org.apache.flink.table.planner.utils.DateTimeTestUtil._
import org.apache.flink.table.runtime.typeutils.{LegacyInstantTypeInfo, LegacyLocalDateTimeTypeInfo}
import org.apache.flink.table.typeutils.TimeIntervalTypeInfo
import org.apache.flink.types.Row

import org.junit.Test

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.{Instant, ZoneId, ZoneOffset}
import java.util.{Locale, TimeZone}

class TemporalTypesTest extends ExpressionTestBase {

  @Test
  def testTimePointLiterals(): Unit = {
    testAllApis(
      "1990-10-14".toDate,
      "'1990-10-14'.toDate",
      "DATE '1990-10-14'",
      "1990-10-14")

    testTableApi(
      localDate2Literal(localDate("2040-09-11")),
      "'2040-09-11'.toDate",
      "2040-09-11")

    testAllApis(
      "1500-04-30".cast(DataTypes.DATE),
      "'1500-04-30'.cast(SQL_DATE)",
      "CAST('1500-04-30' AS DATE)",
      "1500-04-30")

    testAllApis(
      "15:45:59".toTime,
      "'15:45:59'.toTime",
      "TIME '15:45:59'",
      "15:45:59")

    testTableApi(
      localTime2Literal(DateTimeTestUtil.localTime("00:00:00")),
      "'00:00:00'.toTime",
      "00:00:00")

    testAllApis(
      "1:30:00".cast(DataTypes.TIME),
      "'1:30:00'.cast(SQL_TIME)",
      "CAST('1:30:00' AS TIME)",
      "01:30:00")

    testAllApis(
      "1990-10-14 23:00:00.123".toTimestamp,
      "'1990-10-14 23:00:00.123'.toTimestamp",
      "TIMESTAMP '1990-10-14 23:00:00.123'",
      "1990-10-14 23:00:00.123")

    testTableApi(
      localDateTime2Literal(localDateTime("2040-09-11 00:00:00.000")),
      "2040-09-11 00:00:00")

    testAllApis(
      "1500-04-30 12:00:00".cast(DataTypes.TIMESTAMP(3)),
      "'1500-04-30 12:00:00'.cast(SQL_TIMESTAMP)",
      "CAST('1500-04-30 12:00:00' AS TIMESTAMP(3))",
      "1500-04-30 12:00:00.000")

    testSqlApi(
      "TIMESTAMP '1500-04-30 12:00:00.123456789'",
      "1500-04-30 12:00:00.123456789")

    testSqlApi(
      "TIMESTAMP '1500-04-30 12:00:00.12345678'",
      "1500-04-30 12:00:00.12345678")

    testSqlApi(
      "TIMESTAMP '1500-04-30 12:00:00.123456'",
      "1500-04-30 12:00:00.123456")

    testSqlApi(
      "TIMESTAMP '1500-04-30 12:00:00.1234'",
      "1500-04-30 12:00:00.1234")

    testSqlApi(
      "CAST('1500-04-30 12:00:00.123456789' AS TIMESTAMP(9))",
      "1500-04-30 12:00:00.123456789")

    // by default, it's TIMESTAMP(6)
    testSqlApi(
      "CAST('1500-04-30 12:00:00.123456789' AS TIMESTAMP)",
      "1500-04-30 12:00:00.123456")

    testSqlApi(
      "CAST('1999-9-10 05:20:10.123456' AS TIMESTAMP)",
      "1999-09-10 05:20:10.123456")

    testSqlApi(
      "CAST('1999-9-10' AS TIMESTAMP)",
      "1999-09-10 00:00:00.000000")
  }

  @Test
  def testTimeIntervalLiterals(): Unit = {
    testAllApis(
      1.year,
      "1.year",
      "INTERVAL '1' YEAR",
      "+1-00")

    testAllApis(
      1.month,
      "1.month",
      "INTERVAL '1' MONTH",
      "+0-01")

    testAllApis(
      12.days,
      "12.days",
      "INTERVAL '12' DAY",
      "+12 00:00:00.000")

    testAllApis(
      1.hour,
      "1.hour",
      "INTERVAL '1' HOUR",
      "+0 01:00:00.000")

    testAllApis(
      3.minutes,
      "3.minutes",
      "INTERVAL '3' MINUTE",
      "+0 00:03:00.000")

    testAllApis(
      3.seconds,
      "3.seconds",
      "INTERVAL '3' SECOND",
      "+0 00:00:03.000")

    testAllApis(
      3.millis,
      "3.millis",
      "INTERVAL '0.003' SECOND",
      "+0 00:00:00.003")
  }

  @Test
  def testTimePointInput(): Unit = {
    testAllApis(
      'f0,
      "f0",
      "f0",
      "1990-10-14")

    testAllApis(
      'f1,
      "f1",
      "f1",
      "10:20:45")

    testAllApis(
      'f2,
      "f2",
      "f2",
      "1990-10-14 10:20:45.123")
  }

  @Test
  def testTimeIntervalInput(): Unit = {
    testAllApis(
      'f9,
      "f9",
      "f9",
      "+2-00")

    testAllApis(
      'f10,
      "f10",
      "f10",
      "+0 00:00:12.000")
  }

  @Test
  def testTimePointCasting(): Unit = {
    testAllApis(
      'f0.cast(DataTypes.TIMESTAMP(3)),
      "f0.cast(SQL_TIMESTAMP)",
      "CAST(f0 AS TIMESTAMP(3))",
      "1990-10-14 00:00:00.000")

    testAllApis(
      'f1.cast(DataTypes.TIMESTAMP(3)),
      "f1.cast(SQL_TIMESTAMP)",
      "CAST(f1 AS TIMESTAMP(3))",
      "1970-01-01 10:20:45.000")

    testAllApis(
      'f2.cast(DataTypes.DATE),
      "f2.cast(SQL_DATE)",
      "CAST(f2 AS DATE)",
      "1990-10-14")

    testAllApis(
      'f2.cast(DataTypes.TIME),
      "f2.cast(SQL_TIME)",
      "CAST(f2 AS TIME)",
      "10:20:45")

    testAllApis(
      'f2.cast(DataTypes.TIME),
      "f2.cast(SQL_TIME)",
      "CAST(f2 AS TIME)",
      "10:20:45")

    testTableApi(
      'f7.cast(DataTypes.DATE),
      "f7.cast(SQL_DATE)",
      "2002-11-09")

    testTableApi(
      'f7.cast(DataTypes.DATE).cast(DataTypes.INT),
      "f7.cast(SQL_DATE).cast(INT)",
      "12000")

    testTableApi(
      'f7.cast(DataTypes.TIME),
      "f7.cast(SQL_TIME)",
      "00:00:12")

    testTableApi(
      'f7.cast(DataTypes.TIME).cast(DataTypes.INT),
      "f7.cast(SQL_TIME).cast(INT)",
      "12000")

    testTableApi(
      'f15.cast(DataTypes.TIMESTAMP(3)),
      "f15.cast(SQL_TIMESTAMP)",
      "2016-06-27 07:23:33.000")

    testTableApi(
      'f15.toTimestamp,
      "f15.toTimestamp",
      "2016-06-27 07:23:33.000")

    testTableApi(
      'f8.cast(DataTypes.TIMESTAMP(3)).cast(DataTypes.BIGINT()),
      "f8.cast(SQL_TIMESTAMP).cast(LONG)",
      "1467012213000")

    testSqlApi(
      "CAST(CAST('123' as DECIMAL(5, 2)) AS TIMESTAMP)",
      "1970-01-01 00:02:03.000000")

    testSqlApi(
      "CAST(TIMESTAMP '1970-01-01 00:02:03' AS DECIMAL(5, 2))",
      "123.00")

    testSqlApi(
      "CAST(CAST('123' AS FLOAT) AS TIMESTAMP)",
      "1970-01-01 00:02:03.000000")

    testSqlApi(
      "CAST(TIMESTAMP '1970-01-01 00:02:03' AS FLOAT)",
      "123.0")

    testSqlApi(
      "CAST(CAST('123' AS DOUBLE) AS TIMESTAMP)",
      "1970-01-01 00:02:03.000000")

    testSqlApi(
      "CAST(TIMESTAMP '1970-01-01 00:02:03' AS DOUBLE)",
      "123.0")

    testSqlApi(
      "CAST(TIMESTAMP '1970-01-01 00:02:03' AS TINYINT)",
      "123")

    testSqlApi(
      "CAST(TIMESTAMP '1970-01-01 00:02:03' AS SMALLINT)",
      "123")

    testSqlApi(
      "CAST(TIMESTAMP '1970-01-01 00:02:03' AS INT)",
      "123")

    testSqlApi(
      "CAST(f0 AS TIMESTAMP(3) WITH LOCAL TIME ZONE)",
      "1990-10-14 00:00:00.000")

    testSqlApi(
      "CAST(f1 AS TIMESTAMP(3) WITH LOCAL TIME ZONE)",
      "1970-01-01 10:20:45.000")

    testSqlApi(
      s"CAST(${timestampTz("2018-03-14 01:02:03")} AS TIME)",
      "01:02:03")

    testSqlApi(
      s"CAST(${timestampTz("2018-03-14 01:02:03")} AS DATE)",
      "2018-03-14")
  }

  @Test
  def testTimeIntervalCasting(): Unit = {
    testTableApi(
      'f7.cast(DataTypes.INTERVAL(DataTypes.MONTH)),
      "f7.cast(INTERVAL_MONTHS)",
      "+1000-00")

    testTableApi(
      'f8.cast(DataTypes.INTERVAL(DataTypes.MINUTE())),
      "f8.cast(INTERVAL_MILLIS)",
      "+16979 07:23:33.000")
  }

  @Test
  def testTimePointComparison(): Unit = {
    testAllApis(
      'f0 < 'f3,
      "f0 < f3",
      "f0 < f3",
      "false")

    testAllApis(
      'f0 < 'f4,
      "f0 < f4",
      "f0 < f4",
      "true")

    testAllApis(
      'f1 < 'f5,
      "f1 < f5",
      "f1 < f5",
      "false")

    testAllApis(
      'f0.cast(DataTypes.TIMESTAMP(3)) !== 'f2,
      "f0.cast(SQL_TIMESTAMP) !== f2",
      "CAST(f0 AS TIMESTAMP(3)) <> f2",
      "true")

    testAllApis(
      'f0.cast(DataTypes.TIMESTAMP(3)) === 'f6,
      "f0.cast(SQL_TIMESTAMP) === f6",
      "CAST(f0 AS TIMESTAMP(3)) = f6",
      "true")
  }

  @Test
  def testTimeIntervalArithmetic(): Unit = {

    // interval months comparison

    testAllApis(
      12.months < 24.months,
      "12.months < 24.months",
      "INTERVAL '12' MONTH < INTERVAL '24' MONTH",
      "true")

    testAllApis(
      8.years === 8.years,
      "8.years === 8.years",
      "INTERVAL '8' YEAR = INTERVAL '8' YEAR",
      "true")

    // interval millis comparison

    testAllApis(
      8.millis > 10.millis,
      "8.millis > 10.millis",
      "INTERVAL '0.008' SECOND > INTERVAL '0.010' SECOND",
      "false")

    testAllApis(
      8.millis === 8.millis,
      "8.millis === 8.millis",
      "INTERVAL '0.008' SECOND = INTERVAL '0.008' SECOND",
      "true")

    // interval months addition/subtraction

    testAllApis(
      8.years + 10.months,
      "8.years + 10.months",
      "INTERVAL '8' YEAR + INTERVAL '10' MONTH",
      "+8-10")

    testAllApis(
      2.years - 12.months,
      "2.years - 12.months",
      "INTERVAL '2' YEAR - INTERVAL '12' MONTH",
      "+1-00")

    testAllApis(
      -2.years,
      "-2.years",
      "-INTERVAL '2' YEAR",
      "-2-00")

    // interval millis addition/subtraction

    testAllApis(
      8.hours + 10.minutes + 12.seconds + 5.millis,
      "8.hours + 10.minutes + 12.seconds + 5.millis",
      "INTERVAL '8' HOUR + INTERVAL '10' MINUTE + INTERVAL '12.005' SECOND",
      "+0 08:10:12.005")

    testAllApis(
      1.minute - 10.seconds,
      "1.minute - 10.seconds",
      "INTERVAL '1' MINUTE - INTERVAL '10' SECOND",
      "+0 00:00:50.000")

    testAllApis(
      -10.seconds,
      "-10.seconds",
      "-INTERVAL '10' SECOND",
      "-0 00:00:10.000")

    // addition to date

    // interval millis
    testAllApis(
      'f0 + 2.days,
      "f0 + 2.days",
      "f0 + INTERVAL '2' DAY",
      "1990-10-16")

    // interval millis
    testAllApis(
      30.days + 'f0,
      "30.days + f0",
      "INTERVAL '30' DAY + f0",
      "1990-11-13")

    // interval months
    testAllApis(
      'f0 + 2.months,
      "f0 + 2.months",
      "f0 + INTERVAL '2' MONTH",
      "1990-12-14")

    // interval months
    testAllApis(
      2.months + 'f0,
      "2.months + f0",
      "INTERVAL '2' MONTH + f0",
      "1990-12-14")

    // addition to time

    // interval millis
    testAllApis(
      'f1 + 12.hours,
      "f1 + 12.hours",
      "f1 + INTERVAL '12' HOUR",
      "22:20:45")

    // interval millis
    testAllApis(
      12.hours + 'f1,
      "12.hours + f1",
      "INTERVAL '12' HOUR + f1",
      "22:20:45")

    // addition to timestamp

    // interval millis
    testAllApis(
      'f2 + 10.days + 4.millis,
      "f2 + 10.days + 4.millis",
      "f2 + INTERVAL '10 00:00:00.004' DAY TO SECOND",
      "1990-10-24 10:20:45.127")

    // interval millis
    testAllApis(
      10.days + 'f2 + 4.millis,
      "10.days + f2 + 4.millis",
      "INTERVAL '10 00:00:00.004' DAY TO SECOND + f2",
      "1990-10-24 10:20:45.127")

    // interval months
    testAllApis(
      'f2 + 10.years,
      "f2 + 10.years",
      "f2 + INTERVAL '10' YEAR",
      "2000-10-14 10:20:45.123")

    // interval months
    testAllApis(
      10.years + 'f2,
      "10.years + f2",
      "INTERVAL '10' YEAR + f2",
      "2000-10-14 10:20:45.123")

    // subtraction from date

    // interval millis
    testAllApis(
      'f0 - 2.days,
      "f0 - 2.days",
      "f0 - INTERVAL '2' DAY",
      "1990-10-12")

    // interval millis
    testAllApis(
      -30.days + 'f0,
      "-30.days + f0",
      "INTERVAL '-30' DAY + f0",
      "1990-09-14")

    // interval months
    testAllApis(
      'f0 - 2.months,
      "f0 - 2.months",
      "f0 - INTERVAL '2' MONTH",
      "1990-08-14")

    // interval months
    testAllApis(
      -2.months + 'f0,
      "-2.months + f0",
      "-INTERVAL '2' MONTH + f0",
      "1990-08-14")

    // subtraction from time

    // interval millis
    testAllApis(
      'f1 - 12.hours,
      "f1 - 12.hours",
      "f1 - INTERVAL '12' HOUR",
      "22:20:45")

    // interval millis
    testAllApis(
      -12.hours + 'f1,
      "-12.hours + f1",
      "INTERVAL '-12' HOUR + f1",
      "22:20:45")

    // subtraction from timestamp

    // interval millis
    testAllApis(
      'f2 - 10.days - 4.millis,
      "f2 - 10.days - 4.millis",
      "f2 - INTERVAL '10 00:00:00.004' DAY TO SECOND",
      "1990-10-04 10:20:45.119")

    // interval millis
    testAllApis(
      -10.days + 'f2 - 4.millis,
      "-10.days + f2 - 4.millis",
      "INTERVAL '-10 00:00:00.004' DAY TO SECOND + f2",
      "1990-10-04 10:20:45.119")

    // interval months
    testAllApis(
      'f2 - 10.years,
      "f2 - 10.years",
      "f2 - INTERVAL '10' YEAR",
      "1980-10-14 10:20:45.123")

    // interval months
    testAllApis(
      -10.years + 'f2,
      "-10.years + f2",
      "INTERVAL '-10' YEAR + f2",
      "1980-10-14 10:20:45.123")

    // casting

    testAllApis(
      -'f9.cast(DataTypes.INTERVAL(DataTypes.MONTH)),
      "-f9.cast(INTERVAL_MONTHS)",
      "-CAST(f9 AS INTERVAL YEAR)",
      "-2-00")

    testAllApis(
      -'f10.cast(DataTypes.INTERVAL(DataTypes.MINUTE())),
      "-f10.cast(INTERVAL_MILLIS)",
      "-CAST(f10 AS INTERVAL SECOND)",
      "-0 00:00:12.000")

    // addition/subtraction of interval millis and interval months

    testAllApis(
      'f0 + 2.days + 1.month,
      "f0 + 2.days + 1.month",
      "f0 + INTERVAL '2' DAY + INTERVAL '1' MONTH",
      "1990-11-16")

    testAllApis(
      'f0 - 2.days - 1.month,
      "f0 - 2.days - 1.month",
      "f0 - INTERVAL '2' DAY - INTERVAL '1' MONTH",
      "1990-09-12")

    testAllApis(
      'f2 + 2.days + 1.month,
      "f2 + 2.days + 1.month",
      "f2 + INTERVAL '2' DAY + INTERVAL '1' MONTH",
      "1990-11-16 10:20:45.123")

    testAllApis(
      'f2 - 2.days - 1.month,
      "f2 - 2.days - 1.month",
      "f2 - INTERVAL '2' DAY - INTERVAL '1' MONTH",
      "1990-09-12 10:20:45.123")
  }

  @Test
  def testSelectNullValues(): Unit ={
    testAllApis(
      'f11,
      "f11",
      "f11",
      "null"
    )
    testAllApis(
      'f12,
      "f12",
      "f12",
      "null"
    )
    testAllApis(
      'f13,
      "f13",
      "f13",
      "null"
    )
  }

  @Test
  def testTemporalNullValues() = {
    testAllApis(
      'f13.extract(TimeIntervalUnit.HOUR),
      "f13.extract(HOUR)",
      "extract(HOUR FROM f13)",
      "null"
    )

    testAllApis(
      'f13.floor(TimeIntervalUnit.HOUR),
      "f13.floor(HOUR)",
      "FLOOR(f13 TO HOUR)",
      "null"
    )

    testSqlApi(
      "TO_TIMESTAMP(SUBSTRING('', 2, -1))",
      "null"
    )

    testSqlApi(
      "TO_TIMESTAMP(f14, 'yyyy-mm-dd')",
      "null"
    )
  }

  @Test
  def testDateFormat(): Unit = {
    config.setLocalTimeZone(ZoneId.of("UTC"))

    testSqlApi(
      "DATE_FORMAT('2018-03-14 01:02:03', 'yyyy/MM/dd HH:mm:ss')",
      "2018/03/14 01:02:03")

    testSqlApi(
      "DATE_FORMAT(TIMESTAMP '2018-03-14 01:02:03.123456', 'yyyy/MM/dd HH:mm:ss.SSSSSS')",
      "2018/03/14 01:02:03.123456")

    testSqlApi(
      s"DATE_FORMAT(${timestampTz("2018-03-14 01:02:03")}, 'yyyy-MM-dd HH:mm:ss')",
      "2018-03-14 01:02:03")

    testSqlApi(
      s"DATE_FORMAT(${timestampTz("2018-03-14 01:02:03.123456")}, 'yyyy-MM-dd HH:mm:ss.SSSSSS')",
      "2018-03-14 01:02:03.123456")
  }

  @Test
  def testDateFormatShanghai(): Unit = {
    config.setLocalTimeZone(ZoneId.of("Asia/Shanghai"))

    testSqlApi(
      "DATE_FORMAT('2018-03-14 01:02:03', 'yyyy/MM/dd HH:mm:ss')",
      "2018/03/14 01:02:03")

    testSqlApi(
      "DATE_FORMAT(TIMESTAMP '2018-03-14 01:02:03.123456', 'yyyy/MM/dd HH:mm:ss.SSSSSS')",
      "2018/03/14 01:02:03.123456")

    testSqlApi(
      s"DATE_FORMAT(${timestampTz("2018-03-14 01:02:03")}, 'yyyy-MM-dd HH:mm:ss')",
      "2018-03-14 01:02:03")

    testSqlApi(
      s"DATE_FORMAT(${timestampTz("2018-03-14 01:02:03.123456")}, 'yyyy-MM-dd HH:mm:ss.SSSSSS')",
      "2018-03-14 01:02:03.123456")

  }

  @Test
  def testDateFormatLosAngeles(): Unit = {
    config.setLocalTimeZone(ZoneId.of("America/Los_Angeles"))

    testSqlApi(
      "DATE_FORMAT('2018-03-14 01:02:03', 'yyyy/MM/dd HH:mm:ss')",
      "2018/03/14 01:02:03")

    testSqlApi(
      "DATE_FORMAT(TIMESTAMP '2018-03-14 01:02:03.123456', 'yyyy/MM/dd HH:mm:ss.SSSSSS')",
      "2018/03/14 01:02:03.123456")

    testSqlApi(
      s"DATE_FORMAT(${timestampTz("2018-03-14 01:02:03")}, 'yyyy-MM-dd HH:mm:ss')",
      "2018-03-14 01:02:03")

    testSqlApi(
      s"DATE_FORMAT(${timestampTz("2018-03-14 01:02:03.123456")}, 'yyyy-MM-dd HH:mm:ss.SSSSSS')",
      "2018-03-14 01:02:03.123456")

  }

  @Test
  def testDateAndTime(): Unit = {
    testSqlApi(
      "DATE '2018-03-14'",
      "2018-03-14")
    testSqlApi(
      "TIME '19:01:02.123'",
      "19:01:02")

    // DATE & TIME
    testSqlApi("CAST('12:44:31' AS TIME)", "12:44:31")
    testSqlApi("CAST('2018-03-18' AS DATE)", "2018-03-18")
    testSqlApi("TIME '12:44:31'", "12:44:31")
    testSqlApi("TO_DATE('2018-03-18')", "2018-03-18")

    // EXTRACT
    //testSqlApi("TO_DATE(1521331200)", "2018-03-18")
    testSqlApi("EXTRACT(HOUR FROM TIME '06:07:08')", "6")
    testSqlApi("EXTRACT(MINUTE FROM TIME '06:07:08')", "7")
    //testSqlApi("EXTRACT(HOUR FROM TO_TIME('06:07:08'))", "6")  NO TO_TIME funciton
    testSqlApi("EXTRACT(HOUR FROM CAST('06:07:08' AS TIME))", "6")
    testSqlApi("EXTRACT(DAY FROM CAST('2018-03-18' AS DATE))", "18")
    testSqlApi("EXTRACT(DAY FROM DATE '2018-03-18')", "18")
    testSqlApi("EXTRACT(DAY FROM TO_DATE('2018-03-18'))", "18")
    testSqlApi("EXTRACT(MONTH FROM TO_DATE('2018-01-01'))", "1")
    testSqlApi("EXTRACT(YEAR FROM TO_DATE('2018-01-01'))", "2018")
    testSqlApi("EXTRACT(QUARTER FROM TO_DATE('2018-01-01'))", "1")

    // Floor & Ceil
    // TODO: fix this legacy bug
    //testSqlApi("CEIL(TO_DATE('2018-03-18') TO DAY)", "2018-04-01")
    //testSqlApi("CEIL(TIMESTAMP '2018-03-20 06:10:31' TO HOUR)", "2018-03-20 07:00:00.000")
  }

  private def timestampTz(str: String): String = {
    val precision = extractPrecision(str)
    timestampTz(str, precision)
  }

  private def timestampTz(str: String, precision: Int): String = {
    s"CAST(TIMESTAMP '$str' AS TIMESTAMP($precision) WITH LOCAL TIME ZONE)"
  }

  // According to SQL standard, the length of second fraction is
  // the precision of the Timestamp literal
  private def extractPrecision(str: String): Int = {
    val dot = str.indexOf('.')
    if (dot == -1) {
      0
    } else {
      str.length - dot - 1
    }
  }


  @Test
  def testTemporalShanghai(): Unit = {
    config.setLocalTimeZone(ZoneId.of("Asia/Shanghai"))

    testSqlApi(timestampTz("2018-03-14 19:01:02.123"), "2018-03-14 19:01:02.123")
    testSqlApi(timestampTz("2018-03-14 19:00:00.010"), "2018-03-14 19:00:00.010")

    testSqlApi(
      timestampTz("2018-03-14 19:00:00.010") + " > " + "f25",
      "true")

    testSqlApi(
      s"${timestampTz("2018-03-14 01:02:03.123456789", 9)}",
      "2018-03-14 01:02:03.123456789")

    testSqlApi(
      s"${timestampTz("2018-03-14 01:02:03.123456", 6)}",
      "2018-03-14 01:02:03.123456")


    // DATE_FORMAT
    testSqlApi(
      "DATE_FORMAT('2018-03-14 01:02:03', 'yyyy/MM/dd HH:mm:ss')",
      "2018/03/14 01:02:03")

    testSqlApi(
      s"DATE_FORMAT(${timestampTz("2018-03-14 01:02:03")}, 'yyyy-MM-dd HH:mm:ss')",
      "2018-03-14 01:02:03")

    // EXTRACT
    val extractT1 = timestampTz("2018-03-20 07:59:59")
    testSqlApi(s"EXTRACT(DAY FROM $extractT1)", "20")
    testSqlApi(s"EXTRACT(HOUR FROM $extractT1)", "7")
    testSqlApi(s"EXTRACT(MONTH FROM $extractT1)", "3")
    testSqlApi(s"EXTRACT(YEAR FROM $extractT1)", "2018")
    testSqlApi("EXTRACT(DAY FROM INTERVAL '19 12:10:10.123' DAY TO SECOND(3))", "19")
    testSqlApi("EXTRACT(HOUR FROM TIME '01:02:03')", "1")
    testSqlApi("EXTRACT(DAY FROM INTERVAL '19 12:10:10.123' DAY TO SECOND(3))", "19")

    // FLOOR & CEIL
    testSqlApi("FLOOR(TIME '12:44:31' TO MINUTE)", "12:44:00")
    testSqlApi("FLOOR(TIME '12:44:31' TO HOUR)", "12:00:00")
    testSqlApi("CEIL(TIME '12:44:31' TO MINUTE)", "12:45:00")
    testSqlApi("CEIL(TIME '12:44:31' TO HOUR)", "13:00:00")

    testSqlApi("FLOOR(TIMESTAMP '2018-03-20 06:44:31' TO HOUR)", "2018-03-20 06:00:00")
    testSqlApi("FLOOR(TIMESTAMP '2018-03-20 06:44:31' TO DAY)", "2018-03-20 00:00:00")
    testSqlApi("FLOOR(TIMESTAMP '2018-03-20 00:00:00' TO DAY)", "2018-03-20 00:00:00")
    testSqlApi("FLOOR(TIMESTAMP '2018-04-01 06:44:31' TO MONTH)", "2018-04-01 00:00:00")
    testSqlApi("FLOOR(TIMESTAMP '2018-01-01 06:44:31' TO MONTH)", "2018-01-01 00:00:00")
    testSqlApi("CEIL(TIMESTAMP '2018-03-20 06:44:31' TO HOUR)", "2018-03-20 07:00:00")
    testSqlApi("CEIL(TIMESTAMP '2018-03-20 06:00:00' TO HOUR)", "2018-03-20 06:00:00")
    testSqlApi("CEIL(TIMESTAMP '2018-03-20 06:44:31' TO DAY)", "2018-03-21 00:00:00")
    testSqlApi("CEIL(TIMESTAMP '2018-03-01 00:00:00' TO DAY)", "2018-03-01 00:00:00")
    testSqlApi("CEIL(TIMESTAMP '2018-03-31 00:00:01' TO DAY)", "2018-04-01 00:00:00")
    testSqlApi("CEIL(TIMESTAMP '2018-03-01 21:00:01' TO MONTH)", "2018-03-01 00:00:00")
    testSqlApi("CEIL(TIMESTAMP '2018-03-01 00:00:00' TO MONTH)", "2018-03-01 00:00:00")
    testSqlApi("CEIL(TIMESTAMP '2018-12-02 00:00:00' TO MONTH)", "2019-01-01 00:00:00")
    testSqlApi("CEIL(TIMESTAMP '2018-01-01 21:00:01' TO YEAR)", "2018-01-01 00:00:00")
    testSqlApi("CEIL(TIMESTAMP '2018-01-02 21:00:01' TO YEAR)", "2019-01-01 00:00:00")

    testSqlApi(s"FLOOR(${timestampTz("2018-03-20 06:44:31")} TO HOUR)", "2018-03-20 06:00:00")
    testSqlApi(s"FLOOR(${timestampTz("2018-03-20 06:44:31")} TO DAY)", "2018-03-20 00:00:00")
    testSqlApi(s"FLOOR(${timestampTz("2018-03-20 00:00:00")} TO DAY)", "2018-03-20 00:00:00")
    testSqlApi(s"FLOOR(${timestampTz("2018-04-01 06:44:31")} TO MONTH)", "2018-04-01 00:00:00")
    testSqlApi(s"FLOOR(${timestampTz("2018-01-01 06:44:31")} TO MONTH)", "2018-01-01 00:00:00")
    testSqlApi(s"CEIL(${timestampTz("2018-03-20 06:44:31")} TO HOUR)", "2018-03-20 07:00:00")
    testSqlApi(s"CEIL(${timestampTz("2018-03-20 06:00:00")} TO HOUR)", "2018-03-20 06:00:00")
    testSqlApi(s"CEIL(${timestampTz("2018-03-20 06:44:31")} TO DAY)", "2018-03-21 00:00:00")
    testSqlApi(s"CEIL(${timestampTz("2018-03-1 00:00:00")} TO DAY)", "2018-03-01 00:00:00")
    testSqlApi(s"CEIL(${timestampTz("2018-03-31 00:00:01")} TO DAY)", "2018-04-01 00:00:00")
    testSqlApi(s"CEIL(${timestampTz("2018-03-01 21:00:01")} TO MONTH)", "2018-03-01 00:00:00")
    testSqlApi(s"CEIL(${timestampTz("2018-03-01 00:00:00")} TO MONTH)", "2018-03-01 00:00:00")
    testSqlApi(s"CEIL(${timestampTz("2018-12-02 00:00:00")} TO MONTH)", "2019-01-01 00:00:00")
    testSqlApi(s"CEIL(${timestampTz("2018-01-01 21:00:01")} TO YEAR)", "2018-01-01 00:00:00")
    testSqlApi(s"CEIL(${timestampTz("2018-01-02 21:00:01")} TO YEAR)", "2019-01-01 00:00:00")

    // others
    testSqlApi("QUARTER(DATE '2016-04-12')", "2")
    testSqlApi(
      "(TIME '2:55:00', INTERVAL '1' HOUR) OVERLAPS (TIME '3:30:00', INTERVAL '2' HOUR)",
      "true")
    testSqlApi(
      "CEIL(f17 TO HOUR)",
      "1990-10-14 08:00:00.000"
    )
    testSqlApi(
      "FLOOR(f17 TO DAY)",
      "1990-10-14 00:00:00.000"
    )

    // TIMESTAMP_ADD
    // 1520960523000  "2018-03-14T01:02:03+0800"
    testSqlApi("TIMESTAMPADD(HOUR, +8, TIMESTAMP '2017-11-29 10:58:58.998')",
      "2017-11-29 18:58:58.998")

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    sdf.setTimeZone(TimeZone.getTimeZone("UTC"))
    val currMillis = System.currentTimeMillis()
    val ts = new Timestamp(currMillis)
    val currDateStr = sdf.format(ts)
    testSqlApi("CURRENT_DATE", currDateStr)
    //testSqlApi("CURRENT_TIME", "")
  }

  @Test
  def testDaylightSavingTimeZone(): Unit = {
    // test from MySQL
    // https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_unix-timestamp
    // due to conventions for local time zone changes such as Daylight Saving Time (DST),
    // it is possible for UNIX_TIMESTAMP() to map two values that are distinct in a non-UTC
    // time zone to the same Unix timestamp value
    config.setLocalTimeZone(ZoneId.of("MET")) // Europe/Amsterdam

    testSqlApi("UNIX_TIMESTAMP('2005-03-27 03:00:00')", "1111885200")
    testSqlApi("UNIX_TIMESTAMP('2005-03-27 02:00:00')", "1111885200")
    testSqlApi("FROM_UNIXTIME(1111885200)", "2005-03-27 03:00:00")
  }

  @Test
  def testHourUnitRangoonTimeZone(): Unit = {
    // Asia/Rangoon UTC Offset 6.5
    config.setLocalTimeZone(ZoneId.of("Asia/Rangoon"))

    val t1 = timestampTz("2018-03-20 06:10:31")
    val t2 = timestampTz("2018-03-20 06:00:00")
    // 1521502831000,  2018-03-19 23:40:31 UTC,  2018-03-20 06:10:31 +06:30
    testSqlApi(s"EXTRACT(HOUR FROM $t1)", "6")
    testSqlApi(s"FLOOR($t1 TO HOUR)", "2018-03-20 06:00:00")
    testSqlApi(s"FLOOR($t2 TO HOUR)", "2018-03-20 06:00:00")
    testSqlApi(s"CEIL($t2 TO HOUR)", "2018-03-20 06:00:00")
    testSqlApi(s"CEIL($t1 TO HOUR)", "2018-03-20 07:00:00")
  }

  @Test
  def testNullableCases(): Unit = {
    testSqlApi("CONVERT_TZ(cast(NULL as varchar), 'UTC', 'Asia/Shanghai')",
      nullable)

    testSqlApi("DATE_FORMAT(cast(NULL as varchar), 'yyyy/MM/dd HH:mm:ss')", nullable)

    testSqlApi("FROM_UNIXTIME(cast(NULL as bigInt))", nullable)

    testSqlApi("TO_DATE(cast(NULL as varchar))", nullable)
  }

  @Test
  def testInvalidInputCase(): Unit = {
    val invalidStr = "invalid value"
    testSqlApi(s"DATE_FORMAT('$invalidStr', 'yyyy/MM/dd HH:mm:ss')", nullable)
    testSqlApi(s"TO_TIMESTAMP('$invalidStr', 'yyyy-mm-dd')", nullable)
    testSqlApi(s"TO_DATE('$invalidStr')", nullable)
    testSqlApi(
      s"CONVERT_TZ('$invalidStr', 'UTC', 'Asia/Shanghai')",
      nullable)
  }

  @Test
  def testTypeInferenceWithInvalidInput(): Unit = {
    val invalidStr = "invalid value"
    val cases = Seq(
      s"DATE_FORMAT('$invalidStr', 'yyyy/MM/dd HH:mm:ss')",
      s"TO_TIMESTAMP('$invalidStr', 'yyyy-mm-dd')",
      s"TO_DATE('$invalidStr')",
      s"CONVERT_TZ('$invalidStr', 'UTC', 'Asia/Shanghai')")

    cases.foreach {
      caseExpr =>
        testSqlApi(caseExpr, "null")
    }
  }

  @Test
  def testConvertTZ(): Unit = {
    testSqlApi("CONVERT_TZ('2018-03-14 11:00:00', 'UTC', 'Asia/Shanghai')",
               "2018-03-14 19:00:00")
  }

  @Test
  def testFromUnixTime(): Unit = {
    val sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US)
    val fmt2 = "yyyy-MM-dd HH:mm:ss.SSS"
    val sdf2 = new SimpleDateFormat(fmt2, Locale.US)
    val fmt3 = "yy-MM-dd HH-mm-ss"
    val sdf3 = new SimpleDateFormat(fmt3, Locale.US)

    testSqlApi(
      "from_unixtime(f21)",
      sdf1.format(new Timestamp(44000)))
    testSqlApi(
      s"from_unixtime(f21, '$fmt2')",
      sdf2.format(new Timestamp(44000)))
    testSqlApi(
      s"from_unixtime(f21, '$fmt3')",
      sdf3.format(new Timestamp(44000)))

    testSqlApi(
      "from_unixtime(f22)",
      sdf1.format(new Timestamp(3000)))
    testSqlApi(
      s"from_unixtime(f22, '$fmt2')",
      sdf2.format(new Timestamp(3000)))
    testSqlApi(
      s"from_unixtime(f22, '$fmt3')",
      sdf3.format(new Timestamp(3000)))

    // test with null input
    testSqlApi(
      "from_unixtime(cast(null as int))",
      "null")
  }

  @Test
  def testFromUnixTimeInTokyo(): Unit = {
    config.setLocalTimeZone(ZoneId.of("Asia/Tokyo"))
    val fmt = "yy-MM-dd HH-mm-ss"
    testSqlApi(
      "from_unixtime(f21)",
      "1970-01-01 09:00:44")
    testSqlApi(
      s"from_unixtime(f21, '$fmt')",
      "70-01-01 09-00-44")

    testSqlApi(
      "from_unixtime(f22)",
      "1970-01-01 09:00:03")
    testSqlApi(
      s"from_unixtime(f22, '$fmt')",
      "70-01-01 09-00-03")
  }

  @Test
  def testUnixTimestamp(): Unit = {
    val ts1 = Timestamp.valueOf("2015-07-24 10:00:00.3")
    val ts2 = Timestamp.valueOf("2015-07-25 02:02:02.2")
    val s1 = "2015/07/24 10:00:00.5"
    val s2 = "2015/07/25 02:02:02.6"
    val ss1 = "2015-07-24 10:00:00"
    val ss2 = "2015-07-25 02:02:02"
    val fmt = "yyyy/MM/dd HH:mm:ss.S"

    testSqlApi(s"UNIX_TIMESTAMP('$ss1')", (ts1.getTime / 1000L).toString)
    testSqlApi(s"UNIX_TIMESTAMP('$ss2')", (ts2.getTime / 1000L).toString)
    testSqlApi(s"UNIX_TIMESTAMP('$s1', '$fmt')", (ts1.getTime / 1000L).toString)
    testSqlApi(s"UNIX_TIMESTAMP('$s2', '$fmt')", (ts2.getTime / 1000L).toString)
  }

  @Test
  def testUnixTimestampInTokyo(): Unit = {
    config.setLocalTimeZone(ZoneId.of("Asia/Tokyo"))
    testSqlApi(
      "UNIX_TIMESTAMP('2015-07-24 10:00:00')",
      "1437699600")
    testSqlApi(
      "UNIX_TIMESTAMP('2015/07/24 10:00:00.5', 'yyyy/MM/dd HH:mm:ss.S')",
      "1437699600")
  }

  @Test
  def testHighPrecisionTimestamp(): Unit = {
    // EXTRACT should support millisecond/microsecond/nanosecond
    testSqlApi(
      "EXTRACT(MILLISECOND FROM TIMESTAMP '1970-01-01 00:00:00.123456789')",
      "123")
    testSqlApi(
      "EXTRACT(MICROSECOND FROM TIMESTAMP '1970-01-01 00:00:00.123456789')",
      "123456")
    testSqlApi(
      "EXTRACT(NANOSECOND FROM TIMESTAMP '1970-01-01 00:00:00.123456789')",
      "123456789")

    testSqlApi(
      s"EXTRACT(MILLISECOND FROM ${timestampTz("1970-01-01 00:00:00.123456789", 9)})",
      "123")

    testSqlApi(
      s"EXTRACT(MICROSECOND FROM ${timestampTz("1970-01-01 00:00:00.123456789", 9)})",
      "123456")

    testSqlApi(
      s"EXTRACT(NANOSECOND FROM ${timestampTz("1970-01-01 00:00:00.123456789", 9)})",
      "123456789")


    // TIMESTAMPADD should support microsecond/nanosecond
    // TODO: https://issues.apache.org/jira/browse/CALCITE-3530
    //  (1970-01-01 00:00:00.123455789:TIMESTAMP(9), /INT(*(1:INTERVAL MICROSECOND, 1), 1000))
    // testSqlApi(
    //  "TIMESTAMPADD(MICROSECOND, 1, TIMESTAMP '1970-01-01 00:00:00.123455789')",
    //  "1970-01-01 00:00:00.123456789"
    //)

    // TIMESTAMPDIFF should support microsecond/nanosecond
    // TODO: https://issues.apache.org/jira/browse/CALCITE-3530 and
    //   https://issues.apache.org/jira/browse/CALCITE-3529
    //  *(
    //  CAST(
    //    /INT(
    //      Reinterpret(
    //        -(1970-01-01 00:00:00.123455789:TIMESTAMP(9),
    //          1970-01-01 00:00:00.123456789:TIMESTAMP(9))),
    //      1000)
    //  ):INTEGER NOT NULL,
    //  1000000)
    //testSqlApi(
    //  "TIMESTAMPDIFF(MICROSECOND, TIMESTAMP '1970-01-01 00:00:00.123456789', " +
    //    "TIMESTAMP '1970-01-01 00:00:00.123455789')",
    //  "1")

    // TO_TIMESTAMP should support up to nanosecond
    testSqlApi(
      "TO_TIMESTAMP('1970-01-01 00:00:00.123456789')",
      "1970-01-01 00:00:00.123456789")

    testSqlApi(
      "TO_TIMESTAMP('1970-01-01 00:00:00.12345', 'yyyy-MM-dd HH:mm:ss.SSSSS')",
      "1970-01-01 00:00:00.12345")

    testSqlApi("TO_TIMESTAMP('abc')", "null")

    // TO_TIMESTAMP should complement YEAR/MONTH/DAY/HOUR/MINUTE/SECOND/NANO_OF_SECOND
    testSqlApi(
      "TO_TIMESTAMP('2000020210', 'yyyyMMddHH')",
      "2000-02-02 10:00:00.000")

    testSqlApi(
      "TO_TIMESTAMP('20000202 59:59.1234567', 'yyyyMMdd mm:ss.SSSSSSS')",
      "2000-02-02 00:59:59.1234567")

    testSqlApi(
      "TO_TIMESTAMP('1234567', 'SSSSSSS')",
      "1970-01-01 00:00:00.1234567")

    // CAST between two TIMESTAMPs
    testSqlApi(
      "CAST(TIMESTAMP '1970-01-01 00:00:00.123456789' AS TIMESTAMP(6))",
      "1970-01-01 00:00:00.123456")

    testSqlApi(
      "CAST(TIMESTAMP '1970-01-01 00:00:00.123456789' AS TIMESTAMP(9))",
      "1970-01-01 00:00:00.123456789")

    testSqlApi(
      "CAST(TIMESTAMP '1970-01-01 00:00:00.123456789' AS TIMESTAMP)",
      "1970-01-01 00:00:00.123456")

    testSqlApi(
      "CAST(TO_TIMESTAMP('1970-01-01 00:00:00.123456789') AS TIMESTAMP(0))",
      "1970-01-01 00:00:00")

    testSqlApi(
      s"CAST(${timestampTz("1970-01-01 00:00:00.123456789", 9)} " +
        "AS TIMESTAMP(6) WITH LOCAL TIME ZONE)",
      "1970-01-01 00:00:00.123456"
    )

    testSqlApi(
      s"CAST(f23 AS TIMESTAMP(6))",
      "1970-01-01 00:00:00.123456"
    )

    testSqlApi(
      s"CAST(f23 AS TIMESTAMP(6) WITH LOCAL TIME ZONE)",
      "1970-01-01 00:00:00.123456"
    )

    testSqlApi(
      s"CAST(f24 AS TIMESTAMP(6))",
      "1970-01-01 00:00:00.123456"
    )

    testSqlApi(
      s"CAST(f24 AS TIMESTAMP(6) WITH LOCAL TIME ZONE)",
      "1970-01-01 00:00:00.123456"
    )


    // DATETIME +/- INTERVAL should support nanosecond
    testSqlApi(
      "TIMESTAMP '1970-02-01 00:00:00.123456789' - INTERVAL '1' MONTH",
      "1970-01-01 00:00:00.123456789")

    testSqlApi(
      "TIMESTAMP '1970-02-01 00:00:00.123456789' + INTERVAL '1' MONTH",
      "1970-03-01 00:00:00.123456789")

    testSqlApi(
      "TIMESTAMP '1970-02-01 00:00:00.123456789' - INTERVAL '1' SECOND",
      "1970-01-31 23:59:59.123456789")

    testSqlApi(
      "TIMESTAMP '1970-02-01 00:00:00.123456789' + INTERVAL '1' SECOND",
      "1970-02-01 00:00:01.123456789")

    // TIMESTAMP compare should support nanosecond
    testSqlApi(
      "TIMESTAMP '1970-01-01 00:00:00.123456789' > TIMESTAMP '1970-01-01 00:00:00.123456788'",
      "true")

    testSqlApi(
      "TIMESTAMP '1970-01-01 00:00:00.123456788' < TIMESTAMP '1970-01-01 00:00:00.123456789'",
      "true")


    testSqlApi(
      s"${timestampTz("1970-01-01 00:00:00.123456789", 9)} > " +
        s"${timestampTz("1970-01-01 00:00:00.123456788", 9)}",
      "true")

    testSqlApi(
      s"${timestampTz("1970-01-01 00:00:00.123456788", 9)} < " +
        s"${timestampTz("1970-01-01 00:00:00.123456789", 9)}",
      "true")


    // DATE_FORMAT() should support nanosecond
    testSqlApi(
      "DATE_FORMAT(TIMESTAMP '1970-01-01 00:00:00.123456789', 'yyyy/MM/dd HH:mm:ss.SSSSSSSSS')",
      "1970/01/01 00:00:00.123456789")

    testSqlApi(
      s"DATE_FORMAT(${timestampTz("2018-03-14 01:02:03.123456789", 9)}, " +
        "'yyyy-MM-dd HH:mm:ss.SSSSSSSSS')",
      "2018-03-14 01:02:03.123456789")

  }

  @Test
  def testTimestampDiff(): Unit = {
    testSqlApi(
      "TIMESTAMPDIFF(MONTH, TIMESTAMP '2019-09-01 00:00:00', TIMESTAMP '2020-03-01 00:00:00')",
      "6")
    testSqlApi(
      "TIMESTAMPDIFF(MONTH, TIMESTAMP '2019-09-01 00:00:00', TIMESTAMP '2016-08-01 00:00:00')",
      "-37")
    testSqlApi(
      "TIMESTAMPDIFF(MONTH, DATE '2019-09-01', DATE '2020-03-01')",
      "6")
    testSqlApi(
      "TIMESTAMPDIFF(MONTH, DATE '2019-09-01', DATE '2016-08-01')",
      "-37")
  }

  // ----------------------------------------------------------------------------------------------

  override def testData: Row = {
    val testData = new Row(26)
    testData.setField(0, localDate("1990-10-14"))
    testData.setField(1, DateTimeTestUtil.localTime("10:20:45"))
    testData.setField(2, localDateTime("1990-10-14 10:20:45.123"))
    testData.setField(3, localDate("1990-10-13"))
    testData.setField(4, localDate("1990-10-15"))
    testData.setField(5, DateTimeTestUtil.localTime("00:00:00"))
    testData.setField(6, localDateTime("1990-10-14 00:00:00.0"))
    testData.setField(7, 12000)
    testData.setField(8, 1467012213000L)
    testData.setField(9, 24)
    testData.setField(10, 12000L)
    // null selection test.
    testData.setField(11, null)
    testData.setField(12, null)
    testData.setField(13, null)
    testData.setField(14, null)

    testData.setField(15, 1467012213L)
    testData.setField(16,
      localDateTime("1990-10-14 10:20:45.123").atZone(ZoneId.of("UTC")).toInstant)
    testData.setField(17,
      localDateTime("1990-10-14 00:00:00.0").atZone(ZoneId.of("UTC")).toInstant)
    testData.setField(18, Instant.ofEpochMilli(1521025200000L))
    testData.setField(19, Instant.ofEpochMilli(1520960523000L))
    testData.setField(20, Instant.ofEpochMilli(1520827201000L))
    testData.setField(21, 44L)
    testData.setField(22, 3)
    testData.setField(23, localDateTime("1970-01-01 00:00:00.123456789")
      .atZone(config.getLocalTimeZone).toInstant)
    testData.setField(24, localDateTime("1970-01-01 00:00:00.123456789"))
    testData.setField(25, localDateTime("1970-01-01 00:00:00.123456789").toInstant(ZoneOffset.UTC))
    testData
  }

  override def typeInfo: RowTypeInfo = {
    new RowTypeInfo(
      /* 0 */  Types.LOCAL_DATE,
      /* 1 */  Types.LOCAL_TIME,
      /* 2 */  Types.LOCAL_DATE_TIME,
      /* 3 */  Types.LOCAL_DATE,
      /* 4 */  Types.LOCAL_DATE,
      /* 5 */  Types.LOCAL_TIME,
      /* 6 */  Types.LOCAL_DATE_TIME,
      /* 7 */  Types.INT,
      /* 8 */  Types.LONG,
      /* 9 */  TimeIntervalTypeInfo.INTERVAL_MONTHS,
      /* 10 */ TimeIntervalTypeInfo.INTERVAL_MILLIS,
      /* 11 */ Types.LOCAL_DATE,
      /* 12 */ Types.LOCAL_TIME,
      /* 13 */ Types.LOCAL_DATE_TIME,
      /* 14 */ Types.STRING,
      /* 15 */ Types.LONG,
      /* 16 */ Types.INSTANT,
      /* 17 */ Types.INSTANT,
      /* 18 */ Types.INSTANT,
      /* 19 */ Types.INSTANT,
      /* 20 */ Types.INSTANT,
      /* 21 */ Types.LONG,
      /* 22 */ Types.INT,
      /* 23 */ new LegacyInstantTypeInfo(9),
      /* 24 */ new LegacyLocalDateTimeTypeInfo(9),
      /* 25 */ Types.INSTANT
    )
  }
}
