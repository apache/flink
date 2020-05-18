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

package org.apache.flink.table.expressions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api._
import org.apache.flink.table.expressions.utils.ExpressionTestBase
import org.apache.flink.types.Row

import org.junit.Test

import java.sql.{Date, Time, Timestamp}

class TemporalTypesTest extends ExpressionTestBase {

  @Test
  def testTimePointLiterals(): Unit = {
    testAllApis(
      "1990-10-14".toDate,
      "'1990-10-14'.toDate",
      "DATE '1990-10-14'",
      "1990-10-14")

    testTableApi(
      Date.valueOf("2040-09-11"),
      "'2040-09-11'.toDate",
      "2040-09-11")

    testAllApis(
      "1500-04-30".cast(Types.SQL_DATE),
      "'1500-04-30'.cast(SQL_DATE)",
      "CAST('1500-04-30' AS DATE)",
      "1500-04-30")

    testAllApis(
      "15:45:59".toTime,
      "'15:45:59'.toTime",
      "TIME '15:45:59'",
      "15:45:59")

    testTableApi(
      Time.valueOf("00:00:00"),
      "'00:00:00'.toTime",
      "00:00:00")

    testAllApis(
      "1:30:00".cast(Types.SQL_TIME),
      "'1:30:00'.cast(SQL_TIME)",
      "CAST('1:30:00' AS TIME)",
      "01:30:00")

    testAllApis(
      "1990-10-14 23:00:00.123".toTimestamp,
      "'1990-10-14 23:00:00.123'.toTimestamp",
      "TIMESTAMP '1990-10-14 23:00:00.123'",
      "1990-10-14 23:00:00.123")

    testTableApi(
      Timestamp.valueOf("2040-09-11 00:00:00.000"),
      "'2040-09-11 00:00:00.000'.toTimestamp",
      "2040-09-11 00:00:00.000")

    testAllApis(
      "1500-04-30 12:00:00".cast(Types.SQL_TIMESTAMP),
      "'1500-04-30 12:00:00'.cast(SQL_TIMESTAMP)",
      "CAST('1500-04-30 12:00:00' AS TIMESTAMP)",
      "1500-04-30 12:00:00.000")
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
      'f0.cast(Types.SQL_TIMESTAMP),
      "f0.cast(SQL_TIMESTAMP)",
      "CAST(f0 AS TIMESTAMP)",
      "1990-10-14 00:00:00.000")

    testAllApis(
      'f1.cast(Types.SQL_TIMESTAMP),
      "f1.cast(SQL_TIMESTAMP)",
      "CAST(f1 AS TIMESTAMP)",
      "1970-01-01 10:20:45.000")

    testAllApis(
      'f2.cast(Types.SQL_DATE),
      "f2.cast(SQL_DATE)",
      "CAST(f2 AS DATE)",
      "1990-10-14")

    testAllApis(
      'f2.cast(Types.SQL_TIME),
      "f2.cast(SQL_TIME)",
      "CAST(f2 AS TIME)",
      "10:20:45")

    testAllApis(
      'f2.cast(Types.SQL_TIME),
      "f2.cast(SQL_TIME)",
      "CAST(f2 AS TIME)",
      "10:20:45")

    testTableApi(
      'f7.cast(Types.SQL_DATE),
      "f7.cast(SQL_DATE)",
      "2002-11-09")

    testTableApi(
      'f7.cast(Types.SQL_DATE).cast(Types.INT),
      "f7.cast(SQL_DATE).cast(INT)",
      "12000")

    testTableApi(
      'f7.cast(Types.SQL_TIME),
      "f7.cast(SQL_TIME)",
      "00:00:12")

    testTableApi(
      'f7.cast(Types.SQL_TIME).cast(Types.INT),
      "f7.cast(SQL_TIME).cast(INT)",
      "12000")

    testTableApi(
      'f8.cast(Types.SQL_TIMESTAMP),
      "f8.cast(SQL_TIMESTAMP)",
      "2016-06-27 07:23:33.000")

    testTableApi(
      'f8.cast(Types.SQL_TIMESTAMP).cast(Types.LONG),
      "f8.cast(SQL_TIMESTAMP).cast(LONG)",
      "1467012213000")
  }

  @Test
  def testTimeIntervalCasting(): Unit = {
    testTableApi(
      'f7.cast(Types.INTERVAL_MONTHS),
      "f7.cast(INTERVAL_MONTHS)",
      "+1000-00")

    testTableApi(
      'f8.cast(Types.INTERVAL_MILLIS),
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
      'f0.cast(Types.SQL_TIMESTAMP) !== 'f2,
      "f0.cast(SQL_TIMESTAMP) !== f2",
      "CAST(f0 AS TIMESTAMP) <> f2",
      "true")

    testAllApis(
      'f0.cast(Types.SQL_TIMESTAMP) === 'f6,
      "f0.cast(SQL_TIMESTAMP) === f6",
      "CAST(f0 AS TIMESTAMP) = f6",
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
      -'f9.cast(Types.INTERVAL_MONTHS),
      "-f9.cast(INTERVAL_MONTHS)",
      "-CAST(f9 AS INTERVAL YEAR)",
      "-2-00")

    testAllApis(
      -'f10.cast(Types.INTERVAL_MILLIS),
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
  // ----------------------------------------------------------------------------------------------

  def testData: Row = {
    val testData = new Row(14)
    testData.setField(0, Date.valueOf("1990-10-14"))
    testData.setField(1, Time.valueOf("10:20:45"))
    testData.setField(2, Timestamp.valueOf("1990-10-14 10:20:45.123"))
    testData.setField(3, Date.valueOf("1990-10-13"))
    testData.setField(4, Date.valueOf("1990-10-15"))
    testData.setField(5, Time.valueOf("00:00:00"))
    testData.setField(6, Timestamp.valueOf("1990-10-14 00:00:00.0"))
    testData.setField(7, 12000)
    testData.setField(8, 1467012213000L)
    testData.setField(9, 24)
    testData.setField(10, 12000L)
    // null selection test.
    testData.setField(11, null)
    testData.setField(12, null)
    testData.setField(13, null)
    testData
  }

  def typeInfo: TypeInformation[Any] = {
    new RowTypeInfo(
      Types.SQL_DATE,
      Types.SQL_TIME,
      Types.SQL_TIMESTAMP,
      Types.SQL_DATE,
      Types.SQL_DATE,
      Types.SQL_TIME,
      Types.SQL_TIMESTAMP,
      Types.INT,
      Types.LONG,
      Types.INTERVAL_MONTHS,
      Types.INTERVAL_MILLIS,
      Types.SQL_DATE,
      Types.SQL_TIME,
      Types.SQL_TIMESTAMP).asInstanceOf[TypeInformation[Any]]
  }
}
