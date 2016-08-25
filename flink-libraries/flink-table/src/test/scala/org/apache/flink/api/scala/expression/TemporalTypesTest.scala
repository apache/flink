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

package org.apache.flink.api.scala.expression

import java.sql.{Date, Time, Timestamp}

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.table._
import org.apache.flink.api.table.expressions.utils.ExpressionTestBase
import org.apache.flink.api.table.typeutils.RowTypeInfo
import org.apache.flink.api.table.{Row, Types}
import org.junit.Test

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
      "1500-04-30".cast(Types.DATE),
      "'1500-04-30'.cast(DATE)",
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
      "1:30:00".cast(Types.TIME),
      "'1:30:00'.cast(TIME)",
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
      "2040-09-11 00:00:00.0")

    testAllApis(
      "1500-04-30 12:00:00".cast(Types.TIMESTAMP),
      "'1500-04-30 12:00:00'.cast(TIMESTAMP)",
      "CAST('1500-04-30 12:00:00' AS TIMESTAMP)",
      "1500-04-30 12:00:00.0")
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
      12.day,
      "12.day",
      "INTERVAL '12' DAY",
      "+12 00:00:00.000")

    testAllApis(
      1.hour,
      "1.hour",
      "INTERVAL '1' HOUR",
      "+0 01:00:00.000")

    testAllApis(
      3.minute,
      "3.minute",
      "INTERVAL '3' MINUTE",
      "+0 00:03:00.000")

    testAllApis(
      3.second,
      "3.second",
      "INTERVAL '3' SECOND",
      "+0 00:00:03.000")

    testAllApis(
      3.milli,
      "3.milli",
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
      'f0.cast(Types.TIMESTAMP),
      "f0.cast(TIMESTAMP)",
      "CAST(f0 AS TIMESTAMP)",
      "1990-10-14 00:00:00.0")

    testAllApis(
      'f1.cast(Types.TIMESTAMP),
      "f1.cast(TIMESTAMP)",
      "CAST(f1 AS TIMESTAMP)",
      "1970-01-01 10:20:45.0")

    testAllApis(
      'f2.cast(Types.DATE),
      "f2.cast(DATE)",
      "CAST(f2 AS DATE)",
      "1990-10-14")

    testAllApis(
      'f2.cast(Types.TIME),
      "f2.cast(TIME)",
      "CAST(f2 AS TIME)",
      "10:20:45")

    testAllApis(
      'f2.cast(Types.TIME),
      "f2.cast(TIME)",
      "CAST(f2 AS TIME)",
      "10:20:45")

    testTableApi(
      'f7.cast(Types.DATE),
      "f7.cast(DATE)",
      "2002-11-09")

    testTableApi(
      'f7.cast(Types.DATE).cast(Types.INT),
      "f7.cast(DATE).cast(INT)",
      "12000")

    testTableApi(
      'f7.cast(Types.TIME),
      "f7.cast(TIME)",
      "00:00:12")

    testTableApi(
      'f7.cast(Types.TIME).cast(Types.INT),
      "f7.cast(TIME).cast(INT)",
      "12000")

    testTableApi(
      'f8.cast(Types.TIMESTAMP),
      "f8.cast(TIMESTAMP)",
      "2016-06-27 07:23:33.0")

    testTableApi(
      'f8.cast(Types.TIMESTAMP).cast(Types.LONG),
      "f8.cast(TIMESTAMP).cast(LONG)",
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
      'f0.cast(Types.TIMESTAMP) !== 'f2,
      "f0.cast(TIMESTAMP) !== f2",
      "CAST(f0 AS TIMESTAMP) <> f2",
      "true")

    testAllApis(
      'f0.cast(Types.TIMESTAMP) === 'f6,
      "f0.cast(TIMESTAMP) === f6",
      "CAST(f0 AS TIMESTAMP) = f6",
      "true")
  }

  @Test
  def testTimeIntervalArithmetic(): Unit = {
    testAllApis(
      12.month < 24.month,
      "12.month < 24.month",
      "INTERVAL '12' MONTH < INTERVAL '24' MONTH",
      "true")

    testAllApis(
      8.milli > 10.milli,
      "8.milli > 10.milli",
      "INTERVAL '0.008' SECOND > INTERVAL '0.010' SECOND",
      "false")

    testAllApis(
      8.year === 8.year,
      "8.year === 8.year",
      "INTERVAL '8' YEAR = INTERVAL '8' YEAR",
      "true")

    testAllApis(
      8.year + 10.month,
      "8.year + 10.month",
      "INTERVAL '8' YEAR + INTERVAL '10' MONTH",
      "+8-10")

    testAllApis(
      8.hour + 10.minute + 12.second + 5.milli,
      "8.hour + 10.minute + 12.second + 5.milli",
      "INTERVAL '8' HOUR + INTERVAL '10' MINUTE + INTERVAL '12.005' SECOND",
      "+0 08:10:12.005")

    testAllApis(
      1.minute - 10.second,
      "1.minute - 10.second",
      "INTERVAL '1' MINUTE - INTERVAL '10' SECOND",
      "+0 00:00:50.000")

    testAllApis(
      2.year - 12.month,
      "2.year - 12.month",
      "INTERVAL '2' YEAR - INTERVAL '12' MONTH",
      "+1-00")

    testAllApis(
      -'f9.cast(Types.INTERVAL_MONTHS),
      "-f9.cast(INTERVAL_MONTHS)",
      "-CAST(f9 AS INTERVAL YEAR)",
      "-2-00")

    testAllApis(
      'f0 + 2.day,
      "f0 + 2.day",
      "f0 + INTERVAL '2' DAY",
      "1990-10-16")

    testAllApis(
      30.day + 'f0,
      "30.day + f0",
      "INTERVAL '30' DAY + f0",
      "1990-11-13")

    testAllApis(
      'f1 + 12.hour,
      "f1 + 12.hour",
      "f1 + INTERVAL '12' HOUR",
      "22:20:45")

    testAllApis(
      24.hour + 'f1,
      "24.hour + f1",
      "INTERVAL '24' HOUR + f1",
      "10:20:45")

    testAllApis(
      'f2 + 10.day + 4.milli,
      "f2 + 10.day + 4.milli",
      "f2 + INTERVAL '10 00:00:00.004' DAY TO SECOND",
      "1990-10-24 10:20:45.127")
  }

  // ----------------------------------------------------------------------------------------------

  def testData = {
    val testData = new Row(11)
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
    testData
  }

  def typeInfo = {
    new RowTypeInfo(Seq(
      Types.DATE,
      Types.TIME,
      Types.TIMESTAMP,
      Types.DATE,
      Types.DATE,
      Types.TIME,
      Types.TIMESTAMP,
      Types.INT,
      Types.LONG,
      Types.INTERVAL_MONTHS,
      Types.INTERVAL_MILLIS)).asInstanceOf[TypeInformation[Any]]
  }
}
