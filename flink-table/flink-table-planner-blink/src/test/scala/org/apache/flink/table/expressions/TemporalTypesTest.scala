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

import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.expressions.utils.ExpressionTestBase
import org.apache.flink.table.typeutils.TimeIntervalTypeInfo
import org.apache.flink.table.util.DateTimeTestUtil._
import org.apache.flink.types.Row
import org.junit.Test

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.TimeZone

class TemporalTypesTest extends ExpressionTestBase {

  @Test
  def testTimePointLiterals(): Unit = {
    testSqlApi(
      "DATE '1990-10-14'",
      "1990-10-14")

    testSqlApi(
      "CAST('1500-04-30' AS DATE)",
      "1500-04-30")

    testSqlApi(
      "TIME '15:45:59'",
      "15:45:59")

    testSqlApi(
      "CAST('1:30:00' AS TIME)",
      "01:30:00")

    testSqlApi(
      "TIMESTAMP '1990-10-14 23:00:00.123'",
      "1990-10-14 23:00:00.123")

    testSqlApi(
      "CAST('1500-04-30 12:00:00' AS TIMESTAMP)",
      "1500-04-30 12:00:00.000")
  }

  @Test
  def testTimeIntervalLiterals(): Unit = {
    testSqlApi(
      "INTERVAL '1' YEAR",
      "+1-00")

    testSqlApi(
      "INTERVAL '1' MONTH",
      "+0-01")

    testSqlApi(
      "INTERVAL '12' DAY",
      "+12 00:00:00.000")

    testSqlApi(
      "INTERVAL '1' HOUR",
      "+0 01:00:00.000")

    testSqlApi(
      "INTERVAL '3' MINUTE",
      "+0 00:03:00.000")

    testSqlApi(
      "INTERVAL '3' SECOND",
      "+0 00:00:03.000")

    testSqlApi(
      "INTERVAL '0.003' SECOND",
      "+0 00:00:00.003")
  }

  @Test
  def testTimePointInput(): Unit = {
    testSqlApi(
      "f0",
      "1990-10-14")

    testSqlApi(
      "f1",
      "10:20:45")

    testSqlApi(
      "f2",
      "1990-10-14 10:20:45.123")
  }

  @Test
  def testTimeIntervalInput(): Unit = {
    testSqlApi(
      "f9",
      "+2-00")

    testSqlApi(
      "f10",
      "+0 00:00:12.000")
  }

  @Test
  def testTimePointCasting(): Unit = {
    testSqlApi(
      "CAST(f0 AS TIMESTAMP)",
      "1990-10-14 00:00:00.000")

    testSqlApi(
      "CAST(f1 AS TIMESTAMP)",
      "1970-01-01 10:20:45.000")

    testSqlApi(
      "CAST(f2 AS DATE)",
      "1990-10-14")

    testSqlApi(
      "CAST(f2 AS TIME)",
      "10:20:45")

    testSqlApi(
      "CAST(f2 AS TIME)",
      "10:20:45")

  }

  @Test
  def testTimePointComparison(): Unit = {
    testSqlApi(
      "f0 < f3",
      "false")

    testSqlApi(
      "f0 < f4",
      "true")

    testSqlApi(
      "f1 < f5",
      "false")

    testSqlApi(
      "CAST(f0 AS TIMESTAMP) <> f2",
      "true")

    testSqlApi(
      "CAST(f0 AS TIMESTAMP) = f6",
      "true")
  }

  @Test
  def testTimeIntervalArithmetic(): Unit = {

    // interval months comparison

    testSqlApi(
      "INTERVAL '12' MONTH < INTERVAL '24' MONTH",
      "true")

    testSqlApi(
      "INTERVAL '8' YEAR = INTERVAL '8' YEAR",
      "true")

    // interval millis comparison

    testSqlApi(
      "INTERVAL '0.008' SECOND > INTERVAL '0.010' SECOND",
      "false")

    testSqlApi(
      "INTERVAL '0.008' SECOND = INTERVAL '0.008' SECOND",
      "true")

    // interval months addition/subtraction

    testSqlApi(
      "INTERVAL '8' YEAR + INTERVAL '10' MONTH",
      "+8-10")

    testSqlApi(
      "INTERVAL '2' YEAR - INTERVAL '12' MONTH",
      "+1-00")

    testSqlApi(
      "-INTERVAL '2' YEAR",
      "-2-00")

    // interval millis addition/subtraction

    testSqlApi(
      "INTERVAL '8' HOUR + INTERVAL '10' MINUTE + INTERVAL '12.005' SECOND",
      "+0 08:10:12.005")

    testSqlApi(
      "INTERVAL '1' MINUTE - INTERVAL '10' SECOND",
      "+0 00:00:50.000")

    testSqlApi(
      "-INTERVAL '10' SECOND",
      "-0 00:00:10.000")

    // addition to date

    // interval millis
    testSqlApi(
      "f0 + INTERVAL '2' DAY",
      "1990-10-16")

    // interval millis
    testSqlApi(
      "INTERVAL '30' DAY + f0",
      "1990-11-13")

    // interval months
    testSqlApi(
      "f0 + INTERVAL '2' MONTH",
      "1990-12-14")

    // interval months
    testSqlApi(
      "INTERVAL '2' MONTH + f0",
      "1990-12-14")

    // addition to time

    // interval millis
    testSqlApi(
      "f1 + INTERVAL '12' HOUR",
      "22:20:45")

    // interval millis
    testSqlApi(
      "INTERVAL '12' HOUR + f1",
      "22:20:45")

    // addition to timestamp

    // interval millis
    testSqlApi(
      "f2 + INTERVAL '10 00:00:00.004' DAY TO SECOND",
      "1990-10-24 10:20:45.127")

    // interval millis
    testSqlApi(
      "INTERVAL '10 00:00:00.004' DAY TO SECOND + f2",
      "1990-10-24 10:20:45.127")

    // interval months
    testSqlApi(
      "f2 + INTERVAL '10' YEAR",
      "2000-10-14 10:20:45.123")

    // interval months
    testSqlApi(
      "INTERVAL '10' YEAR + f2",
      "2000-10-14 10:20:45.123")

    // subtraction from date

    // interval millis
    testSqlApi(
      "f0 - INTERVAL '2' DAY",
      "1990-10-12")

    // interval millis
    testSqlApi(
      "INTERVAL '-30' DAY + f0",
      "1990-09-14")

    // interval months
    testSqlApi(
      "f0 - INTERVAL '2' MONTH",
      "1990-08-14")

    // interval months
    testSqlApi(
      "-INTERVAL '2' MONTH + f0",
      "1990-08-14")

    // subtraction from time

    // interval millis
    testSqlApi(
      "f1 - INTERVAL '12' HOUR",
      "22:20:45")

    // interval millis
    testSqlApi(
      "INTERVAL '-12' HOUR + f1",
      "22:20:45")

    // subtraction from timestamp

    // interval millis
    testSqlApi(
      "f2 - INTERVAL '10 00:00:00.004' DAY TO SECOND",
      "1990-10-04 10:20:45.119")

    // interval millis
    testSqlApi(
      "INTERVAL '-10 00:00:00.004' DAY TO SECOND + f2",
      "1990-10-04 10:20:45.119")

    // interval months
    testSqlApi(
      "f2 - INTERVAL '10' YEAR",
      "1980-10-14 10:20:45.123")

    // interval months
    testSqlApi(
      "INTERVAL '-10' YEAR + f2",
      "1980-10-14 10:20:45.123")

    // casting

    testSqlApi(
      "-CAST(f9 AS INTERVAL YEAR)",
      "-2-00")

    testSqlApi(
      "-CAST(f10 AS INTERVAL SECOND)",
      "-0 00:00:12.000")

    // addition/subtraction of interval millis and interval months

    testSqlApi(
      "f0 + INTERVAL '2' DAY + INTERVAL '1' MONTH",
      "1990-11-16")

    testSqlApi(
      "f0 - INTERVAL '2' DAY - INTERVAL '1' MONTH",
      "1990-09-12")

    testSqlApi(
      "f2 + INTERVAL '2' DAY + INTERVAL '1' MONTH",
      "1990-11-16 10:20:45.123")

    testSqlApi(
      "f2 - INTERVAL '2' DAY - INTERVAL '1' MONTH",
      "1990-09-12 10:20:45.123")
  }

  @Test
  def testSelectNullValues(): Unit ={
    testSqlApi(
      "f11",
      "null"
    )
    testSqlApi(
      "f12",
      "null"
    )
    testSqlApi(
      "f13",
      "null"
    )
  }

  @Test
  def testTemporalNullValues() = {
    testSqlApi(
      "extract(HOUR FROM f13)",
      "null"
    )

    testSqlApi(
      "FLOOR(f13 TO HOUR)",
      "null"
    )

    testSqlApi(
      "FROM_TIMESTAMP(f13)",
      "null"
    )

    testSqlApi(
      "TO_TIMESTAMP(SUBSTR('', 2, -1))",
      "null"
    )

    testSqlApi(
      "TO_TIMESTAMP(f14, 'yyyy-mm-dd')",
      "null"
    )
  }

  @Test
  def testDateAndTime(): Unit = {
    val zones = Seq (
      "UTC",
      "Asia/Kolkata",
      "Asia/Rangoon",
      "Asia/Shanghai",
      "America/New_York"
    )

    zones.foreach { tz =>
      config.setTimeZone(TimeZone.getTimeZone(tz))

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
  }

  @Test
  def testTemporalShanghai(): Unit = {
    config.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))

    // Test Calcite's RexLiteral
    // TIMESTAMP, DATE, TIME
    testSqlApi(
      "TIMESTAMP '2018-03-14 19:01:02.123'",
      "2018-03-14 19:01:02.123")

    // TO_TIMESTAMP
    testSqlApi("TO_TIMESTAMP('2018-03-14 19:00:00.0', 'yyyy-MM-dd HH:mm:ss')",
      "2018-03-14 19:00:00.000")
    testSqlApi("TO_TIMESTAMP('2018-03-14 19:00:00.010')",
      "2018-03-14 19:00:00.010")
    testSqlApi("TO_TIMESTAMP(1521025200000)", "2018-03-14 19:00:00.000")

    // UNIX_TIMESTAMP
    testSqlApi("UNIX_TIMESTAMP(TO_TIMESTAMP(1521025200000))", "1521025200")
    testSqlApi("UNIX_TIMESTAMP('2018-03-14 19:00:00')", "1521025200")
    testSqlApi("UNIX_TIMESTAMP('2018/03/14 19:00:00','yyyy/MM/dd HH:mm:ss')", "1521025200")

    // FROM_UNIXTIME
    testSqlApi("FROM_UNIXTIME(1521025200)",
      "2018-03-14 19:00:00")
    testSqlApi("FROM_UNIXTIME(1521025200, 'yyyy-MM-dd HH:mm:ss')",
      "2018-03-14 19:00:00")
    testSqlApi("FROM_UNIXTIME(UNIX_TIMESTAMP(TO_TIMESTAMP('2018-03-14 19:00:00.0')))",
      "2018-03-14 19:00:00")
    testSqlApi("FROM_UNIXTIME(UNIX_TIMESTAMP('2018-03-14 19:00:00.0'))",  "2018-03-14 19:00:00")

    // DATE_ADD/SUB
    // 1520960523000  "2018-03-14T01:02:03+0800"
    testSqlApi("Date_ADD(TO_TIMESTAMP(1520960523000), 2)", "2018-03-16")
    testSqlApi("Date_ADD(TO_TIMESTAMP('2018-03-14 01:02:03'), 2)", "2018-03-16")
    testSqlApi("Date_ADD('2018-03-14 01:02:03', 2)", "2018-03-16")
    testSqlApi("Date_SUB(TO_TIMESTAMP(1520960523000), 2)", "2018-03-12")
    testSqlApi("Date_SUB(TO_TIMESTAMP('2018-03-14 01:02:03'), 2)", "2018-03-12")
    testSqlApi("Date_SUB('2018-03-14 01:02:03', 14)", "2018-02-28")
    testSqlApi("date_add('2017--10-11', 30)", "null")
    testSqlApi("date_sub('2017--10-11', 30)", "null")

    // DATE_DIFF
    // 1520827201000  2018-03-12T04:00:01+0000, 2018-03-12T12:00:01+0800
    // 1520740801000  2018-03-11T04:00:01+0000, 2018-03-11T12:00:01+0800
    testSqlApi("DATEDIFF(TO_TIMESTAMP(1520827201000), '2018-03-11 12:00:01')", "1")
    testSqlApi("DATEDIFF(TO_TIMESTAMP(1520827201000), TO_TIMESTAMP(1520740801000))", "1")
    testSqlApi("DATEDIFF('2018-03-12 12:00:01', '2018-03-11 12:00:01')", "1")
    testSqlApi("DATEDIFF('2018-03-13 00:00:00', '2018-03-12 23:59:59.123')", "1")
    testSqlApi("DATEDIFF('2018-03-12 12:00:01', TO_TIMESTAMP(1520740801000))", "1")
    testSqlApi("DATEDIFF(TO_TIMESTAMP(1520740801000), '2018-03-12 12:00:01')", "-1")

    // TO_DATE
    testSqlApi("TO_DATE('1970-01-01 00:00:00','yyyy-MM-dd HH:mm:ss')", "1970-01-01")
    testSqlApi("TO_DATE('1970-01-01')", "1970-01-01")
    testSqlApi("TO_DATE(0)", "1970-01-01")

    // DATE_FORMAT
    // 1520960523000  "2018-03-14T01:02:03+0800"
    testSqlApi("DATE_FORMAT('2018-03-14 01:02:03', 'yyyy/MM/dd HH:mm:ss')",
      "2018/03/14 01:02:03")
    testSqlApi("DATE_FORMAT('2018-03-14 01:02:03', 'yyyy-MM-dd HH:mm:ss', " +
      "'yyyy/MM/dd HH:mm:ss')", "2018/03/14 01:02:03")
    testSqlApi("DATE_FORMAT(TO_TIMESTAMP(1520960523000), 'yyyy-MM-dd HH:mm:ss')",
      "2018-03-14 01:02:03")


    // EXTRACT
    // 1521503999000  2018-03-19T23:59:59+0000, 2018-03-20T07:59:59+0000
    testSqlApi("EXTRACT(DAY FROM TO_TIMESTAMP(1521503999000))", "20")
    testSqlApi("EXTRACT(HOUR FROM TO_TIMESTAMP(1521503999000))", "7")
    testSqlApi("EXTRACT(DAY FROM INTERVAL '19 12:10:10.123' DAY TO SECOND(3))", "19")
    testSqlApi("EXTRACT(HOUR FROM TIME '01:02:03')", "1")
    testSqlApi("EXTRACT(DAY FROM INTERVAL '19 12:10:10.123' DAY TO SECOND(3))", "19")
    testSqlApi("EXTRACT(HOUR FROM TIMESTAMP '2018-03-20 01:02:03')", "1")
    testSqlApi("EXTRACT(DAY FROM TIMESTAMP '2018-03-20 01:02:03')", "20")
    testSqlApi("EXTRACT(MONTH FROM TIMESTAMP '2018-03-20 01:02:03')", "3")

    // FLOOR & CEIL
    testSqlApi("FLOOR(TIME '12:44:31' TO MINUTE)", "12:44:00")
    testSqlApi("FLOOR(TIME '12:44:31' TO HOUR)", "12:00:00")
    testSqlApi("FLOOR(TIMESTAMP '2018-03-20 06:44:31' TO HOUR)", "2018-03-20 06:00:00.000")
    testSqlApi("FLOOR(TIMESTAMP '2018-03-20 06:44:31' TO DAY)", "2018-03-20 00:00:00.000")
    testSqlApi("FLOOR(TIMESTAMP '2018-03-20 00:00:00' TO DAY)", "2018-03-20 00:00:00.000")
    testSqlApi("FLOOR(TIMESTAMP '2018-04-01 06:44:31' TO MONTH)", "2018-04-01 00:00:00.000")
    testSqlApi("FLOOR(TIMESTAMP '2018-01-01 06:44:31' TO MONTH)", "2018-01-01 00:00:00.000")
    testSqlApi("FLOOR(TIMESTAMP '2018-02-01 21:00:01' TO QUARTER)", "2018-01-01 00:00:00.000")
    testSqlApi("FLOOR(TIMESTAMP '2018-01-02 21:00:01' TO QUARTER)", "2018-01-01 00:00:00.000")
    testSqlApi("FLOOR(TIMESTAMP '2018-05-02 21:00:01' TO QUARTER)", "2018-04-01 00:00:00.000")
    testSqlApi("CEIL(TIME '12:44:31' TO MINUTE)", "12:45:00")
    testSqlApi("CEIL(TIME '12:44:31' TO HOUR)", "13:00:00")
    testSqlApi("CEIL(TIMESTAMP '2018-03-20 06:44:31' TO HOUR)", "2018-03-20 07:00:00.000")
    testSqlApi("CEIL(TIMESTAMP '2018-03-20 06:00:00' TO HOUR)", "2018-03-20 06:00:00.000")
    testSqlApi("CEIL(TIMESTAMP '2018-03-20 06:44:31' TO DAY)", "2018-03-21 00:00:00.000")
    testSqlApi("CEIL(TIMESTAMP '2018-03-01 00:00:00' TO DAY)", "2018-03-01 00:00:00.000")
    testSqlApi("CEIL(TIMESTAMP '2018-03-31 00:00:01' TO DAY)", "2018-04-01 00:00:00.000")
    testSqlApi("CEIL(TIMESTAMP '2018-03-01 21:00:01' TO MONTH)", "2018-03-01 00:00:00.000")
    testSqlApi("CEIL(TIMESTAMP '2018-03-01 00:00:00' TO MONTH)", "2018-03-01 00:00:00.000")
    testSqlApi("CEIL(TIMESTAMP '2018-12-02 00:00:00' TO MONTH)", "2019-01-01 00:00:00.000")
    testSqlApi("CEIL(TIMESTAMP '2018-01-01 21:00:01' TO YEAR)", "2018-01-01 00:00:00.000")
    testSqlApi("CEIL(TIMESTAMP '2018-01-02 21:00:01' TO YEAR)", "2019-01-01 00:00:00.000")
    testSqlApi("CEIL(TIMESTAMP '2018-01-01 21:00:01' TO QUARTER)", "2018-01-01 00:00:00.000")
    testSqlApi("CEIL(TIMESTAMP '2018-01-02 21:00:01' TO QUARTER)", "2018-04-01 00:00:00.000")
    testSqlApi("CEIL(TIMESTAMP '2018-02-01 21:00:01' TO QUARTER)", "2018-04-01 00:00:00.000")
    testSqlApi("CEIL(TIMESTAMP '2018-05-01 21:00:01' TO QUARTER)", "2018-07-01 00:00:00.000")
    testSqlApi("QUARTER(DATE '2016-04-12')", "2")
    testSqlApi(
      "(TIME '2:55:00', INTERVAL '1' HOUR) OVERLAPS (TIME '3:30:00', INTERVAL '2' HOUR)",
      "true")
    testSqlApi(
      "CEIL(f6 TO HOUR)",
      "1990-10-14 08:00:00.000"
    )
    testSqlApi(
      "FLOOR(f6 TO DAY)",
      "1990-10-14 00:00:00.000"
    )

    // TIMESTAMP_ADD
    // 1520960523000  "2018-03-14T01:02:03+0800"
    testSqlApi("TIMESTAMPADD(HOUR, +8, TIMESTAMP '2017-11-29 10:58:58.998')",
      "2017-11-29 18:58:58.998")

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    sdf.setTimeZone(config.getTimeZone)
    val currMillis = System.currentTimeMillis()
    val ts = new Timestamp(currMillis)
    val currDateStr = sdf.format(ts)
    testSqlApi("CURRENT_DATE", currDateStr)
    //testSqlApi("CURRENT_TIME", "")
  }

  @Test
  def testUTCTimeZone(): Unit = {
    config.setTimeZone(TimeZone.getTimeZone("UTC"))

    // Test Calcite's RexLiteral
    // 1521025200000  =  UTC: 2018-03-14 11:00:00
    testSqlApi("TIMESTAMP '2018-03-14 11:00:00'", "2018-03-14 11:00:00.000")

    testSqlApi("DATE '2018-03-14'", "2018-03-14")
    testSqlApi("TIME '19:20:21'", "19:20:21")

    testSqlApi("TO_TIMESTAMP('2018-03-14 11:00:00', 'yyyy-MM-dd HH:mm:ss')",
      "2018-03-14 11:00:00.000")
    testSqlApi("TO_TIMESTAMP('2018-03-14 11:00:00')",
      "2018-03-14 11:00:00.000")
    testSqlApi("TO_TIMESTAMP(1521025200000)", "2018-03-14 11:00:00.000")

    // 1521025200000  "2018-03-14T11:00:00+0000"
    testSqlApi("FROM_UNIXTIME(1521025200)",
      "2018-03-14 11:00:00")
    testSqlApi("FROM_UNIXTIME(1521025200, 'yyyy-MM-dd HH:mm:ss')",
      "2018-03-14 11:00:00")
    testSqlApi("FROM_UNIXTIME(UNIX_TIMESTAMP(TO_TIMESTAMP('2018-03-14 11:00:00.0')))",
      "2018-03-14 11:00:00")
    testSqlApi("FROM_UNIXTIME(UNIX_TIMESTAMP('2018-03-14 11:00:00.0'))",
      "2018-03-14 11:00:00")

    // 1520960523000  "2018-03-13T17:02:03+0000"
    testSqlApi("Date_ADD(TO_TIMESTAMP(1520960523000), 2)", "2018-03-15")
    testSqlApi("Date_ADD(TO_TIMESTAMP('2018-03-13 17:02:03'), 2)", "2018-03-15")
    testSqlApi("Date_ADD('2018-03-13 17:02:03', 2)", "2018-03-15")
    testSqlApi("Date_SUB(TO_TIMESTAMP(1520960523000), 2)", "2018-03-11")
    testSqlApi("Date_SUB(TO_TIMESTAMP('2018-03-13 17:02:03'), 2)", "2018-03-11")
    testSqlApi("Date_SUB('2018-03-13 17:02:03', 2)", "2018-03-11")
    testSqlApi("date_add('2017--10-11', 30)", "null")
    testSqlApi("date_sub('2017--10-11', 30)", "null")

    // DATE_DIFF
    testSqlApi("DATEDIFF(TO_TIMESTAMP(1520960523000), '2018-03-13 17:02:03')", "0")
    testSqlApi("DATEDIFF(TO_TIMESTAMP(1520827201000), TO_TIMESTAMP(1520740801000))", "1")

    // DATE_FORMAT
    // 1520960523000  "2018-03-13 17:02:03+0000"
    testSqlApi("DATE_FORMAT('2018-03-13 17:02:03', 'yyyy-MM-dd HH:mm:ss', " +
      "'yyyy/MM/dd HH:mm:ss')", "2018/03/13 17:02:03")
    testSqlApi("DATE_FORMAT(TO_TIMESTAMP(1520960523000), 'yyyy-MM-dd HH:mm:ss')",
      "2018-03-13 17:02:03")
  }

  @Test
  def testDaylightSavingTimeZone(): Unit = {
    config.setTimeZone(TimeZone.getTimeZone("America/New_York"))

    // TODO: add more testcases & fully support DST
    // Daylight Saving
    // America/New_York:  -5:00,  -4:00(DST)
    // 2018-03-11 02:00:00 -> 2018:-3-11 03:00:00
    // 2018-11-04 02:00:00 -> 2018-11-04 01:00:00

    // Test Calcite's RexLiteral
    testSqlApi("TIMESTAMP '2018-03-14 07:00:00'", "2018-03-14 07:00:00.000")

    testSqlApi("TO_TIMESTAMP('2018-03-14 07:00:00', 'yyyy-MM-dd HH:mm:ss')",
      "2018-03-14 07:00:00.000")
    testSqlApi("TO_TIMESTAMP('2018-03-14 07:00:00')",
      "2018-03-14 07:00:00.000")
    testSqlApi("TO_TIMESTAMP(1521025200000)", "2018-03-14 07:00:00.000")

    // 1521025200000  "2018-03-14T11:00:00-0400"
    testSqlApi("FROM_UNIXTIME(1521025200)",
      "2018-03-14 07:00:00")
    testSqlApi("FROM_UNIXTIME(1521025200, 'yyyy-MM-dd HH:mm:ss')",
      "2018-03-14 07:00:00")
    testSqlApi("FROM_UNIXTIME(UNIX_TIMESTAMP(TO_TIMESTAMP('2018-03-14 07:00:00.0')))",
      "2018-03-14 07:00:00")
    testSqlApi("FROM_UNIXTIME(UNIX_TIMESTAMP('2018-03-14 07:00:00.0'))",
      "2018-03-14 07:00:00")

    // 1520960523000  "2018-03-13T13:02:03-0400" // DST-0400
    testSqlApi("Date_ADD(TO_TIMESTAMP(1520960523000), 2)", "2018-03-15")
    testSqlApi("Date_ADD(TO_TIMESTAMP('2018-03-13 13:02:03'), 2)", "2018-03-15")
    testSqlApi("Date_ADD('2018-03-13 13:02:03', 2)", "2018-03-15")
    testSqlApi("Date_SUB(TO_TIMESTAMP(1520960523000), 2)", "2018-03-11")
    testSqlApi("Date_SUB(TO_TIMESTAMP('2018-03-13 13:02:03'), 2)", "2018-03-11")
    testSqlApi("Date_SUB('2018-03-13 13:02:03', 2)", "2018-03-11")

    // DATE_FORMAT
    // 1520960523000  "2018-03-13 13:02:03-0400"
    testSqlApi("DATE_FORMAT('2018-03-13 13:02:03', 'yyyy-MM-dd HH:mm:ss', " +
      "'yyyy/MM/dd HH:mm:ss')", "2018/03/13 13:02:03")
    testSqlApi("DATE_FORMAT(TO_TIMESTAMP(1520960523000), 'yyyy-MM-dd HH:mm:ss')",
      "2018-03-13 13:02:03")

    // DATE_DIFF, Pay attention to the DaylightSaving corner case
    testSqlApi("DATEDIFF(TO_TIMESTAMP(1520960523000), '2018-03-13 13:02:03')", "0")
    // 1520827201000  2018-03-12T04:00:01+0000, 2018-03-12T00:00:01-0400(DST)
    // 1520740801000  2018-03-11T04:00:01+0000, 2018-03-10T23:00:01-0400(DST)
    testSqlApi("DATEDIFF(TO_TIMESTAMP(1520827201000), TO_TIMESTAMP(1520740801000))", "2")
    testSqlApi("DATEDIFF('2018-03-12 00:00:01', TO_TIMESTAMP(1520740801000))", "2")
    testSqlApi("DATEDIFF(TO_TIMESTAMP(1520827201000), '2018-03-10 23:00:01')", "2")
    testSqlApi("DATEDIFF('2018-03-12 00:00:01', '2018-03-10 23:00:01')", "2")

    // 'Current Time Functions'
    //val ldt = LocalDateTime.now()
    //testSqlApi("LOCALTIMESTAMP, PROCTIME()", ldt.toString)
    //testSqlApi("PROCTIME()", ldt.toString)
  }

  @Test
  def testHourUnitRangoonTimeZone(): Unit = {
    // Asia/Rangoon UTC Offset 6.5
    config.setTimeZone(TimeZone.getTimeZone("Asia/Rangoon"))

    // 1521502831000,  2018-03-19 23:40:31 UTC,  2018-03-20 06:10:31 +06:30
    testSqlApi("EXTRACT(HOUR FROM TO_TIMESTAMP('2018-03-20 06:10:31'))", "6")
    testSqlApi("EXTRACT(HOUR FROM TO_TIMESTAMP(1521502831000))", "6")
    testSqlApi("EXTRACT(HOUR FROM TIMESTAMP '2018-03-20 06:10:31')", "6")
    testSqlApi("FLOOR(TIMESTAMP '2018-03-20 06:10:31' TO HOUR)", "2018-03-20 06:00:00.000")
    testSqlApi("FLOOR(TO_TIMESTAMP(1521502831000) TO HOUR)", "2018-03-20 06:00:00.000")
    testSqlApi("FLOOR(TIMESTAMP '2018-03-20 06:00:00' TO HOUR)", "2018-03-20 06:00:00.000")
    testSqlApi("CEIL(TIMESTAMP '2018-03-20 06:00:00' TO HOUR)", "2018-03-20 06:00:00.000")
    testSqlApi("CEIL(TO_TIMESTAMP(1521502831000) TO HOUR)", "2018-03-20 07:00:00.000")
    testSqlApi("CEIL(TIMESTAMP '2018-03-20 06:10:31' TO HOUR)", "2018-03-20 07:00:00.000")
  }

  @Test
  def testNullableCases(): Unit = {
    testSqlApi(
      "DATE_FORMAT_TZ(TO_TIMESTAMP(cast(NUll as bigInt)), 'yyyy/MM/dd HH:mm:ss', 'Asia/Shanghai')",
      nullable)

    testSqlApi("CONVERT_TZ(cast(NUll as varchar), 'yyyy-MM-dd HH:mm:ss', 'UTC', 'Asia/Shanghai')",
      nullable)

    testSqlApi("FROM_TIMESTAMP(f13)", nullable)

    testSqlApi("DATE_FORMAT(cast(NUll as varchar), 'yyyy/MM/dd HH:mm:ss')", nullable)

    testSqlApi("UNIX_TIMESTAMP(TO_TIMESTAMP(cast(NUll as bigInt)))", nullable)

    testSqlApi("FROM_UNIXTIME(cast(NUll as bigInt))", nullable)

  }

  @Test
  def testTimeZoneFunction(): Unit = {
    config.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))

    testSqlApi("TO_TIMESTAMP_TZ('2018-03-14 11:00:00', 'UTC')", "2018-03-14 19:00:00.000")
    testSqlApi("TO_TIMESTAMP_TZ('2018-03-14 11:00:00', 'yyyy-MM-dd HH:mm:ss', 'UTC')",
      "2018-03-14 19:00:00.000")

    //testSqlApi("TO_TIMESTAMP(1521025200000)", "2018-03-14 19:00:00.000")
    testSqlApi("DATE_FORMAT_TZ(TO_TIMESTAMP(1521025200000), 'UTC')", "2018-03-14 11:00:00")
    testSqlApi(
      "DATE_FORMAT_TZ(TO_TIMESTAMP(1521025200000), 'yyyy/MM/dd HH:mm:ss', 'Asia/Shanghai')",
      "2018/03/14 19:00:00")

    testSqlApi(
      "TO_TIMESTAMP_TZ(DATE_FORMAT_TZ(TO_TIMESTAMP(1521025200000), 'UTC'), 'Asia/Shanghai')",
      "2018-03-14 11:00:00.000")

    testSqlApi("CONVERT_TZ('2018-03-14 11:00:00', 'yyyy-MM-dd HH:mm:ss', 'UTC', 'Asia/Shanghai')",
      "2018-03-14 19:00:00")

    testSqlApi("TO_TIMESTAMP_TZ(f14, 'UTC')", "null")

    // Note that, if timezone is invalid, here we follow the default behavior of JDK's getTimeZone()
    // It will use UTC timezone by default.
    // TODO: it is would be better to report the error at compiling stage. timezone/format codegen
    testSqlApi("TO_TIMESTAMP_TZ('2018-03-14 11:00:00', 'invalid_tz')", "2018-03-14 19:00:00.000")
  }


  // ----------------------------------------------------------------------------------------------

  override def testData: Row = {
    val testData = new Row(16)
    testData.setField(0, UTCDate("1990-10-14"))
    testData.setField(1, UTCTime("10:20:45"))
    testData.setField(2, UTCTimestamp("1990-10-14 10:20:45.123"))
    testData.setField(3, UTCDate("1990-10-13"))
    testData.setField(4, UTCDate("1990-10-15"))
    testData.setField(5, UTCTime("00:00:00"))
    testData.setField(6, UTCTimestamp("1990-10-14 00:00:00.0"))
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
    testData
  }

  override def typeInfo: RowTypeInfo = {
    new RowTypeInfo(
      /* 0 */  Types.SQL_DATE,
      /* 1 */  Types.SQL_TIME,
      /* 2 */  Types.SQL_TIMESTAMP,
      /* 3 */  Types.SQL_DATE,
      /* 4 */  Types.SQL_DATE,
      /* 5 */  Types.SQL_TIME,
      /* 6 */  Types.SQL_TIMESTAMP,
      /* 7 */  Types.INT,
      /* 8 */  Types.LONG,
      /* 9 */  TimeIntervalTypeInfo.INTERVAL_MONTHS,
      /* 10 */ TimeIntervalTypeInfo.INTERVAL_MILLIS,
      /* 11 */ Types.SQL_DATE,
      /* 12 */ Types.SQL_TIME,
      /* 13 */ Types.SQL_TIMESTAMP,
      /* 14 */ Types.STRING,
      /* 15 */ Types.LONG)
  }
}
