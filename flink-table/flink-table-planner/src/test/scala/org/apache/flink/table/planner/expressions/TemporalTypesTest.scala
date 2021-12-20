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

import org.apache.flink.table.api._
import org.apache.flink.table.expressions.TimeIntervalUnit
import org.apache.flink.table.planner.codegen.CodeGenException
import org.apache.flink.table.planner.expressions.utils.ExpressionTestBase
import org.apache.flink.table.planner.utils.DateTimeTestUtil
import org.apache.flink.table.planner.utils.DateTimeTestUtil._
import org.apache.flink.table.types.DataType
import org.apache.flink.types.Row

import org.junit.Test

import java.lang.{Double => JDouble, Float => JFloat, Integer => JInt, Long => JLong}
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.{Instant, ZoneId, ZoneOffset}
import java.util.Locale

class TemporalTypesTest extends ExpressionTestBase {

  @Test
  def testTimePointLiterals(): Unit = {
    testAllApis(
      "1990-10-14".toDate,
      "DATE '1990-10-14'",
      "1990-10-14")

    testTableApi(
      localDate2Literal(localDate("2040-09-11")),
      "2040-09-11")

    testAllApis(
      "1500-04-30".cast(DataTypes.DATE),
      "CAST('1500-04-30' AS DATE)",
      "1500-04-30")

    testAllApis(
      "15:45:59".toTime,
      "TIME '15:45:59'",
      "15:45:59")

    testTableApi(
      localTime2Literal(DateTimeTestUtil.localTime("00:00:00")),
      "00:00:00")

    testAllApis(
      "1:30:00".cast(DataTypes.TIME),
      "CAST('1:30:00' AS TIME)",
      "01:30:00")

    testAllApis(
      "1990-10-14 23:00:00.123".toTimestamp,
      "TIMESTAMP '1990-10-14 23:00:00.123'",
      "1990-10-14 23:00:00.123")

    testTableApi(
      localDateTime2Literal(localDateTime("2040-09-11 00:00:00.000")),
      "2040-09-11 00:00:00")

    testAllApis(
      "1500-04-30 12:00:00".cast(DataTypes.TIMESTAMP(3)),
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
      "INTERVAL '1' YEAR",
      "+1-00")

    testAllApis(
      1.month,
      "INTERVAL '1' MONTH",
      "+0-01")

    testAllApis(
      12.days,
      "INTERVAL '12' DAY",
      "+12 00:00:00.000")

    testAllApis(
      1.hour,
      "INTERVAL '1' HOUR",
      "+0 01:00:00.000")

    testAllApis(
      3.minutes,
      "INTERVAL '3' MINUTE",
      "+0 00:03:00.000")

    testAllApis(
      3.seconds,
      "INTERVAL '3' SECOND",
      "+0 00:00:03.000")

    testAllApis(
      3.millis,
      "INTERVAL '0.003' SECOND",
      "+0 00:00:00.003")
  }

  @Test
  def testTimePointInput(): Unit = {
    testAllApis(
      'f0,
      "f0",
      "1990-10-14")

    testAllApis(
      'f1,
      "f1",
      "10:20:45")

    testAllApis(
      'f2,
      "f2",
      "1990-10-14 10:20:45.123")
  }

  @Test
  def testTimeIntervalInput(): Unit = {
    testAllApis(
      'f9,
      "f9",
      "+2-00")

    testAllApis(
      'f10,
      "f10",
      "+0 00:00:12.000")
  }

  @Test
  def testTimePointCasting(): Unit = {
    // DATE -> TIMESTAMP
    testAllApis(
      'f0.cast(DataTypes.TIMESTAMP(3)),
      "CAST(f0 AS TIMESTAMP(3))",
      "1990-10-14 00:00:00.000")

    // TIME -> TIMESTAMP
    testAllApis(
      'f1.cast(DataTypes.TIMESTAMP(3)),
      "CAST(f1 AS TIMESTAMP(3))",
      "1970-01-01 10:20:45.000")

    // TIMESTAMP -> DATE
    testAllApis(
      'f2.cast(DataTypes.DATE),
      "CAST(f2 AS DATE)",
      "1990-10-14")

    // TIMESTAMP -> TIME
    testAllApis(
      'f2.cast(DataTypes.TIME),
      "CAST(f2 AS TIME)",
      "10:20:45")
  }

  @Test
  def testTimestampLtzCastInUTC(): Unit = {
    config.setLocalTimeZone(ZoneId.of("UTC"))

    //DATE -> TIMESTAMP_LTZ
    testSqlApi(
      "CAST(f0 AS TIMESTAMP_LTZ(3))",
      "1990-10-14 00:00:00.000")

    //TIME -> TIMESTAMP_LTZ
    testSqlApi(
      "CAST(f1 AS TIMESTAMP_LTZ(3))",
      "1970-01-01 10:20:45.000")

    //TIMESTAMP_LTZ -> TIME
    testSqlApi(
      s"CAST(${timestampLtz("2018-03-14 01:02:03")} AS TIME)",
      "01:02:03")

    //TIMESTAMP_LTZ -> DATE
    testSqlApi(
      s"CAST(${timestampLtz("2018-03-14 01:02:03")} AS DATE)",
      "2018-03-14")
  }

  @Test
  def testTimestampLtzCastInShanghai(): Unit = {
    config.setLocalTimeZone(ZoneId.of("Asia/Shanghai"))

    // DATE -> TIMESTAMP_LTZ
    testSqlApi(
      "CAST(f0 AS TIMESTAMP_LTZ(3))",
      "1990-10-14 00:00:00.000")

    // TIME -> TIMESTAMP_LTZ
    testSqlApi(
      "CAST(f1 AS TIMESTAMP_LTZ(3))",
      "1970-01-01 10:20:45.000")

    // TIMESTAMP -> TIMESTAMP_LTZ
    testSqlApi(
      "CAST(f2 AS TIMESTAMP_LTZ(3))",
      "1990-10-14 10:20:45.123")

    // TIMESTAMP_LTZ -> TIME
    testSqlApi(
      s"CAST(${timestampLtz("2018-03-14 01:02:03")} AS TIME)",
      "01:02:03")

    // TIMESTAMP_LTZ -> DATE
    testSqlApi(
      s"CAST(${timestampLtz("2018-03-14 01:02:03")} AS DATE)",
      "2018-03-14")

    // TIMESTAMP_LTZ -> TIMESTAMP
    testSqlApi(
      s"CAST(${timestampLtz("2018-03-14 01:02:03")} AS TIMESTAMP(3))",
      "2018-03-14 01:02:03.000")

    // test precision when cast between TIMESTAMP_LTZ and TIMESTAMP_LTZ
    testSqlApi(
      s"CAST(${timestampLtz("1970-01-01 08:00:01.123456")} AS TIMESTAMP_LTZ(3))",
      "1970-01-01 08:00:01.123")
    testSqlApi(
      s"CAST(${timestampLtz("1970-01-01 08:00:01.123456")} AS TIMESTAMP_LTZ(6))",
      "1970-01-01 08:00:01.123456")
    testSqlApi(
      s"CAST(${timestampLtz("1970-01-01 08:00:01.123456")} AS TIMESTAMP_LTZ(9))",
      "1970-01-01 08:00:01.123456000")
  }

  @Test
  def tesInvalidCastBetweenNumericAndTimestampLtz(): Unit = {
    val castFromTimestampLtzExceptionMsg = "The cast conversion from TIMESTAMP_LTZ type to" +
      " NUMERIC type is not allowed."

    val castToTimestampLtzExceptionMsg = "The cast conversion from NUMERIC type to TIMESTAMP_LTZ" +
      " type is not allowed, it's recommended to use" +
      " TO_TIMESTAMP_LTZ(numeric_col, precision) instead."

    // TINYINT -> TIMESTAMP_LTZ
    testExpectedSqlException(
      "CAST(CAST(100 AS TINYINT) AS TIMESTAMP_LTZ(3))",
      castToTimestampLtzExceptionMsg)

    // SMALLINT -> TIMESTAMP_LTZ
    testExpectedSqlException(
      "CAST(CAST(100 AS SMALLINT) AS TIMESTAMP_LTZ(3))",
      castToTimestampLtzExceptionMsg)

    // INT -> TIMESTAMP_LTZ
    testExpectedSqlException(
      "CAST(100 AS TIMESTAMP_LTZ(3))",
      castToTimestampLtzExceptionMsg)

    // BIGINT -> TIMESTAMP_LTZ
    testExpectedSqlException(
      "CAST(CAST(100 AS BIGINT) AS TIMESTAMP_LTZ(3))",
      castToTimestampLtzExceptionMsg)

    // FLOAT -> TIMESTAMP_LTZ
    testExpectedSqlException(
      "CAST(CAST(100.01 AS FLOAT) AS TIMESTAMP_LTZ(3))",
      castToTimestampLtzExceptionMsg)

    // DOUBLE -> TIMESTAMP_LTZ
    testExpectedSqlException(
      "CAST(CAST(100.123 AS DOUBLE) AS TIMESTAMP_LTZ(3))",
      castToTimestampLtzExceptionMsg)

    // DECIMAL -> TIMESTAMP_LTZ
    testExpectedSqlException(
      "CAST(CAST(100.1234 as DECIMAL(38, 18)) AS TIMESTAMP_LTZ(3))",
      castToTimestampLtzExceptionMsg)

    // TIMESTAMP_LTZ -> TINYINT
    testExpectedSqlException(
      s"CAST(${timestampLtz("1970-01-01 08:02:03.123")} AS TINYINT)",
      castFromTimestampLtzExceptionMsg)

    // TIMESTAMP_LTZ -> SMALLINT
    testExpectedSqlException(
      s"CAST(${timestampLtz("1970-01-01 08:02:03.123")} AS SMALLINT)",
      castFromTimestampLtzExceptionMsg)

    // TIMESTAMP_LTZ -> INT
    testExpectedSqlException(
      s"CAST(${timestampLtz("1970-01-01 08:02:03.123")} AS INT)",
      castFromTimestampLtzExceptionMsg)

    // TIMESTAMP_LTZ -> BIGINT
    testExpectedSqlException(
      s"CAST(${timestampLtz("1970-01-01 08:02:03.123")} AS BIGINT)",
      castFromTimestampLtzExceptionMsg)

    // TIMESTAMP_LTZ -> FLOAT
    testExpectedSqlException(
      s"CAST(${timestampLtz("1970-01-01 08:02:03.123")} AS FLOAT)",
      castFromTimestampLtzExceptionMsg)

    // TIMESTAMP_LTZ -> DOUBLE
    testExpectedSqlException(
      s"CAST(${timestampLtz("1970-01-01 08:02:03.123")} AS DOUBLE)",
      castFromTimestampLtzExceptionMsg)

    // TIMESTAMP_LTZ -> DECIMAL
    testExpectedSqlException(
      s"CAST(${timestampLtz("1970-01-01 08:02:03.123")} AS DECIMAL(38, 3))",
      castFromTimestampLtzExceptionMsg)
  }

  @Test
  def tesInvalidCastBetweenNumericAndTimestamp(): Unit = {
    val castFromTimestampExceptionMsg = "The cast conversion from TIMESTAMP type to NUMERIC type" +
      " is not allowed, it's recommended to use" +
      " UNIX_TIMESTAMP(CAST(timestamp_col AS STRING)) instead."

    val castToTimestampExceptionMsg = "The cast conversion from NUMERIC type to TIMESTAMP type" +
      " is not allowed, it's recommended to use TO_TIMESTAMP(FROM_UNIXTIME(numeric_col))" +
      " instead, note the numeric is in seconds."

    testExpectedSqlException(
      "CAST(CAST(123 as TINYINT) AS TIMESTAMP)",
      castToTimestampExceptionMsg)

    testExpectedSqlException(
      "CAST(CAST(123 AS SMALLINT) AS TIMESTAMP)",
      castToTimestampExceptionMsg)

    testExpectedSqlException(
      "CAST(CAST(123 AS INT) AS TIMESTAMP)",
      castToTimestampExceptionMsg)

    testExpectedSqlException(
      "CAST(CAST(123 AS BIGINT) AS TIMESTAMP)",
      castToTimestampExceptionMsg)

    testExpectedSqlException(
      "CAST(CAST(123 AS FLOAT) AS TIMESTAMP)",
      castToTimestampExceptionMsg)

    testExpectedSqlException(
      "CAST(CAST(123 AS DOUBLE) AS TIMESTAMP)",
      castToTimestampExceptionMsg)

    testExpectedSqlException(
    "CAST(CAST(123 as DECIMAL(5, 2)) AS TIMESTAMP)",
        castToTimestampExceptionMsg)

    testExpectedSqlException(
      "CAST(TIMESTAMP '1970-01-01 00:02:03' AS TINYINT)",
      castFromTimestampExceptionMsg)

    testExpectedSqlException(
      "CAST(TIMESTAMP '1970-01-01 00:02:03' AS SMALLINT)",
      castFromTimestampExceptionMsg)

    testExpectedSqlException(
      "CAST(TIMESTAMP '1970-01-01 00:02:03' AS INT)",
      castFromTimestampExceptionMsg)

    testExpectedSqlException(
      "CAST(TIMESTAMP '1970-01-01 00:02:03' AS BIGINT)",
      castFromTimestampExceptionMsg)

    testExpectedSqlException(
      "CAST(TIMESTAMP '1970-01-01 00:02:03' AS FLOAT)",
      castFromTimestampExceptionMsg)

    testExpectedSqlException(
      "CAST(TIMESTAMP '1970-01-01 00:02:03' AS DOUBLE)",
      castFromTimestampExceptionMsg)

    testExpectedSqlException(
      "CAST(TIMESTAMP '1970-01-01 00:02:03' AS DECIMAL(5, 2))",
      castFromTimestampExceptionMsg)
  }

  @Test
  def testTimeIntervalCasting(): Unit = {
    testTableApi(
      'f7.cast(DataTypes.INTERVAL(DataTypes.MONTH)),
      "+1000-00")

    testTableApi(
      'f8.cast(DataTypes.INTERVAL(DataTypes.MINUTE())),
      "+16979 07:23:33.000")
  }

  @Test
  def testTimePointComparison(): Unit = {
    testAllApis(
      'f0 < 'f3,
      "f0 < f3",
      "false")

    testAllApis(
      'f0 < 'f4,
      "f0 < f4",
      "true")

    testAllApis(
      'f1 < 'f5,
      "f1 < f5",
      "false")

    testAllApis(
      'f0.cast(DataTypes.TIMESTAMP(3)) !== 'f2,
      "CAST(f0 AS TIMESTAMP(9)) <> f2",
      "true")

    testAllApis(
      'f0.cast(DataTypes.TIMESTAMP(9)) === 'f6,
      "CAST(f0 AS TIMESTAMP(9)) = f6",
      "true")
  }

  @Test
  def testTimeIntervalArithmetic(): Unit = {

    // interval months comparison

    testAllApis(
      12.months < 24.months,
      "INTERVAL '12' MONTH < INTERVAL '24' MONTH",
      "true")

    testAllApis(
      8.years === 8.years,
      "INTERVAL '8' YEAR = INTERVAL '8' YEAR",
      "true")

    // interval millis comparison

    testAllApis(
      8.millis > 10.millis,
      "INTERVAL '0.008' SECOND > INTERVAL '0.010' SECOND",
      "false")

    testAllApis(
      8.millis === 8.millis,
      "INTERVAL '0.008' SECOND = INTERVAL '0.008' SECOND",
      "true")

    // interval months addition/subtraction

    testAllApis(
      8.years + 10.months,
      "INTERVAL '8' YEAR + INTERVAL '10' MONTH",
      "+8-10")

    testAllApis(
      2.years - 12.months,
      "INTERVAL '2' YEAR - INTERVAL '12' MONTH",
      "+1-00")

    testAllApis(
      -2.years,
      "-INTERVAL '2' YEAR",
      "-2-00")

    // interval millis addition/subtraction

    testAllApis(
      8.hours + 10.minutes + 12.seconds + 5.millis,
      "INTERVAL '8' HOUR + INTERVAL '10' MINUTE + INTERVAL '12.005' SECOND",
      "+0 08:10:12.005")

    testAllApis(
      1.minute - 10.seconds,
      "INTERVAL '1' MINUTE - INTERVAL '10' SECOND",
      "+0 00:00:50.000")

    testAllApis(
      -10.seconds,
      "-INTERVAL '10' SECOND",
      "-0 00:00:10.000")

    // addition to date

    // interval millis
    testAllApis(
      'f0 + 2.days,
      "f0 + INTERVAL '2' DAY",
      "1990-10-16")

    // interval millis
    testAllApis(
      30.days + 'f0,
      "INTERVAL '30' DAY + f0",
      "1990-11-13")

    // interval months
    testAllApis(
      'f0 + 2.months,
      "f0 + INTERVAL '2' MONTH",
      "1990-12-14")

    // interval months
    testAllApis(
      2.months + 'f0,
      "INTERVAL '2' MONTH + f0",
      "1990-12-14")

    // addition to time

    // interval millis
    testAllApis(
      'f1 + 12.hours,
      "f1 + INTERVAL '12' HOUR",
      "22:20:45")

    // interval millis
    testAllApis(
      12.hours + 'f1,
      "INTERVAL '12' HOUR + f1",
      "22:20:45")

    // addition to timestamp

    // interval millis
    testAllApis(
      'f2 + 10.days + 4.millis,
      "f2 + INTERVAL '10 00:00:00.004' DAY TO SECOND",
      "1990-10-24 10:20:45.127")

    // interval millis
    testAllApis(
      10.days + 'f2 + 4.millis,
      "INTERVAL '10 00:00:00.004' DAY TO SECOND + f2",
      "1990-10-24 10:20:45.127")

    // interval months
    testAllApis(
      'f2 + 10.years,
      "f2 + INTERVAL '10' YEAR",
      "2000-10-14 10:20:45.123")

    // interval months
    testAllApis(
      10.years + 'f2,
      "INTERVAL '10' YEAR + f2",
      "2000-10-14 10:20:45.123")

    // subtraction from date

    // interval millis
    testAllApis(
      'f0 - 2.days,
      "f0 - INTERVAL '2' DAY",
      "1990-10-12")

    // interval millis
    testAllApis(
      -30.days + 'f0,
      "INTERVAL '-30' DAY + f0",
      "1990-09-14")

    // interval months
    testAllApis(
      'f0 - 2.months,
      "f0 - INTERVAL '2' MONTH",
      "1990-08-14")

    // interval months
    testAllApis(
      -2.months + 'f0,
      "-INTERVAL '2' MONTH + f0",
      "1990-08-14")

    // subtraction from time

    // interval millis
    testAllApis(
      'f1 - 12.hours,
      "f1 - INTERVAL '12' HOUR",
      "22:20:45")

    // interval millis
    testAllApis(
      -12.hours + 'f1,
      "INTERVAL '-12' HOUR + f1",
      "22:20:45")

    // subtraction from timestamp

    // interval millis
    testAllApis(
      'f2 - 10.days - 4.millis,
      "f2 - INTERVAL '10 00:00:00.004' DAY TO SECOND",
      "1990-10-04 10:20:45.119")

    // interval millis
    testAllApis(
      -10.days + 'f2 - 4.millis,
      "INTERVAL '-10 00:00:00.004' DAY TO SECOND + f2",
      "1990-10-04 10:20:45.119")

    // interval months
    testAllApis(
      'f2 - 10.years,
      "f2 - INTERVAL '10' YEAR",
      "1980-10-14 10:20:45.123")

    // interval months
    testAllApis(
      -10.years + 'f2,
      "INTERVAL '-10' YEAR + f2",
      "1980-10-14 10:20:45.123")

    // casting

    testAllApis(
      // TODO fix after FLIP-51
      -'f9.cast(DataTypes.INTERVAL(DataTypes.MONTH).bridgedTo(classOf[JInt])),
      "-CAST(f9 AS INTERVAL MONTH)",
      "-2-00")

    testAllApis(
      // TODO fix after FLIP-51
      -'f10.cast(DataTypes.INTERVAL(DataTypes.SECOND(3)).bridgedTo(classOf[JLong])),
      "-CAST(f10 AS INTERVAL SECOND(3))",
      "-0 00:00:12.000")

    // addition/subtraction of interval millis and interval months

    testAllApis(
      'f0 + 2.days + 1.month,
      "f0 + INTERVAL '2' DAY + INTERVAL '1' MONTH",
      "1990-11-16")

    testAllApis(
      'f0 - 2.days - 1.month,
      "f0 - INTERVAL '2' DAY - INTERVAL '1' MONTH",
      "1990-09-12")

    testAllApis(
      'f2 + 2.days + 1.month,
      "f2 + INTERVAL '2' DAY + INTERVAL '1' MONTH",
      "1990-11-16 10:20:45.123")

    testAllApis(
      'f2 - 2.days - 1.month,
      "f2 - INTERVAL '2' DAY - INTERVAL '1' MONTH",
      "1990-09-12 10:20:45.123")
  }

  @Test
  def testSelectNullValues(): Unit ={
    testAllApis(
      'f11,
      "f11",
      "null"
    )
    testAllApis(
      'f12,
      "f12",
      "null"
    )
    testAllApis(
      'f13,
      "f13",
      "null"
    )
  }

  @Test
  def testTemporalNullValues(): Unit = {
    testAllApis(
      'f13.extract(TimeIntervalUnit.HOUR),
      "extract(HOUR FROM f13)",
      "null"
    )

    testAllApis(
      'f13.floor(TimeIntervalUnit.HOUR),
      "FLOOR(f13 TO HOUR)",
      "null"
    )

    testSqlApi(
      "TO_TIMESTAMP(SUBSTRING('', 2, -1))",
      "null"
    )

    testSqlApi(
      "TO_TIMESTAMP(f14, 'yyyy-MM-dd')",
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
      s"DATE_FORMAT(${timestampLtz("2018-03-14 01:02:03")}, 'yyyy-MM-dd HH:mm:ss')",
      "2018-03-14 01:02:03")

    testSqlApi(
      s"DATE_FORMAT(${timestampLtz("2018-03-14 01:02:03.123456")}, 'yyyy-MM-dd HH:mm:ss.SSSSSS')",
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
      s"DATE_FORMAT(${timestampLtz("2018-03-14 01:02:03")}, 'yyyy-MM-dd HH:mm:ss')",
      "2018-03-14 01:02:03")

    testSqlApi(
      s"DATE_FORMAT(${timestampLtz("2018-03-14 01:02:03.123456")}, 'yyyy-MM-dd HH:mm:ss.SSSSSS')",
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
      s"DATE_FORMAT(${timestampLtz("2018-03-14 01:02:03")}, 'yyyy-MM-dd HH:mm:ss')",
      "2018-03-14 01:02:03")

    testSqlApi(
      s"DATE_FORMAT(${timestampLtz("2018-03-14 01:02:03.123456")}, 'yyyy-MM-dd HH:mm:ss.SSSSSS')",
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

  @Test
  def testTemporalShanghai(): Unit = {
    config.setLocalTimeZone(ZoneId.of("Asia/Shanghai"))

    testSqlApi(timestampLtz("2018-03-14 19:01:02.123"), "2018-03-14 19:01:02.123")
    testSqlApi(timestampLtz("2018-03-14 19:00:00.010"), "2018-03-14 19:00:00.010")

    testSqlApi(
      timestampLtz("2018-03-14 19:00:00.010") + " > " + "f25",
      "true")

    testSqlApi(
      s"${timestampLtz("2018-03-14 01:02:03.123456789", 9)}",
      "2018-03-14 01:02:03.123456789")

    testSqlApi(
      s"${timestampLtz("2018-03-14 01:02:03.123456", 6)}",
      "2018-03-14 01:02:03.123456")


    // DATE_FORMAT
    testSqlApi(
      "DATE_FORMAT('2018-03-14 01:02:03', 'yyyy/MM/dd HH:mm:ss')",
      "2018/03/14 01:02:03")

    testSqlApi(
      s"DATE_FORMAT(${timestampLtz("2018-03-14 01:02:03")}, 'yyyy-MM-dd HH:mm:ss')",
      "2018-03-14 01:02:03")

    // EXTRACT
    val extractT1 = timestampLtz("2018-03-20 07:59:59")
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

    testSqlApi("FLOOR( DATE '2021-02-27' TO WEEK)", "2021-02-21")
    testSqlApi("FLOOR( DATE '2021-03-01' TO WEEK)", "2021-02-28")
    testSqlApi("CEIL( DATE '2021-02-27' TO WEEK)", "2021-02-28")
    testSqlApi("CEIL( DATE '2021-03-01' TO WEEK)", "2021-03-07")

    testSqlApi("FLOOR(TIMESTAMP '2018-03-20 06:44:31' TO HOUR)", "2018-03-20 06:00:00")
    testSqlApi("FLOOR(TIMESTAMP '2018-03-20 06:44:31' TO DAY)", "2018-03-20 00:00:00")
    testSqlApi("FLOOR(TIMESTAMP '2018-03-20 00:00:00' TO DAY)", "2018-03-20 00:00:00")
    testSqlApi("FLOOR(TIMESTAMP '2021-02-27 00:00:00' TO WEEK)", "2021-02-21 00:00:00")
    testSqlApi("FLOOR(TIMESTAMP '2021-03-01 00:00:00' TO WEEK)", "2021-02-28 00:00:00")
    testSqlApi("FLOOR(TIMESTAMP '2018-04-01 06:44:31' TO MONTH)", "2018-04-01 00:00:00")
    testSqlApi("FLOOR(TIMESTAMP '2018-01-01 06:44:31' TO MONTH)", "2018-01-01 00:00:00")
    testSqlApi("CEIL(TIMESTAMP '2018-03-20 06:44:31' TO HOUR)", "2018-03-20 07:00:00")
    testSqlApi("CEIL(TIMESTAMP '2018-03-20 06:00:00' TO HOUR)", "2018-03-20 06:00:00")
    testSqlApi("CEIL(TIMESTAMP '2018-03-20 06:44:31' TO DAY)", "2018-03-21 00:00:00")
    testSqlApi("CEIL(TIMESTAMP '2018-03-01 00:00:00' TO DAY)", "2018-03-01 00:00:00")
    testSqlApi("CEIL(TIMESTAMP '2018-03-31 00:00:01' TO DAY)", "2018-04-01 00:00:00")
    testSqlApi("CEIL(TIMESTAMP '2021-02-27 00:00:00' TO WEEK)", "2021-02-28 00:00:00")
    testSqlApi("CEIL(TIMESTAMP '2021-03-01 00:00:00' TO WEEK)", "2021-03-07 00:00:00")
    testSqlApi("CEIL(TIMESTAMP '2018-03-01 21:00:01' TO MONTH)", "2018-03-01 00:00:00")
    testSqlApi("CEIL(TIMESTAMP '2018-03-01 00:00:00' TO MONTH)", "2018-03-01 00:00:00")
    testSqlApi("CEIL(TIMESTAMP '2018-12-02 00:00:00' TO MONTH)", "2019-01-01 00:00:00")
    testSqlApi("CEIL(TIMESTAMP '2018-01-01 21:00:01' TO YEAR)", "2018-01-01 00:00:00")
    testSqlApi("CEIL(TIMESTAMP '2018-01-02 21:00:01' TO YEAR)", "2019-01-01 00:00:00")

    testSqlApi(s"FLOOR(${timestampLtz("2018-03-20 06:44:31")} TO HOUR)", "2018-03-20 06:00:00")
    testSqlApi(s"FLOOR(${timestampLtz("2018-03-20 06:44:31")} TO DAY)", "2018-03-20 00:00:00")
    testSqlApi(s"FLOOR(${timestampLtz("2018-03-20 00:00:00")} TO DAY)", "2018-03-20 00:00:00")
    testSqlApi(s"FLOOR(${timestampLtz("2021-02-27 00:00:00")} TO WEEK)", "2021-02-21 00:00:00")
    testSqlApi(s"FLOOR(${timestampLtz("2021-03-01 00:00:00")} TO WEEK)", "2021-02-28 00:00:00")
    testSqlApi(s"FLOOR(${timestampLtz("2018-04-01 06:44:31")} TO MONTH)", "2018-04-01 00:00:00")
    testSqlApi(s"FLOOR(${timestampLtz("2018-01-01 06:44:31")} TO MONTH)", "2018-01-01 00:00:00")
    testSqlApi(s"CEIL(${timestampLtz("2018-03-20 06:44:31")} TO HOUR)", "2018-03-20 07:00:00")
    testSqlApi(s"CEIL(${timestampLtz("2018-03-20 06:00:00")} TO HOUR)", "2018-03-20 06:00:00")
    testSqlApi(s"CEIL(${timestampLtz("2018-03-20 06:44:31")} TO DAY)", "2018-03-21 00:00:00")
    testSqlApi(s"CEIL(${timestampLtz("2018-03-1 00:00:00")} TO DAY)", "2018-03-01 00:00:00")
    testSqlApi(s"CEIL(${timestampLtz("2018-03-31 00:00:01")} TO DAY)", "2018-04-01 00:00:00")
    testSqlApi(s"CEIL(${timestampLtz("2021-02-27 00:00:00")} TO WEEK)", "2021-02-28 00:00:00")
    testSqlApi(s"CEIL(${timestampLtz("2021-03-01 00:00:00")} TO WEEK)", "2021-03-07 00:00:00")
    testSqlApi(s"CEIL(${timestampLtz("2018-03-01 21:00:01")} TO MONTH)", "2018-03-01 00:00:00")
    testSqlApi(s"CEIL(${timestampLtz("2018-03-01 00:00:00")} TO MONTH)", "2018-03-01 00:00:00")
    testSqlApi(s"CEIL(${timestampLtz("2018-12-02 00:00:00")} TO MONTH)", "2019-01-01 00:00:00")
    testSqlApi(s"CEIL(${timestampLtz("2018-01-01 21:00:01")} TO YEAR)", "2018-01-01 00:00:00")
    testSqlApi(s"CEIL(${timestampLtz("2018-01-02 21:00:01")} TO YEAR)", "2019-01-01 00:00:00")

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

    val t1 = timestampLtz("2018-03-20 06:10:31")
    val t2 = timestampLtz("2018-03-20 06:00:00")
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

    // test null value input
    testAllApis(
      toTimestampLtz(nullOf(DataTypes.BIGINT()), 0),
      "TO_TIMESTAMP_LTZ(cast(NULL as BIGINT), 0)",
      nullable)
  }

  @Test
  def testInvalidInputCase(): Unit = {
    val invalidStr = "invalid value"
    testSqlApi(s"DATE_FORMAT('$invalidStr', 'yyyy/MM/dd HH:mm:ss')", nullable)
    testSqlApi(s"TO_TIMESTAMP('$invalidStr', 'yyyy-MM-dd')", nullable)
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
      s"TO_TIMESTAMP('$invalidStr', 'yyyy-MM-dd')",
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

    testSqlApi(
      s"from_unixtime(f26, '$fmt2')",
      sdf2.format(new Timestamp(124000)))
    testSqlApi(
      s"from_unixtime(f26, '$fmt3')",
      sdf3.format(new Timestamp(124000)))

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

  /**
   * now Flink only support TIMESTAMP(3) as the return type in TO_TIMESTAMP
   * See: https://issues.apache.org/jira/browse/FLINK-14925
   */
  @Test
  def testToTimeStampFunctionWithHighPrecision(): Unit = {
    testSqlApi(
      "TO_TIMESTAMP('1970-01-01 00:00:00.123456789')",
      "1970-01-01 00:00:00.123")

    testSqlApi(
      "TO_TIMESTAMP('1970-01-01 00:00:00.12345', 'yyyy-MM-dd HH:mm:ss.SSSSS')",
      "1970-01-01 00:00:00.123")

    testSqlApi(
      "TO_TIMESTAMP('20000202 59:59.1234567', 'yyyyMMdd mm:ss.SSSSSSS')",
      "2000-02-02 00:59:59.123")

    testSqlApi(
      "TO_TIMESTAMP('1234567', 'SSSSSSS')",
      "1970-01-01 00:00:00.123")
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
      s"EXTRACT(MILLISECOND FROM ${timestampLtz("1970-01-01 00:00:00.123456789", 9)})",
      "123")

    testSqlApi(
      s"EXTRACT(MICROSECOND FROM ${timestampLtz("1970-01-01 00:00:00.123456789", 9)})",
      "123456")

    testSqlApi(
      s"EXTRACT(NANOSECOND FROM ${timestampLtz("1970-01-01 00:00:00.123456789", 9)})",
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

    testSqlApi("TO_TIMESTAMP('abc')", "null")

    // TO_TIMESTAMP should complement YEAR/MONTH/DAY/HOUR/MINUTE/SECOND/NANO_OF_SECOND
    testSqlApi(
      "TO_TIMESTAMP('2000020210', 'yyyyMMddHH')",
      "2000-02-02 10:00:00.000")

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
      s"CAST(${timestampLtz("1970-01-01 00:00:00.123456789", 9)} " +
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
      s"${timestampLtz("1970-01-01 00:00:00.123456789", 9)} > " +
        s"${timestampLtz("1970-01-01 00:00:00.123456788", 9)}",
      "true")

    testSqlApi(
      s"${timestampLtz("1970-01-01 00:00:00.123456788", 9)} < " +
        s"${timestampLtz("1970-01-01 00:00:00.123456789", 9)}",
      "true")


    // DATE_FORMAT() should support nanosecond
    testSqlApi(
      "DATE_FORMAT(TIMESTAMP '1970-01-01 00:00:00.123456789', 'yyyy/MM/dd HH:mm:ss.SSSSSSSSS')",
      "1970/01/01 00:00:00.123456789")

    testSqlApi(
      s"DATE_FORMAT(${timestampLtz("2018-03-14 01:02:03.123456789", 9)}, " +
        "'yyyy-MM-dd HH:mm:ss.SSSSSSSSS')",
      "2018-03-14 01:02:03.123456789")

  }

  @Test
  def testToTimestampLtzShanghai(): Unit = {
    config.setLocalTimeZone(ZoneId.of("Asia/Shanghai"))

    // INT -> TIMESTAMP_LTZ
    testAllApis(
      toTimestampLtz(100, 0),
      "TO_TIMESTAMP_LTZ(100, 0)",
      "1970-01-01 08:01:40.000")

    // TINYINT -> TIMESTAMP_LTZ
    testAllApis(
      toTimestampLtz(100.cast(DataTypes.TINYINT()), 0),
      "TO_TIMESTAMP_LTZ(CAST(100 AS TINYINT), 0)",
      "1970-01-01 08:01:40.000")

    // BIGINT -> TIMESTAMP_LTZ
    testAllApis(
      toTimestampLtz(100.cast(DataTypes.BIGINT()), 0),
      "TO_TIMESTAMP_LTZ(CAST(100 AS BIGINT), 0)",
      "1970-01-01 08:01:40.000")

    // FLOAT -> TIMESTAMP_LTZ
    testAllApis(
      toTimestampLtz(100.01.cast(DataTypes.FLOAT()), 0),
      "TO_TIMESTAMP_LTZ(CAST(100.01 AS FLOAT), 0)",
      "1970-01-01 08:01:40.010")

    // DOUBLE -> TIMESTAMP_LTZ
    testAllApis(
      toTimestampLtz(100.123.cast(DataTypes.DOUBLE()), 0),
      "TO_TIMESTAMP_LTZ(CAST(100.123 AS DOUBLE), 0)",
      "1970-01-01 08:01:40.123")

    // DECIMAL -> TIMESTAMP_LTZ
    testAllApis(
      toTimestampLtz(100.cast(DataTypes.DECIMAL(38, 18)), 0),
      "TO_TIMESTAMP_LTZ(100, 0)",
      "1970-01-01 08:01:40.000")
    testAllApis(
      toTimestampLtz(-100.cast(DataTypes.DECIMAL(38, 18)), 0),
      "TO_TIMESTAMP_LTZ(-100, 0)",
      "1970-01-01 07:58:20.000")

    // keep scale
    testAllApis(
      toTimestampLtz(1234, 3),
      "TO_TIMESTAMP_LTZ(1234, 3)",
      "1970-01-01 08:00:01.234")
    // drop scale
    testAllApis(
      toTimestampLtz(0.01, 3),
      "TO_TIMESTAMP_LTZ(0.01, 3)",
      "1970-01-01 08:00:00.000")
  }

  @Test
  def testToTimestampLtzUTC(): Unit = {
    config.setLocalTimeZone(ZoneId.of("UTC"))
    testAllApis(
      toTimestampLtz(100, 0),
      "TO_TIMESTAMP_LTZ(100, 0)",
      "1970-01-01 00:01:40.000")

    testAllApis(
      toTimestampLtz(100, 0),
      "TO_TIMESTAMP_LTZ(100, 0)",
      "1970-01-01 00:01:40.000")

    testAllApis(
      toTimestampLtz(1234, 3),
      "TO_TIMESTAMP_LTZ(1234, 3)",
      "1970-01-01 00:00:01.234")

    testAllApis(
      toTimestampLtz(-100, 0),
      "TO_TIMESTAMP_LTZ(-100, 0)",
      "1969-12-31 23:58:20.000")
  }

  @Test
  def testBoundaryForToTimestampLtz(): Unit = {
    config.setLocalTimeZone(ZoneId.of("UTC"))

    // INT
    testAllApis(
      toTimestampLtz(JInt.MIN_VALUE.cast(DataTypes.INT()), 0),
      s"TO_TIMESTAMP_LTZ(CAST(${JInt.MIN_VALUE} AS INTEGER), 0)",
      "1901-12-13 20:45:52.000")
    testAllApis(
      toTimestampLtz(JInt.MAX_VALUE.cast(DataTypes.INT()), 0),
      s"TO_TIMESTAMP_LTZ(CAST(${JInt.MAX_VALUE} AS INTEGER), 0)",
      "2038-01-19 03:14:07.000")

    // TINYINT
    testAllApis(
      toTimestampLtz(-128.cast(DataTypes.TINYINT()), 0),
      s"TO_TIMESTAMP_LTZ(CAST(-128 AS TINYINT), 0)",
      "1969-12-31 23:57:52.000")
    testAllApis(
      toTimestampLtz(127.cast(DataTypes.TINYINT()), 0),
      s"TO_TIMESTAMP_LTZ(CAST(127 AS TINYINT), 0)",
      "1970-01-01 00:02:07.000")

    // BIGINT
    testAllApis(
      toTimestampLtz(JLong.MIN_VALUE.cast(DataTypes.BIGINT()), 0),
      s"TO_TIMESTAMP_LTZ(CAST(${JLong.MIN_VALUE} AS BIGINT), 0)",
      "null")
    testAllApis(
      toTimestampLtz(JLong.MAX_VALUE.cast(DataTypes.BIGINT()), 0),
      s"TO_TIMESTAMP_LTZ(CAST(${JLong.MAX_VALUE} AS BIGINT), 0)",
      "null")

    // FLOAT
    testAllApis(
      toTimestampLtz((-JFloat.MAX_VALUE).cast(DataTypes.FLOAT()), 0),
      s"TO_TIMESTAMP_LTZ(CAST(-${JFloat.MAX_VALUE} AS FLOAT), 0)",
      "null")
    testAllApis(
      toTimestampLtz(JFloat.MAX_VALUE.cast(DataTypes.FLOAT()), 0),
      s"TO_TIMESTAMP_LTZ(CAST(${JFloat.MAX_VALUE} AS FLOAT), 0)",
      "null")

    // DOUBLE
    testAllApis(
      toTimestampLtz((-JDouble.MAX_VALUE).cast(DataTypes.DOUBLE()), 0),
      s"TO_TIMESTAMP_LTZ(CAST(-${JDouble.MAX_VALUE} AS DOUBLE), 0)",
      "null")
    testAllApis(
      toTimestampLtz(JDouble.MAX_VALUE.cast(DataTypes.DOUBLE()), 0),
      s"TO_TIMESTAMP_LTZ(CAST(${JDouble.MAX_VALUE} AS DOUBLE), 0)",
      "null")

    // DECIMAL
    testAllApis(
      toTimestampLtz((-JDouble.MAX_VALUE).cast(DataTypes.DECIMAL(38, 18)), 0),
      s"TO_TIMESTAMP_LTZ(-${JDouble.MAX_VALUE}, 0)",
      "null")
    testAllApis(
      toTimestampLtz(JDouble.MAX_VALUE.cast(DataTypes.DECIMAL(38, 18)), 0),
      s"TO_TIMESTAMP_LTZ(${JDouble.MAX_VALUE}, 0)",
      "null")

    // test valid min/max epoch mills
    testAllApis(
      toTimestampLtz(-62167219200000L, 3),
      s"TO_TIMESTAMP_LTZ(-62167219200000, 3)",
      "0000-01-01 00:00:00.000")
    testAllApis(
      toTimestampLtz(253402300799999L, 3),
      s"TO_TIMESTAMP_LTZ(253402300799999, 3)",
      "9999-12-31 23:59:59.999")
  }

  @Test
  def testInvalidToTimestampLtz(): Unit = {

    // test exceeds valid min/max epoch mills
    testAllApis(
      toTimestampLtz(-62167219200001L, 3),
      s"TO_TIMESTAMP_LTZ(-62167219200001, 3)",
      "null")
    testAllApis(
      toTimestampLtz(253402300800000L, 3),
      s"TO_TIMESTAMP_LTZ(253402300800000, 3)",
      "null")

    // test invalid number of arguments
    testExpectedSqlException(
      "TO_TIMESTAMP_LTZ(123)",
      "Invalid number of arguments to function 'TO_TIMESTAMP_LTZ'. Was expecting 2 arguments")

    // invalid precision
    testExpectedAllApisException(
      toTimestampLtz(12, 1),
      "TO_TIMESTAMP_LTZ(12, 1)",
      "The precision value '1' for function TO_TIMESTAMP_LTZ(numeric, precision) is unsupported," +
        " the supported value is '0' for second or '3' for millisecond.",
      classOf[TableException])

    // invalid precision
    testExpectedAllApisException(
      toTimestampLtz(1000000000, 9),
      "TO_TIMESTAMP_LTZ(1000000000, 9)",
      "The precision value '9' for function TO_TIMESTAMP_LTZ(numeric, precision) is unsupported," +
        " the supported value is '0' for second or '3' for millisecond.",
      classOf[TableException])

    // invalid type for the first input
    testExpectedSqlException(
      "TO_TIMESTAMP_LTZ('test_string_type', 0)",
      "Cannot apply 'TO_TIMESTAMP_LTZ' to arguments of type" +
        " 'TO_TIMESTAMP_LTZ(<CHAR(16)>, <INTEGER>)'. Supported form(s):" +
        " 'TO_TIMESTAMP_LTZ(<NUMERIC>, <INTEGER>)'",
      classOf[ValidationException])
    testExpectedTableApiException(
      toTimestampLtz("test_string_type", 0),
      "toTimestampLtz(test_string_type, 0) requires numeric type for the first input," +
        " but the actual type 'String'.")

    // invalid type for the second input
    testExpectedSqlException(
      "TO_TIMESTAMP_LTZ(123, 'test_string_type')",
      "Cannot apply 'TO_TIMESTAMP_LTZ' to arguments of type" +
        " 'TO_TIMESTAMP_LTZ(<INTEGER>, <CHAR(16)>)'. Supported form(s):" +
        " 'TO_TIMESTAMP_LTZ(<NUMERIC>, <INTEGER>)'")

    testExpectedTableApiException(
      toTimestampLtz(123, "test_string_type"),
      "toTimestampLtz(123, test_string_type) requires numeric type for the second input," +
        " but the actual type 'String'.")
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
    testSqlApi(
      "TIMESTAMPDIFF(MONTH, TIMESTAMP '2021-01-04 00:00:00', DATE '2021-02-04')",
      "1")
    testSqlApi(
      "TIMESTAMPDIFF(MONTH, DATE '2020-01-04', TIMESTAMP '2021-02-04 12:00:00')",
      "13")
    testSqlApi(
      "TIMESTAMPDIFF(MONTH, TIMESTAMP '2021-01-04 00:00:00', TIME '00:00:00')",
      "-612")
    testSqlApi(
      "TIMESTAMPDIFF(MONTH, TIME '00:00:00', TIMESTAMP '2021-02-04 12:00:00')",
      "613")
    testSqlApi(
      "TIMESTAMPDIFF(MONTH, DATE '2021-01-04', TIME '00:00:00')",
      "-612")
    testSqlApi(
      "TIMESTAMPDIFF(MONTH, TIME '00:00:00', DATE '2021-02-04')",
      "613")
  }

  @Test
  def testTimestampLtzArithmetic(): Unit = {
    // TIMESTAMP_LTZ +/- INTERVAL should support nanosecond
    testSqlApi(
      s"${timestampLtz("1970-02-01 00:00:00.123456789")} + INTERVAL '1' YEAR",
      "1971-02-01 00:00:00.123456789")

    testSqlApi(
      s"${timestampLtz("1970-02-01 00:00:00.123456789")} - INTERVAL '1' MONTH",
      "1970-01-01 00:00:00.123456789")

    testSqlApi(
      s"${timestampLtz("1970-02-01 00:00:00.123456789")} + INTERVAL '1' DAY",
      "1970-02-02 00:00:00.123456789")

    testSqlApi(
      s"${timestampLtz("1970-02-01 00:00:00.123456789")} - INTERVAL '1' HOUR",
      "1970-01-31 23:00:00.123456789")

    testSqlApi(
      s"${timestampLtz("1970-02-01 00:00:00.123456789")} + INTERVAL '1' MINUTE",
      "1970-02-01 00:01:00.123456789")

    testSqlApi(
      s"${timestampLtz("1970-02-01 00:00:00.123456789")} - INTERVAL '1' SECOND",
      "1970-01-31 23:59:59.123456789")

    // test TIMESTAMPDIFF for TIMESTAMP_LTZ type
    testSqlApi(
      s"TIMESTAMPDIFF(YEAR, ${timestampLtz("1970-01-01 00:00:00.123456789")}," +
        s" ${timestampLtz("1971-01-02 01:02:03.123456789")})",
      "1")

    testSqlApi(
      s"TIMESTAMPDIFF(MONTH, ${timestampLtz("1970-01-01 00:00:00.123456789")}," +
        s" ${timestampLtz("1971-01-02 01:02:03.123456789")})",
      "12")

    testSqlApi(
      s"TIMESTAMPDIFF(DAY, ${timestampLtz("1970-01-01 00:00:00.123")}," +
        s" ${timestampLtz("1971-01-02 01:02:03.123")})",
      "366")

    testSqlApi(
      s"TIMESTAMPDIFF(HOUR, ${timestampLtz("1970-01-01 00:00:00.123")}," +
        s" ${timestampLtz("1970-01-01 01:02:03.123")})",
      "1")

    testSqlApi(
      s"TIMESTAMPDIFF(MINUTE, ${timestampLtz("1970-01-01 01:02:03.123")}," +
        s" ${timestampLtz("1970-01-01 00:00:00")})",
      "-62")

    testSqlApi(
      s"TIMESTAMPDIFF(SECOND, ${timestampLtz("1970-01-01 00:00:00.123")}," +
        s" ${timestampLtz("1970-01-01 00:02:03.234")})",
      "123")

    // test null input
    testSqlApi(
      s"TIMESTAMPDIFF(SECOND, CAST(null AS TIMESTAMP_LTZ)," +
        s" ${timestampLtz("1970-01-01 00:02:03.234")})",
      "null")
  }

  @Test
  def testInvalidTimestampLtzArithmetic(): Unit = {
    val exceptionMsg = "TIMESTAMP_LTZ only supports diff between the same type."

    // unsupported operand type
    testExpectedSqlException(
      s"TIMESTAMPDIFF(MONTH, ${timestampLtz("1970-01-01 00:00:00.123")}, TIME '00:00:01')",
      exceptionMsg,
      classOf[CodeGenException])

    testExpectedSqlException(
      s"TIMESTAMPDIFF(MONTH, ${timestampLtz("1970-01-01 00:00:00.123")}, DATE '1970-01-01')",
      exceptionMsg,
      classOf[CodeGenException])

    testExpectedSqlException(
      s"TIMESTAMPDIFF(MONTH, ${timestampLtz("1970-01-01 00:00:00.123")}," +
        s" TIMESTAMP '1970-01-01 00:00:00.123')",
      exceptionMsg,
      classOf[CodeGenException])

    testExpectedSqlException(
      s"TIMESTAMPDIFF(SECOND, ${timestampLtz("1970-01-01 00:00:00.123")}," +
        s" TIME '00:00:00.123')",
      exceptionMsg,
      classOf[CodeGenException])

    // invalid operand type
    testExpectedSqlException(
      s"TIMESTAMPDIFF(SECOND, ${timestampLtz("1970-01-01 00:00:00.123")}, 'test_string_type')",
      "Cannot apply 'TIMESTAMPDIFF' to arguments of type" +
        " 'TIMESTAMPDIFF(<SYMBOL>, <TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)>, <CHAR(16)>)'." +
        " Supported form(s): 'TIMESTAMPDIFF(<ANY>, <DATETIME>, <DATETIME>)'")
  }

  // ----------------------------------------------------------------------------------------------

  override def testData: Row = {
    val testData = new Row(27)
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
    testData.setField(24, localDateTime("1970-01-01 00:00:00.123456789")
      .atZone(config.getLocalTimeZone).toInstant)
    testData.setField(25, localDateTime("1970-01-01 00:00:00.123456789").toInstant(ZoneOffset.UTC))
    testData setField(26, new Integer(124).byteValue())
    testData
  }

  override def testDataType: DataType = DataTypes.ROW(
    DataTypes.FIELD("f0", DataTypes.DATE()),
    DataTypes.FIELD("f1", DataTypes.TIME(0)),
    DataTypes.FIELD("f2", DataTypes.TIMESTAMP(3)),
    DataTypes.FIELD("f3", DataTypes.DATE()),
    DataTypes.FIELD("f4", DataTypes.DATE()),
    DataTypes.FIELD("f5", DataTypes.TIME(0)),
    DataTypes.FIELD("f6", DataTypes.TIMESTAMP(9)),
    DataTypes.FIELD("f7", DataTypes.INT()),
    DataTypes.FIELD("f8", DataTypes.BIGINT()),
    DataTypes.FIELD("f9", DataTypes.INTERVAL(DataTypes.MONTH()).bridgedTo(classOf[JInt])),
    DataTypes.FIELD("f10", DataTypes.INTERVAL(DataTypes.SECOND(3)).bridgedTo(classOf[JLong])),
    DataTypes.FIELD("f11", DataTypes.DATE()),
    DataTypes.FIELD("f12", DataTypes.TIME(0)),
    DataTypes.FIELD("f13", DataTypes.TIMESTAMP(9)),
    DataTypes.FIELD("f14", DataTypes.STRING()),
    DataTypes.FIELD("f15", DataTypes.BIGINT()),
    DataTypes.FIELD("f16", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(9)),
    DataTypes.FIELD("f17", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)),
    DataTypes.FIELD("f18", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(9)),
    DataTypes.FIELD("f19", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(9)),
    DataTypes.FIELD("f20", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(9)),
    DataTypes.FIELD("f21", DataTypes.BIGINT()),
    DataTypes.FIELD("f22", DataTypes.INT()),
    DataTypes.FIELD("f23", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(9)),
    DataTypes.FIELD("f24", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(9)),
    DataTypes.FIELD("f25", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(9)),
    DataTypes.FIELD("f26", DataTypes.TINYINT())
  )

  override def containsLegacyTypes: Boolean = false
}
