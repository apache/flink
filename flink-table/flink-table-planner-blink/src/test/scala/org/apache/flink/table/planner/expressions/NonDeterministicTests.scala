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

import java.sql.Time
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}

import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.planner.expressions.utils.ExpressionTestBase
import org.apache.flink.types.Row

import org.junit.Test

/**
  * Tests that check all non-deterministic functions can be executed.
  */
class NonDeterministicTests extends ExpressionTestBase {

  @Test
  def testCurrentDate(): Unit = {
    testAllApis(
      currentDate().isGreater("1970-01-01".toDate),
      "currentDate() > '1970-01-01'.toDate",
      "CURRENT_DATE > DATE '1970-01-01'",
      "true")
  }

  @Test
  def testCurrentTime(): Unit = {
    testAllApis(
      currentTime().isGreaterOrEqual("00:00:00".toTime),
      "currentTime() >= '00:00:00'.toTime",
      "CURRENT_TIME >= TIME '00:00:00'",
      "true")
  }

  @Test
  def testCurrentTimestamp(): Unit = {
    testAllApis(
      currentTimestamp().isGreater("1970-01-01 00:00:00".toTimestamp),
      "currentTimestamp() > '1970-01-01 00:00:00'.toTimestamp",
      "CURRENT_TIMESTAMP > TIMESTAMP '1970-01-01 00:00:00'",
      "true")
  }

  @Test
  def testNow(): Unit = {
    testSqlApi(
      "NOW() > TIMESTAMP '1970-01-01 00:00:00'",
      "true")
  }

  @Test
  def testLocalTimestampInUTC(): Unit = {
    config.setLocalTimeZone(ZoneId.of("UTC"))
    val localDateTime = LocalDateTime.now(ZoneId.of("UTC"))

    val formattedLocalTime = localDateTime
      .toLocalTime
      .format(DateTimeFormatter.ofPattern("HH:mm:ss"))
    val formattedLocalDateTime = localDateTime
      .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))

    // the LOCALTIME/LOCALTIMESTAMP functions are not deterministic, thus we
    // use following pattern to check it return SQL timestamp in session time zone UTC
    testSqlApi(
      s"TIMESUB(LOCALTIME, TIME '$formattedLocalTime') <= 60000",
      "true")
    testSqlApi(
      s"TIMESTAMPDIFF(SECOND, TIMESTAMP '$formattedLocalDateTime', LOCALTIMESTAMP) <= 60",
      "true")
  }

  @Test
  def testLocalTimestampInShanghai(): Unit = {
    config.setLocalTimeZone(ZoneId.of("Asia/Shanghai"))
    val localDateTime = LocalDateTime.now(ZoneId.of("Asia/Shanghai"))

    val formattedLocalTime = localDateTime
      .toLocalTime
      .format(DateTimeFormatter.ofPattern("HH:mm:ss"))
    val formattedLocalDateTime = localDateTime
      .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))

    // the LOCALTIME/LOCALTIMESTAMP functions are not deterministic, thus we
    // use following pattern to check it return SQL timestamp in session time zone Shanghai
    testSqlApi(
      s"TIMESUB(LOCALTIME, TIME '$formattedLocalTime') <= 60000",
      "true")
    testSqlApi(
      s"TIMESTAMPDIFF(SECOND, TIMESTAMP '$formattedLocalDateTime', LOCALTIMESTAMP) <= 60",
      "true")
  }

  @Test
  def testLocalTime(): Unit = {
    testAllApis(
      localTime().isGreaterOrEqual("00:00:00".toTime),
      "localTime() >= '00:00:00'.toTime",
      "LOCALTIME >= TIME '00:00:00'",
      "true")
  }

  @Test
  def testUUID(): Unit = {
    testAllApis(
      uuid().charLength(),
      "uuid().charLength",
      "CHARACTER_LENGTH(UUID())",
      "36")
  }

  // ----------------------------------------------------------------------------------------------

  override def testData: Row = new Row(0)

  override def typeInfo: RowTypeInfo = new RowTypeInfo()

  override def functions: Map[String, ScalarFunction] = Map(
    "TIMESUB" -> TimeDiffFun
  )
}

object TimeDiffFun extends ScalarFunction {

  val millsInDay = 24 * 60 * 60 * 1000L

  def eval(t1: Time, t2: Time): Long = {
    // when the two time points crosses two days, e.g:
    // the t1 may be '00:00:01.001' and the t2 may be '23:59:59.999'
    // we simply assume the two times were produced less than 1 minute
    if (t1.getTime < t2.getTime && millsInDay - Math.abs(t1.getTime - t2.getTime) < 60000) {
        t1.getTime + millsInDay - t2.getTime
    }
    else {
      t1.getTime - t2.getTime
    }
  }
}
