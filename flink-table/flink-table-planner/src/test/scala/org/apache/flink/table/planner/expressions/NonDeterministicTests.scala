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
import java.time.{LocalDate, LocalDateTime, ZoneId}
import java.util.TimeZone
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.configuration.ExecutionOptions
import org.apache.flink.table.api._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.planner.expressions.utils.ExpressionTestBase
import org.apache.flink.table.planner.utils.InternalConfigOptions
import org.apache.flink.types.Row

import org.junit.Assert.assertEquals
import org.junit.Test

import scala.collection.mutable

/**
  * Tests that check all non-deterministic functions can be executed.
  */
class NonDeterministicTests extends ExpressionTestBase {

  @Test
  def testTemporalFunctionsInStreamMode(): Unit = {
    val temporalFunctions = getCodeGenFunctions(List(
      "CURRENT_DATE",
      "CURRENT_TIME",
      "CURRENT_TIMESTAMP",
      "CURRENT_ROW_TIMESTAMP()",
      "NOW()",
      "LOCALTIME",
      "LOCALTIMESTAMP"))
    val round1 = evaluateFunctionResult(temporalFunctions)
    Thread.sleep(1 * 1000L)
    val round2: List[String] = evaluateFunctionResult(temporalFunctions)

    assertEquals(round1.size, round2.size)
    round1.zip(round2).zipWithIndex.foreach {
      case ((result1: String, result2: String), index: Int) =>
        // CURRENT_DATE may be same between two records
        if (index == 0) {
          assert(result1 <= result2)
        } else {
          assert(result1 < result2)
        }
    }

    // check CURRENT_TIMESTAMP function and CURRENT_ROW_TIMESTAMP() function
    // should return same value for one record in stream job
    val currentTimeStampIndex = 2
    val currentRowTimestampIndex = 3
    assertEquals(round1(currentTimeStampIndex), round1(currentRowTimestampIndex))
    assertEquals(round2(currentTimeStampIndex), round2(currentRowTimestampIndex))

  }

  @Test
  def testTemporalFunctionsInBatchMode(): Unit = {
    val zoneId = ZoneId.of("Asia/Shanghai")
    config.setLocalTimeZone(zoneId)
    config.getConfiguration.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH)

    config.getConfiguration.setLong(InternalConfigOptions.TABLE_QUERY_START_EPOCH_TIME, 1123L)
    config.getConfiguration.setLong(InternalConfigOptions.TABLE_QUERY_START_LOCAL_TIME,
      1123L + TimeZone.getTimeZone(zoneId).getOffset(1123L))

    val temporalFunctions = getCodeGenFunctions(List(
      "CURRENT_DATE",
      "CURRENT_TIME",
      "CURRENT_TIMESTAMP",
      "NOW()",
      "LOCALTIME",
      "LOCALTIMESTAMP"))

    val expected = mutable.MutableList[String](
      "1970-01-01",
      "08:00:01",
      "1970-01-01 08:00:01.123",
      "1970-01-01 08:00:01.123",
      "08:00:01",
      "1970-01-01 08:00:01.123")

    val result = evaluateFunctionResult(temporalFunctions)
    assertEquals(expected.toList.sorted, result.sorted)

  }

  @Test
  def testCurrentRowTimestampFunctionsInBatchMode(): Unit = {
    config.getConfiguration.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH)
    val temporalFunctions = getCodeGenFunctions(List("CURRENT_ROW_TIMESTAMP()"))

    val round1 = evaluateFunctionResult(temporalFunctions)
    Thread.sleep(1 * 1000L)
    val round2: List[String] = evaluateFunctionResult(temporalFunctions)

    assertEquals(round1.size, round2.size)
    round1.zip(round2).foreach {
      case (result1: String, result2: String) =>
        assert(result1 < result2)
    }
  }

  @Test
  def testTemporalFunctionsInUTC(): Unit = {
    testTemporalTimestamp(ZoneId.of("UTC"))
  }

  @Test
  def testTemporalFunctionsInShanghai(): Unit = {
    testTemporalTimestamp(ZoneId.of("Asia/Shanghai"))
  }

  private def testTemporalTimestamp(zoneId: ZoneId) :Unit = {
    config.setLocalTimeZone(zoneId)
    val localDateTime = LocalDateTime.now(zoneId)

    val formattedLocalTime = localDateTime
      .toLocalTime
      .format(DateTimeFormatter.ofPattern("HH:mm:ss"))
    val formattedLocalDateTime = localDateTime
      .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    val formattedCurrentDate = localDateTime
      .format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    val formattedCurrentTime = localDateTime
      .toLocalTime
      .format(DateTimeFormatter.ofPattern("HH:mm:ss"))
    val formattedCurrentTimestamp = localDateTime
      .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))

    // the LOCALTIME/LOCALTIMESTAMP/CURRENT_DATE/CURRENT_TIME/CURRENT_TIMESTAMP/NOW() functions
    // are not deterministic, thus we use following pattern to check the returned SQL value
    // in session time zone
    testSqlApi(
      s"TIME_SUB(LOCALTIME, TIME '$formattedLocalTime') <= 60000",
      "true")
    testSqlApi(
      s"TIMESTAMPDIFF(SECOND, TIMESTAMP '$formattedLocalDateTime', LOCALTIMESTAMP) <= 60",
      "true")
    testSqlApi(
      s"DATE_SUB(CURRENT_DATE, DATE '$formattedCurrentDate') >= 0",
      "true")

    testSqlApi(
      s"TIME_SUB(CURRENT_TIME, TIME '$formattedCurrentTime') <= 60000",
      "true")

    testSqlApi(
      s"TIMESTAMPDIFF(SECOND, ${timestampLtz(formattedCurrentTimestamp)}, CURRENT_TIMESTAMP) <= 60",
      "true")

    testSqlApi(
      s"TIMESTAMPDIFF(SECOND, ${timestampLtz(formattedCurrentTimestamp)}, NOW()) <= 60",
      "true")

    testSqlApi(
      s"TIMESTAMPDIFF(SECOND, " +
        s"${timestampLtz(formattedCurrentTimestamp)}, CURRENT_ROW_TIMESTAMP()) <= 60",
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
    "TIME_SUB" -> TimeDiffFun,
    "DATE_SUB" -> DateDiffFun
  )
}

object TimeDiffFun extends ScalarFunction {

  private val millsInDay = 24 * 60 * 60 * 1000L

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

object DateDiffFun extends ScalarFunction {

  def eval(d1: LocalDate, d2: LocalDate): Long = {
    d1.toEpochDay - d2.toEpochDay
  }
}
