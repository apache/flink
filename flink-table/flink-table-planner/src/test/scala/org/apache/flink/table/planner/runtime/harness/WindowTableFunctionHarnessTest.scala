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

package org.apache.flink.table.planner.runtime.harness

import org.apache.flink.api.scala._
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.data.{RowData, TimestampData}
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.{HEAP_BACKEND, ROCKSDB_BACKEND, StateBackendMode}
import org.apache.flink.table.planner.runtime.utils.TestData
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor
import org.apache.flink.table.runtime.util.StreamRecordUtils.binaryRecord
import org.apache.flink.table.runtime.util.TimeWindowUtil.toUtcTimestampMills
import org.apache.flink.types.Row
import org.apache.flink.types.RowKind.INSERT

import java.time.{LocalDateTime, ZoneId}
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.{Collection => JCollection}

import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Test}

import scala.collection.JavaConversions._

/**
 * Harness tests for processing-time window table function.
 */
@RunWith(classOf[Parameterized])
class WindowTableFunctionHarnessTest(backend: StateBackendMode, shiftTimeZone: ZoneId)
  extends HarnessTestBase(backend) {

  private val UTC_ZONE_ID = ZoneId.of("UTC")

  private val assertor = new RowDataHarnessAssertor(
    Array(
      DataTypes.STRING().getLogicalType,
      DataTypes.DOUBLE().getLogicalType,
      DataTypes.STRING().getLogicalType,
      DataTypes.STRING().getLogicalType,
      DataTypes.TIMESTAMP_LTZ(3).getLogicalType,
      DataTypes.TIMESTAMP_LTZ(3).getLogicalType,
      DataTypes.TIMESTAMP_LTZ(3).getLogicalType))

  @Before
  override def before(): Unit = {
    super.before()
    val dataId = TestValuesTableFactory.registerData(TestData.windowDataWithTimestamp)
    tEnv.getConfig.setLocalTimeZone(shiftTimeZone)
    tEnv.executeSql(
      s"""
         |CREATE TABLE T1 (
         | `ts` STRING,
         | `int` INT,
         | `double` DOUBLE,
         | `float` FLOAT,
         | `bigdec` DECIMAL(10, 2),
         | `string` STRING,
         | `name` STRING,
         | proctime AS PROCTIME()
         |) WITH (
         | 'connector' = 'values',
         | 'data-id' = '$dataId'
         |)
         |""".stripMargin)
  }

  @Test
  def testProcessingTimeTumbleWindow(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM TABLE(TUMBLE(TABLE T1, DESCRIPTOR(proctime), INTERVAL '5' SECOND))
      """.stripMargin
    val t1 = tEnv.sqlQuery(sql)
    val testHarness = createHarnessTesterForNoState(t1.toAppendStream[Row], "WindowTableFunction")

    testHarness.open()
    ingestData(testHarness)
    val expected = new ConcurrentLinkedQueue[Object]()
    expected.add(record("a", 1.0D, "Hi", null,
      localMills("1970-01-01T00:00:00"),
      localMills("1970-01-01T00:00:05"),
      mills("1970-01-01T00:00:04.999")))
    expected.add(record("a", 2.0D, "Comment#1", null,
      localMills("1970-01-01T00:00:00"),
      localMills("1970-01-01T00:00:05"),
      mills("1970-01-01T00:00:04.999")))
    expected.add(record("a", 2.0D, "Comment#1", null,
      localMills("1970-01-01T00:00:00"),
      localMills("1970-01-01T00:00:05"),
      mills("1970-01-01T00:00:04.999")))
    expected.add(record("a", 5.0D, null, null,
      localMills("1970-01-01T00:00:00"),
      localMills("1970-01-01T00:00:05"),
      mills("1970-01-01T00:00:04.999")))
    expected.add(record("a", 5.0D, "Hi", null,
      localMills("1970-01-01T00:00:00"),
      localMills("1970-01-01T00:00:05"),
      mills("1970-01-01T00:00:04.999")))
    expected.add(record("b", 6.0D, "Hi", null,
      localMills("1970-01-01T00:00:05"),
      localMills("1970-01-01T00:00:10"),
      mills("1970-01-01T00:00:09.999")))
    expected.add(record("b", 3.0D, "Hello", null,
      localMills("1970-01-01T00:00:05"),
      localMills("1970-01-01T00:00:10"),
      mills("1970-01-01T00:00:09.999")))
    expected.add(record("a", null, "Comment#2", null,
      localMills("1970-01-01T00:00:05"),
      localMills("1970-01-01T00:00:10"),
      mills("1970-01-01T00:00:09.999")))
    expected.add(record("b", 4.0D, "Hi", null,
      localMills("1970-01-01T00:00:15"),
      localMills("1970-01-01T00:00:20"),
      mills("1970-01-01T00:00:19.999")))
    expected.add(record(null, 7.0D, null, null,
      localMills("1970-01-01T00:00:30"),
      localMills("1970-01-01T00:00:35"),
      mills("1970-01-01T00:00:34.999")))
    expected.add(record("b", 3.0D, "Comment#3", null,
      localMills("1970-01-01T00:00:30"),
      localMills("1970-01-01T00:00:35"),
      mills("1970-01-01T00:00:34.999")))
    assertor.assertOutputEqualsSorted("result mismatch", expected, testHarness.getOutput)

    testHarness.close()
  }

  @Test
  def testProcessingTimeHopWindow(): Unit = {
    val sql =
      """
        |SELECT * FROM TABLE(
        |  HOP(TABLE T1, DESCRIPTOR(proctime), INTERVAL '5' SECOND, INTERVAL '10' SECOND))
      """.stripMargin
    val t1 = tEnv.sqlQuery(sql)
    val testHarness = createHarnessTesterForNoState(t1.toAppendStream[Row], "WindowTableFunction")

    testHarness.open()
    ingestData(testHarness)

    val expected = new ConcurrentLinkedQueue[Object]()
    expected.add(record("a", 1.0D, "Hi", null,
      localMills("1969-12-31T23:59:55"),
      localMills("1970-01-01T00:00:05"),
      mills("1970-01-01T00:00:04.999")))
    expected.add(record("a", 1.0D, "Hi", null,
      localMills("1970-01-01T00:00"),
      localMills("1970-01-01T00:00:10"),
      mills("1970-01-01T00:00:09.999")))
    expected.add(record("a", 2.0D, "Comment#1", null,
      localMills("1969-12-31T23:59:55"),
      localMills("1970-01-01T00:00:05"),
      mills("1970-01-01T00:00:04.999")))
    expected.add(record("a", 2.0D, "Comment#1", null,
      localMills("1970-01-01T00:00"),
      localMills("1970-01-01T00:00:10"),
      mills("1970-01-01T00:00:09.999")))
    expected.add(record("a", 2.0D, "Comment#1", null,
      localMills("1969-12-31T23:59:55"),
      localMills("1970-01-01T00:00:05"),
      mills("1970-01-01T00:00:04.999")))
    expected.add(record("a", 2.0D, "Comment#1", null,
      localMills("1970-01-01T00:00"),
      localMills("1970-01-01T00:00:10"),
      mills("1970-01-01T00:00:09.999")))
    expected.add(record("a", 5.0D, null, null,
      localMills("1969-12-31T23:59:55"),
      localMills("1970-01-01T00:00:05"),
      mills("1970-01-01T00:00:04.999")))
    expected.add(record("a", 5.0D, null, null,
      localMills("1970-01-01T00:00"),
      localMills("1970-01-01T00:00:10"),
      mills("1970-01-01T00:00:09.999")))
    expected.add(record("a", 5.0D, "Hi", null,
      localMills("1969-12-31T23:59:55"),
      localMills("1970-01-01T00:00:05"),
      mills("1970-01-01T00:00:04.999")))
    expected.add(record("a", 5.0D, "Hi", null,
      localMills("1970-01-01T00:00"),
      localMills("1970-01-01T00:00:10"),
      mills("1970-01-01T00:00:09.999")))
    expected.add(record("b", 6.0D, "Hi", null,
      localMills("1970-01-01T00:00:00"),
      localMills("1970-01-01T00:00:10"),
      mills("1970-01-01T00:00:09.999")))
    expected.add(record("b", 6.0D, "Hi", null,
      localMills("1970-01-01T00:00:05"),
      localMills("1970-01-01T00:00:15"),
      mills("1970-01-01T00:00:14.999")))
    expected.add(record("b", 3.0D, "Hello", null,
      localMills("1970-01-01T00:00:00"),
      localMills("1970-01-01T00:00:10"),
      mills("1970-01-01T00:00:09.999")))
    expected.add(record("b", 3.0D, "Hello", null,
      localMills("1970-01-01T00:00:05"),
      localMills("1970-01-01T00:00:15"),
      mills("1970-01-01T00:00:14.999")))
    expected.add(record("a", null, "Comment#2", null,
      localMills("1970-01-01T00:00:00"),
      localMills("1970-01-01T00:00:10"),
      mills("1970-01-01T00:00:09.999")))
    expected.add(record("a", null, "Comment#2", null,
      localMills("1970-01-01T00:00:05"),
      localMills("1970-01-01T00:00:15"),
      mills("1970-01-01T00:00:14.999")))
    expected.add(record("b", 4.0D, "Hi", null,
      localMills("1970-01-01T00:00:10"),
      localMills("1970-01-01T00:00:20"),
      mills("1970-01-01T00:00:19.999")))
    expected.add(record("b", 4.0D, "Hi", null,
      localMills("1970-01-01T00:00:15"),
      localMills("1970-01-01T00:00:25"),
      mills("1970-01-01T00:00:24.999")))
    expected.add(record(null, 7.0D, null, null,
      localMills("1970-01-01T00:00:25"),
      localMills("1970-01-01T00:00:35"),
      mills("1970-01-01T00:00:34.999")))
    expected.add(record(null, 7.0D, null, null,
      localMills("1970-01-01T00:00:30"),
      localMills("1970-01-01T00:00:40"),
      mills("1970-01-01T00:00:39.999")))
    expected.add(record("b", 3.0D, "Comment#3", null,
      localMills("1970-01-01T00:00:25"),
      localMills("1970-01-01T00:00:35"),
      mills("1970-01-01T00:00:34.999")))
    expected.add(record("b", 3.0D, "Comment#3", null,
      localMills("1970-01-01T00:00:30"),
      localMills("1970-01-01T00:00:40"),
      mills("1970-01-01T00:00:39.999")))
    assertor.assertOutputEqualsSorted("result mismatch", expected, testHarness.getOutput)

    testHarness.close()
  }

  @Test
  def testProcessingTimeCumulateWindow(): Unit = {
    val sql =
      """
        |SELECT * FROM TABLE(
        |  CUMULATE(TABLE T1, DESCRIPTOR(proctime), INTERVAL '5' SECOND, INTERVAL '15' SECOND))
      """.stripMargin
    val t1 = tEnv.sqlQuery(sql)
    val testHarness = createHarnessTesterForNoState(t1.toAppendStream[Row], "WindowTableFunction")

    testHarness.open()
    ingestData(testHarness)

    val expected = new ConcurrentLinkedQueue[Object]()
    expected.add(record("a", 1.0D, "Hi", null,
      localMills("1970-01-01T00:00:00"),
      localMills("1970-01-01T00:00:05"),
      mills("1970-01-01T00:00:04.999")))
    expected.add(record("a", 1.0D, "Hi", null,
      localMills("1970-01-01T00:00"),
      localMills("1970-01-01T00:00:10"),
      mills("1970-01-01T00:00:09.999")))
    expected.add(record("a", 1.0D, "Hi", null,
      localMills("1970-01-01T00:00"),
      localMills("1970-01-01T00:00:15"),
      mills("1970-01-01T00:00:14.999")))
    expected.add(record("a", 2.0D, "Comment#1", null,
      localMills("1970-01-01T00:00:00"),
      localMills("1970-01-01T00:00:05"),
      mills("1970-01-01T00:00:04.999")))
    expected.add(record("a", 2.0D, "Comment#1", null,
      localMills("1970-01-01T00:00"),
      localMills("1970-01-01T00:00:10"),
      mills("1970-01-01T00:00:09.999")))
    expected.add(record("a", 2.0D, "Comment#1", null,
      localMills("1970-01-01T00:00"),
      localMills("1970-01-01T00:00:15"),
      mills("1970-01-01T00:00:14.999")))
    expected.add(record("a", 2.0D, "Comment#1", null,
      localMills("1970-01-01T00:00:00"),
      localMills("1970-01-01T00:00:05"),
      mills("1970-01-01T00:00:04.999")))
    expected.add(record("a", 2.0D, "Comment#1", null,
      localMills("1970-01-01T00:00"),
      localMills("1970-01-01T00:00:10"),
      mills("1970-01-01T00:00:09.999")))
    expected.add(record("a", 2.0D, "Comment#1", null,
      localMills("1970-01-01T00:00"),
      localMills("1970-01-01T00:00:15"),
      mills("1970-01-01T00:00:14.999")))
    expected.add(record("a", 5.0D, null, null,
      localMills("1970-01-01T00:00:00"),
      localMills("1970-01-01T00:00:05"),
      mills("1970-01-01T00:00:04.999")))
    expected.add(record("a", 5.0D, null, null,
      localMills("1970-01-01T00:00"),
      localMills("1970-01-01T00:00:10"),
      mills("1970-01-01T00:00:09.999")))
    expected.add(record("a", 5.0D, null, null,
      localMills("1970-01-01T00:00"),
      localMills("1970-01-01T00:00:15"),
      mills("1970-01-01T00:00:14.999")))
    expected.add(record("a", 5.0D, "Hi", null,
      localMills("1970-01-01T00:00:00"),
      localMills("1970-01-01T00:00:05"),
      mills("1970-01-01T00:00:04.999")))
    expected.add(record("a", 5.0D, "Hi", null,
      localMills("1970-01-01T00:00"),
      localMills("1970-01-01T00:00:10"),
      mills("1970-01-01T00:00:09.999")))
    expected.add(record("a", 5.0D, "Hi", null,
      localMills("1970-01-01T00:00"),
      localMills("1970-01-01T00:00:15"),
      mills("1970-01-01T00:00:14.999")))
    expected.add(record("b", 6.0D, "Hi", null,
      localMills("1970-01-01T00:00:00"),
      localMills("1970-01-01T00:00:10"),
      mills("1970-01-01T00:00:09.999")))
    expected.add(record("b", 6.0D, "Hi", null,
      localMills("1970-01-01T00:00:00"),
      localMills("1970-01-01T00:00:15"),
      mills("1970-01-01T00:00:14.999")))
    expected.add(record("b", 3.0D, "Hello", null,
      localMills("1970-01-01T00:00:00"),
      localMills("1970-01-01T00:00:10"),
      mills("1970-01-01T00:00:09.999")))
    expected.add(record("b", 3.0D, "Hello", null,
      localMills("1970-01-01T00:00:00"),
      localMills("1970-01-01T00:00:15"),
      mills("1970-01-01T00:00:14.999")))
    expected.add(record("a", null, "Comment#2", null,
      localMills("1970-01-01T00:00:00"),
      localMills("1970-01-01T00:00:10"),
      mills("1970-01-01T00:00:09.999")))
    expected.add(record("a", null, "Comment#2", null,
      localMills("1970-01-01T00:00:00"),
      localMills("1970-01-01T00:00:15"),
      mills("1970-01-01T00:00:14.999")))
    expected.add(record("b", 4.0D, "Hi", null,
      localMills("1970-01-01T00:00:15"),
      localMills("1970-01-01T00:00:20"),
      mills("1970-01-01T00:00:19.999")))
    expected.add(record("b", 4.0D, "Hi", null,
      localMills("1970-01-01T00:00:15"),
      localMills("1970-01-01T00:00:25"),
      mills("1970-01-01T00:00:24.999")))
    expected.add(record("b", 4.0D, "Hi", null,
      localMills("1970-01-01T00:00:15"),
      localMills("1970-01-01T00:00:30"),
      mills("1970-01-01T00:00:29.999")))
    expected.add(record(null, 7.0D, null, null,
      localMills("1970-01-01T00:00:30"),
      localMills("1970-01-01T00:00:35"),
      mills("1970-01-01T00:00:34.999")))
    expected.add(record(null, 7.0D, null, null,
      localMills("1970-01-01T00:00:30"),
      localMills("1970-01-01T00:00:40"),
      mills("1970-01-01T00:00:39.999")))
    expected.add(record(null, 7.0D, null, null,
      localMills("1970-01-01T00:00:30"),
      localMills("1970-01-01T00:00:45"),
      mills("1970-01-01T00:00:44.999")))
    expected.add(record("b", 3.0D, "Comment#3", null,
      localMills("1970-01-01T00:00:30"),
      localMills("1970-01-01T00:00:35"),
      mills("1970-01-01T00:00:34.999")))
    expected.add(record("b", 3.0D, "Comment#3", null,
      localMills("1970-01-01T00:00:30"),
      localMills("1970-01-01T00:00:40"),
      mills("1970-01-01T00:00:39.999")))
    expected.add(record("b", 3.0D, "Comment#3", null,
      localMills("1970-01-01T00:00:30"),
      localMills("1970-01-01T00:00:45"),
      mills("1970-01-01T00:00:44.999")))
    assertor.assertOutputEqualsSorted("result mismatch", expected, testHarness.getOutput)

    testHarness.close()
  }

  /**
   * Ingests testing data, the input schema is [name, double, string, proctime].
   * We follow the test data in [[TestData.windowDataWithTimestamp]] to have the same produced
   * result.
   */
  private def ingestData(
      testHarness: OneInputStreamOperatorTestHarness[RowData, RowData]): Unit = {
    // input schema: [name, double, string, proctime]
    testHarness.setProcessingTime(1000L)
    testHarness.processElement(record("a", 1d, "Hi", null))
    testHarness.setProcessingTime(2000L)
    testHarness.processElement(record("a", 2d, "Comment#1", null))
    testHarness.setProcessingTime(3000L)
    testHarness.processElement(record("a", 2d, "Comment#1", null))
    testHarness.setProcessingTime(4000L)
    testHarness.processElement(record("a", 5d, null, null))
    testHarness.processElement(record("a", 5d, "Hi", null))

    testHarness.setProcessingTime(6000L)
    testHarness.processElement(record("b", 6d, "Hi", null))
    testHarness.setProcessingTime(7000L)
    testHarness.processElement(record("b", 3d, "Hello", null))
    testHarness.setProcessingTime(8000L)
    testHarness.processElement(record("a", null, "Comment#2", null))

    testHarness.setProcessingTime(16000L)
    testHarness.processElement(record("b", 4d, "Hi", null))
    testHarness.setProcessingTime(32000L)
    testHarness.processElement(record(null, 7d, null, null))
    testHarness.setProcessingTime(34000L)
    testHarness.processElement(record("b", 3d, "Comment#3", null))
    testHarness.setProcessingTime(50000L)
  }

  private def record(args: Any*): StreamRecord[RowData] = {
    val objs = args.map {
      case l: Long => Long.box(l)
      case d: Double => Double.box(d)
      case arg@_ => arg.asInstanceOf[Object]
    }.toArray
    binaryRecord(INSERT, objs: _*)
  }

  private def localMills(dateTime: String): TimestampData = {
    val windowDateTime = LocalDateTime.parse(dateTime).atZone(UTC_ZONE_ID)
    TimestampData.fromEpochMillis(
      toUtcTimestampMills(windowDateTime.toInstant.toEpochMilli, shiftTimeZone))
  }

  private def mills(dateTime: String): TimestampData = {
    val windowDateTime = LocalDateTime.parse(dateTime).atZone(UTC_ZONE_ID)
    TimestampData.fromEpochMillis(windowDateTime.toInstant.toEpochMilli)
  }
}

object WindowTableFunctionHarnessTest {

  @Parameterized.Parameters(name = "StateBackend={0}, TimeZone={1}")
  def parameters(): JCollection[Array[java.lang.Object]] = {
    Seq[Array[AnyRef]](
      Array(HEAP_BACKEND, ZoneId.of("UTC")),
      Array(HEAP_BACKEND, ZoneId.of("Asia/Shanghai")),
      Array(ROCKSDB_BACKEND, ZoneId.of("UTC")),
      Array(ROCKSDB_BACKEND, ZoneId.of("Asia/Shanghai")))
  }
}
