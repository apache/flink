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
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.config.OptimizerConfigOptions
import org.apache.flink.table.data.{RowData, TimestampData}
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.{HEAP_BACKEND, ROCKSDB_BACKEND, StateBackendMode}
import org.apache.flink.table.planner.runtime.utils.TestData
import org.apache.flink.table.runtime.typeutils.RowDataSerializer
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor
import org.apache.flink.table.runtime.util.StreamRecordUtils.binaryRecord
import org.apache.flink.table.runtime.util.TimeWindowUtil.toUtcTimestampMills
import org.apache.flink.table.types.logical.LogicalType
import org.apache.flink.types.Row
import org.apache.flink.types.RowKind.INSERT

import org.junit.{Before, Test}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import java.time.{LocalDateTime, ZoneId}
import java.util.{Collection => JCollection}
import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.JavaConversions._

/**
 * Harness tests for processing-time window aggregate. We can't test them in
 * [[WindowAggregateITCase]] because the result is non-deterministic, therefore we use harness to
 * test them.
 */
@RunWith(classOf[Parameterized])
class WindowAggregateHarnessTest(backend: StateBackendMode, shiftTimeZone: ZoneId)
  extends HarnessTestBase(backend) {

  private val UTC_ZONE_ID = ZoneId.of("UTC")

  @Before
  override def before(): Unit = {
    super.before()
    val dataId = TestValuesTableFactory.registerData(TestData.windowDataWithTimestamp)
    tEnv.getConfig.setLocalTimeZone(shiftTimeZone)
    tEnv.executeSql(s"""
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

  /**
   * The produced result should be the same with
   * [[WindowAggregateITCase.testEventTimeTumbleWindow()]].
   */
  @Test
  def testProcessingTimeTumbleWindow(): Unit = {
    val (testHarness, outputTypes) = createProcessingTimeTumbleWindowOperator
    val assertor = new RowDataHarnessAssertor(outputTypes)

    testHarness.open()
    ingestData(testHarness)
    val expected = new ConcurrentLinkedQueue[Object]()
    expected.add(
      record(
        "a",
        4L,
        5.0d,
        2L,
        localMills("1970-01-01T00:00:00"),
        localMills("1970-01-01T00:00:05")))
    expected.add(
      record(
        "a",
        1L,
        null,
        1L,
        localMills("1970-01-01T00:00:05"),
        localMills("1970-01-01T00:00:10")))
    expected.add(
      record(
        "b",
        2L,
        6.0d,
        2L,
        localMills("1970-01-01T00:00:05"),
        localMills("1970-01-01T00:00:10")))
    expected.add(
      record(
        "b",
        1L,
        4.0d,
        1L,
        localMills("1970-01-01T00:00:15"),
        localMills("1970-01-01T00:00:20")))
    expected.add(
      record(
        "b",
        1L,
        3.0d,
        1L,
        localMills("1970-01-01T00:00:30"),
        localMills("1970-01-01T00:00:35")))
    expected.add(
      record(
        null,
        1L,
        7.0d,
        0L,
        localMills("1970-01-01T00:00:30"),
        localMills("1970-01-01T00:00:35")))

    assertor.assertOutputEqualsSorted("result mismatch", expected, testHarness.getOutput)

    testHarness.close()
  }

  private def createProcessingTimeTumbleWindowOperator()
      : (KeyedOneInputStreamOperatorTestHarness[RowData, RowData, RowData], Array[LogicalType]) = {
    val sql =
      """
        |SELECT
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(*),
        |  MAX(`double`),
        |  COUNT(DISTINCT `string`)
        |FROM TABLE(
        |   TUMBLE(TABLE T1, DESCRIPTOR(proctime), INTERVAL '5' SECOND))
        |GROUP BY `name`, window_start, window_end
      """.stripMargin
    val t1 = tEnv.sqlQuery(sql)
    val testHarness = createHarnessTester(t1.toAppendStream[Row], "WindowAggregate")
    // window aggregate put window properties at the end of aggs
    val outputTypes =
      Array(
        DataTypes.STRING().getLogicalType,
        DataTypes.BIGINT().getLogicalType,
        DataTypes.DOUBLE().getLogicalType,
        DataTypes.BIGINT().getLogicalType,
        DataTypes.TIMESTAMP_LTZ(3).getLogicalType,
        DataTypes.TIMESTAMP_LTZ(3).getLogicalType
      )
    (testHarness, outputTypes)
  }

  /**
   * The produced result should be the same with [[WindowAggregateITCase.testEventTimeHopWindow()]].
   */
  @Test
  def testProcessingTimeHopWindow(): Unit = {
    val sql =
      """
        |SELECT
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(*),
        |  MAX(`double`),
        |  COUNT(DISTINCT `string`)
        |FROM TABLE(
        |   HOP(TABLE T1, DESCRIPTOR(proctime), INTERVAL '5' SECOND, INTERVAL '10' SECOND))
        |GROUP BY `name`, window_start, window_end
      """.stripMargin
    val t1 = tEnv.sqlQuery(sql)
    val testHarness = createHarnessTester(t1.toAppendStream[Row], "WindowAggregate")
    // window aggregate put window properties at the end of aggs
    val assertor = new RowDataHarnessAssertor(
      Array(
        DataTypes.STRING().getLogicalType,
        DataTypes.BIGINT().getLogicalType,
        DataTypes.DOUBLE().getLogicalType,
        DataTypes.BIGINT().getLogicalType,
        DataTypes.TIMESTAMP_LTZ(3).getLogicalType,
        DataTypes.TIMESTAMP_LTZ(3).getLogicalType
      ))

    testHarness.open()
    ingestData(testHarness)

    val expected = new ConcurrentLinkedQueue[Object]()
    expected.add(
      record(
        "a",
        4L,
        5.0d,
        2L,
        localMills("1969-12-31T23:59:55"),
        localMills("1970-01-01T00:00:05")))
    expected.add(
      record(
        "a",
        5L,
        5.0d,
        3L,
        localMills("1970-01-01T00:00:00"),
        localMills("1970-01-01T00:00:10")))
    expected.add(
      record(
        "a",
        1L,
        null,
        1L,
        localMills("1970-01-01T00:00:05"),
        localMills("1970-01-01T00:00:15")))
    expected.add(
      record(
        "b",
        2L,
        6.0d,
        2L,
        localMills("1970-01-01T00:00:00"),
        localMills("1970-01-01T00:00:10")))
    expected.add(
      record(
        "b",
        2L,
        6.0d,
        2L,
        localMills("1970-01-01T00:00:05"),
        localMills("1970-01-01T00:00:15")))
    expected.add(
      record(
        "b",
        1L,
        4.0d,
        1L,
        localMills("1970-01-01T00:00:10"),
        localMills("1970-01-01T00:00:20")))
    expected.add(
      record(
        "b",
        1L,
        4.0d,
        1L,
        localMills("1970-01-01T00:00:15"),
        localMills("1970-01-01T00:00:25")))
    expected.add(
      record(
        "b",
        1L,
        3.0d,
        1L,
        localMills("1970-01-01T00:00:25"),
        localMills("1970-01-01T00:00:35")))
    expected.add(
      record(
        "b",
        1L,
        3.0d,
        1L,
        localMills("1970-01-01T00:00:30"),
        localMills("1970-01-01T00:00:40")))
    expected.add(
      record(
        null,
        1L,
        7.0d,
        0L,
        localMills("1970-01-01T00:00:25"),
        localMills("1970-01-01T00:00:35")))
    expected.add(
      record(
        null,
        1L,
        7.0d,
        0L,
        localMills("1970-01-01T00:00:30"),
        localMills("1970-01-01T00:00:40")))

    assertor.assertOutputEqualsSorted("result mismatch", expected, testHarness.getOutput)

    testHarness.close()
  }

  /**
   * The produced result should be the same with
   * [[WindowAggregateITCase.testEventTimeCumulateWindow()]].
   */
  @Test
  def testProcessingTimeCumulateWindow(): Unit = {
    val sql =
      """
        |SELECT
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(*),
        |  MAX(`double`),
        |  COUNT(DISTINCT `string`)
        |FROM TABLE(
        |   CUMULATE(
        |     TABLE T1,
        |     DESCRIPTOR(proctime),
        |     INTERVAL '5' SECOND,
        |     INTERVAL '15' SECOND))
        |GROUP BY `name`, window_start, window_end
      """.stripMargin
    val t1 = tEnv.sqlQuery(sql)
    val testHarness = createHarnessTester(t1.toAppendStream[Row], "WindowAggregate")
    // window aggregate put window properties at the end of aggs
    val assertor = new RowDataHarnessAssertor(
      Array(
        DataTypes.STRING().getLogicalType,
        DataTypes.BIGINT().getLogicalType,
        DataTypes.DOUBLE().getLogicalType,
        DataTypes.BIGINT().getLogicalType,
        DataTypes.TIMESTAMP_LTZ(3).getLogicalType,
        DataTypes.TIMESTAMP_LTZ(3).getLogicalType
      ))

    testHarness.open()
    ingestData(testHarness)

    val expected = new ConcurrentLinkedQueue[Object]()
    expected.add(
      record(
        "a",
        4L,
        5.0d,
        2L,
        localMills("1970-01-01T00:00:00"),
        localMills("1970-01-01T00:00:05")))
    expected.add(
      record(
        "a",
        5L,
        5.0d,
        3L,
        localMills("1970-01-01T00:00:00"),
        localMills("1970-01-01T00:00:10")))
    expected.add(
      record(
        "a",
        5L,
        5.0d,
        3L,
        localMills("1970-01-01T00:00:00"),
        localMills("1970-01-01T00:00:15")))
    expected.add(
      record(
        "b",
        2L,
        6.0d,
        2L,
        localMills("1970-01-01T00:00:00"),
        localMills("1970-01-01T00:00:10")))
    expected.add(
      record(
        "b",
        2L,
        6.0d,
        2L,
        localMills("1970-01-01T00:00:00"),
        localMills("1970-01-01T00:00:15")))
    expected.add(
      record(
        "b",
        1L,
        4.0d,
        1L,
        localMills("1970-01-01T00:00:15"),
        localMills("1970-01-01T00:00:20")))
    expected.add(
      record(
        "b",
        1L,
        4.0d,
        1L,
        localMills("1970-01-01T00:00:15"),
        localMills("1970-01-01T00:00:25")))
    expected.add(
      record(
        "b",
        1L,
        4.0d,
        1L,
        localMills("1970-01-01T00:00:15"),
        localMills("1970-01-01T00:00:30")))
    expected.add(
      record(
        "b",
        1L,
        3.0d,
        1L,
        localMills("1970-01-01T00:00:30"),
        localMills("1970-01-01T00:00:35")))
    expected.add(
      record(
        "b",
        1L,
        3.0d,
        1L,
        localMills("1970-01-01T00:00:30"),
        localMills("1970-01-01T00:00:40")))
    expected.add(
      record(
        "b",
        1L,
        3.0d,
        1L,
        localMills("1970-01-01T00:00:30"),
        localMills("1970-01-01T00:00:45")))
    expected.add(
      record(
        null,
        1L,
        7.0d,
        0L,
        localMills("1970-01-01T00:00:30"),
        localMills("1970-01-01T00:00:35")))
    expected.add(
      record(
        null,
        1L,
        7.0d,
        0L,
        localMills("1970-01-01T00:00:30"),
        localMills("1970-01-01T00:00:40")))
    expected.add(
      record(
        null,
        1L,
        7.0d,
        0L,
        localMills("1970-01-01T00:00:30"),
        localMills("1970-01-01T00:00:45")))

    assertor.assertOutputEqualsSorted("result mismatch", expected, testHarness.getOutput)

    testHarness.close()
  }

  @Test
  def testCloseWithoutOpen(): Unit = {
    val (testHarness, outputTypes) = createProcessingTimeTumbleWindowOperator
    testHarness.setup(new RowDataSerializer(outputTypes: _*))
    // simulate a failover after a failed task open, expect no exception happens
    testHarness.close()
  }

  /** Processing time window doesn't support two-phase, so add a single two-phase test. */
  @Test
  def testTwoPhaseWindowAggregateCloseWithoutOpen(): Unit = {
    val timestampDataId = TestValuesTableFactory.registerData(TestData.windowDataWithTimestamp)
    tEnv.executeSql(s"""
                       |CREATE TABLE T2 (
                       | `ts` STRING,
                       | `int` INT,
                       | `double` DOUBLE,
                       | `float` FLOAT,
                       | `bigdec` DECIMAL(10, 2),
                       | `string` STRING,
                       | `name` STRING,
                       | `rowtime` AS
                       | TO_TIMESTAMP(`ts`),
                       | WATERMARK for `rowtime` AS `rowtime` - INTERVAL '1' SECOND
                       |) WITH (
                       | 'connector' = 'values',
                       | 'data-id' = '$timestampDataId',
                       | 'failing-source' = 'false'
                       |)
                       |""".stripMargin)

    tEnv.getConfig.set(OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY, "TWO_PHASE")

    val sql =
      """
        |SELECT
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(*),
        |  MAX(`double`),
        |  COUNT(DISTINCT `string`)
        |FROM TABLE(
        |   TUMBLE(TABLE T2, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |GROUP BY `name`, window_start, window_end
      """.stripMargin
    val t1 = tEnv.sqlQuery(sql)
    val stream: DataStream[Row] = t1.toAppendStream[Row]

    val testHarness = createHarnessTesterForNoState(stream, "LocalWindowAggregate")
    // window aggregate put window properties at the end of aggs
    val outputTypes = Array(
      DataTypes.STRING().getLogicalType,
      DataTypes.BIGINT().getLogicalType,
      DataTypes.DOUBLE().getLogicalType,
      DataTypes.BIGINT().getLogicalType,
      DataTypes.TIMESTAMP_LTZ(3).getLogicalType,
      DataTypes.TIMESTAMP_LTZ(3).getLogicalType
    )
    testHarness.setup(new RowDataSerializer(outputTypes: _*))

    // simulate a failover after a failed task open, expect no exception happens
    testHarness.close()

    val testHarness1 = createHarnessTester(stream, "GlobalWindowAggregate")
    testHarness1.setup(new RowDataSerializer(outputTypes: _*))

    // simulate a failover after a failed task open, expect no exception happens
    testHarness1.close()
  }

  /**
   * Ingests testing data, the input schema is [name, double, string, proctime]. We follow the test
   * data in [[TestData.windowDataWithTimestamp]] to have the same produced result. The only
   * difference is we don't ingest the late data in this test, so they should produce same result.
   */
  private def ingestData(
      testHarness: KeyedOneInputStreamOperatorTestHarness[RowData, RowData, RowData]): Unit = {
    // input schema: [name, double, string, proctime]
    testHarness.setProcessingTime(1000L)
    testHarness.processElement(record("a", 1d, "Hi", null))
    testHarness.setProcessingTime(2000L)
    testHarness.processElement(record("a", 2d, "Comment#1", null))
    testHarness.setProcessingTime(3000L)
    testHarness.processElement(record("a", 2d, "Comment#1", null))
    testHarness.setProcessingTime(4000L)
    testHarness.processElement(record("a", 5d, null, null))

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
      case arg @ _ => arg.asInstanceOf[Object]
    }.toArray
    binaryRecord(INSERT, objs: _*)
  }

  private def localMills(dateTime: String): TimestampData = {
    val windowDateTime = LocalDateTime.parse(dateTime).atZone(UTC_ZONE_ID)
    TimestampData.fromEpochMillis(
      toUtcTimestampMills(windowDateTime.toInstant.toEpochMilli, shiftTimeZone))
  }
}

object WindowAggregateHarnessTest {

  @Parameterized.Parameters(name = "StateBackend={0}, TimeZone={1}")
  def parameters(): JCollection[Array[java.lang.Object]] = {
    Seq[Array[AnyRef]](
      Array(HEAP_BACKEND, ZoneId.of("UTC")),
      Array(HEAP_BACKEND, ZoneId.of("Asia/Shanghai")),
      Array(ROCKSDB_BACKEND, ZoneId.of("UTC")),
      Array(ROCKSDB_BACKEND, ZoneId.of("Asia/Shanghai"))
    )
  }
}
