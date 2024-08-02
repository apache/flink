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

import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.config.{AggregatePhaseStrategy, OptimizerConfigOptions}
import org.apache.flink.table.data.{RowData, TimestampData}
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.runtime.harness.WindowAggregateHarnessTest.{CUMULATE, HOP, TUMBLE}
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.{HEAP_BACKEND, ROCKSDB_BACKEND, StateBackendMode}
import org.apache.flink.table.planner.runtime.utils.TestData
import org.apache.flink.table.runtime.typeutils.RowDataSerializer
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor
import org.apache.flink.table.runtime.util.StreamRecordUtils.binaryRecord
import org.apache.flink.table.runtime.util.TimeWindowUtil.toUtcTimestampMills
import org.apache.flink.table.types.logical.LogicalType
import org.apache.flink.testutils.junit.extensions.parameterized.{ParameterizedTestExtension, Parameters}
import org.apache.flink.types.{Row, RowKind}
import org.apache.flink.types.RowKind.{DELETE, INSERT, UPDATE_AFTER, UPDATE_BEFORE}

import org.junit.jupiter.api.{BeforeEach, TestTemplate}
import org.junit.jupiter.api.extension.ExtendWith

import java.time.{LocalDateTime, ZoneId}
import java.util.{Collection => JCollection}
import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.JavaConversions._

/**
 * Harness tests for processing-time window aggregate. We can't test them in
 * [[WindowAggregateITCase]] because the result is non-deterministic, therefore we use harness to
 * test them.
 */
@ExtendWith(Array(classOf[ParameterizedTestExtension]))
class WindowAggregateHarnessTest(backend: StateBackendMode, shiftTimeZone: ZoneId)
  extends HarnessTestBase(backend) {

  private val UTC_ZONE_ID = ZoneId.of("UTC")

  @BeforeEach
  override def before(): Unit = {
    super.before()
    tEnv.getConfig.setLocalTimeZone(shiftTimeZone)

    val insertOnlyDataId = TestValuesTableFactory.registerData(TestData.windowDataWithTimestamp)
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
                       | 'data-id' = '$insertOnlyDataId'
                       |)
                       |""".stripMargin)

    val changelogDataId =
      TestValuesTableFactory.registerData(TestData.windowChangelogDataWithTimestamp)
    tEnv.executeSql(s"""
                       |CREATE TABLE T1_CDC (
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
                       | 'data-id' = '$changelogDataId',
                       | 'changelog-mode' = 'I,UA,UB,D'
                       |)
                       |""".stripMargin)
  }

  /**
   * The produced result should be the same with
   * [[WindowAggregateITCase.testEventTimeTumbleWindow()]].
   */
  @TestTemplate
  def testProcessingTimeTumbleWindow(): Unit = {
    val (testHarness, outputTypes) = createProcessingTimeWindowOperator(TUMBLE, isCDCSource = false)
    val assertor = new RowDataHarnessAssertor(outputTypes)

    testHarness.open()
    ingestData(testHarness, isCDCSource = false)
    val expected = new ConcurrentLinkedQueue[Object]()
    expected.add(
      insertRecord(
        "a",
        4L,
        5.0d,
        2L,
        localMills("1970-01-01T00:00:00"),
        localMills("1970-01-01T00:00:05")))
    expected.add(
      insertRecord(
        "a",
        1L,
        null,
        1L,
        localMills("1970-01-01T00:00:05"),
        localMills("1970-01-01T00:00:10")))
    expected.add(
      insertRecord(
        "b",
        2L,
        6.0d,
        2L,
        localMills("1970-01-01T00:00:05"),
        localMills("1970-01-01T00:00:10")))
    expected.add(
      insertRecord(
        "b",
        1L,
        4.0d,
        1L,
        localMills("1970-01-01T00:00:15"),
        localMills("1970-01-01T00:00:20")))
    expected.add(
      insertRecord(
        "b",
        1L,
        3.0d,
        1L,
        localMills("1970-01-01T00:00:30"),
        localMills("1970-01-01T00:00:35")))
    expected.add(
      insertRecord(
        null,
        1L,
        7.0d,
        0L,
        localMills("1970-01-01T00:00:30"),
        localMills("1970-01-01T00:00:35")))

    assertor.assertOutputEqualsSorted("result mismatch", expected, testHarness.getOutput)

    testHarness.close()
  }

  /**
   * The produced result should be the same with
   * [[WindowAggregateITCase.testEventTimeTumbleWindowWithCDCSource()]].
   */
  @TestTemplate
  def testProcessingTimeTumbleWindowWithCDCSource(): Unit = {
    val (testHarness, outputTypes) = createProcessingTimeWindowOperator(TUMBLE, isCDCSource = true)
    val assertor = new RowDataHarnessAssertor(outputTypes)

    testHarness.open()
    ingestData(testHarness, true)
    val expected = new ConcurrentLinkedQueue[Object]()
    expected.add(
      insertRecord(
        "a",
        3L,
        22.0d,
        2L,
        localMills("1970-01-01T00:00:00"),
        localMills("1970-01-01T00:00:05")))
    expected.add(
      insertRecord(
        "a",
        1L,
        null,
        1L,
        localMills("1970-01-01T00:00:05"),
        localMills("1970-01-01T00:00:10")))
    expected.add(
      insertRecord(
        "b",
        2L,
        6.0d,
        2L,
        localMills("1970-01-01T00:00:05"),
        localMills("1970-01-01T00:00:10")))
    expected.add(
      insertRecord(
        "b",
        1L,
        4.0d,
        1L,
        localMills("1970-01-01T00:00:15"),
        localMills("1970-01-01T00:00:20")))

    assertor.assertOutputEqualsSorted("result mismatch", expected, testHarness.getOutput)

    testHarness.close()
  }

  /**
   * The produced result will be the a little different with
   * [[WindowAggregateITCase.testEventTimeHopWindow()]] because of missing late data.
   */
  @TestTemplate
  def testProcessingTimeHopWindow(): Unit = {
    val (testHarness, outputTypes) = createProcessingTimeWindowOperator(HOP, isCDCSource = false)
    val assertor = new RowDataHarnessAssertor(outputTypes)

    testHarness.open()
    ingestData(testHarness, isCDCSource = false)

    val expected = new ConcurrentLinkedQueue[Object]()
    expected.add(
      insertRecord(
        "a",
        4L,
        5.0d,
        2L,
        localMills("1969-12-31T23:59:55"),
        localMills("1970-01-01T00:00:05")))
    expected.add(
      insertRecord(
        "a",
        5L,
        5.0d,
        3L,
        localMills("1970-01-01T00:00:00"),
        localMills("1970-01-01T00:00:10")))
    expected.add(
      insertRecord(
        "a",
        1L,
        null,
        1L,
        localMills("1970-01-01T00:00:05"),
        localMills("1970-01-01T00:00:15")))
    expected.add(
      insertRecord(
        "b",
        2L,
        6.0d,
        2L,
        localMills("1970-01-01T00:00:00"),
        localMills("1970-01-01T00:00:10")))
    expected.add(
      insertRecord(
        "b",
        2L,
        6.0d,
        2L,
        localMills("1970-01-01T00:00:05"),
        localMills("1970-01-01T00:00:15")))
    expected.add(
      insertRecord(
        "b",
        1L,
        4.0d,
        1L,
        localMills("1970-01-01T00:00:10"),
        localMills("1970-01-01T00:00:20")))
    expected.add(
      insertRecord(
        "b",
        1L,
        4.0d,
        1L,
        localMills("1970-01-01T00:00:15"),
        localMills("1970-01-01T00:00:25")))
    expected.add(
      insertRecord(
        "b",
        1L,
        3.0d,
        1L,
        localMills("1970-01-01T00:00:25"),
        localMills("1970-01-01T00:00:35")))
    expected.add(
      insertRecord(
        "b",
        1L,
        3.0d,
        1L,
        localMills("1970-01-01T00:00:30"),
        localMills("1970-01-01T00:00:40")))
    expected.add(
      insertRecord(
        null,
        1L,
        7.0d,
        0L,
        localMills("1970-01-01T00:00:25"),
        localMills("1970-01-01T00:00:35")))
    expected.add(
      insertRecord(
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
   * The produced result will be the a little different with
   * [[WindowAggregateITCase.testEventTimeHopWindowWithCDCSource()]] because of missing late data.
   */
  @TestTemplate
  def testProcessingTimeHopWindowWithCDCSource(): Unit = {
    val (testHarness, outputTypes) = createProcessingTimeWindowOperator(HOP, isCDCSource = true)
    val assertor = new RowDataHarnessAssertor(outputTypes)

    testHarness.open()
    ingestData(testHarness, isCDCSource = true)

    val expected = new ConcurrentLinkedQueue[Object]()
    expected.add(
      insertRecord(
        "a",
        3L,
        22.0d,
        2L,
        localMills("1969-12-31T23:59:55"),
        localMills("1970-01-01T00:00:05")))
    expected.add(
      insertRecord(
        "a",
        4L,
        22.0d,
        3L,
        localMills("1970-01-01T00:00:00"),
        localMills("1970-01-01T00:00:10")))
    expected.add(
      insertRecord(
        "a",
        1L,
        null,
        1L,
        localMills("1970-01-01T00:00:05"),
        localMills("1970-01-01T00:00:15")))
    expected.add(
      insertRecord(
        "b",
        2L,
        6.0d,
        2L,
        localMills("1970-01-01T00:00:00"),
        localMills("1970-01-01T00:00:10")))
    expected.add(
      insertRecord(
        "b",
        2L,
        6.0d,
        2L,
        localMills("1970-01-01T00:00:05"),
        localMills("1970-01-01T00:00:15")))
    expected.add(
      insertRecord(
        "b",
        1L,
        4.0d,
        1L,
        localMills("1970-01-01T00:00:10"),
        localMills("1970-01-01T00:00:20")))
    expected.add(
      insertRecord(
        "b",
        1L,
        4.0d,
        1L,
        localMills("1970-01-01T00:00:15"),
        localMills("1970-01-01T00:00:25")))

    assertor.assertOutputEqualsSorted("result mismatch", expected, testHarness.getOutput)

    testHarness.close()
  }

  /**
   * The produced result will be the a little different with
   * [[WindowAggregateITCase.testEventTimeCumulateWindow()]] because of missing late data.
   */
  @TestTemplate
  def testProcessingTimeCumulateWindow(): Unit = {
    val (testHarness, outputTypes) =
      createProcessingTimeWindowOperator(CUMULATE, isCDCSource = false)
    val assertor = new RowDataHarnessAssertor(outputTypes)

    testHarness.open()
    ingestData(testHarness, isCDCSource = false)

    val expected = new ConcurrentLinkedQueue[Object]()
    expected.add(
      insertRecord(
        "a",
        4L,
        5.0d,
        2L,
        localMills("1970-01-01T00:00:00"),
        localMills("1970-01-01T00:00:05")))
    expected.add(
      insertRecord(
        "a",
        5L,
        5.0d,
        3L,
        localMills("1970-01-01T00:00:00"),
        localMills("1970-01-01T00:00:10")))
    expected.add(
      insertRecord(
        "a",
        5L,
        5.0d,
        3L,
        localMills("1970-01-01T00:00:00"),
        localMills("1970-01-01T00:00:15")))
    expected.add(
      insertRecord(
        "b",
        2L,
        6.0d,
        2L,
        localMills("1970-01-01T00:00:00"),
        localMills("1970-01-01T00:00:10")))
    expected.add(
      insertRecord(
        "b",
        2L,
        6.0d,
        2L,
        localMills("1970-01-01T00:00:00"),
        localMills("1970-01-01T00:00:15")))
    expected.add(
      insertRecord(
        "b",
        1L,
        4.0d,
        1L,
        localMills("1970-01-01T00:00:15"),
        localMills("1970-01-01T00:00:20")))
    expected.add(
      insertRecord(
        "b",
        1L,
        4.0d,
        1L,
        localMills("1970-01-01T00:00:15"),
        localMills("1970-01-01T00:00:25")))
    expected.add(
      insertRecord(
        "b",
        1L,
        4.0d,
        1L,
        localMills("1970-01-01T00:00:15"),
        localMills("1970-01-01T00:00:30")))
    expected.add(
      insertRecord(
        "b",
        1L,
        3.0d,
        1L,
        localMills("1970-01-01T00:00:30"),
        localMills("1970-01-01T00:00:35")))
    expected.add(
      insertRecord(
        "b",
        1L,
        3.0d,
        1L,
        localMills("1970-01-01T00:00:30"),
        localMills("1970-01-01T00:00:40")))
    expected.add(
      insertRecord(
        "b",
        1L,
        3.0d,
        1L,
        localMills("1970-01-01T00:00:30"),
        localMills("1970-01-01T00:00:45")))
    expected.add(
      insertRecord(
        null,
        1L,
        7.0d,
        0L,
        localMills("1970-01-01T00:00:30"),
        localMills("1970-01-01T00:00:35")))
    expected.add(
      insertRecord(
        null,
        1L,
        7.0d,
        0L,
        localMills("1970-01-01T00:00:30"),
        localMills("1970-01-01T00:00:40")))
    expected.add(
      insertRecord(
        null,
        1L,
        7.0d,
        0L,
        localMills("1970-01-01T00:00:30"),
        localMills("1970-01-01T00:00:45")))

    assertor.assertOutputEqualsSorted("result mismatch", expected, testHarness.getOutput)

    testHarness.close()
  }

  /**
   * The produced result will be the a little different with
   * [[WindowAggregateITCase.testEventTimeCumulateWindowWithCDCSource()]] because of missing late
   * data.
   */
  @TestTemplate
  def testProcessingTimeCumulateWindowWithCDCSource(): Unit = {
    val (testHarness, outputTypes) =
      createProcessingTimeWindowOperator(CUMULATE, isCDCSource = true)
    val assertor = new RowDataHarnessAssertor(outputTypes)

    testHarness.open()
    ingestData(testHarness, isCDCSource = true)

    val expected = new ConcurrentLinkedQueue[Object]()
    expected.add(
      insertRecord(
        "a",
        3L,
        22.0d,
        2L,
        localMills("1970-01-01T00:00:00"),
        localMills("1970-01-01T00:00:05")))
    expected.add(
      insertRecord(
        "a",
        4L,
        22.0d,
        3L,
        localMills("1970-01-01T00:00:00"),
        localMills("1970-01-01T00:00:10")))
    expected.add(
      insertRecord(
        "a",
        4L,
        22.0d,
        3L,
        localMills("1970-01-01T00:00:00"),
        localMills("1970-01-01T00:00:15")))
    expected.add(
      insertRecord(
        "b",
        2L,
        6.0d,
        2L,
        localMills("1970-01-01T00:00:00"),
        localMills("1970-01-01T00:00:10")))
    expected.add(
      insertRecord(
        "b",
        2L,
        6.0d,
        2L,
        localMills("1970-01-01T00:00:00"),
        localMills("1970-01-01T00:00:15")))
    expected.add(
      insertRecord(
        "b",
        1L,
        4.0d,
        1L,
        localMills("1970-01-01T00:00:15"),
        localMills("1970-01-01T00:00:20")))
    expected.add(
      insertRecord(
        "b",
        1L,
        4.0d,
        1L,
        localMills("1970-01-01T00:00:15"),
        localMills("1970-01-01T00:00:25")))
    expected.add(
      insertRecord(
        "b",
        1L,
        4.0d,
        1L,
        localMills("1970-01-01T00:00:15"),
        localMills("1970-01-01T00:00:30")))

    assertor.assertOutputEqualsSorted("result mismatch", expected, testHarness.getOutput)

    testHarness.close()
  }

  @TestTemplate
  def testCloseWithoutOpen(): Unit = {
    val (testHarness, outputTypes) = createProcessingTimeWindowOperator(TUMBLE, isCDCSource = false)
    testHarness.setup(new RowDataSerializer(outputTypes: _*))
    // simulate a failover after a failed task open, expect no exception happens
    testHarness.close()
  }

  /** Processing time window doesn't support two-phase, so add a single two-phase test. */
  @TestTemplate
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

    tEnv.getConfig.set(
      OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY,
      AggregatePhaseStrategy.TWO_PHASE)

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
    val stream: DataStream[Row] = t1.toDataStream

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

  @TestTemplate
  def testProcessingTimeTumbleWindowWithFutureWatermark(): Unit = {
    val (testHarness, outputTypes) =
      createProcessingTimeWindowOperator(TUMBLE, isCDCSource = false)
    val assertor = new RowDataHarnessAssertor(outputTypes)

    testHarness.open()

    // mock a large watermark arrives before proctime
    testHarness.processWatermark(10000L)

    testHarness.setProcessingTime(1000L)
    testHarness.processElement(insertRecord("a", 1d, "str1", null))
    testHarness.setProcessingTime(2000L)
    testHarness.processElement(insertRecord("a", 2d, "str2", null))
    testHarness.setProcessingTime(3000L)
    testHarness.processElement(insertRecord("a", 3d, "str2", null))

    testHarness.setProcessingTime(6000L)
    testHarness.processElement(insertRecord("a", 4d, "str1", null))

    testHarness.setProcessingTime(50000L)

    val expected = new ConcurrentLinkedQueue[Object]()
    expected.add(new Watermark(10000L))
    expected.add(
      insertRecord(
        "a",
        3L,
        3.0d,
        2L,
        localMills("1970-01-01T00:00:00"),
        localMills("1970-01-01T00:00:05")))
    expected.add(
      insertRecord(
        "a",
        1L,
        4.0d,
        1L,
        localMills("1970-01-01T00:00:05"),
        localMills("1970-01-01T00:00:10")))
    assertor.assertOutputEqualsSorted("result mismatch", expected, testHarness.getOutput)
    testHarness.close()
  }

  private def createProcessingTimeWindowOperator(testWindow: String, isCDCSource: Boolean)
      : (KeyedOneInputStreamOperatorTestHarness[RowData, RowData, RowData], Array[LogicalType]) = {
    val windowDDL = testWindow match {
      case WindowAggregateHarnessTest.TUMBLE =>
        s"""
           |TUMBLE(
           |      TABLE ${if (isCDCSource) "T1_CDC" else "T1"},
           |      DESCRIPTOR(proctime),
           |      INTERVAL '5' SECOND)
           |""".stripMargin
      case WindowAggregateHarnessTest.HOP =>
        s"""
           |HOP(
           |      TABLE ${if (isCDCSource) "T1_CDC" else "T1"},
           |      DESCRIPTOR(proctime),
           |      INTERVAL '5' SECOND,
           |      INTERVAL '10' SECOND)
           |""".stripMargin
      case WindowAggregateHarnessTest.CUMULATE =>
        s"""
           |CUMULATE(
           |      TABLE ${if (isCDCSource) "T1_CDC" else "T1"},
           |      DESCRIPTOR(proctime),
           |      INTERVAL '5' SECOND,
           |      INTERVAL '15' SECOND)
           |""".stripMargin
    }

    val sql =
      s"""
         |SELECT
         |  `name`,
         |  window_start,
         |  window_end,
         |  COUNT(*),
         |  MAX(`double`),
         |  COUNT(DISTINCT `string`)
         |FROM TABLE($windowDDL)
         |GROUP BY `name`, window_start, window_end
      """.stripMargin
    val table = tEnv.sqlQuery(sql)
    val testHarness = createHarnessTester(table.toDataStream, "WindowAggregate")
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
   * Ingests testing data, the input schema is [name, double, string, proctime]. We follow the test
   * data in [[TestData.windowDataWithTimestamp]] or [[TestData.windowChangelogDataWithTimestamp]]
   * to have the same produced result. The only difference is we don't ingest the late data in this
   * test, so they will produce same result with TUMBLE Window, but produce different result with
   * HOP Window, CUMULATE Window.
   */
  private def ingestData(
      testHarness: KeyedOneInputStreamOperatorTestHarness[RowData, RowData, RowData],
      isCDCSource: Boolean): Unit = {
    // input schema: [name, double, string, proctime]
    if (isCDCSource) {
      testHarness.setProcessingTime(1000L)
      testHarness.processElement(changelogRecord(INSERT, "a", 1d, "Hi", null))
      testHarness.setProcessingTime(2000L)
      testHarness.processElement(changelogRecord(INSERT, "a", 2d, "Comment#1", null))
      testHarness.setProcessingTime(3000L)
      testHarness.processElement(changelogRecord(DELETE, "a", 1d, "Hi", null))
      testHarness.processElement(changelogRecord(INSERT, "a", 2d, "Comment#1", null))
      testHarness.setProcessingTime(4000L)
      testHarness.processElement(changelogRecord(INSERT, "a", 5d, null, null))
      testHarness.processElement(changelogRecord(UPDATE_BEFORE, "a", 2d, "Comment#1", null))
      testHarness.processElement(changelogRecord(UPDATE_AFTER, "a", 22d, "Comment#22", null))

      testHarness.setProcessingTime(6000L)
      testHarness.processElement(changelogRecord(INSERT, "b", 6d, "Hi", null))
      testHarness.setProcessingTime(7000L)
      testHarness.processElement(changelogRecord(INSERT, "b", 3d, "Hello", null))
      testHarness.setProcessingTime(8000L)
      testHarness.processElement(changelogRecord(INSERT, "a", null, "Comment#2", null))

      testHarness.setProcessingTime(16000L)
      testHarness.processElement(changelogRecord(INSERT, "b", 4d, "Hi", null))
      testHarness.setProcessingTime(38000L)
      testHarness.processElement(changelogRecord(INSERT, "b", 8d, "Comment#4", null))
      testHarness.setProcessingTime(39000L)
      testHarness.processElement(changelogRecord(DELETE, "b", 8d, "Comment#4", null))
    } else {
      testHarness.setProcessingTime(1000L)
      testHarness.processElement(insertRecord("a", 1d, "Hi", null))
      testHarness.setProcessingTime(2000L)
      testHarness.processElement(insertRecord("a", 2d, "Comment#1", null))
      testHarness.setProcessingTime(3000L)
      testHarness.processElement(insertRecord("a", 2d, "Comment#1", null))
      testHarness.setProcessingTime(4000L)
      testHarness.processElement(insertRecord("a", 5d, null, null))

      testHarness.setProcessingTime(6000L)
      testHarness.processElement(insertRecord("b", 6d, "Hi", null))
      testHarness.setProcessingTime(7000L)
      testHarness.processElement(insertRecord("b", 3d, "Hello", null))
      testHarness.setProcessingTime(8000L)
      testHarness.processElement(insertRecord("a", null, "Comment#2", null))

      testHarness.setProcessingTime(16000L)
      testHarness.processElement(insertRecord("b", 4d, "Hi", null))
      testHarness.setProcessingTime(32000L)
      testHarness.processElement(insertRecord(null, 7d, null, null))
      testHarness.setProcessingTime(34000L)
      testHarness.processElement(insertRecord("b", 3d, "Comment#3", null))
      testHarness.setProcessingTime(50000L)
    }
  }

  private def insertRecord(args: Any*): StreamRecord[RowData] = {
    val objs = args.map {
      case l: Long => Long.box(l)
      case d: Double => Double.box(d)
      case arg @ _ => arg.asInstanceOf[Object]
    }.toArray
    binaryRecord(INSERT, objs: _*)
  }

  private def changelogRecord(rowKind: RowKind, args: Any*): StreamRecord[RowData] = {
    val objs = args.map {
      case l: Long => Long.box(l)
      case d: Double => Double.box(d)
      case arg @ _ => arg.asInstanceOf[Object]
    }.toArray
    binaryRecord(rowKind, objs: _*)
  }

  private def localMills(dateTime: String): TimestampData = {
    val windowDateTime = LocalDateTime.parse(dateTime).atZone(UTC_ZONE_ID)
    TimestampData.fromEpochMillis(
      toUtcTimestampMills(windowDateTime.toInstant.toEpochMilli, shiftTimeZone))
  }
}

object WindowAggregateHarnessTest {

  @Parameters(name = "StateBackend={0}, TimeZone={1}")
  def parameters(): JCollection[Array[java.lang.Object]] = {
    Seq[Array[AnyRef]](
      Array(HEAP_BACKEND, ZoneId.of("UTC")),
      Array(HEAP_BACKEND, ZoneId.of("Asia/Shanghai")),
      Array(ROCKSDB_BACKEND, ZoneId.of("UTC")),
      Array(ROCKSDB_BACKEND, ZoneId.of("Asia/Shanghai"))
    )
  }

  val TUMBLE: String = "TUMBLE"
  val HOP: String = "HOP"
  val CUMULATE: String = "CUMULATE"
}
