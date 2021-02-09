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
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.data.{RowData, TimestampData}
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.runtime.stream.sql.WindowAggregateITCase
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.planner.runtime.utils.TestData
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor
import org.apache.flink.table.runtime.util.StreamRecordUtils.binaryRecord
import org.apache.flink.types.Row
import org.apache.flink.types.RowKind.INSERT

import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Test}

import java.util.concurrent.ConcurrentLinkedQueue

/**
 * Harness tests for processing-time window aggregate. We can't test them in
 * [[WindowAggregateITCase]] because the result is non-deterministic, therefore
 * we use harness to test them.
 */
@RunWith(classOf[Parameterized])
class WindowAggregateHarnessTest(backend: StateBackendMode)
  extends HarnessTestBase(backend) {

  @Before
  override def before(): Unit = {
    super.before()
    val dataId = TestValuesTableFactory.registerData(TestData.windowData)
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

  /**
   * The produced result should be the same with
   * [[WindowAggregateITCase.testEventTimeTumbleWindow()]].
   */
  @Test
  def testProcessingTimeTumbleWindow(): Unit = {
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
    val assertor = new RowDataHarnessAssertor(
      Array(
        DataTypes.STRING().getLogicalType,
        DataTypes.BIGINT().getLogicalType,
        DataTypes.DOUBLE().getLogicalType,
        DataTypes.BIGINT().getLogicalType,
        DataTypes.TIMESTAMP(3).getLogicalType,
        DataTypes.TIMESTAMP(3).getLogicalType))

    testHarness.open()
    ingestData(testHarness)

    val expectedOutput = new ConcurrentLinkedQueue[Object]()
    expectedOutput.add(record("a", 4L, 5.0D, 2L, timestamp(0L), timestamp(5000L)))
    expectedOutput.add(record("a", 1L, null, 1L, timestamp(5000L), timestamp(10000L)))
    expectedOutput.add(record("b", 2L, 6.0D, 2L, timestamp(5000L), timestamp(10000L)))
    expectedOutput.add(record("b", 1L, 4.0D, 1L, timestamp(15000L), timestamp(20000L)))
    expectedOutput.add(record("b", 1L, 3.0D, 1L, timestamp(30000L), timestamp(35000L)))
    expectedOutput.add(record(null, 1L, 7.0D, 0L, timestamp(30000L), timestamp(35000L)))

    assertor.assertOutputEqualsSorted("result mismatch", expectedOutput, testHarness.getOutput)

    testHarness.close()
  }

  /**
   * The produced result should be the same with
   * [[WindowAggregateITCase.testEventTimeHopWindow()]].
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
        DataTypes.TIMESTAMP(3).getLogicalType,
        DataTypes.TIMESTAMP(3).getLogicalType))

    testHarness.open()
    ingestData(testHarness)

    val expectedOutput = new ConcurrentLinkedQueue[Object]()
    expectedOutput.add(record("a", 4L, 5.0D, 2L, timestamp(-5000L), timestamp(5000L)))
    expectedOutput.add(record("a", 5L, 5.0D, 3L, timestamp(0L), timestamp(10000L)))
    expectedOutput.add(record("a", 1L, null, 1L, timestamp(5000L), timestamp(15000L)))
    expectedOutput.add(record("b", 2L, 6.0D, 2L, timestamp(0L), timestamp(10000L)))
    expectedOutput.add(record("b", 2L, 6.0D, 2L, timestamp(5000L), timestamp(15000L)))
    expectedOutput.add(record("b", 1L, 4.0D, 1L, timestamp(10000L), timestamp(20000L)))
    expectedOutput.add(record("b", 1L, 4.0D, 1L, timestamp(15000L), timestamp(25000L)))
    expectedOutput.add(record("b", 1L, 3.0D, 1L, timestamp(25000L), timestamp(35000L)))
    expectedOutput.add(record("b", 1L, 3.0D, 1L, timestamp(30000L), timestamp(40000L)))
    expectedOutput.add(record(null, 1L, 7.0D, 0L, timestamp(25000L), timestamp(35000L)))
    expectedOutput.add(record(null, 1L, 7.0D, 0L, timestamp(30000L), timestamp(40000L)))

    assertor.assertOutputEqualsSorted("result mismatch", expectedOutput, testHarness.getOutput)

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
        DataTypes.TIMESTAMP(3).getLogicalType,
        DataTypes.TIMESTAMP(3).getLogicalType))

    testHarness.open()
    ingestData(testHarness)

    val expectedOutput = new ConcurrentLinkedQueue[Object]()
    expectedOutput.add(record("a", 4L, 5.0D, 2L, timestamp(0L), timestamp(5000L)))
    expectedOutput.add(record("a", 5L, 5.0D, 3L, timestamp(0L), timestamp(10000L)))
    expectedOutput.add(record("a", 5L, 5.0D, 3L, timestamp(0L), timestamp(15000L)))
    expectedOutput.add(record("b", 2L, 6.0D, 2L, timestamp(0L), timestamp(10000L)))
    expectedOutput.add(record("b", 2L, 6.0D, 2L, timestamp(0L), timestamp(15000L)))
    expectedOutput.add(record("b", 1L, 4.0D, 1L, timestamp(15000L), timestamp(20000L)))
    expectedOutput.add(record("b", 1L, 4.0D, 1L, timestamp(15000L), timestamp(25000L)))
    expectedOutput.add(record("b", 1L, 4.0D, 1L, timestamp(15000L), timestamp(30000L)))
    expectedOutput.add(record("b", 1L, 3.0D, 1L, timestamp(30000L), timestamp(35000L)))
    expectedOutput.add(record("b", 1L, 3.0D, 1L, timestamp(30000L), timestamp(40000L)))
    expectedOutput.add(record("b", 1L, 3.0D, 1L, timestamp(30000L), timestamp(45000L)))
    expectedOutput.add(record(null, 1L, 7.0D, 0L, timestamp(30000L), timestamp(35000L)))
    expectedOutput.add(record(null, 1L, 7.0D, 0L, timestamp(30000L), timestamp(40000L)))
    expectedOutput.add(record(null, 1L, 7.0D, 0L, timestamp(30000L), timestamp(45000L)))

    assertor.assertOutputEqualsSorted("result mismatch", expectedOutput, testHarness.getOutput)

    testHarness.close()
  }

  /**
   * Ingests testing data, the input schema is [name, double, string, proctime].
   * We follow the test data in [[TestData.windowData]] to have the same produced result.
   * The only difference is we don't ingest the late data in this test, so they should produce
   * same result.
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
      case arg@_ => arg.asInstanceOf[Object]
    }.toArray
    binaryRecord(INSERT, objs: _*)
  }
  
  private def timestamp(ms: Long): TimestampData = {
    TimestampData.fromEpochMillis(ms)
  }
}
