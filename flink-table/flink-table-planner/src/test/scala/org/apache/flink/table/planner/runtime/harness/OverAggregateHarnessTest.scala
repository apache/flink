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
import org.apache.flink.table.api.bridge.scala.internal.StreamTableEnvironmentImpl
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.runtime.typeutils.RowDataSerializer
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor
import org.apache.flink.table.runtime.util.StreamRecordUtils.{binaryrow, row}
import org.apache.flink.table.types.logical.LogicalType
import org.apache.flink.types.Row

import org.junit.{Before, Test}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import java.lang.{Long => JLong}
import java.time.Duration
import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.mutable

@RunWith(classOf[Parameterized])
class OverAggregateHarnessTest(mode: StateBackendMode) extends HarnessTestBase(mode) {

  @Before
  override def before(): Unit = {
    super.before()
    val setting = EnvironmentSettings.newInstance().inStreamingMode().build()
    this.tEnv = StreamTableEnvironmentImpl.create(env, setting)
  }

  @Test
  def testProcTimeBoundedRowsOver(): Unit = {
    val (testHarness, outputType) = createProcTimeBoundedRowsOver
    val assertor = new RowDataHarnessAssertor(outputType)
    testHarness.open()

    // register cleanup timer with 3001
    testHarness.setProcessingTime(1)

    testHarness.processElement(new StreamRecord(binaryrow(1L: JLong, "aaa", 1L: JLong, null)))
    testHarness.processElement(new StreamRecord(binaryrow(1L: JLong, "bbb", 10L: JLong, null)))
    testHarness.processElement(new StreamRecord(binaryrow(1L: JLong, "aaa", 2L: JLong, null)))
    testHarness.processElement(new StreamRecord(binaryrow(1L: JLong, "aaa", 3L: JLong, null)))

    // register cleanup timer with 4100
    testHarness.setProcessingTime(1100)
    testHarness.processElement(new StreamRecord(binaryrow(1L: JLong, "bbb", 20L: JLong, null)))
    testHarness.processElement(new StreamRecord(binaryrow(1L: JLong, "aaa", 4L: JLong, null)))
    testHarness.processElement(new StreamRecord(binaryrow(1L: JLong, "aaa", 5L: JLong, null)))
    testHarness.processElement(new StreamRecord(binaryrow(1L: JLong, "aaa", 6L: JLong, null)))
    testHarness.processElement(new StreamRecord(binaryrow(1L: JLong, "bbb", 30L: JLong, null)))

    // register cleanup timer with 6001
    testHarness.setProcessingTime(3001)
    testHarness.processElement(new StreamRecord(binaryrow(2L: JLong, "aaa", 7L: JLong, null)))
    testHarness.processElement(new StreamRecord(binaryrow(2L: JLong, "aaa", 8L: JLong, null)))
    testHarness.processElement(new StreamRecord(binaryrow(2L: JLong, "aaa", 9L: JLong, null)))

    // trigger cleanup timer and register cleanup timer with 9002
    testHarness.setProcessingTime(6002)
    testHarness.processElement(new StreamRecord(binaryrow(2L: JLong, "aaa", 10L: JLong, null)))
    testHarness.processElement(new StreamRecord(binaryrow(2L: JLong, "bbb", 40L: JLong, null)))

    val result = testHarness.getOutput

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    expectedOutput.add(
      new StreamRecord(row(1L: JLong, "aaa", 1L: JLong, null, 1L: JLong, 1L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(1L: JLong, "bbb", 10L: JLong, null, 10L: JLong, 10L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(1L: JLong, "aaa", 2L: JLong, null, 1L: JLong, 2L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(1L: JLong, "aaa", 3L: JLong, null, 2L: JLong, 3L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(1L: JLong, "bbb", 20L: JLong, null, 10L: JLong, 20L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(1L: JLong, "aaa", 4L: JLong, null, 3L: JLong, 4L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(1L: JLong, "aaa", 5L: JLong, null, 4L: JLong, 5L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(1L: JLong, "aaa", 6L: JLong, null, 5L: JLong, 6L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(1L: JLong, "bbb", 30L: JLong, null, 20L: JLong, 30L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(2L: JLong, "aaa", 7L: JLong, null, 6L: JLong, 7L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(2L: JLong, "aaa", 8L: JLong, null, 7L: JLong, 8L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(2L: JLong, "aaa", 10L: JLong, null, 10L: JLong, 10L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(2L: JLong, "aaa", 9L: JLong, null, 8L: JLong, 9L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(2L: JLong, "bbb", 40L: JLong, null, 40L: JLong, 40L: JLong)))

    assertor.assertOutputEqualsSorted("result mismatch", expectedOutput, result)

    testHarness.close()
  }

  private def createProcTimeBoundedRowsOver()
      : (KeyedOneInputStreamOperatorTestHarness[RowData, RowData, RowData], Array[LogicalType]) = {
    val data = new mutable.MutableList[(Long, String, Long)]
    val t = env.fromCollection(data).toTable(tEnv, 'currtime, 'b, 'c, 'proctime.proctime)
    tEnv.createTemporaryView("T", t)

    val sql =
      """
        |SELECT currtime, b, c,
        | min(c) OVER
        |   (PARTITION BY b ORDER BY proctime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW),
        | max(c) OVER
        |   (PARTITION BY b ORDER BY proctime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
        |FROM T
      """.stripMargin
    val t1 = tEnv.sqlQuery(sql)

    tEnv.getConfig.setIdleStateRetention(Duration.ofSeconds(2))
    val testHarness = createHarnessTester(t1.toAppendStream[Row], "OverAggregate")
    val outputType = Array(
      DataTypes.BIGINT().getLogicalType,
      DataTypes.STRING().getLogicalType,
      DataTypes.BIGINT().getLogicalType,
      DataTypes.BIGINT().getLogicalType,
      DataTypes.BIGINT().getLogicalType,
      DataTypes.BIGINT().getLogicalType,
      DataTypes.BIGINT().getLogicalType
    )
    (testHarness, outputType)
  }

  /** NOTE: all elements at the same proc timestamp have the same value per key */
  @Test
  def testProcTimeBoundedRangeOver(): Unit = {

    val data = new mutable.MutableList[(Long, String, Long)]
    val t = env.fromCollection(data).toTable(tEnv, 'currtime, 'b, 'c, 'proctime.proctime)
    tEnv.createTemporaryView("T", t)

    val sql =
      """
        |SELECT currtime, b, c,
        | min(c) OVER
        |   (PARTITION BY b ORDER BY proctime
        |   RANGE BETWEEN INTERVAL '4' SECOND PRECEDING AND CURRENT ROW),
        | max(c) OVER
        |   (PARTITION BY b ORDER BY proctime
        |   RANGE BETWEEN INTERVAL '4' SECOND PRECEDING AND CURRENT ROW)
        |FROM T
      """.stripMargin
    val t1 = tEnv.sqlQuery(sql)

    val testHarness = createHarnessTester(t1.toAppendStream[Row], "OverAggregate")
    val assertor = new RowDataHarnessAssertor(
      Array(
        DataTypes.BIGINT().getLogicalType,
        DataTypes.STRING().getLogicalType,
        DataTypes.BIGINT().getLogicalType,
        DataTypes.BIGINT().getLogicalType,
        DataTypes.BIGINT().getLogicalType,
        DataTypes.BIGINT().getLogicalType
      ))
    testHarness.open()

    testHarness.setProcessingTime(3)
    testHarness.processElement(new StreamRecord(binaryrow(0L: JLong, "aaa", 1L: JLong, null)))
    testHarness.processElement(new StreamRecord(binaryrow(0L: JLong, "bbb", 10L: JLong, null)))

    testHarness.setProcessingTime(4)
    testHarness.processElement(new StreamRecord(binaryrow(0L: JLong, "aaa", 2L: JLong, null)))

    testHarness.setProcessingTime(3003)
    testHarness.processElement(new StreamRecord(binaryrow(0L: JLong, "aaa", 3L: JLong, null)))
    testHarness.processElement(new StreamRecord(binaryrow(0L: JLong, "bbb", 20L: JLong, null)))

    testHarness.setProcessingTime(5)
    testHarness.processElement(new StreamRecord(binaryrow(0L: JLong, "aaa", 4L: JLong, null)))

    testHarness.setProcessingTime(6002)

    testHarness.setProcessingTime(7002)
    testHarness.processElement(new StreamRecord(binaryrow(0L: JLong, "aaa", 5L: JLong, null)))
    testHarness.processElement(new StreamRecord(binaryrow(0L: JLong, "aaa", 6L: JLong, null)))
    testHarness.processElement(new StreamRecord(binaryrow(0L: JLong, "bbb", 30L: JLong, null)))

    testHarness.setProcessingTime(11002)
    testHarness.processElement(new StreamRecord(binaryrow(0L: JLong, "aaa", 7L: JLong, null)))

    testHarness.setProcessingTime(11004)
    testHarness.processElement(new StreamRecord(binaryrow(0L: JLong, "aaa", 8L: JLong, null)))

    testHarness.processElement(new StreamRecord(binaryrow(0L: JLong, "aaa", 9L: JLong, null)))
    testHarness.processElement(new StreamRecord(binaryrow(0L: JLong, "aaa", 10L: JLong, null)))
    testHarness.processElement(new StreamRecord(binaryrow(0L: JLong, "bbb", 40L: JLong, null)))

    testHarness.setProcessingTime(11006)

    val result = testHarness.getOutput

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    // all elements at the same proc timestamp have the same value per key
    expectedOutput.add(
      new StreamRecord(row(0L: JLong, "aaa", 1L: JLong, null, 1L: JLong, 1L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(0L: JLong, "bbb", 10L: JLong, null, 10L: JLong, 10L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(0L: JLong, "aaa", 2L: JLong, null, 1L: JLong, 2L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(0L: JLong, "aaa", 3L: JLong, null, 1L: JLong, 4L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(0L: JLong, "bbb", 20L: JLong, null, 10L: JLong, 20L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(0L: JLong, "aaa", 4L: JLong, null, 1L: JLong, 4L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(0L: JLong, "aaa", 5L: JLong, null, 3L: JLong, 6L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(0L: JLong, "aaa", 6L: JLong, null, 3L: JLong, 6L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(0L: JLong, "bbb", 30L: JLong, null, 20L: JLong, 30L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(0L: JLong, "aaa", 7L: JLong, null, 5L: JLong, 7L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(0L: JLong, "aaa", 8L: JLong, null, 7L: JLong, 10L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(0L: JLong, "aaa", 9L: JLong, null, 7L: JLong, 10L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(0L: JLong, "aaa", 10L: JLong, null, 7L: JLong, 10L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(0L: JLong, "bbb", 40L: JLong, null, 40L: JLong, 40L: JLong)))

    assertor.assertOutputEqualsSorted("result mismatch", expectedOutput, result)

    testHarness.close()
  }

  @Test
  def testProcTimeUnboundedOver(): Unit = {

    val data = new mutable.MutableList[(Long, String, Long)]
    val t = env.fromCollection(data).toTable(tEnv, 'currtime, 'b, 'c, 'proctime.proctime)
    tEnv.createTemporaryView("T", t)

    val sql =
      """
        |SELECT currtime, b, c,
        | min(c) OVER
        |   (PARTITION BY b ORDER BY proctime ROWS BETWEEN UNBOUNDED preceding AND CURRENT ROW),
        | max(c) OVER
        |   (PARTITION BY b ORDER BY proctime ROWS BETWEEN UNBOUNDED preceding AND CURRENT ROW)
        |FROM T
      """.stripMargin
    val t1 = tEnv.sqlQuery(sql)

    tEnv.getConfig.setIdleStateRetention(Duration.ofSeconds(2))
    val testHarness = createHarnessTester(t1.toAppendStream[Row], "OverAggregate")
    val assertor = new RowDataHarnessAssertor(
      Array(
        DataTypes.BIGINT().getLogicalType,
        DataTypes.STRING().getLogicalType,
        DataTypes.BIGINT().getLogicalType,
        DataTypes.BIGINT().getLogicalType,
        DataTypes.BIGINT().getLogicalType,
        DataTypes.BIGINT().getLogicalType
      ))

    testHarness.open()

    testHarness.setStateTtlProcessingTime(1003)

    testHarness.processElement(new StreamRecord(binaryrow(0L: JLong, "aaa", 1L: JLong, null)))
    testHarness.processElement(new StreamRecord(binaryrow(0L: JLong, "bbb", 10L: JLong, null)))
    testHarness.processElement(new StreamRecord(binaryrow(0L: JLong, "aaa", 2L: JLong, null)))
    testHarness.processElement(new StreamRecord(binaryrow(0L: JLong, "aaa", 3L: JLong, null)))
    testHarness.processElement(new StreamRecord(binaryrow(0L: JLong, "bbb", 20L: JLong, null)))
    testHarness.processElement(new StreamRecord(binaryrow(0L: JLong, "aaa", 4L: JLong, null)))
    testHarness.processElement(new StreamRecord(binaryrow(0L: JLong, "aaa", 5L: JLong, null)))
    testHarness.processElement(new StreamRecord(binaryrow(0L: JLong, "aaa", 6L: JLong, null)))
    testHarness.processElement(new StreamRecord(binaryrow(0L: JLong, "bbb", 30L: JLong, null)))
    testHarness.processElement(new StreamRecord(binaryrow(0L: JLong, "aaa", 7L: JLong, null)))
    testHarness.processElement(new StreamRecord(binaryrow(0L: JLong, "aaa", 8L: JLong, null)))

    testHarness.setStateTtlProcessingTime(5003)
    testHarness.processElement(new StreamRecord(binaryrow(0L: JLong, "aaa", 9L: JLong, null)))
    testHarness.processElement(new StreamRecord(binaryrow(0L: JLong, "aaa", 10L: JLong, null)))
    testHarness.processElement(new StreamRecord(binaryrow(0L: JLong, "bbb", 40L: JLong, null)))

    val result = testHarness.getOutput

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    expectedOutput.add(
      new StreamRecord(row(0L: JLong, "aaa", 1L: JLong, null, 1L: JLong, 1L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(0L: JLong, "bbb", 10L: JLong, null, 10L: JLong, 10L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(0L: JLong, "aaa", 2L: JLong, null, 1L: JLong, 2L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(0L: JLong, "aaa", 3L: JLong, null, 1L: JLong, 3L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(0L: JLong, "bbb", 20L: JLong, null, 10L: JLong, 20L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(0L: JLong, "aaa", 4L: JLong, null, 1L: JLong, 4L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(0L: JLong, "aaa", 5L: JLong, null, 1L: JLong, 5L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(0L: JLong, "aaa", 6L: JLong, null, 1L: JLong, 6L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(0L: JLong, "bbb", 30L: JLong, null, 10L: JLong, 30L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(0L: JLong, "aaa", 7L: JLong, null, 1L: JLong, 7L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(0L: JLong, "aaa", 8L: JLong, null, 1L: JLong, 8L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(0L: JLong, "aaa", 9L: JLong, null, 9L: JLong, 9L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(0L: JLong, "aaa", 10L: JLong, null, 9L: JLong, 10L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(0L: JLong, "bbb", 40L: JLong, null, 40L: JLong, 40L: JLong)))

    assertor.assertOutputEqualsSorted("result mismatch", expectedOutput, result)
    testHarness.close()
  }

  /** all elements at the same row-time have the same value per key */
  @Test
  def testRowTimeBoundedRangeOver(): Unit = {

    val data = new mutable.MutableList[(Long, String, Long)]
    val t = env.fromCollection(data).toTable(tEnv, 'rowtime.rowtime, 'b, 'c)
    tEnv.createTemporaryView("T", t)

    val sql =
      """
        |SELECT rowtime, b, c,
        | min(c) OVER
        |   (PARTITION BY b ORDER BY rowtime
        |   RANGE BETWEEN INTERVAL '4' SECOND PRECEDING AND CURRENT ROW),
        | max(c) OVER
        |   (PARTITION BY b ORDER BY rowtime
        |   RANGE BETWEEN INTERVAL '4' SECOND PRECEDING AND CURRENT ROW)
        |FROM T
      """.stripMargin
    val t1 = tEnv.sqlQuery(sql)

    val testHarness = createHarnessTester(t1.toAppendStream[Row], "OverAggregate")
    val assertor = new RowDataHarnessAssertor(
      Array(
        DataTypes.BIGINT().getLogicalType,
        DataTypes.STRING().getLogicalType,
        DataTypes.BIGINT().getLogicalType,
        DataTypes.BIGINT().getLogicalType,
        DataTypes.BIGINT().getLogicalType
      ))

    testHarness.open()

    testHarness.processWatermark(1)
    testHarness.processElement(new StreamRecord(binaryrow(2L: JLong, "aaa", 1L: JLong)))

    testHarness.processWatermark(2)
    testHarness.processElement(new StreamRecord(binaryrow(3L: JLong, "bbb", 10L: JLong)))

    testHarness.processWatermark(4000)
    testHarness.processElement(new StreamRecord(binaryrow(4001L: JLong, "aaa", 2L: JLong)))

    testHarness.processWatermark(4001)
    testHarness.processElement(new StreamRecord(binaryrow(4002L: JLong, "aaa", 3L: JLong)))

    testHarness.processWatermark(4002)
    testHarness.processElement(new StreamRecord(binaryrow(4003L: JLong, "aaa", 4L: JLong)))

    testHarness.processWatermark(4800)
    testHarness.processElement(new StreamRecord(binaryrow(4801L: JLong, "bbb", 25L: JLong)))

    testHarness.processWatermark(6500)
    testHarness.processElement(new StreamRecord(binaryrow(6501L: JLong, "aaa", 5L: JLong)))
    testHarness.processElement(new StreamRecord(binaryrow(6501L: JLong, "aaa", 6L: JLong)))
    testHarness.processElement(new StreamRecord(binaryrow(6501L: JLong, "bbb", 30L: JLong)))

    testHarness.processWatermark(7000)
    testHarness.processElement(new StreamRecord(binaryrow(7001L: JLong, "aaa", 7L: JLong)))

    testHarness.processWatermark(8000)
    testHarness.processElement(new StreamRecord(binaryrow(8001L: JLong, "aaa", 8L: JLong)))

    testHarness.processWatermark(12000)
    testHarness.processElement(new StreamRecord(binaryrow(12001L: JLong, "aaa", 9L: JLong)))
    testHarness.processElement(new StreamRecord(binaryrow(12001L: JLong, "aaa", 10L: JLong)))
    testHarness.processElement(new StreamRecord(binaryrow(12001L: JLong, "bbb", 40L: JLong)))

    testHarness.processWatermark(19000)

    val result = dropWatermarks(testHarness.getOutput.toArray)

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    // all elements at the same row-time have the same value per key
    expectedOutput.add(new StreamRecord(row(2L: JLong, "aaa", 1L: JLong, 1L: JLong, 1L: JLong)))
    expectedOutput.add(new StreamRecord(row(3L: JLong, "bbb", 10L: JLong, 10L: JLong, 10L: JLong)))
    expectedOutput.add(new StreamRecord(row(4001L: JLong, "aaa", 2L: JLong, 1L: JLong, 2L: JLong)))
    expectedOutput.add(new StreamRecord(row(4002L: JLong, "aaa", 3L: JLong, 1L: JLong, 3L: JLong)))
    expectedOutput.add(new StreamRecord(row(4003L: JLong, "aaa", 4L: JLong, 2L: JLong, 4L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(4801L: JLong, "bbb", 25L: JLong, 25L: JLong, 25L: JLong)))
    expectedOutput.add(new StreamRecord(row(6501L: JLong, "aaa", 5L: JLong, 2L: JLong, 6L: JLong)))
    expectedOutput.add(new StreamRecord(row(6501L: JLong, "aaa", 6L: JLong, 2L: JLong, 6L: JLong)))
    expectedOutput.add(new StreamRecord(row(7001L: JLong, "aaa", 7L: JLong, 2L: JLong, 7L: JLong)))
    expectedOutput.add(new StreamRecord(row(8001L: JLong, "aaa", 8L: JLong, 2L: JLong, 8L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(6501L: JLong, "bbb", 30L: JLong, 25L: JLong, 30L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(12001L: JLong, "aaa", 9L: JLong, 8L: JLong, 10L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(12001L: JLong, "aaa", 10L: JLong, 8L: JLong, 10L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(12001L: JLong, "bbb", 40L: JLong, 40L: JLong, 40L: JLong)))

    assertor.assertOutputEqualsSorted("result mismatch", expectedOutput, result)
    testHarness.close()
  }

  @Test
  def testRowTimeBoundedRowsOver(): Unit = {

    val data = new mutable.MutableList[(Long, String, Long)]
    val t = env.fromCollection(data).toTable(tEnv, 'rowtime.rowtime, 'b, 'c)
    tEnv.createTemporaryView("T", t)

    val sql =
      """
        |SELECT rowtime, b, c,
        | min(c) OVER
        |   (PARTITION BY b ORDER BY rowtime
        |   ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),
        | max(c) OVER
        |   (PARTITION BY b ORDER BY rowtime
        |   ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
        |FROM T
      """.stripMargin
    val t1 = tEnv.sqlQuery(sql)

    tEnv.getConfig.setIdleStateRetention(Duration.ofSeconds(1))
    val testHarness = createHarnessTester(t1.toAppendStream[Row], "OverAggregate")
    val assertor = new RowDataHarnessAssertor(
      Array(
        DataTypes.BIGINT().getLogicalType,
        DataTypes.STRING().getLogicalType,
        DataTypes.BIGINT().getLogicalType,
        DataTypes.BIGINT().getLogicalType,
        DataTypes.BIGINT().getLogicalType
      ))

    testHarness.open()

    testHarness.processWatermark(800)
    testHarness.processElement(new StreamRecord(binaryrow(801L: JLong, "aaa", 1L: JLong)))

    testHarness.processWatermark(2500)
    testHarness.processElement(new StreamRecord(binaryrow(2501L: JLong, "bbb", 10L: JLong)))

    testHarness.processWatermark(4000)
    testHarness.processElement(new StreamRecord(binaryrow(4001L: JLong, "aaa", 2L: JLong)))
    testHarness.processElement(new StreamRecord(binaryrow(4001L: JLong, "aaa", 3L: JLong)))
    testHarness.processElement(new StreamRecord(binaryrow(4001L: JLong, "bbb", 20L: JLong)))

    testHarness.processWatermark(4800)
    testHarness.processElement(new StreamRecord(binaryrow(4801L: JLong, "aaa", 4L: JLong)))

    testHarness.processWatermark(6500)
    testHarness.processElement(new StreamRecord(binaryrow(6501L: JLong, "aaa", 5L: JLong)))
    testHarness.processElement(new StreamRecord(binaryrow(6501L: JLong, "aaa", 6L: JLong)))
    testHarness.processElement(new StreamRecord(binaryrow(6501L: JLong, "bbb", 30L: JLong)))

    testHarness.processWatermark(7000)
    testHarness.processElement(new StreamRecord(binaryrow(7001L: JLong, "aaa", 7L: JLong)))

    testHarness.processWatermark(8000)
    testHarness.processElement(new StreamRecord(binaryrow(8001L: JLong, "aaa", 8L: JLong)))

    testHarness.processWatermark(12000)
    testHarness.processElement(new StreamRecord(binaryrow(12001L: JLong, "aaa", 9L: JLong)))
    testHarness.processElement(new StreamRecord(binaryrow(12001L: JLong, "aaa", 10L: JLong)))
    testHarness.processElement(new StreamRecord(binaryrow(12001L: JLong, "bbb", 40L: JLong)))

    testHarness.processWatermark(19000)

    // test cleanup
    testHarness.setProcessingTime(1000)
    testHarness.processWatermark(20000)

    // check that state is removed after max retention time
    testHarness.processElement(
      new StreamRecord(binaryrow(20001L: JLong, "ccc", 1L: JLong))
    ) // clean-up 3000
    testHarness.setProcessingTime(2500)
    testHarness.processElement(
      new StreamRecord(binaryrow(20002L: JLong, "ccc", 2L: JLong))
    ) // clean-up 4500
    testHarness.processWatermark(20010) // compute output

    testHarness.setProcessingTime(4499)
    testHarness.setProcessingTime(4500)

    // check that state is only removed if all data was processed
    testHarness.processElement(
      new StreamRecord(binaryrow(20011L: JLong, "ccc", 3L: JLong))
    ) // clean-up 6500

    testHarness.setProcessingTime(6500) // clean-up attempt but rescheduled to 8500

    testHarness.processWatermark(20020) // schedule emission

    testHarness.setProcessingTime(8499) // clean-up
    testHarness.setProcessingTime(8500) // clean-up

    val result = dropWatermarks(testHarness.getOutput.toArray)

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    expectedOutput.add(new StreamRecord(row(801L: JLong, "aaa", 1L: JLong, 1L: JLong, 1L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(2501L: JLong, "bbb", 10L: JLong, 10L: JLong, 10L: JLong)))
    expectedOutput.add(new StreamRecord(row(4001L: JLong, "aaa", 2L: JLong, 1L: JLong, 2L: JLong)))
    expectedOutput.add(new StreamRecord(row(4001L: JLong, "aaa", 3L: JLong, 1L: JLong, 3L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(4001L: JLong, "bbb", 20L: JLong, 10L: JLong, 20L: JLong)))
    expectedOutput.add(new StreamRecord(row(4801L: JLong, "aaa", 4L: JLong, 2L: JLong, 4L: JLong)))
    expectedOutput.add(new StreamRecord(row(6501L: JLong, "aaa", 5L: JLong, 3L: JLong, 5L: JLong)))
    expectedOutput.add(new StreamRecord(row(6501L: JLong, "aaa", 6L: JLong, 4L: JLong, 6L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(6501L: JLong, "bbb", 30L: JLong, 10L: JLong, 30L: JLong)))
    expectedOutput.add(new StreamRecord(row(7001L: JLong, "aaa", 7L: JLong, 5L: JLong, 7L: JLong)))
    expectedOutput.add(new StreamRecord(row(8001L: JLong, "aaa", 8L: JLong, 6L: JLong, 8L: JLong)))
    expectedOutput.add(new StreamRecord(row(12001L: JLong, "aaa", 9L: JLong, 7L: JLong, 9L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(12001L: JLong, "aaa", 10L: JLong, 8L: JLong, 10L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(12001L: JLong, "bbb", 40L: JLong, 20L: JLong, 40L: JLong)))

    expectedOutput.add(new StreamRecord(row(20001L: JLong, "ccc", 1L: JLong, 1L: JLong, 1L: JLong)))
    expectedOutput.add(new StreamRecord(row(20002L: JLong, "ccc", 2L: JLong, 1L: JLong, 2L: JLong)))
    expectedOutput.add(new StreamRecord(row(20011L: JLong, "ccc", 3L: JLong, 3L: JLong, 3L: JLong)))

    assertor.assertOutputEqualsSorted("result mismatch", expectedOutput, result)
    testHarness.close()
  }

  /** all elements at the same row-time have the same value per key */
  @Test
  def testRowTimeUnboundedRangeOver(): Unit = {

    val data = new mutable.MutableList[(Long, String, Long)]
    val t = env.fromCollection(data).toTable(tEnv, 'rowtime.rowtime, 'b, 'c)
    tEnv.createTemporaryView("T", t)

    val sql =
      """
        |SELECT rowtime, b, c,
        | min(c) OVER
        |   (PARTITION BY b ORDER BY rowtime
        |   RANGE BETWEEN UNBOUNDED preceding AND CURRENT ROW),
        | max(c) OVER
        |   (PARTITION BY b ORDER BY rowtime
        |   RANGE BETWEEN UNBOUNDED preceding AND CURRENT ROW)
        |FROM T
      """.stripMargin
    val t1 = tEnv.sqlQuery(sql)

    tEnv.getConfig.setIdleStateRetention(Duration.ofSeconds(1))
    val testHarness = createHarnessTester(t1.toAppendStream[Row], "OverAggregate")
    val assertor = new RowDataHarnessAssertor(
      Array(
        DataTypes.BIGINT().getLogicalType,
        DataTypes.STRING().getLogicalType,
        DataTypes.BIGINT().getLogicalType,
        DataTypes.BIGINT().getLogicalType,
        DataTypes.BIGINT().getLogicalType
      ))

    testHarness.open()

    testHarness.setProcessingTime(1000)
    testHarness.processWatermark(800)
    testHarness.processElement(new StreamRecord(binaryrow(801L: JLong, "aaa", 1L: JLong)))

    testHarness.processWatermark(2500)
    testHarness.processElement(new StreamRecord(binaryrow(2501L: JLong, "bbb", 10L: JLong)))

    testHarness.processWatermark(4000)
    testHarness.processElement(new StreamRecord(binaryrow(4001L: JLong, "aaa", 2L: JLong)))
    testHarness.processElement(new StreamRecord(binaryrow(4001L: JLong, "aaa", 3L: JLong)))
    testHarness.processElement(new StreamRecord(binaryrow(4001L: JLong, "bbb", 20L: JLong)))

    testHarness.processWatermark(4800)
    testHarness.processElement(new StreamRecord(binaryrow(4801L: JLong, "aaa", 4L: JLong)))

    testHarness.processWatermark(6500)
    testHarness.processElement(new StreamRecord(binaryrow(6501L: JLong, "aaa", 5L: JLong)))
    testHarness.processElement(new StreamRecord(binaryrow(6501L: JLong, "aaa", 6L: JLong)))
    testHarness.processElement(new StreamRecord(binaryrow(6501L: JLong, "bbb", 30L: JLong)))

    testHarness.processWatermark(7000)
    testHarness.processElement(new StreamRecord(binaryrow(7001L: JLong, "aaa", 7L: JLong)))

    testHarness.processWatermark(8000)
    testHarness.processElement(new StreamRecord(binaryrow(8001L: JLong, "aaa", 8L: JLong)))

    testHarness.processWatermark(12000)
    testHarness.processElement(new StreamRecord(binaryrow(12001L: JLong, "aaa", 9L: JLong)))
    testHarness.processElement(new StreamRecord(binaryrow(12001L: JLong, "aaa", 10L: JLong)))
    testHarness.processElement(new StreamRecord(binaryrow(12001L: JLong, "bbb", 40L: JLong)))

    testHarness.processWatermark(19000)

    // test cleanup
    testHarness.setProcessingTime(2999) // clean up timer is 3000, so nothing should happen
    testHarness.setProcessingTime(3000) // clean up is triggered

    testHarness.processWatermark(20000)
    testHarness.processElement(
      new StreamRecord(binaryrow(20000L: JLong, "ccc", 1L: JLong))
    ) // test for late data

    testHarness.processElement(
      new StreamRecord(binaryrow(20001L: JLong, "ccc", 1L: JLong))
    ) // clean-up 5000
    testHarness.setProcessingTime(2500)
    testHarness.processElement(
      new StreamRecord(binaryrow(20002L: JLong, "ccc", 2L: JLong))
    ) // clean-up 5000

    testHarness.setProcessingTime(5000) // does not clean up, because data left. New timer 7000
    testHarness.processWatermark(20010) // compute output

    testHarness.setProcessingTime(6999) // clean up timer is 3000, so nothing should happen
    testHarness.setProcessingTime(7000) // clean up is triggered

    val result = dropWatermarks(testHarness.getOutput.toArray)

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    // all elements at the same row-time have the same value per key
    expectedOutput.add(new StreamRecord(row(801L: JLong, "aaa", 1L: JLong, 1L: JLong, 1L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(2501L: JLong, "bbb", 10L: JLong, 10L: JLong, 10L: JLong)))
    expectedOutput.add(new StreamRecord(row(4001L: JLong, "aaa", 2L: JLong, 1L: JLong, 3L: JLong)))
    expectedOutput.add(new StreamRecord(row(4001L: JLong, "aaa", 3L: JLong, 1L: JLong, 3L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(4001L: JLong, "bbb", 20L: JLong, 10L: JLong, 20L: JLong)))
    expectedOutput.add(new StreamRecord(row(4801L: JLong, "aaa", 4L: JLong, 1L: JLong, 4L: JLong)))
    expectedOutput.add(new StreamRecord(row(6501L: JLong, "aaa", 5L: JLong, 1L: JLong, 6L: JLong)))
    expectedOutput.add(new StreamRecord(row(6501L: JLong, "aaa", 6L: JLong, 1L: JLong, 6L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(6501L: JLong, "bbb", 30L: JLong, 10L: JLong, 30L: JLong)))
    expectedOutput.add(new StreamRecord(row(7001L: JLong, "aaa", 7L: JLong, 1L: JLong, 7L: JLong)))
    expectedOutput.add(new StreamRecord(row(8001L: JLong, "aaa", 8L: JLong, 1L: JLong, 8L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(12001L: JLong, "aaa", 9L: JLong, 1L: JLong, 10L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(12001L: JLong, "aaa", 10L: JLong, 1L: JLong, 10L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(12001L: JLong, "bbb", 40L: JLong, 10L: JLong, 40L: JLong)))

    expectedOutput.add(new StreamRecord(row(20001L: JLong, "ccc", 1L: JLong, 1L: JLong, 1L: JLong)))
    expectedOutput.add(new StreamRecord(row(20002L: JLong, "ccc", 2L: JLong, 1L: JLong, 2L: JLong)))

    assertor.assertOutputEqualsSorted("result mismatch", expectedOutput, result)
    testHarness.close()
  }

  @Test
  def testRowTimeUnboundedRowsOver(): Unit = {

    val data = new mutable.MutableList[(Long, String, Long)]
    val t = env.fromCollection(data).toTable(tEnv, 'rowtime.rowtime, 'b, 'c)
    tEnv.createTemporaryView("T", t)

    val sql =
      """
        |SELECT rowtime, b, c,
        | min(c) OVER
        |   (PARTITION BY b ORDER BY rowtime
        |   ROWS BETWEEN UNBOUNDED preceding AND CURRENT ROW),
        | max(c) OVER
        |   (PARTITION BY b ORDER BY rowtime
        |   ROWS BETWEEN UNBOUNDED preceding AND CURRENT ROW)
        |FROM T
      """.stripMargin
    val t1 = tEnv.sqlQuery(sql)

    tEnv.getConfig.setIdleStateRetention(Duration.ofSeconds(1))
    val testHarness = createHarnessTester(t1.toAppendStream[Row], "OverAggregate")
    val assertor = new RowDataHarnessAssertor(
      Array(
        DataTypes.BIGINT().getLogicalType,
        DataTypes.STRING().getLogicalType,
        DataTypes.BIGINT().getLogicalType,
        DataTypes.BIGINT().getLogicalType,
        DataTypes.BIGINT().getLogicalType
      ))

    testHarness.open()

    testHarness.setProcessingTime(1000)
    testHarness.processWatermark(800)
    testHarness.processElement(new StreamRecord(binaryrow(801L: JLong, "aaa", 1L: JLong)))

    testHarness.processWatermark(2500)
    testHarness.processElement(new StreamRecord(binaryrow(2501L: JLong, "bbb", 10L: JLong)))

    testHarness.processWatermark(4000)
    testHarness.processElement(new StreamRecord(binaryrow(4001L: JLong, "aaa", 2L: JLong)))
    testHarness.processElement(new StreamRecord(binaryrow(4001L: JLong, "aaa", 3L: JLong)))
    testHarness.processElement(new StreamRecord(binaryrow(4001L: JLong, "bbb", 20L: JLong)))

    testHarness.processWatermark(4800)
    testHarness.processElement(new StreamRecord(binaryrow(4801L: JLong, "aaa", 4L: JLong)))

    testHarness.processWatermark(6500)
    testHarness.processElement(new StreamRecord(binaryrow(6501L: JLong, "aaa", 5L: JLong)))
    testHarness.processElement(new StreamRecord(binaryrow(6501L: JLong, "aaa", 6L: JLong)))
    testHarness.processElement(new StreamRecord(binaryrow(6501L: JLong, "bbb", 30L: JLong)))

    testHarness.processWatermark(7000)
    testHarness.processElement(new StreamRecord(binaryrow(7001L: JLong, "aaa", 7L: JLong)))

    testHarness.processWatermark(8000)
    testHarness.processElement(new StreamRecord(binaryrow(8001L: JLong, "aaa", 8L: JLong)))

    testHarness.processWatermark(12000)
    testHarness.processElement(new StreamRecord(binaryrow(12001L: JLong, "aaa", 9L: JLong)))
    testHarness.processElement(new StreamRecord(binaryrow(12001L: JLong, "aaa", 10L: JLong)))
    testHarness.processElement(new StreamRecord(binaryrow(12001L: JLong, "bbb", 40L: JLong)))

    testHarness.processWatermark(19000)

    // test cleanup
    testHarness.setProcessingTime(2999) // clean up timer is 3000, so nothing should happen
    testHarness.setProcessingTime(3000) // clean up is triggered

    testHarness.processWatermark(20000)
    testHarness.processElement(
      new StreamRecord(binaryrow(20000L: JLong, "ccc", 2L: JLong))
    ) // test for late data

    testHarness.processElement(
      new StreamRecord(binaryrow(20001L: JLong, "ccc", 1L: JLong))
    ) // clean-up 5000
    testHarness.setProcessingTime(2500)
    testHarness.processElement(
      new StreamRecord(binaryrow(20002L: JLong, "ccc", 2L: JLong))
    ) // clean-up 5000

    testHarness.setProcessingTime(5000) // does not clean up, because data left. New timer 7000
    testHarness.processWatermark(20010) // compute output

    testHarness.setProcessingTime(6999) // clean up timer is 3000, so nothing should happen
    testHarness.setProcessingTime(7000) // clean up is triggered

    val result = dropWatermarks(testHarness.getOutput.toArray)

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    expectedOutput.add(new StreamRecord(row(801L: JLong, "aaa", 1L: JLong, 1L: JLong, 1L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(2501L: JLong, "bbb", 10L: JLong, 10L: JLong, 10L: JLong)))
    expectedOutput.add(new StreamRecord(row(4001L: JLong, "aaa", 2L: JLong, 1L: JLong, 2L: JLong)))
    expectedOutput.add(new StreamRecord(row(4001L: JLong, "aaa", 3L: JLong, 1L: JLong, 3L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(4001L: JLong, "bbb", 20L: JLong, 10L: JLong, 20L: JLong)))
    expectedOutput.add(new StreamRecord(row(4801L: JLong, "aaa", 4L: JLong, 1L: JLong, 4L: JLong)))
    expectedOutput.add(new StreamRecord(row(6501L: JLong, "aaa", 5L: JLong, 1L: JLong, 5L: JLong)))
    expectedOutput.add(new StreamRecord(row(6501L: JLong, "aaa", 6L: JLong, 1L: JLong, 6L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(6501L: JLong, "bbb", 30L: JLong, 10L: JLong, 30L: JLong)))
    expectedOutput.add(new StreamRecord(row(7001L: JLong, "aaa", 7L: JLong, 1L: JLong, 7L: JLong)))
    expectedOutput.add(new StreamRecord(row(8001L: JLong, "aaa", 8L: JLong, 1L: JLong, 8L: JLong)))
    expectedOutput.add(new StreamRecord(row(12001L: JLong, "aaa", 9L: JLong, 1L: JLong, 9L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(12001L: JLong, "aaa", 10L: JLong, 1L: JLong, 10L: JLong)))
    expectedOutput.add(
      new StreamRecord(row(12001L: JLong, "bbb", 40L: JLong, 10L: JLong, 40L: JLong)))

    expectedOutput.add(new StreamRecord(row(20001L: JLong, "ccc", 1L: JLong, 1L: JLong, 1L: JLong)))
    expectedOutput.add(new StreamRecord(row(20002L: JLong, "ccc", 2L: JLong, 1L: JLong, 2L: JLong)))

    assertor.assertOutputEqualsSorted("result mismatch", expectedOutput, result)
    testHarness.close()
  }

  @Test
  def testCloseWithoutOpen(): Unit = {
    val (testHarness, outputType) = createProcTimeBoundedRowsOver
    testHarness.setup(new RowDataSerializer(outputType: _*))
    // simulate a failover after a failed task open, expect no exception happens
    testHarness.open()
  }
}
