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
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.bridge.scala.internal.StreamTableEnvironmentImpl
import org.apache.flink.table.api.config.ExecutionConfigOptions.{TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, TABLE_EXEC_MINIBATCH_ENABLED, TABLE_EXEC_MINIBATCH_SIZE}
import org.apache.flink.table.api.config.OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY
import org.apache.flink.table.api.{EnvironmentSettings, _}
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.runtime.utils.StreamingWithMiniBatchTestBase.{MiniBatchMode, MiniBatchOff, MiniBatchOn}
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.{HEAP_BACKEND, ROCKSDB_BACKEND, StateBackendMode}
import org.apache.flink.table.planner.runtime.utils.UserDefinedFunctionTestUtils.CountNullNonNull
import org.apache.flink.table.runtime.typeutils.RowDataSerializer
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor
import org.apache.flink.table.runtime.util.StreamRecordUtils.binaryRecord
import org.apache.flink.table.types.logical.LogicalType
import org.apache.flink.types.Row
import org.apache.flink.types.RowKind._

import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Test}

import java.lang.{Long => JLong}
import java.time.Duration
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.{Collection => JCollection}

import scala.collection.JavaConversions._
import scala.collection.mutable

@RunWith(classOf[Parameterized])
class GroupAggregateHarnessTest(mode: StateBackendMode, miniBatch: MiniBatchMode)
  extends HarnessTestBase(mode) {

  @Before
  override def before(): Unit = {
    super.before()
    val setting = EnvironmentSettings.newInstance().inStreamingMode().build()
    val config = new TestTableConfig
    this.tEnv = StreamTableEnvironmentImpl.create(env, setting, config)
    // set mini batch
    val tableConfig = tEnv.getConfig
    miniBatch match {
      case MiniBatchOn =>
        tableConfig.getConfiguration.setBoolean(TABLE_EXEC_MINIBATCH_ENABLED, true)
        tableConfig.getConfiguration.set(TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, Duration.ofSeconds(1))
        // trigger every record for easier harness test
        tableConfig.getConfiguration.setLong(TABLE_EXEC_MINIBATCH_SIZE, 1L)
        // disable local-global to only test the MiniBatchGroupAggFunction
        tableConfig.getConfiguration.setString(TABLE_OPTIMIZER_AGG_PHASE_STRATEGY, "ONE_PHASE")
      case MiniBatchOff =>
        tableConfig.getConfiguration.removeConfig(TABLE_EXEC_MINIBATCH_ALLOW_LATENCY)
    }
  }

  @Test
  def testAggregateWithRetraction(): Unit = {
    val data = new mutable.MutableList[(String, String, Long)]
    val t = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.createTemporaryView("T", t)

    val sql =
      """
        |SELECT a, SUM(c)
        |FROM (
        |  SELECT a, b, SUM(c) as c
        |  FROM T GROUP BY a, b
        |)GROUP BY a
      """.stripMargin
    val t1 = tEnv.sqlQuery(sql)

    tEnv.getConfig.setIdleStateRetention(Duration.ofSeconds(2))
    val testHarness = createHarnessTester(t1.toRetractStream[Row], "GroupAggregate")
    val assertor = new RowDataHarnessAssertor(
      Array(
        DataTypes.STRING().getLogicalType,
        DataTypes.BIGINT().getLogicalType))

    testHarness.open()

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    // set TtlTimeProvider with 1
    testHarness.setStateTtlProcessingTime(1)

    // insertion
    testHarness.processElement(binaryRecord(INSERT, "aaa", 1L: JLong))
    expectedOutput.add(binaryRecord(INSERT, "aaa", 1L: JLong))

    // insertion
    testHarness.processElement(binaryRecord(INSERT, "bbb", 1L: JLong))
    expectedOutput.add(binaryRecord(INSERT, "bbb", 1L: JLong))

    // update for insertion
    testHarness.processElement(binaryRecord(INSERT, "aaa", 2L: JLong))
    expectedOutput.add(binaryRecord(UPDATE_BEFORE, "aaa", 1L: JLong))
    expectedOutput.add(binaryRecord(UPDATE_AFTER, "aaa", 3L: JLong))

    // retract for deletion
    testHarness.processElement(binaryRecord(DELETE, "aaa", 2L: JLong))
    expectedOutput.add(binaryRecord(UPDATE_BEFORE, "aaa", 3L: JLong))
    expectedOutput.add(binaryRecord(UPDATE_AFTER, "aaa", 1L: JLong))

    // insertion
    testHarness.processElement(binaryRecord(INSERT, "ccc", 3L: JLong))
    expectedOutput.add(binaryRecord(INSERT, "ccc", 3L: JLong))

    // set TtlTimeProvider with 3002 to trigger expired state cleanup
    testHarness.setStateTtlProcessingTime(3002)

    // retract after clean up
    testHarness.processElement(binaryRecord(UPDATE_BEFORE, "ccc", 3L: JLong))
    // not output

    // accumulate
    testHarness.processElement(binaryRecord(INSERT, "aaa", 4L: JLong))
    expectedOutput.add(binaryRecord(INSERT, "aaa", 4L: JLong))
    testHarness.processElement(binaryRecord(INSERT, "bbb", 2L: JLong))
    expectedOutput.add(binaryRecord(INSERT, "bbb", 2L: JLong))

    // retract
    testHarness.processElement(binaryRecord(INSERT, "aaa", 5L: JLong))
    expectedOutput.add(binaryRecord(UPDATE_BEFORE, "aaa", 4L: JLong))
    expectedOutput.add(binaryRecord(UPDATE_AFTER, "aaa", 9L: JLong))

    // accumulate
    testHarness.processElement(binaryRecord(INSERT, "eee", 6L: JLong))
    expectedOutput.add(binaryRecord(INSERT, "eee", 6L: JLong))

    // retract
    testHarness.processElement(binaryRecord(INSERT, "aaa", 7L: JLong))
    expectedOutput.add(binaryRecord(UPDATE_BEFORE, "aaa", 9L: JLong))
    expectedOutput.add(binaryRecord(UPDATE_AFTER, "aaa", 16L: JLong))
    testHarness.processElement(binaryRecord(INSERT, "bbb", 3L: JLong))
    expectedOutput.add(binaryRecord(UPDATE_BEFORE, "bbb", 2L: JLong))
    expectedOutput.add(binaryRecord(UPDATE_AFTER, "bbb", 5L: JLong))

    val result = testHarness.getOutput

    assertor.assertOutputEqualsSorted("result mismatch", expectedOutput, result)

    testHarness.close()
  }

  @Test
  def testAggregationWithDistinct(): Unit = {
    val (testHarness, outputTypes) = createAggregationWithDistinct
    val assertor = new RowDataHarnessAssertor(outputTypes)

    testHarness.open()

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    // set ttl processing time to 1
    testHarness.setStateTtlProcessingTime(1)

    // insertion
    testHarness.processElement(binaryRecord(INSERT, "aaa", "a1", 1L: JLong))
    expectedOutput.add(binaryRecord(INSERT, "aaa", 1L: JLong, "1|0", 1L: JLong, 1L: JLong))

    // insertion
    testHarness.processElement(binaryRecord(INSERT, "bbb", "b1", 2L: JLong))
    expectedOutput.add(binaryRecord(INSERT, "bbb", 1L: JLong, "1|0", 1L: JLong, 2L: JLong))

    // advance ttl processing time
    testHarness.setStateTtlProcessingTime(1000)

    // update for insertion
    testHarness.processElement(binaryRecord(INSERT, "aaa", "a2", 2L: JLong))
    expectedOutput.add(binaryRecord(UPDATE_BEFORE, "aaa", 1L: JLong, "1|0", 1L: JLong, 1L: JLong))
    expectedOutput.add(binaryRecord(UPDATE_AFTER, "aaa", 2L: JLong, "2|0", 2L: JLong, 3L: JLong))

    // this should expire "bbb" state
    testHarness.setStateTtlProcessingTime(2001)

    // accumulate from initial state
    testHarness.processElement(binaryRecord(INSERT, "bbb", "b3", 3L: JLong))
    expectedOutput.add(binaryRecord(INSERT, "bbb", 1L: JLong, "1|0", 1L: JLong, 3L: JLong))
    // "aaa" is not expired
    testHarness.processElement(binaryRecord(INSERT, "aaa", "a2", 3L: JLong))
    expectedOutput.add(binaryRecord(UPDATE_BEFORE, "aaa", 2L: JLong, "2|0", 2L: JLong, 3L: JLong))
    expectedOutput.add(binaryRecord(UPDATE_AFTER, "aaa", 2L: JLong, "2|0", 3L: JLong, 6L: JLong))
    // test null key
    testHarness.processElement(binaryRecord(INSERT, "aaa", null, 4L: JLong))
    expectedOutput.add(binaryRecord(UPDATE_BEFORE, "aaa", 2L: JLong, "2|0", 3L: JLong, 6L: JLong))
    expectedOutput.add(binaryRecord(UPDATE_AFTER, "aaa", 2L: JLong, "2|1", 4L: JLong, 10L: JLong))

    // this should expire "aaa" state
    testHarness.setStateTtlProcessingTime(5001)

    // accumulate from initial state
    testHarness.processElement(binaryRecord(INSERT, "aaa", null, 4L: JLong))
    expectedOutput.add(binaryRecord(INSERT, "aaa", 0L: JLong, "0|1", 1L: JLong, 4L: JLong))
    testHarness.processElement(binaryRecord(INSERT, "aaa", "a2", 2L: JLong))
    expectedOutput.add(binaryRecord(UPDATE_BEFORE, "aaa", 0L: JLong, "0|1", 1L: JLong, 4L: JLong))
    expectedOutput.add(binaryRecord(UPDATE_AFTER, "aaa", 1L: JLong, "1|1", 2L: JLong, 6L: JLong))
    testHarness.processElement(binaryRecord(INSERT, "bbb", "b4", 4L: JLong))
    expectedOutput.add(binaryRecord(INSERT, "bbb", 1L: JLong, "1|0", 1L: JLong, 4L: JLong))

    val result = testHarness.getOutput

    assertor.assertOutputEqualsSorted("result mismatch", expectedOutput, result)

    testHarness.close()
  }

  private def createAggregationWithDistinct()
    : (KeyedOneInputStreamOperatorTestHarness[RowData, RowData, RowData], Array[LogicalType]) = {
    val data = new mutable.MutableList[(String, String, Long)]
    val t = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.createTemporaryView("T", t)
    tEnv.createTemporarySystemFunction("CntNullNonNull", new CountNullNonNull)

    val sql =
      """
        |SELECT a, COUNT(DISTINCT b), CntNullNonNull(DISTINCT b), COUNT(*), SUM(c)
        |FROM T
        |GROUP BY a
      """.stripMargin
    val t1 = tEnv.sqlQuery(sql)

    tEnv.getConfig.setIdleStateRetention(Duration.ofSeconds(2))
    val testHarness = createHarnessTester(t1.toRetractStream[Row], "GroupAggregate")
    val outputTypes = Array(
      DataTypes.STRING().getLogicalType,
      DataTypes.BIGINT().getLogicalType,
      DataTypes.STRING().getLogicalType,
      DataTypes.BIGINT().getLogicalType,
      DataTypes.BIGINT().getLogicalType)

    (testHarness, outputTypes)
  }

  @Test
  def testCloseWithoutOpen(): Unit = {
    val (testHarness, outputType) = createAggregationWithDistinct
    testHarness.setup(new RowDataSerializer(outputType: _*))
    // simulate a failover after a failed task open(e.g., stuck on initializing)
    // expect no exception happens
    testHarness.close()
  }
}

object GroupAggregateHarnessTest {

  @Parameterized.Parameters(name = "StateBackend={0}, MiniBatch={1}")
  def parameters(): JCollection[Array[java.lang.Object]] = {
    Seq[Array[AnyRef]](
      Array(HEAP_BACKEND, MiniBatchOff),
      Array(HEAP_BACKEND, MiniBatchOn),
      Array(ROCKSDB_BACKEND, MiniBatchOff),
      Array(ROCKSDB_BACKEND, MiniBatchOn)
    )
  }
}
