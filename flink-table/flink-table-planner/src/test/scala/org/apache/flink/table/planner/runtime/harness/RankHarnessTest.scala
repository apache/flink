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
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.bridge.scala.internal.StreamTableEnvironmentImpl
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedTableFunctions
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor
import org.apache.flink.table.runtime.util.StreamRecordUtils.binaryRecord
import org.apache.flink.types.Row
import org.apache.flink.types.RowKind._

import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Test}

import java.lang.{Long => JLong}
import java.time.Duration
import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.mutable

@RunWith(classOf[Parameterized])
class RankHarnessTest(mode: StateBackendMode) extends HarnessTestBase(mode) {

  @Before
  override def before(): Unit = {
    super.before()
    val setting = EnvironmentSettings.newInstance().inStreamingMode().build()
    val config = new TestTableConfig
    this.tEnv = StreamTableEnvironmentImpl.create(env, setting, config)
  }

  @Test
  def testRetractRankWithRowNumber(): Unit = {
    val data = new mutable.MutableList[(String, String, Long)]
    val t = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.createTemporaryView("T", t)
    tEnv.createTemporarySystemFunction(
      "STRING_SPLIT", new JavaUserDefinedTableFunctions.StringSplit)
    tEnv.getConfig.setIdleStateRetention(Duration.ofSeconds(1))

    val sql =
      """
        |SELECT a, b, c, id, rn1
        |FROM (
        |   SELECT a, b, c, t3.id id,
        |    ROW_NUMBER() OVER (PARTITION BY a, t3.id ORDER BY c DESC) AS rn1
        |   FROM (
        |       SELECT a, b, c, rn
        |       FROM
        |       (
        |           -- append rank
        |           SELECT a, b, c,
        |               ROW_NUMBER() OVER (PARTITION BY a ORDER BY c DESC) AS rn
        |           FROM T
        |       ) t1
        |       WHERE rn = 1
        |   ) t2, LATERAL TABLE(STRING_SPLIT(b, '#')) AS t3(id)
        |) WHERE rn1 <= 2
      """.stripMargin

    val t1 = tEnv.sqlQuery(sql)

    val testHarness = createHarnessTester(t1.toRetractStream[Row], "Rank(strategy=[RetractStrategy")
    val assertor = new RowDataHarnessAssertor(
      Array(
        DataTypes.STRING().getLogicalType,
        DataTypes.STRING().getLogicalType,
        DataTypes.BIGINT().getLogicalType,
        DataTypes.STRING().getLogicalType,
        DataTypes.BIGINT().getLogicalType))

    testHarness.open()

    // set TtlTimeProvider with 1
    testHarness.setStateTtlProcessingTime(1)

    testHarness.processElement(binaryRecord(INSERT, "a", "1", 2L: JLong, "1"))
    testHarness.processElement(binaryRecord(INSERT, "a", "1", 2L: JLong, "1"))
    testHarness.processElement(binaryRecord(INSERT, "a", "1", 2L: JLong, "1"))
    testHarness.processElement(binaryRecord(INSERT, "a", "1", 2L: JLong, "1"))
    testHarness.processElement(binaryRecord(INSERT, "a", "1", 1L: JLong, "1"))

    testHarness.setStateTtlProcessingTime(1000)
    testHarness.processElement(binaryRecord(DELETE, "a", "1", 2L: JLong, "1"))
    testHarness.processElement(binaryRecord(DELETE, "a", "1", 2L: JLong, "1"))

    // set TtlTimeProvider with 1001 to trigger expired state cleanup
    testHarness.setStateTtlProcessingTime(1001)
    testHarness.processElement(binaryRecord(DELETE, "a", "1", 2L: JLong, "1"))

    // currently there should only exists one record in state, test adding two new record
    testHarness.processElement(binaryRecord(INSERT, "a", "1", 2L: JLong, "1"))
    testHarness.processElement(binaryRecord(INSERT, "a", "1", 2L: JLong, "1"))

    val result = dropWatermarks(testHarness.getOutput.toArray)

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    expectedOutput.add(binaryRecord(INSERT, "a", "1", 2L: JLong, "1", 1L: JLong))
    expectedOutput.add(binaryRecord(INSERT, "a", "1", 2L: JLong, "1", 2L: JLong))

    expectedOutput.add(binaryRecord(UPDATE_BEFORE, "a", "1", 2L: JLong, "1", 1L: JLong))
    expectedOutput.add(binaryRecord(UPDATE_AFTER, "a", "1", 2L: JLong, "1", 1L: JLong))
    expectedOutput.add(binaryRecord(UPDATE_BEFORE, "a", "1", 2L: JLong, "1", 2L: JLong))
    expectedOutput.add(binaryRecord(UPDATE_AFTER, "a", "1", 2L: JLong, "1", 2L: JLong))
    expectedOutput.add(binaryRecord(UPDATE_BEFORE, "a", "1", 2L: JLong, "1", 1L: JLong))
    expectedOutput.add(binaryRecord(UPDATE_AFTER, "a", "1", 2L: JLong, "1", 1L: JLong))
    expectedOutput.add(binaryRecord(UPDATE_BEFORE, "a", "1", 2L: JLong, "1", 2L: JLong))
    expectedOutput.add(binaryRecord(UPDATE_AFTER, "a", "1", 2L: JLong, "1", 2L: JLong))

    expectedOutput.add(binaryRecord(UPDATE_BEFORE, "a", "1", 2L: JLong, "1", 1L: JLong))
    expectedOutput.add(binaryRecord(UPDATE_AFTER, "a", "1", 2L: JLong, "1", 1L: JLong))

    expectedOutput.add(binaryRecord(DELETE, "a", "1", 2L: JLong, "1", 2L: JLong))

    // if not expired, this is expected result
    // expectedOutput.add(binaryRecord(INSERT, "a", "1", 1L: JLong, "1", 2L: JLong))

    // only output one result
    expectedOutput.add(binaryRecord(INSERT, "a", "1", 2L: JLong, "1", 2L: JLong))

    assertor.assertOutputEqualsSorted("result mismatch", expectedOutput, result)
    testHarness.close()
  }

  @Test
  def testRetractRankWithoutRowNumber(): Unit = {
    val data = new mutable.MutableList[(String, String, Long)]
    val t = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.createTemporaryView("T", t)
    tEnv.createTemporarySystemFunction(
      "STRING_SPLIT", new JavaUserDefinedTableFunctions.StringSplit)
    tEnv.getConfig.setIdleStateRetention(Duration.ofSeconds(1))

    val sql =
      """
        |SELECT a, b, c, id
        |FROM (
        |   SELECT a, b, c, t3.id id,
        |    ROW_NUMBER() OVER (PARTITION BY a, t3.id ORDER BY c DESC) AS rn1
        |   FROM (
        |       SELECT a, b, c, rn
        |       FROM
        |       (
        |           -- append rank
        |           SELECT a, b, c,
        |               ROW_NUMBER() OVER (PARTITION BY a ORDER BY c DESC) AS rn
        |           FROM T
        |       ) t1
        |       WHERE rn = 1
        |   ) t2, LATERAL TABLE(STRING_SPLIT(b, '#')) AS t3(id)
        |) WHERE rn1 = 1
      """.stripMargin

    val t1 = tEnv.sqlQuery(sql)

    val testHarness = createHarnessTester(t1.toRetractStream[Row], "Rank(strategy=[RetractStrategy")
    val assertor = new RowDataHarnessAssertor(
      Array(
        DataTypes.STRING().getLogicalType,
        DataTypes.STRING().getLogicalType,
        DataTypes.BIGINT().getLogicalType,
        DataTypes.STRING().getLogicalType))

    testHarness.open()

    // set TtlTimeProvider with 1
    testHarness.setStateTtlProcessingTime(1)

    testHarness.processElement(binaryRecord(INSERT, "a", "1", 2L: JLong, "1"))
    testHarness.processElement(binaryRecord(INSERT, "a", "1", 2L: JLong, "1"))
    testHarness.processElement(binaryRecord(INSERT, "a", "1", 1L: JLong, "1"))

    testHarness.setStateTtlProcessingTime(1000)
    testHarness.processElement(binaryRecord(DELETE, "a", "1", 2L: JLong, "1"))

    // set TtlTimeProvider with 1001 to trigger expired state cleanup
    testHarness.setStateTtlProcessingTime(1001)
    testHarness.processElement(binaryRecord(DELETE, "a", "1", 2L: JLong, "1"))

    // currently there should not exists any record in state, test adding two new record
    testHarness.processElement(binaryRecord(INSERT, "a", "1", 2L: JLong, "1"))
    testHarness.processElement(binaryRecord(INSERT, "a", "1", 2L: JLong, "1"))

    val result = dropWatermarks(testHarness.getOutput.toArray)

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    expectedOutput.add(binaryRecord(INSERT, "a", "1", 2L: JLong, "1"))
    expectedOutput.add(binaryRecord(DELETE, "a", "1", 2L: JLong, "1"))
    expectedOutput.add(binaryRecord(INSERT, "a", "1", 2L: JLong, "1"))
    expectedOutput.add(binaryRecord(DELETE, "a", "1", 2L: JLong, "1"))

    // if not expired, this is expected result
    // expectedOutput.add(binaryRecord(INSERT, "a", "1", 1L: JLong, "1"))

    expectedOutput.add(binaryRecord(INSERT, "a", "1", 2L: JLong, "1"))

    assertor.assertOutputEqualsSorted("result mismatch", expectedOutput, result)
    testHarness.close()
  }
}
