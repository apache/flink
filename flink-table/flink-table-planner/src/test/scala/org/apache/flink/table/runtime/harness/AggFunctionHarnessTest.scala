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

package org.apache.flink.table.runtime.harness

import org.apache.flink.api.scala._
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.dataview.MapView
import org.apache.flink.table.dataview.StateMapView
import org.apache.flink.table.runtime.aggregate.GroupAggProcessFunction
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.types.Row

import org.junit.Assert.assertTrue
import org.junit.Test

import java.lang.{Integer => JInt}
import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.JavaConverters._
import scala.collection.mutable

class AggFunctionHarnessTest extends HarnessTestBase {

  @Test
  def testCollectAggregate(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(
      env, EnvironmentSettings.newInstance().useOldPlanner().build())

    val data = new mutable.MutableList[(JInt, String)]
    val t = env.fromCollection(data).toTable(tEnv, 'a, 'b)
    tEnv.registerTable("T", t)
    val sqlQuery = tEnv.sqlQuery(
      s"""
         |SELECT
         |  b, collect(a)
         |FROM (
         |  SELECT a, b
         |  FROM T
         |  GROUP BY a, b
         |) GROUP BY b
         |""".stripMargin)

    val testHarness = createHarnessTester[String, CRow, CRow](
      sqlQuery.toRetractStream[Row], "groupBy")

    testHarness.setStateBackend(getStateBackend)
    testHarness.open()

    val operator = getOperator(testHarness)
    val state = getState(
      operator,
      "function",
      classOf[GroupAggProcessFunction[Row]],
      "acc0_map_dataview").asInstanceOf[MapView[JInt, JInt]]
    assertTrue(state.isInstanceOf[StateMapView[_, _]])
    assertTrue(operator.getKeyedStateBackend.isInstanceOf[RocksDBKeyedStateBackend[_]])

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    testHarness.processElement(new StreamRecord(CRow(1: JInt, "aaa"), 1))
    expectedOutput.add(new StreamRecord(CRow("aaa", Map(1 -> 1).asJava), 1))

    testHarness.processElement(new StreamRecord(CRow(1: JInt, "bbb"), 1))
    expectedOutput.add(new StreamRecord(CRow("bbb", Map(1 -> 1).asJava), 1))

    testHarness.processElement(new StreamRecord(CRow(1: JInt, "aaa"), 1))
    expectedOutput.add(new StreamRecord(CRow(false, "aaa", Map(1 -> 1).asJava), 1))
    expectedOutput.add(new StreamRecord(CRow("aaa", Map(1 -> 2).asJava), 1))

    testHarness.processElement(new StreamRecord(CRow(2: JInt, "aaa"), 1))
    expectedOutput.add(new StreamRecord(CRow(false, "aaa", Map(1 -> 2).asJava), 1))
    expectedOutput.add(new StreamRecord(CRow("aaa", Map(1 -> 2, 2 -> 1).asJava), 1))

    // remove some state: state may be cleaned up by the state backend
    // if not accessed beyond ttl time
    operator.setCurrentKey(Row.of("aaa"))
    state.remove(2)

    // retract after state has been cleaned up
    testHarness.processElement(new StreamRecord(CRow(false, 2: JInt, "aaa"), 1))

    val result = testHarness.getOutput

    verify(expectedOutput, result)

    testHarness.close()
  }

  @Test
  def testMinMaxAggFunctionWithRetract(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(
      env, EnvironmentSettings.newInstance().useOldPlanner().build())

    val data = new mutable.MutableList[(JInt, JInt, String)]
    val t = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("T", t)
    val sqlQuery = tEnv.sqlQuery(
      s"""
         |SELECT
         |  c, min(a), max(b)
         |FROM (
         |  SELECT a, b, c
         |  FROM T
         |  GROUP BY a, b, c
         |) GROUP BY c
         |""".stripMargin)

    val testHarness = createHarnessTester[String, CRow, CRow](
      sqlQuery.toRetractStream[Row], "groupBy")

    testHarness.setStateBackend(getStateBackend)
    testHarness.open()

    val operator = getOperator(testHarness)
    val minState = getState(
      operator,
      "function",
      classOf[GroupAggProcessFunction[Row]],
      "acc0_map_dataview").asInstanceOf[MapView[JInt, JInt]]
    val maxState = getState(
      operator,
      "function",
      classOf[GroupAggProcessFunction[Row]],
      "acc1_map_dataview").asInstanceOf[MapView[JInt, JInt]]
    assertTrue(minState.isInstanceOf[StateMapView[_, _]])
    assertTrue(maxState.isInstanceOf[StateMapView[_, _]])
    assertTrue(operator.getKeyedStateBackend.isInstanceOf[RocksDBKeyedStateBackend[_]])

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    testHarness.processElement(new StreamRecord(CRow(1: JInt, 1: JInt, "aaa"), 1))
    expectedOutput.add(new StreamRecord(CRow("aaa", 1, 1), 1))

    testHarness.processElement(new StreamRecord(CRow(1: JInt, 1: JInt, "bbb"), 1))
    expectedOutput.add(new StreamRecord(CRow("bbb", 1, 1), 1))

    // min/max doesn't change
    testHarness.processElement(new StreamRecord(CRow(2: JInt, 0: JInt, "aaa"), 1))

    // min/max changed
    testHarness.processElement(new StreamRecord(CRow(0: JInt, 2: JInt, "aaa"), 1))
    expectedOutput.add(new StreamRecord(CRow(false, "aaa", 1, 1), 1))
    expectedOutput.add(new StreamRecord(CRow("aaa", 0, 2), 1))

    // retract the min/max value
    testHarness.processElement(new StreamRecord(CRow(false, 0: JInt, 2: JInt, "aaa"), 1))
    expectedOutput.add(new StreamRecord(CRow(false, "aaa", 0, 2), 1))
    expectedOutput.add(new StreamRecord(CRow("aaa", 1, 1), 1))

    // remove some state: state may be cleaned up by the state backend
    // if not accessed beyond ttl time
    operator.setCurrentKey(Row.of("aaa"))
    minState.remove(1)
    maxState.remove(1)

    // retract after state has been cleaned up
    testHarness.processElement(new StreamRecord(CRow(false, 2: JInt, 0: JInt, "aaa"), 1))

    testHarness.processElement(new StreamRecord(CRow(false, 1: JInt, 1: JInt, "aaa"), 1))
    expectedOutput.add(new StreamRecord(CRow(false, "aaa", 1, 1), 1))

    val result = testHarness.getOutput

    verify(expectedOutput, result)

    testHarness.close()
  }
}
