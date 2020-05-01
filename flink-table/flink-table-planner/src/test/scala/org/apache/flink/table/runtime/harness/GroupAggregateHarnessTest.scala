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

import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.operators.KeyedProcessOperator
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.internal.StreamTableEnvironmentImpl
import org.apache.flink.table.runtime.aggregate._
import org.apache.flink.table.runtime.harness.HarnessTestBase._
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.table.runtime.utils.JavaUserDefinedAggFunctions.{MultiArgCount, MultiArgSum}
import org.apache.flink.types.Row

import org.junit.Assert.assertTrue
import org.junit.Test

import java.lang.{Integer => JInt, Long => JLong}
import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.mutable

class GroupAggregateHarnessTest extends HarnessTestBase {

  private val config = new TestTableConfig
  config.setIdleStateRetentionTime(Time.seconds(2), Time.seconds(3))

  @Test
  def testAggregate(): Unit = {

    val processFunction = new KeyedProcessOperator[String, CRow, CRow](
      new GroupAggProcessFunction[String](
        genSumAggFunction,
        sumAggregationStateType,
        false,
        config.getMinIdleStateRetentionTime,
        config.getMaxIdleStateRetentionTime))

    val testHarness =
      createHarnessTester(
        processFunction,
        new TupleRowKeySelector[String](2),
        BasicTypeInfo.STRING_TYPE_INFO)

    testHarness.open()

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    // register cleanup timer with 3001
    testHarness.setProcessingTime(1)

    testHarness.processElement(new StreamRecord(CRow(1L: JLong, 1: JInt, "aaa"), 1))
    expectedOutput.add(new StreamRecord(CRow(1L: JLong, 1: JInt), 1))
    testHarness.processElement(new StreamRecord(CRow(2L: JLong, 1: JInt, "bbb"), 1))
    expectedOutput.add(new StreamRecord(CRow(2L: JLong, 1: JInt), 1))
    // reuse timer 3001
    testHarness.setProcessingTime(1000)
    testHarness.processElement(new StreamRecord(CRow(3L: JLong, 2: JInt, "aaa"), 1))
    expectedOutput.add(new StreamRecord(CRow(3L: JLong, 3: JInt), 1))
    testHarness.processElement(new StreamRecord(CRow(4L: JLong, 3: JInt, "aaa"), 1))
    expectedOutput.add(new StreamRecord(CRow(4L: JLong, 6: JInt), 1))

    // register cleanup timer with 4002
    testHarness.setProcessingTime(1002)
    testHarness.processElement(new StreamRecord(CRow(5L: JLong, 4: JInt, "aaa"), 1))
    expectedOutput.add(new StreamRecord(CRow(5L: JLong, 10: JInt), 1))
    testHarness.processElement(new StreamRecord(CRow(6L: JLong, 2: JInt, "bbb"), 1))
    expectedOutput.add(new StreamRecord(CRow(6L: JLong, 3: JInt), 1))

    // trigger cleanup timer and register cleanup timer with 7003
    testHarness.setProcessingTime(4003)
    testHarness.processElement(new StreamRecord(CRow(7L: JLong, 5: JInt, "aaa"), 1))
    expectedOutput.add(new StreamRecord(CRow(7L: JLong, 5: JInt), 1))
    testHarness.processElement(new StreamRecord(CRow(8L: JLong, 6: JInt, "aaa"), 1))
    expectedOutput.add(new StreamRecord(CRow(8L: JLong, 11: JInt), 1))
    testHarness.processElement(new StreamRecord(CRow(9L: JLong, 7: JInt, "aaa"), 1))
    expectedOutput.add(new StreamRecord(CRow(9L: JLong, 18: JInt), 1))
    testHarness.processElement(new StreamRecord(CRow(10L: JLong, 3: JInt, "bbb"), 1))
    expectedOutput.add(new StreamRecord(CRow(10L: JLong, 3: JInt), 1))

    val result = testHarness.getOutput

    verify(expectedOutput, result)

    testHarness.close()
  }

  @Test
  def testAggregateWithRetract(): Unit = {

    val processFunction = new KeyedProcessOperator[String, CRow, CRow](
      new GroupAggProcessFunction[String](
        genSumAggFunction,
        sumAggregationStateType,
        true,
        config.getMinIdleStateRetentionTime,
        config.getMaxIdleStateRetentionTime))

    val testHarness =
      createHarnessTester(
        processFunction,
        new TupleRowKeySelector[String](2),
        BasicTypeInfo.STRING_TYPE_INFO)

    testHarness.open()

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    // register cleanup timer with 3001
    testHarness.setProcessingTime(1)

    // accumulate
    testHarness.processElement(new StreamRecord(CRow(1L: JLong, 1: JInt, "aaa"), 1))
    expectedOutput.add(new StreamRecord(CRow(1L: JLong, 1: JInt), 1))

    // accumulate
    testHarness.processElement(new StreamRecord(CRow(2L: JLong, 1: JInt, "bbb"), 2))
    expectedOutput.add(new StreamRecord(CRow(2L: JLong, 1: JInt), 2))

    // retract for insertion
    testHarness.processElement(new StreamRecord(CRow(3L: JLong, 2: JInt, "aaa"), 3))
    expectedOutput.add(new StreamRecord(CRow(false, 3L: JLong, 1: JInt), 3))
    expectedOutput.add(new StreamRecord(CRow(3L: JLong, 3: JInt), 3))

    // retract for deletion
    testHarness.processElement(new StreamRecord(CRow(false, 3L: JLong, 2: JInt, "aaa"), 3))
    expectedOutput.add(new StreamRecord(CRow(false, 3L: JLong, 3: JInt), 3))
    expectedOutput.add(new StreamRecord(CRow(3L: JLong, 1: JInt), 3))

    // accumulate
    testHarness.processElement(new StreamRecord(CRow(4L: JLong, 3: JInt, "ccc"), 4))
    expectedOutput.add(new StreamRecord(CRow(4L: JLong, 3: JInt), 4))

    // trigger cleanup timer and register cleanup timer with 6002
    testHarness.setProcessingTime(3002)

    // retract after clean up
    testHarness.processElement(new StreamRecord(CRow(false, 4L: JLong, 3: JInt, "ccc"), 4))

    // accumulate
    testHarness.processElement(new StreamRecord(CRow(5L: JLong, 4: JInt, "aaa"), 5))
    expectedOutput.add(new StreamRecord(CRow(5L: JLong, 4: JInt), 5))
    testHarness.processElement(new StreamRecord(CRow(6L: JLong, 2: JInt, "bbb"), 6))
    expectedOutput.add(new StreamRecord(CRow(6L: JLong, 2: JInt), 6))

    // retract
    testHarness.processElement(new StreamRecord(CRow(7L: JLong, 5: JInt, "aaa"), 7))
    expectedOutput.add(new StreamRecord(CRow(false, 7L: JLong, 4: JInt), 7))
    expectedOutput.add(new StreamRecord(CRow(7L: JLong, 9: JInt), 7))

    // accumulate
    testHarness.processElement(new StreamRecord(CRow(8L: JLong, 6: JInt, "eee"), 8))
    expectedOutput.add(new StreamRecord(CRow(8L: JLong, 6: JInt), 8))

    // retract
    testHarness.processElement(new StreamRecord(CRow(9L: JLong, 7: JInt, "aaa"), 9))
    expectedOutput.add(new StreamRecord(CRow(false, 9L: JLong, 9: JInt), 9))
    expectedOutput.add(new StreamRecord(CRow(9L: JLong, 16: JInt), 9))
    testHarness.processElement(new StreamRecord(CRow(10L: JLong, 3: JInt, "bbb"), 10))
    expectedOutput.add(new StreamRecord(CRow(false, 10L: JLong, 2: JInt), 10))
    expectedOutput.add(new StreamRecord(CRow(10L: JLong, 5: JInt), 10))

    val result = testHarness.getOutput

    verify(expectedOutput, result)

    testHarness.close()
  }

  @Test
  def testDistinctAggregateWithRetract(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironmentImpl.create(
      env,
      EnvironmentSettings.newInstance().useOldPlanner().build(),
      config)

    val data = new mutable.MutableList[(JLong, JInt)]
    val t = env.fromCollection(data).toTable(tEnv, 'a, 'b)
    tEnv.registerTable("T", t)
    val sqlQuery = tEnv.sqlQuery(
      s"""
         |SELECT
         |  a, count(distinct b), sum(distinct b)
         |FROM (
         |  SELECT a, b
         |  FROM T
         |  GROUP BY a, b
         |) GROUP BY a
         |""".stripMargin)

    val testHarness = createHarnessTester[String, CRow, CRow](
      sqlQuery.toRetractStream[Row], "groupBy")

    testHarness.setStateBackend(getStateBackend)
    testHarness.open()

    val operator = getOperator(testHarness)
    val fields = getGeneratedAggregationFields(
      operator,
      "function",
      classOf[GroupAggProcessFunction[Row]])

    // check only one DistinctAccumulator is used
    assertTrue(fields.count(_.getName.endsWith("distinctValueMap_dataview")) == 1)

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    // register cleanup timer with 3001
    testHarness.setProcessingTime(1)

    // insert
    testHarness.processElement(new StreamRecord(CRow(1L: JLong, 1: JInt)))
    expectedOutput.add(new StreamRecord(CRow(1L: JLong, 1L: JLong, 1: JInt)))
    testHarness.processElement(new StreamRecord(CRow(2L: JLong, 1: JInt)))
    expectedOutput.add(new StreamRecord(CRow(2L: JLong, 1L: JLong, 1: JInt)))

    // distinct count retract then accumulate for downstream operators
    testHarness.processElement(new StreamRecord(CRow(2L: JLong, 1: JInt)))
    expectedOutput.add(new StreamRecord(CRow(false, 2L: JLong, 1L: JLong, 1: JInt)))
    expectedOutput.add(new StreamRecord(CRow(2L: JLong, 1L: JLong, 1: JInt)))

    // update count for accumulate
    testHarness.processElement(new StreamRecord(CRow(1L: JLong, 2: JInt)))
    expectedOutput.add(new StreamRecord(CRow(false, 1L: JLong, 1L: JLong, 1: JInt)))
    expectedOutput.add(new StreamRecord(CRow(1L: JLong, 2L: JLong, 3: JInt)))

    // update count for retraction
    testHarness.processElement(new StreamRecord(CRow(false, 1L: JLong, 2: JInt)))
    expectedOutput.add(new StreamRecord(CRow(false, 1L: JLong, 2L: JLong, 3: JInt)))
    expectedOutput.add(new StreamRecord(CRow(1L: JLong, 1L: JLong, 1: JInt)))

    // insert
    testHarness.processElement(new StreamRecord(CRow(4L: JLong, 3: JInt)))
    expectedOutput.add(new StreamRecord(CRow(4L: JLong, 1L: JLong, 3: JInt)))

    // retract entirely
    testHarness.processElement(new StreamRecord(CRow(false, 4L: JLong, 3: JInt)))
    expectedOutput.add(new StreamRecord(CRow(false, 4L: JLong, 1L: JLong, 3: JInt)))

    // trigger cleanup timer and register cleanup timer with 6002
    testHarness.setProcessingTime(3002)

    // insert
    testHarness.processElement(new StreamRecord(CRow(1L: JLong, 1: JInt)))
    expectedOutput.add(new StreamRecord(CRow(1L: JLong, 1L: JLong, 1: JInt)))

    // trigger cleanup timer and register cleanup timer with 9002
    testHarness.setProcessingTime(6002)

    // retract after cleanup
    testHarness.processElement(new StreamRecord(CRow(false, 1L: JLong, 1: JInt, 1L: JLong)))

    val result = testHarness.getOutput

    verify(expectedOutput, result)

    testHarness.close()
  }

  @Test
  def testDistinctAggregateWithDifferentArgumentOrder(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironmentImpl.create(
      env,
      EnvironmentSettings.newInstance().useOldPlanner().build(),
      config)

    val data = new mutable.MutableList[(JLong, JLong, JLong)]
    val t = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("T", t)
    tEnv.registerFunction("myCount", new MultiArgCount)
    tEnv.registerFunction("mySum", new MultiArgSum)
    val sqlQuery = tEnv.sqlQuery(
      s"""
         |SELECT
         |  a, myCount(distinct b, c), mySum(distinct c, b)
         |FROM (
         |  SELECT a, b, c
         |  FROM T
         |  GROUP BY a, b, c
         |) GROUP BY a
         |""".stripMargin)

    val testHarness = createHarnessTester[String, CRow, CRow](
      sqlQuery.toRetractStream[Row], "groupBy")

    testHarness.setStateBackend(getStateBackend)
    testHarness.open()

    val operator = getOperator(testHarness)
    val fields = getGeneratedAggregationFields(
      operator,
      "function",
      classOf[GroupAggProcessFunction[Row]])

    // check only one DistinctAccumulator is used
    assertTrue(fields.count(_.getName.endsWith("distinctValueMap_dataview")) == 1)

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    // register cleanup timer with 3001
    testHarness.setProcessingTime(1)

    // insert
    testHarness.processElement(new StreamRecord(CRow(1L: JLong, 1L: JLong, 1L: JLong)))
    expectedOutput.add(new StreamRecord(CRow(1L: JLong, 1L: JLong, 2L: JLong)))
    testHarness.processElement(new StreamRecord(CRow(2L: JLong, 1L: JLong, 1L: JLong)))
    expectedOutput.add(new StreamRecord(CRow(2L: JLong, 1L: JLong, 2L: JLong)))

    // distinct count retract then accumulate for downstream operators
    testHarness.processElement(new StreamRecord(CRow(2L: JLong, 1L: JLong, 1L: JLong)))
    expectedOutput.add(new StreamRecord(CRow(false, 2L: JLong, 1L: JLong, 2L: JLong)))
    expectedOutput.add(new StreamRecord(CRow(2L: JLong, 1L: JLong, 2L: JLong)))

    // update count for accumulate
    testHarness.processElement(new StreamRecord(CRow(1L: JLong, 2L: JLong, 3L: JLong)))
    expectedOutput.add(new StreamRecord(CRow(false, 1L: JLong, 1L: JLong, 2L: JLong)))
    expectedOutput.add(new StreamRecord(CRow(1L: JLong, 2L: JLong, 7L: JLong)))

    testHarness.processElement(new StreamRecord(CRow(1L: JLong, 2L: JLong, 3L: JLong)))
    expectedOutput.add(new StreamRecord(CRow(false, 1L: JLong, 2L: JLong, 7L: JLong)))
    expectedOutput.add(new StreamRecord(CRow(1L: JLong, 2L: JLong, 7L: JLong)))

    // update count for retraction
    testHarness.processElement(new StreamRecord(CRow(false, 1L: JLong, 2L: JLong, 3L: JLong)))
    expectedOutput.add(new StreamRecord(CRow(false, 1L: JLong, 2L: JLong, 7L: JLong)))
    expectedOutput.add(new StreamRecord(CRow(1L: JLong, 2L: JLong, 7L: JLong)))

    testHarness.processElement(new StreamRecord(CRow(false, 1L: JLong, 2L: JLong, 3L: JLong)))
    expectedOutput.add(new StreamRecord(CRow(false, 1L: JLong, 2L: JLong, 7L: JLong)))
    expectedOutput.add(new StreamRecord(CRow(1L: JLong, 1L: JLong, 2L: JLong)))

    // insert
    testHarness.processElement(new StreamRecord(CRow(4L: JLong, 3L: JLong, 3L: JLong)))
    expectedOutput.add(new StreamRecord(CRow(4L: JLong, 1L: JLong, 6L: JLong)))

    // retract entirely
    testHarness.processElement(new StreamRecord(CRow(false, 4L: JLong, 3L: JLong, 3L: JLong)))
    expectedOutput.add(new StreamRecord(CRow(false, 4L: JLong, 1L: JLong, 6L: JLong)))

    // trigger cleanup timer and register cleanup timer with 6002
    testHarness.setProcessingTime(3002)

    // insert
    testHarness.processElement(new StreamRecord(CRow(1L: JLong, 1L: JLong, 2L: JLong)))
    expectedOutput.add(new StreamRecord(CRow(1L: JLong, 1L: JLong, 3L: JLong)))

    // trigger cleanup timer and register cleanup timer with 9002
    testHarness.setProcessingTime(6002)

    // retract after cleanup
    testHarness.processElement(new StreamRecord(CRow(false, 1L: JLong, 1: JInt, 2L: JLong)))

    val result = testHarness.getOutput

    verify(expectedOutput, result)

    testHarness.close()
  }
}
