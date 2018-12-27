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

import java.lang.{Integer => JInt, Long => JLong}
import java.util.concurrent.ConcurrentLinkedQueue

import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.plan.nodes.datastream.DataStreamRel
import org.apache.flink.table.runtime.harness.HarnessTestBase._
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.table.utils.{Top3WithRetractInput, TopN}
import org.junit.Test

import scala.collection.mutable

class TableAggregateHarnessTest extends HarnessTestBase {

  protected var queryConfig =
    new TestStreamQueryConfig(Time.seconds(2), Time.seconds(3))
  val data = new mutable.MutableList[(Int, Long, String)]

  @Test
  def testTableAggregateWithoutEmitRetract(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val top3 = new TopN(3)
    tEnv.registerFunction("top3", top3)
    val source = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c)
    val resultTable = source.groupBy('a)
      .flatAggregate(top3('a, 'b))
      .select('f0 as 'catgory, 'f1 as 'v, 'f2 as 'rank)

    val ds = tEnv.optimize(resultTable.getRelNode, false)
      .asInstanceOf[DataStreamRel]
      .translateToPlan(tEnv, queryConfig)

    val testHarness = createHarnessTester[Int, CRow, CRow](ds, "TableAggregate")

    testHarness.open()

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    // register cleanup timer with 3001
    testHarness.setProcessingTime(1)

    // input with three columns: first parameter, second parameter and the group key
    testHarness.processElement(new StreamRecord(CRow(1: JInt, 1L: JLong, 1: JInt), 1))
    // output with three columns: category, value, rank.
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 1L: JLong, 0: JInt), 1))

    testHarness.processElement(new StreamRecord(CRow(1: JInt, 2L: JLong, 1: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 1L: JLong, 1: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 2L: JLong, 0: JInt), 1))

    testHarness.processElement(new StreamRecord(CRow(1: JInt, 3L: JLong, 1: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 1L: JLong, 2: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 2L: JLong, 1: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 3L: JLong, 0: JInt), 1))

    testHarness.processElement(new StreamRecord(CRow(1: JInt, 1L: JLong, 1: JInt), 1))
    // no output

    // trigger cleanup timer
    testHarness.setProcessingTime(3002)
    testHarness.processElement(new StreamRecord(CRow(1: JInt, 1L: JLong, 1: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 1L: JLong, 0: JInt), 1))

    val result = testHarness.getOutput

    verify(expectedOutput, result)
    testHarness.close()
  }

  @Test
  def testTableAggregateWithEmitRetract(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val top3 = new TopN(3)
    tEnv.registerFunction("top3", top3)
    val source = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c)
    val resultTable = source.groupBy('a)
      .flatAggregate(top3('a, 'b))
      .select('f0 as 'catgory, 'f1 as 'v, 'f2 as 'rank)

    val ds = tEnv.optimize(resultTable.getRelNode, true)
      .asInstanceOf[DataStreamRel]
      .translateToPlan(tEnv, queryConfig)

    val testHarness = createHarnessTester[Int, CRow, CRow](ds, "TableAggregate")

    testHarness.open()

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    // register cleanup timer with 3001
    testHarness.setProcessingTime(1)

    // input with three columns: first parameter, second parameter and the group key
    testHarness.processElement(new StreamRecord(CRow(1: JInt, 1L: JLong, 1: JInt), 1))
    // output with three columns: category, value, rank.
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 1L: JLong, 0: JInt), 1))

    testHarness.processElement(new StreamRecord(CRow(1: JInt, 2L: JLong, 1: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(false, 1: JInt, 1L: JLong, 0: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 1L: JLong, 1: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 2L: JLong, 0: JInt), 1))

    testHarness.processElement(new StreamRecord(CRow(1: JInt, 3L: JLong, 1: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(false, 1: JInt, 1L: JLong, 1: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(false, 1: JInt, 2L: JLong, 0: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 1L: JLong, 2: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 2L: JLong, 1: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 3L: JLong, 0: JInt), 1))

    testHarness.processElement(new StreamRecord(CRow(1: JInt, 1L: JLong, 1: JInt), 1))
    // no output

    // trigger cleanup timer
    testHarness.setProcessingTime(3002)
    testHarness.processElement(new StreamRecord(CRow(1: JInt, 1L: JLong, 1: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 1L: JLong, 0: JInt), 1))

    val result = testHarness.getOutput

    verify(expectedOutput, result)
    testHarness.close()
  }

  @Test
  def testTableAggregateWithRetractInput(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val top3 = new Top3WithRetractInput
    tEnv.registerFunction("top3", top3)
    val source = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c)
    val resultTable = source
      .groupBy('a)
      .select('a, 'b.count as 'b)
      .groupBy('a)
      .flatAggregate(top3('a, 'b))
      .select('f0 as 'catgory, 'f1 as 'v, 'f2 as 'rank)

    val ds = tEnv.optimize(resultTable.getRelNode, false)
      .asInstanceOf[DataStreamRel]
      .translateToPlan(tEnv, queryConfig)

    val testHarness = createHarnessTester[Int, CRow, CRow](ds, "TableAggregate")
    testHarness.open()

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    // register cleanup timer with 3001
    testHarness.setProcessingTime(1)

    // input with three columns: first parameter, second parameter and the group key
    testHarness.processElement(new StreamRecord(CRow(1: JInt, 1L: JLong, 1: JInt), 1))
    // output with three columns: category, value, rank.
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 1L: JLong, 0: JInt), 1))

    testHarness.processElement(new StreamRecord(CRow(1: JInt, 2L: JLong, 1: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 1L: JLong, 1: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 2L: JLong, 0: JInt), 1))

    testHarness.processElement(new StreamRecord(CRow(1: JInt, 3L: JLong, 1: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 1L: JLong, 2: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 2L: JLong, 1: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 3L: JLong, 0: JInt), 1))

    testHarness.processElement(new StreamRecord(CRow(false, 1: JInt, 3L: JLong, 1: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 1L: JLong, 1: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 2L: JLong, 0: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(false, 1: JInt, 1L: JLong, 2: JInt), 1))

    // trigger cleanup timer
    testHarness.setProcessingTime(3002)
    testHarness.processElement(new StreamRecord(CRow(1: JInt, 1L: JLong, 1: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 1L: JLong, 0: JInt), 1))

    val result = testHarness.getOutput

    verify(expectedOutput, result)
    testHarness.close()
  }
}
