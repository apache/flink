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
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.bridge.scala.internal.StreamTableEnvironmentImpl
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.table.utils.{Top3WithEmitRetractValue, Top3WithMapView}
import org.apache.flink.types.Row

import org.junit.Test

import java.lang.{Integer => JInt}
import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.mutable

class TableAggregateHarnessTest extends HarnessTestBase {

  private val tableConfig = new TableConfig {
    override def getMinIdleStateRetentionTime: Long = Time.seconds(2).toMilliseconds

    override def getMaxIdleStateRetentionTime: Long = Time.seconds(2).toMilliseconds
  }
  val data = new mutable.MutableList[(Int, Int)]

  @Test
  def testTableAggregate(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironmentImpl.create(
      env,
      EnvironmentSettings.newInstance().useOldPlanner().build(),
      tableConfig)

    val top3 = new Top3WithMapView
    tEnv.registerFunction("top3", top3)
    val source = env.fromCollection(data).toTable(tEnv, 'a, 'b)
    val resultTable = source
      .groupBy('a)
      .flatAggregate(top3('b) as ('b1, 'b2))
      .select('a, 'b1, 'b2)

    val testHarness = createHarnessTester[Int, CRow, CRow](
      resultTable.toRetractStream[Row], "groupBy: (a)")

    testHarness.open()

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    // register cleanup timer with 3001
    testHarness.setProcessingTime(1)

    // input with two columns: key and value
    testHarness.processElement(new StreamRecord(CRow(1: JInt, 1: JInt), 1))
    // output with three columns: key, value, value. The value is in the top3 of the key
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 1: JInt, 1: JInt), 1))

    testHarness.processElement(new StreamRecord(CRow(1: JInt, 2: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(false, 1: JInt, 1: JInt, 1: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 1: JInt, 1: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 2: JInt, 2: JInt), 1))

    testHarness.processElement(new StreamRecord(CRow(1: JInt, 3: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(false, 1: JInt, 1: JInt, 1: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(false, 1: JInt, 2: JInt, 2: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 1: JInt, 1: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 2: JInt, 2: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 3: JInt, 3: JInt), 1))

    testHarness.processElement(new StreamRecord(CRow(1: JInt, 2: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(false, 1: JInt, 1: JInt, 1: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(false, 1: JInt, 2: JInt, 2: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(false, 1: JInt, 3: JInt, 3: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 2: JInt, 2: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 2: JInt, 2: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 3: JInt, 3: JInt), 1))

    // ingest data with key value of 2
    testHarness.processElement(new StreamRecord(CRow(2: JInt, 2: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(2: JInt, 2: JInt, 2: JInt), 1))

    // trigger cleanup timer
    testHarness.setProcessingTime(3002)
    testHarness.processElement(new StreamRecord(CRow(1: JInt, 2: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 2: JInt, 2: JInt), 1))

    val result = testHarness.getOutput

    verify(expectedOutput, result)
    testHarness.close()
  }

  @Test
  def testTableAggregateEmitRetractValueIncrementally(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironmentImpl.create(
      env,
      EnvironmentSettings.newInstance().useOldPlanner().build(),
      tableConfig)

    val top3 = new Top3WithEmitRetractValue
    val source = env.fromCollection(data).toTable(tEnv, 'a, 'b)
    val resultTable = source
      .groupBy('a)
      .flatAggregate(top3('b) as ('b1, 'b2))
      .select('a, 'b1, 'b2)

    val testHarness = createHarnessTester[Int, CRow, CRow](
      resultTable.toRetractStream[Row], "groupBy: (a)")

    testHarness.open()

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    // register cleanup timer with 3001
    testHarness.setProcessingTime(1)

    // input with two columns: key and value
    testHarness.processElement(new StreamRecord(CRow(1: JInt, 1: JInt), 1))
    // output with three columns: key, value, value. The value is in the top3 of the key
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 1: JInt, 1: JInt), 1))

    testHarness.processElement(new StreamRecord(CRow(1: JInt, 2: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 2: JInt, 2: JInt), 1))

    testHarness.processElement(new StreamRecord(CRow(1: JInt, 3: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 3: JInt, 3: JInt), 1))

    testHarness.processElement(new StreamRecord(CRow(1: JInt, 2: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(false, 1: JInt, 1: JInt, 1: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 2: JInt, 2: JInt), 1))

    // ingest data with key value of 2
    testHarness.processElement(new StreamRecord(CRow(2: JInt, 2: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(2: JInt, 2: JInt, 2: JInt), 1))

    // trigger cleanup timer
    testHarness.setProcessingTime(3002)
    testHarness.processElement(new StreamRecord(CRow(1: JInt, 2: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 2: JInt, 2: JInt), 1))

    val result = testHarness.getOutput

    verify(expectedOutput, result)
    testHarness.close()
  }

  @Test
  def testTableAggregateWithRetractInput(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironmentImpl.create(
      env,
      EnvironmentSettings.newInstance().useOldPlanner().build(),
      tableConfig)

    val top3 = new Top3WithMapView
    tEnv.registerFunction("top3", top3)
    val source = env.fromCollection(data).toTable(tEnv, 'a, 'b)
    val resultTable = source
      .groupBy('a)
      .select('b.sum as 'b)
      .flatAggregate(top3('b) as ('b1, 'b2))
      .select('b1, 'b2)

    val testHarness = createHarnessTester[Int, CRow, CRow](
      resultTable.toRetractStream[Row], "select: (Top3WithMapView")

    testHarness.open()

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    // register cleanup timer with 3001
    testHarness.setProcessingTime(1)

    // input with two columns: key and value
    testHarness.processElement(new StreamRecord(CRow(1: JInt), 1))
    // output with three columns: key, value, value. The value is in the top3 of the key
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 1: JInt), 1))

    testHarness.processElement(new StreamRecord(CRow(false, 1: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(false, 1: JInt, 1: JInt), 1))

    testHarness.processElement(new StreamRecord(CRow(3: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(3: JInt, 3: JInt), 1))

    testHarness.processElement(new StreamRecord(CRow(4: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(false, 3: JInt, 3: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(3: JInt, 3: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(4: JInt, 4: JInt), 1))

    testHarness.processElement(new StreamRecord(CRow(false, 3: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(false, 3: JInt, 3: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(false, 4: JInt, 4: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(4: JInt, 4: JInt), 1))

    testHarness.processElement(new StreamRecord(CRow(5: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(false, 4: JInt, 4: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(4: JInt, 4: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(5: JInt, 5: JInt), 1))

    val result = testHarness.getOutput

    verify(expectedOutput, result)
    testHarness.close()
  }
}
