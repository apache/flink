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
import org.apache.flink.table.api.{EnvironmentSettings, _}
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.planner.utils.{Top3WithMapView, Top3WithRetractInput}
import org.apache.flink.table.runtime.typeutils.RowDataSerializer
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor
import org.apache.flink.table.runtime.util.StreamRecordUtils.{deleteRecord, insertRecord}
import org.apache.flink.table.types.logical.LogicalType
import org.apache.flink.types.Row

import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Test}

import java.lang.{Integer => JInt}
import java.time.Duration
import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.mutable

@RunWith(classOf[Parameterized])
class TableAggregateHarnessTest(mode: StateBackendMode) extends HarnessTestBase(mode) {

  @Before
  override def before(): Unit = {
    super.before()
    val setting = EnvironmentSettings.newInstance().inStreamingMode().build()
    this.tEnv = StreamTableEnvironmentImpl.create(env, setting)
  }

  val data = new mutable.MutableList[(Int, Int)]

  @Test
  def testTableAggregate(): Unit = {
    val top3 = new Top3WithMapView
    tEnv.registerFunction("top3", top3)
    val source = env.fromCollection(data).toTable(tEnv, 'a, 'b)
    val resultTable = source
      .groupBy('a)
      .flatAggregate(top3('b) as ('b1, 'b2))
      .select('a, 'b1, 'b2)

    tEnv.getConfig.setIdleStateRetention(Duration.ofSeconds(2))
    val testHarness = createHarnessTester(
      resultTable.toRetractStream[Row], "GroupTableAggregate")
    val assertor = new RowDataHarnessAssertor(
      Array(
        DataTypes.INT().getLogicalType,
        DataTypes.INT().getLogicalType,
        DataTypes.INT().getLogicalType))

    testHarness.open()
    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    // set TtlTimeProvider with 1
    testHarness.setStateTtlProcessingTime(1)

    // input with two columns: key and value
    testHarness.processElement(insertRecord(1: JInt, 1: JInt))
    // output with three columns: key, value, value. The value is in the top3 of the key
    expectedOutput.add(insertRecord(1: JInt, 1: JInt, 1: JInt))

    testHarness.processElement(insertRecord(1: JInt, 2: JInt))
    expectedOutput.add(deleteRecord(1: JInt, 1: JInt, 1: JInt))
    expectedOutput.add(insertRecord(1: JInt, 1: JInt, 1: JInt))
    expectedOutput.add(insertRecord(1: JInt, 2: JInt, 2: JInt))

    testHarness.processElement(insertRecord(1: JInt, 3: JInt))
    expectedOutput.add(deleteRecord(1: JInt, 1: JInt, 1: JInt))
    expectedOutput.add(deleteRecord(1: JInt, 2: JInt, 2: JInt))
    expectedOutput.add(insertRecord(1: JInt, 1: JInt, 1: JInt))
    expectedOutput.add(insertRecord(1: JInt, 2: JInt, 2: JInt))
    expectedOutput.add(insertRecord(1: JInt, 3: JInt, 3: JInt))

    testHarness.processElement(insertRecord(1: JInt, 2: JInt))
    expectedOutput.add(deleteRecord(1: JInt, 1: JInt, 1: JInt))
    expectedOutput.add(deleteRecord(1: JInt, 2: JInt, 2: JInt))
    expectedOutput.add(deleteRecord(1: JInt, 3: JInt, 3: JInt))
    expectedOutput.add(insertRecord(1: JInt, 2: JInt, 2: JInt))
    expectedOutput.add(insertRecord(1: JInt, 2: JInt, 2: JInt))
    expectedOutput.add(insertRecord(1: JInt, 3: JInt, 3: JInt))

    // ingest data with key value of 2
    testHarness.processElement(insertRecord(2: JInt, 2: JInt))
    expectedOutput.add(insertRecord(2: JInt, 2: JInt, 2: JInt))

    //set TtlTimeProvider with 3002 to trigger expired state cleanup
    testHarness.setStateTtlProcessingTime(3002)
    testHarness.processElement(insertRecord(1: JInt, 2: JInt))
    expectedOutput.add(insertRecord(1: JInt, 2: JInt, 2: JInt))

    val result = testHarness.getOutput
    assertor.assertOutputEqualsSorted("result mismatch", expectedOutput, result)
    testHarness.close()
  }

  @Test
  def testTableAggregateWithRetractInput(): Unit = {
    val (testHarness, outputTypes) = createTableAggregateWithRetract
    val assertor = new RowDataHarnessAssertor(outputTypes)

    testHarness.open()
    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    // set TtlTimeProvider with 1
    testHarness.setStateTtlProcessingTime(1)

    // input with two columns: key and value
    testHarness.processElement(insertRecord(1: JInt))
    // output with three columns: key, value, value. The value is in the top3 of the key
    expectedOutput.add(insertRecord(1: JInt, 1: JInt))

    testHarness.processElement(deleteRecord(1: JInt))
    expectedOutput.add(deleteRecord(1: JInt, 1: JInt))

    testHarness.processElement(insertRecord(3: JInt))
    expectedOutput.add(insertRecord(3: JInt, 3: JInt))

    testHarness.processElement(insertRecord(4: JInt))
    expectedOutput.add(deleteRecord(3: JInt, 3: JInt))
    expectedOutput.add(insertRecord(3: JInt, 3: JInt))
    expectedOutput.add(insertRecord(4: JInt, 4: JInt))

    testHarness.processElement(deleteRecord(3: JInt))
    expectedOutput.add(deleteRecord(3: JInt, 3: JInt))
    expectedOutput.add(deleteRecord(4: JInt, 4: JInt))
    expectedOutput.add(insertRecord(4: JInt, 4: JInt))

    testHarness.processElement(insertRecord(5: JInt))
    expectedOutput.add(deleteRecord(4: JInt, 4: JInt))
    expectedOutput.add(insertRecord(4: JInt, 4: JInt))
    expectedOutput.add(insertRecord(5: JInt, 5: JInt))

    val result = testHarness.getOutput
    assertor.assertOutputEqualsSorted("result mismatch", expectedOutput, result)
    testHarness.close()
  }

  private def createTableAggregateWithRetract()
    : (KeyedOneInputStreamOperatorTestHarness[RowData, RowData, RowData], Array[LogicalType]) = {
    val top3 = new Top3WithRetractInput
    tEnv.registerFunction("top3", top3)
    val source = env.fromCollection(data).toTable(tEnv, 'a, 'b)
    val resultTable = source
        .groupBy('a)
        .select('b.sum as 'b)
        .flatAggregate(top3('b) as('b1, 'b2))
        .select('b1, 'b2)

    tEnv.getConfig.setIdleStateRetention(Duration.ofSeconds(2))
    val testHarness = createHarnessTester(
      resultTable.toRetractStream[Row], "GroupTableAggregate")
    val outputTypes = Array(
      DataTypes.INT().getLogicalType,
      DataTypes.INT().getLogicalType)
    (testHarness, outputTypes)
  }

  @Test
  def testCloseWithoutOpen(): Unit = {
    val (testHarness, outputTypes) = createTableAggregateWithRetract
    testHarness.setup(new RowDataSerializer(outputTypes: _*))
    // simulate a failover after a failed task open, expect no exception happens
    testHarness.close()
  }
}
