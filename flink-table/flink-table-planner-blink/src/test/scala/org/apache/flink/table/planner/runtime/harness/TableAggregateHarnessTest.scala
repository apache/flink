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

import java.lang.{Integer => JInt}
import java.util.concurrent.ConcurrentLinkedQueue

import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.internal.StreamTableEnvironmentImpl
import org.apache.flink.table.api.{EnvironmentSettings, Types}
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.planner.utils.{Top3WithMapView, Top3WithRetractInput}
import org.apache.flink.table.runtime.util.BaseRowHarnessAssertor
import org.apache.flink.table.runtime.util.StreamRecordUtils.{record, retractRecord}
import org.apache.flink.types.Row
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Test}

import scala.collection.mutable

@RunWith(classOf[Parameterized])
class TableAggregateHarnessTest(mode: StateBackendMode) extends HarnessTestBase(mode) {

  @Before
  override def before(): Unit = {
    super.before()
    val setting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val config = new TestTableConfig
    this.tEnv = StreamTableEnvironmentImpl.create(env, setting, config)
  }

  val data = new mutable.MutableList[(Int, Int)]
  val queryConfig = new TestStreamQueryConfig(Time.seconds(2), Time.seconds(2))

  @Test
  def testTableAggregate(): Unit = {
    val top3 = new Top3WithMapView
    tEnv.registerFunction("top3", top3)
    val source = env.fromCollection(data).toTable(tEnv, 'a, 'b)
    val resultTable = source
      .groupBy('a)
      .flatAggregate(top3('b) as ('b1, 'b2))
      .select('a, 'b1, 'b2)

    val testHarness = createHarnessTester(
      resultTable.toRetractStream[Row](queryConfig), "GroupTableAggregate")
    val assertor = new BaseRowHarnessAssertor(Array(Types.INT, Types.INT, Types.INT))

    testHarness.open()
    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    // register cleanup timer with 3001
    testHarness.setProcessingTime(1)

    // input with two columns: key and value
    testHarness.processElement(record(1: JInt, 1: JInt))
    // output with three columns: key, value, value. The value is in the top3 of the key
    expectedOutput.add(record(1: JInt, 1: JInt, 1: JInt))

    testHarness.processElement(record(1: JInt, 2: JInt))
    expectedOutput.add(retractRecord(1: JInt, 1: JInt, 1: JInt))
    expectedOutput.add(record(1: JInt, 1: JInt, 1: JInt))
    expectedOutput.add(record(1: JInt, 2: JInt, 2: JInt))

    testHarness.processElement(record(1: JInt, 3: JInt))
    expectedOutput.add(retractRecord(1: JInt, 1: JInt, 1: JInt))
    expectedOutput.add(retractRecord(1: JInt, 2: JInt, 2: JInt))
    expectedOutput.add(record(1: JInt, 1: JInt, 1: JInt))
    expectedOutput.add(record(1: JInt, 2: JInt, 2: JInt))
    expectedOutput.add(record(1: JInt, 3: JInt, 3: JInt))

    testHarness.processElement(record(1: JInt, 2: JInt))
    expectedOutput.add(retractRecord(1: JInt, 1: JInt, 1: JInt))
    expectedOutput.add(retractRecord(1: JInt, 2: JInt, 2: JInt))
    expectedOutput.add(retractRecord(1: JInt, 3: JInt, 3: JInt))
    expectedOutput.add(record(1: JInt, 2: JInt, 2: JInt))
    expectedOutput.add(record(1: JInt, 2: JInt, 2: JInt))
    expectedOutput.add(record(1: JInt, 3: JInt, 3: JInt))

    // ingest data with key value of 2
    testHarness.processElement(record(2: JInt, 2: JInt))
    expectedOutput.add(record(2: JInt, 2: JInt, 2: JInt))

    // trigger cleanup timer
    testHarness.setProcessingTime(3002)
    testHarness.processElement(record(1: JInt, 2: JInt))
    expectedOutput.add(record(1: JInt, 2: JInt, 2: JInt))

    val result = testHarness.getOutput
    assertor.assertOutputEqualsSorted("result mismatch", expectedOutput, result)
    testHarness.close()
  }

  @Test
  def testTableAggregateWithRetractInput(): Unit = {
    val top3 = new Top3WithRetractInput
    tEnv.registerFunction("top3", top3)
    val source = env.fromCollection(data).toTable(tEnv, 'a, 'b)
    val resultTable = source
      .groupBy('a)
      .select('b.sum as 'b)
      .flatAggregate(top3('b) as ('b1, 'b2))
      .select('b1, 'b2)

    val testHarness = createHarnessTester(
      resultTable.toRetractStream[Row](queryConfig), "GroupTableAggregate")
    val assertor = new BaseRowHarnessAssertor(Array(Types.INT, Types.INT))

    testHarness.open()
    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    // register cleanup timer with 3001
    testHarness.setProcessingTime(1)

    // input with two columns: key and value
    testHarness.processElement(record(1: JInt))
    // output with three columns: key, value, value. The value is in the top3 of the key
    expectedOutput.add(record(1: JInt, 1: JInt))

    testHarness.processElement(retractRecord(1: JInt))
    expectedOutput.add(retractRecord(1: JInt, 1: JInt))

    testHarness.processElement(record(3: JInt))
    expectedOutput.add(record(3: JInt, 3: JInt))

    testHarness.processElement(record(4: JInt))
    expectedOutput.add(retractRecord(3: JInt, 3: JInt))
    expectedOutput.add(record(3: JInt, 3: JInt))
    expectedOutput.add(record(4: JInt, 4: JInt))

    testHarness.processElement(retractRecord(3: JInt))
    expectedOutput.add(retractRecord(3: JInt, 3: JInt))
    expectedOutput.add(retractRecord(4: JInt, 4: JInt))
    expectedOutput.add(record(4: JInt, 4: JInt))

    testHarness.processElement(record(5: JInt))
    expectedOutput.add(retractRecord(4: JInt, 4: JInt))
    expectedOutput.add(record(4: JInt, 4: JInt))
    expectedOutput.add(record(5: JInt, 5: JInt))

    val result = testHarness.getOutput
    assertor.assertOutputEqualsSorted("result mismatch", expectedOutput, result)
    testHarness.close()
  }
}
