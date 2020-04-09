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

import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.internal.StreamTableEnvironmentImpl
import org.apache.flink.table.api.{EnvironmentSettings, Types}
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.runtime.util.BaseRowHarnessAssertor
import org.apache.flink.table.runtime.util.StreamRecordUtils.{binaryrow, retractBinaryRow}
import org.apache.flink.types.Row
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Test}

import java.lang.{Long => JLong}
import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.mutable

@RunWith(classOf[Parameterized])
class GroupAggregateHarnessTest(mode: StateBackendMode) extends HarnessTestBase(mode) {

  @Before
  override def before(): Unit = {
    super.before()
    val setting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val config = new TestTableConfig
    this.tEnv = StreamTableEnvironmentImpl.create(env, setting, config)
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

    val queryConfig = new TestStreamQueryConfig(Time.seconds(2), Time.seconds(3))
    val testHarness = createHarnessTester(t1.toRetractStream[Row](queryConfig), "GroupAggregate")
    val assertor = new BaseRowHarnessAssertor(Array( Types.STRING, Types.LONG))

    testHarness.open()

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    // register cleanup timer with 3001
    testHarness.setProcessingTime(1)

    // accumulate
    testHarness.processElement(new StreamRecord(binaryrow("aaa", 1L: JLong), 1))
    expectedOutput.add(new StreamRecord(binaryrow("aaa", 1L: JLong), 1))

    // accumulate
    testHarness.processElement(new StreamRecord(binaryrow("bbb", 1L: JLong), 2))
    expectedOutput.add(new StreamRecord(binaryrow("bbb", 1L: JLong), 2))

    // retract for insertion
    testHarness.processElement(new StreamRecord(binaryrow("aaa", 2L: JLong), 3))
    expectedOutput.add(new StreamRecord(retractBinaryRow( "aaa", 1L: JLong), 3))
    expectedOutput.add(new StreamRecord(binaryrow("aaa", 3L: JLong), 3))

    // retract for deletion
    testHarness.processElement(new StreamRecord(retractBinaryRow("aaa", 2L: JLong), 3))
    expectedOutput.add(new StreamRecord(retractBinaryRow("aaa", 3L: JLong), 3))
    expectedOutput.add(new StreamRecord(binaryrow("aaa", 1L: JLong), 3))

    // accumulate
    testHarness.processElement(new StreamRecord(binaryrow("ccc", 3L: JLong), 4))
    expectedOutput.add(new StreamRecord(binaryrow("ccc", 3L: JLong), 4))

    // trigger cleanup timer and register cleanup timer with 6002
    testHarness.setProcessingTime(3002)

    // retract after clean up
    testHarness.processElement(new StreamRecord(retractBinaryRow("ccc", 3L: JLong), 4))
    // not output

    // accumulate
    testHarness.processElement(new StreamRecord(binaryrow("aaa", 4L: JLong), 5))
    expectedOutput.add(new StreamRecord(binaryrow("aaa", 4L: JLong), 5))
    testHarness.processElement(new StreamRecord(binaryrow("bbb", 2L: JLong), 6))
    expectedOutput.add(new StreamRecord(binaryrow("bbb", 2L: JLong), 6))

    // retract
    testHarness.processElement(new StreamRecord(binaryrow("aaa", 5L: JLong), 7))
    expectedOutput.add(new StreamRecord(retractBinaryRow("aaa", 4L: JLong), 7))
    expectedOutput.add(new StreamRecord(binaryrow("aaa", 9L: JLong), 7))

    // accumulate
    testHarness.processElement(new StreamRecord(binaryrow("eee", 6L: JLong), 8))
    expectedOutput.add(new StreamRecord(binaryrow("eee", 6L: JLong), 8))

    // retract
    testHarness.processElement(new StreamRecord(binaryrow("aaa", 7L: JLong), 9))
    expectedOutput.add(new StreamRecord(retractBinaryRow("aaa", 9L: JLong), 9))
    expectedOutput.add(new StreamRecord(binaryrow("aaa", 16L: JLong), 9))
    testHarness.processElement(new StreamRecord(binaryrow("bbb", 3L: JLong), 10))
    expectedOutput.add(new StreamRecord(retractBinaryRow("bbb", 2L: JLong), 10))
    expectedOutput.add(new StreamRecord(binaryrow("bbb", 5L: JLong), 10))

    val result = testHarness.getOutput

    assertor.assertOutputEqualsSorted("result mismatch", expectedOutput, result)

    testHarness.close()
  }

}
