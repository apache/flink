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

import java.lang.{Integer => JInt}
import java.util.concurrent.ConcurrentLinkedQueue

import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.table.api.Types
import org.apache.flink.table.api.scala._
import org.apache.flink.table.dataformat.BinaryString.fromString
import org.apache.flink.table.dataformat.GenericRow
import org.apache.flink.table.runtime.utils.BaseRowHarnessAssertor
import org.apache.flink.table.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.dataformat.util.BaseRowUtil._
import org.apache.flink.types.Row
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.mutable

@RunWith(classOf[Parameterized])
class NonWindowHarnessTest(mode: StateBackendMode) extends HarnessTestBase(mode) {

  @Test
  def testProcTimeNonWindow(): Unit = {
    val data = new mutable.MutableList[(Int, String)]
    val t = env.fromCollection(data).toTable(tEnv, 'b, 'c)
    tEnv.registerTable("T", t)
    val t1 = tEnv.sqlQuery("SELECT c, sum(b) FROM T GROUP BY c")

    tEnv.getConfig
      .withIdleStateRetentionTime(
        Time.seconds(2),
        Time.seconds(3))

    val testHarness = createHarnessTester(t1.toRetractStream[Row], "GroupAggregate")
    val assertor = new BaseRowHarnessAssertor(Array(Types.STRING, Types.INT))

    testHarness.open()

    // register cleanup timer with 3001
    testHarness.setProcessingTime(1)

    testHarness.processElement(new StreamRecord(
      setAccumulate(GenericRow.of(1: JInt, fromString("aaa"))), 1))
    testHarness.processElement(new StreamRecord(
      setAccumulate(GenericRow.of(1: JInt, fromString("bbb"))), 1))
    // reuse timer 3001
    testHarness.setProcessingTime(1000)
    testHarness.processElement(new StreamRecord(
      setAccumulate(GenericRow.of(2: JInt, fromString("aaa"))), 1))
    testHarness.processElement(new StreamRecord(
      setAccumulate(GenericRow.of(3: JInt, fromString("aaa"))), 1))

    // register cleanup timer with 4002
    testHarness.setProcessingTime(1002)
    testHarness.processElement(new StreamRecord(
      setAccumulate(GenericRow.of(4: JInt, fromString("aaa"))), 1))
    testHarness.processElement(new StreamRecord(
      setAccumulate(GenericRow.of(2: JInt, fromString("bbb"))), 1))

    // trigger cleanup timer and register cleanup timer with 7003
    testHarness.setProcessingTime(4003)
    testHarness.processElement(new StreamRecord(
      setAccumulate(GenericRow.of(5: JInt, fromString("aaa"))), 1))
    testHarness.processElement(new StreamRecord(
      setAccumulate(GenericRow.of(6: JInt, fromString("aaa"))), 1))
    testHarness.processElement(new StreamRecord(
      setAccumulate(GenericRow.of(7: JInt, fromString("aaa"))), 1))
    testHarness.processElement(new StreamRecord(
      setAccumulate(GenericRow.of(3: JInt, fromString("bbb"))), 1))

    val result = testHarness.getOutput

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    expectedOutput.add(new StreamRecord(
      setAccumulate(GenericRow.of(fromString("aaa"), 1: JInt))))
    expectedOutput.add(new StreamRecord(
      setAccumulate(GenericRow.of(fromString("bbb"), 1: JInt))))
    expectedOutput.add(new StreamRecord(
      setRetract(GenericRow.of(fromString("aaa"), 1: JInt))))
    expectedOutput.add(new StreamRecord(
      setAccumulate(GenericRow.of(fromString("aaa"), 3: JInt))))
    expectedOutput.add(new StreamRecord(
      setRetract(GenericRow.of(fromString("aaa"), 3: JInt))))
    expectedOutput.add(new StreamRecord(
      setAccumulate(GenericRow.of(fromString("aaa"), 6: JInt))))
    expectedOutput.add(new StreamRecord(
      setRetract(GenericRow.of(fromString("aaa"), 6: JInt))))
    expectedOutput.add(new StreamRecord(
      setAccumulate(GenericRow.of(fromString("aaa"), 10: JInt))))
    expectedOutput.add(new StreamRecord(
      setRetract(GenericRow.of(fromString("bbb"), 1: JInt))))
    expectedOutput.add(new StreamRecord(
      setAccumulate(GenericRow.of(fromString("bbb"), 3: JInt))))
    expectedOutput.add(new StreamRecord(
      setAccumulate(GenericRow.of(fromString("aaa"), 5: JInt))))
    expectedOutput.add(new StreamRecord(
      setRetract(GenericRow.of(fromString("aaa"), 5: JInt))))
    expectedOutput.add(new StreamRecord(
      setAccumulate(GenericRow.of(fromString("aaa"), 11: JInt))))
    expectedOutput.add(new StreamRecord(
      setRetract(GenericRow.of(fromString("aaa"), 11: JInt))))
    expectedOutput.add(new StreamRecord(
      setAccumulate(GenericRow.of(fromString("aaa"), 18: JInt))))
    expectedOutput.add(new StreamRecord(
      setAccumulate(GenericRow.of(fromString("bbb"), 3: JInt))))

    assertor.assertOutputEqualsSorted("result error", expectedOutput, result)

    testHarness.close()
  }
}
