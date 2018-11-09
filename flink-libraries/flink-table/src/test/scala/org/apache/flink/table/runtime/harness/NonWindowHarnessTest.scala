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
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.streaming.api.operators.LegacyKeyedProcessOperator
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.table.api.StreamQueryConfig
import org.apache.flink.table.runtime.aggregate._
import org.apache.flink.table.runtime.harness.HarnessTestBase._
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.types.Row
import org.junit.Test

class NonWindowHarnessTest extends HarnessTestBase {

  protected var queryConfig =
    new TestStreamQueryConfig(Time.seconds(2), Time.seconds(3))

  @Test
  def testNonWindow(): Unit = {

    val processFunction = new LegacyKeyedProcessOperator[String, CRow, CRow](
      new GroupAggProcessFunction(
        genSumAggFunction,
        sumAggregationStateType,
        false,
        queryConfig))

    val testHarness =
      createHarnessTester(
        processFunction,
        new TupleRowKeySelector[String](2),
        BasicTypeInfo.STRING_TYPE_INFO)

    testHarness.open()

    // register cleanup timer with 3001
    testHarness.setProcessingTime(1)

    testHarness.processElement(new StreamRecord(CRow(1L: JLong, 1: JInt, "aaa"), 1))
    testHarness.processElement(new StreamRecord(CRow(2L: JLong, 1: JInt, "bbb"), 1))
    // reuse timer 3001
    testHarness.setProcessingTime(1000)
    testHarness.processElement(new StreamRecord(CRow(3L: JLong, 2: JInt, "aaa"), 1))
    testHarness.processElement(new StreamRecord(CRow(4L: JLong, 3: JInt, "aaa"), 1))

    // register cleanup timer with 4002
    testHarness.setProcessingTime(1002)
    testHarness.processElement(new StreamRecord(CRow(5L: JLong, 4: JInt, "aaa"), 1))
    testHarness.processElement(new StreamRecord(CRow(6L: JLong, 2: JInt, "bbb"), 1))

    // trigger cleanup timer and register cleanup timer with 7003
    testHarness.setProcessingTime(4003)
    testHarness.processElement(new StreamRecord(CRow(7L: JLong, 5: JInt, "aaa"), 1))
    testHarness.processElement(new StreamRecord(CRow(8L: JLong, 6: JInt, "aaa"), 1))
    testHarness.processElement(new StreamRecord(CRow(9L: JLong, 7: JInt, "aaa"), 1))
    testHarness.processElement(new StreamRecord(CRow(10L: JLong, 3: JInt, "bbb"), 1))

    val result = testHarness.getOutput

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    expectedOutput.add(new StreamRecord(CRow(1L: JLong, 1: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(2L: JLong, 1: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(3L: JLong, 3: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(4L: JLong, 6: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(5L: JLong, 10: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(6L: JLong, 3: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(7L: JLong, 5: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(8L: JLong, 11: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(9L: JLong, 18: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(10L: JLong, 3: JInt), 1))

    verify(expectedOutput, result)

    testHarness.close()
  }

  @Test
  def testNonWindowWithRetract(): Unit = {

    val processFunction = new LegacyKeyedProcessOperator[String, CRow, CRow](
      new GroupAggProcessFunction(
        genSumAggFunction,
        sumAggregationStateType,
        true,
        queryConfig))

    val testHarness =
      createHarnessTester(
        processFunction,
        new TupleRowKeySelector[String](2),
        BasicTypeInfo.STRING_TYPE_INFO)

    testHarness.open()

    // register cleanup timer with 3001
    testHarness.setProcessingTime(1)

    testHarness.processElement(new StreamRecord(CRow(1L: JLong, 1: JInt, "aaa"), 1))
    testHarness.processElement(new StreamRecord(CRow(2L: JLong, 1: JInt, "bbb"), 2))
    testHarness.processElement(new StreamRecord(CRow(3L: JLong, 2: JInt, "aaa"), 3))
    testHarness.processElement(new StreamRecord(CRow(4L: JLong, 3: JInt, "ccc"), 4))

    // trigger cleanup timer and register cleanup timer with 6002
    testHarness.setProcessingTime(3002)
    testHarness.processElement(new StreamRecord(CRow(5L: JLong, 4: JInt, "aaa"), 5))
    testHarness.processElement(new StreamRecord(CRow(6L: JLong, 2: JInt, "bbb"), 6))
    testHarness.processElement(new StreamRecord(CRow(7L: JLong, 5: JInt, "aaa"), 7))
    testHarness.processElement(new StreamRecord(CRow(8L: JLong, 6: JInt, "eee"), 8))
    testHarness.processElement(new StreamRecord(CRow(9L: JLong, 7: JInt, "aaa"), 9))
    testHarness.processElement(new StreamRecord(CRow(10L: JLong, 3: JInt, "bbb"), 10))

    val result = testHarness.getOutput

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    expectedOutput.add(new StreamRecord(CRow(1L: JLong, 1: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(2L: JLong, 1: JInt), 2))
    expectedOutput.add(new StreamRecord(CRow(false, 3L: JLong, 1: JInt), 3))
    expectedOutput.add(new StreamRecord(CRow(3L: JLong, 3: JInt), 3))
    expectedOutput.add(new StreamRecord(CRow(4L: JLong, 3: JInt), 4))
    expectedOutput.add(new StreamRecord(CRow(5L: JLong, 4: JInt), 5))
    expectedOutput.add(new StreamRecord(CRow(6L: JLong, 2: JInt), 6))
    expectedOutput.add(new StreamRecord(CRow(false, 7L: JLong, 4: JInt), 7))
    expectedOutput.add(new StreamRecord(CRow(7L: JLong, 9: JInt), 7))
    expectedOutput.add(new StreamRecord(CRow(8L: JLong, 6: JInt), 8))
    expectedOutput.add(new StreamRecord(CRow(false, 9L: JLong, 9: JInt), 9))
    expectedOutput.add(new StreamRecord(CRow(9L: JLong, 16: JInt), 9))
    expectedOutput.add(new StreamRecord(CRow(false, 10L: JLong, 2: JInt), 10))
    expectedOutput.add(new StreamRecord(CRow(10L: JLong, 5: JInt), 10))

    verify(expectedOutput, result)

    testHarness.close()
  }
}
