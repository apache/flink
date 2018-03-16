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
    new StreamQueryConfig().withIdleStateRetentionTime(Time.seconds(2), Time.seconds(3))

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

    testHarness.processElement(new StreamRecord(CRow(Row.of(1L: JLong, 1: JInt, "aaa"), true), 1))
    testHarness.processElement(new StreamRecord(CRow(Row.of(2L: JLong, 1: JInt, "bbb"), true), 1))
    // reuse timer 3001
    testHarness.setProcessingTime(1000)
    testHarness.processElement(new StreamRecord(CRow(Row.of(3L: JLong, 2: JInt, "aaa"), true), 1))
    testHarness.processElement(new StreamRecord(CRow(Row.of(4L: JLong, 3: JInt, "aaa"), true), 1))

    // register cleanup timer with 4002
    testHarness.setProcessingTime(1002)
    testHarness.processElement(new StreamRecord(CRow(Row.of(5L: JLong, 4: JInt, "aaa"), true), 1))
    testHarness.processElement(new StreamRecord(CRow(Row.of(6L: JLong, 2: JInt, "bbb"), true), 1))

    // trigger cleanup timer and register cleanup timer with 7003
    testHarness.setProcessingTime(4003)
    testHarness.processElement(new StreamRecord(CRow(Row.of(7L: JLong, 5: JInt, "aaa"), true), 1))
    testHarness.processElement(new StreamRecord(CRow(Row.of(8L: JLong, 6: JInt, "aaa"), true), 1))
    testHarness.processElement(new StreamRecord(CRow(Row.of(9L: JLong, 7: JInt, "aaa"), true), 1))
    testHarness.processElement(new StreamRecord(CRow(Row.of(10L: JLong, 3: JInt, "bbb"), true), 1))

    val result = testHarness.getOutput

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    expectedOutput.add(new StreamRecord(CRow(Row.of(1L: JLong, 1: JInt), true), 1))
    expectedOutput.add(new StreamRecord(CRow(Row.of(2L: JLong, 1: JInt), true), 1))
    expectedOutput.add(new StreamRecord(CRow(Row.of(3L: JLong, 3: JInt), true), 1))
    expectedOutput.add(new StreamRecord(CRow(Row.of(4L: JLong, 6: JInt), true), 1))
    expectedOutput.add(new StreamRecord(CRow(Row.of(5L: JLong, 10: JInt), true), 1))
    expectedOutput.add(new StreamRecord(CRow(Row.of(6L: JLong, 3: JInt), true), 1))
    expectedOutput.add(new StreamRecord(CRow(Row.of(7L: JLong, 5: JInt), true), 1))
    expectedOutput.add(new StreamRecord(CRow(Row.of(8L: JLong, 11: JInt), true), 1))
    expectedOutput.add(new StreamRecord(CRow(Row.of(9L: JLong, 18: JInt), true), 1))
    expectedOutput.add(new StreamRecord(CRow(Row.of(10L: JLong, 3: JInt), true), 1))

    verify(expectedOutput, result, new RowResultSortComparator())

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

    testHarness.processElement(new StreamRecord(CRow(Row.of(1L: JLong, 1: JInt, "aaa"), true), 1))
    testHarness.processElement(new StreamRecord(CRow(Row.of(2L: JLong, 1: JInt, "bbb"), true), 2))
    testHarness.processElement(new StreamRecord(CRow(Row.of(3L: JLong, 2: JInt, "aaa"), true), 3))
    testHarness.processElement(new StreamRecord(CRow(Row.of(4L: JLong, 3: JInt, "ccc"), true), 4))

    // trigger cleanup timer and register cleanup timer with 6002
    testHarness.setProcessingTime(3002)
    testHarness.processElement(new StreamRecord(CRow(Row.of(5L: JLong, 4: JInt, "aaa"), true), 5))
    testHarness.processElement(new StreamRecord(CRow(Row.of(6L: JLong, 2: JInt, "bbb"), true), 6))
    testHarness.processElement(new StreamRecord(CRow(Row.of(7L: JLong, 5: JInt, "aaa"), true), 7))
    testHarness.processElement(new StreamRecord(CRow(Row.of(8L: JLong, 6: JInt, "eee"), true), 8))
    testHarness.processElement(new StreamRecord(CRow(Row.of(9L: JLong, 7: JInt, "aaa"), true), 9))
    testHarness.processElement(new StreamRecord(CRow(Row.of(10L: JLong, 3: JInt, "bbb"), true), 10))

    val result = testHarness.getOutput

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    expectedOutput.add(new StreamRecord(CRow(Row.of(1L: JLong, 1: JInt), true), 1))
    expectedOutput.add(new StreamRecord(CRow(Row.of(2L: JLong, 1: JInt), true), 2))
    expectedOutput.add(new StreamRecord(CRow(Row.of(3L: JLong, 1: JInt), false), 3))
    expectedOutput.add(new StreamRecord(CRow(Row.of(3L: JLong, 3: JInt), true), 3))
    expectedOutput.add(new StreamRecord(CRow(Row.of(4L: JLong, 3: JInt), true), 4))
    expectedOutput.add(new StreamRecord(CRow(Row.of(5L: JLong, 4: JInt), true), 5))
    expectedOutput.add(new StreamRecord(CRow(Row.of(6L: JLong, 2: JInt), true), 6))
    expectedOutput.add(new StreamRecord(CRow(Row.of(7L: JLong, 4: JInt), false), 7))
    expectedOutput.add(new StreamRecord(CRow(Row.of(7L: JLong, 9: JInt), true), 7))
    expectedOutput.add(new StreamRecord(CRow(Row.of(8L: JLong, 6: JInt), true), 8))
    expectedOutput.add(new StreamRecord(CRow(Row.of(9L: JLong, 9: JInt), false), 9))
    expectedOutput.add(new StreamRecord(CRow(Row.of(9L: JLong, 16: JInt), true), 9))
    expectedOutput.add(new StreamRecord(CRow(Row.of(10L: JLong, 2: JInt), false), 10))
    expectedOutput.add(new StreamRecord(CRow(Row.of(10L: JLong, 5: JInt), true), 10))

    verify(expectedOutput, result, new RowResultSortComparator())

    testHarness.close()
  }
}
