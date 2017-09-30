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

import java.lang.{Long => JLong}
import java.util.concurrent.ConcurrentLinkedQueue

import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness
import org.apache.flink.table.api.Types
import org.apache.flink.table.runtime.harness.HarnessTestBase.{RowResultSortComparator, TupleRowKeySelector}
import org.apache.flink.table.runtime.join.{ProcTimeBoundedStreamInnerJoin, RowTimeBoundedStreamInnerJoin}
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.types.Row
import org.junit.Assert.{assertEquals}
import org.junit.Test

class JoinHarnessTest extends HarnessTestBase {

  private val rowType = Types.ROW(
    Types.LONG,
    Types.STRING)

  val funcCode: String =
    """
      |public class TestJoinFunction
      |          extends org.apache.flink.api.common.functions.RichFlatJoinFunction {
      |  transient org.apache.flink.types.Row out =
      |            new org.apache.flink.types.Row(4);
      |  public TestJoinFunction() throws Exception {}
      |
      |  @Override
      |  public void open(org.apache.flink.configuration.Configuration parameters)
      |  throws Exception {}
      |
      |  @Override
      |  public void join(Object _in1, Object _in2, org.apache.flink.util.Collector c)
      |   throws Exception {
      |   org.apache.flink.types.Row in1 = (org.apache.flink.types.Row) _in1;
      |   org.apache.flink.types.Row in2 = (org.apache.flink.types.Row) _in2;
      |
      |   out.setField(0, in1.getField(0));
      |   out.setField(1, in1.getField(1));
      |   out.setField(2, in2.getField(0));
      |   out.setField(3, in2.getField(1));
      |
      |   c.collect(out);
      |
      |  }
      |
      |  @Override
      |  public void close() throws Exception {}
      |}
    """.stripMargin

  /** a.proctime >= b.proctime - 10 and a.proctime <= b.proctime + 20 **/
  @Test
  def testProcTimeJoinWithCommonBounds() {

    val joinProcessFunc = new ProcTimeBoundedStreamInnerJoin(
      -10, 20, 0, rowType, rowType, "TestJoinFunction", funcCode)

    val operator: KeyedCoProcessOperator[Integer, CRow, CRow, CRow] =
      new KeyedCoProcessOperator[Integer, CRow, CRow, CRow](joinProcessFunc)
    val testHarness: KeyedTwoInputStreamOperatorTestHarness[Integer, CRow, CRow, CRow] =
      new KeyedTwoInputStreamOperatorTestHarness[Integer, CRow, CRow, CRow](
        operator,
        new TupleRowKeySelector[Integer](0),
        new TupleRowKeySelector[Integer](0),
        Types.INT,
        1, 1, 0)

    testHarness.open()

    testHarness.setProcessingTime(1)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1L: JLong, "1a1"), true), 1))
    assertEquals(1, testHarness.numProcessingTimeTimers())
    testHarness.setProcessingTime(2)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(2L: JLong, "2a2"), true), 2))

    // timers for key = 1 and key = 2
    assertEquals(2, testHarness.numProcessingTimeTimers())

    testHarness.setProcessingTime(3)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1L: JLong, "1a3"), true), 3))
    assertEquals(4, testHarness.numKeyedStateEntries())

    // The number of timers won't increase.
    assertEquals(2, testHarness.numProcessingTimeTimers())

    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1L: JLong, "1b3"), true), 3))
    testHarness.setProcessingTime(4)
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(2L: JLong, "2b4"), true), 4))

    // The number of states should be doubled.
    assertEquals(8, testHarness.numKeyedStateEntries())
    assertEquals(4, testHarness.numProcessingTimeTimers())

    // Test for -10 boundary (13 - 10 = 3).
    // The left row (key = 1) with timestamp = 1 will be eagerly removed here.
    testHarness.setProcessingTime(13)
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1L: JLong, "1b13"), true), 13))

    // Test for +20 boundary (13 + 20 = 33).
    testHarness.setProcessingTime(33)
    assertEquals(4, testHarness.numKeyedStateEntries())
    assertEquals(2, testHarness.numProcessingTimeTimers())
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1L: JLong, "1a33"), true), 33))

    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(2L: JLong, "2a33"), true), 33))

    // The left row (key = 2) with timestamp = 2 will be eagerly removed here.
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(2L: JLong, "2b33"), true), 33))

    val result = testHarness.getOutput

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1L: JLong, "1a1", 1L: JLong, "1b3"), true), 3))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1L: JLong, "1a3", 1L: JLong, "1b3"), true), 3))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(2L: JLong, "2a2", 2L: JLong, "2b4"), true), 4))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1L: JLong, "1a3", 1L: JLong, "1b13"), true), 13))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1L: JLong, "1a33", 1L: JLong, "1b13"), true), 33))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(2L: JLong, "2a33", 2L: JLong, "2b33"), true), 33))

    verify(expectedOutput, result, new RowResultSortComparator())

    testHarness.close()
  }

  /** a.proctime >= b.proctime - 10 and a.proctime <= b.proctime - 5 **/
  @Test
  def testProcTimeJoinWithNegativeBounds() {

    val joinProcessFunc = new ProcTimeBoundedStreamInnerJoin(
      -10, -5, 0, rowType, rowType, "TestJoinFunction", funcCode)

    val operator: KeyedCoProcessOperator[Integer, CRow, CRow, CRow] =
      new KeyedCoProcessOperator[Integer, CRow, CRow, CRow](joinProcessFunc)
    val testHarness: KeyedTwoInputStreamOperatorTestHarness[Integer, CRow, CRow, CRow] =
      new KeyedTwoInputStreamOperatorTestHarness[Integer, CRow, CRow, CRow](
        operator,
        new TupleRowKeySelector[Integer](0),
        new TupleRowKeySelector[Integer](0),
        Types.INT,
        1, 1, 0)

    testHarness.open()

    testHarness.setProcessingTime(1)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1L: JLong, "1a1"), true), 1))
    testHarness.setProcessingTime(2)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(2L: JLong, "2a2"), true), 2))
    testHarness.setProcessingTime(3)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1L: JLong, "1a3"), true), 3))
    assertEquals(4, testHarness.numKeyedStateEntries())
    assertEquals(2, testHarness.numProcessingTimeTimers())

    // All the right rows will not be cached.
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1L: JLong, "1b3"), true), 3))
    assertEquals(4, testHarness.numKeyedStateEntries())
    assertEquals(2, testHarness.numProcessingTimeTimers())

    testHarness.setProcessingTime(7)

    // Meets a.proctime <= b.proctime - 5.
    // This row will only be joined without being cached (7 >= 7 - 5).
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(2L: JLong, "2b7"), true), 7))
    assertEquals(4, testHarness.numKeyedStateEntries())
    assertEquals(2, testHarness.numProcessingTimeTimers())

    testHarness.setProcessingTime(12)
    // The left row (key = 1) with timestamp = 1 will be eagerly removed here.
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1L: JLong, "1b12"), true), 12))

    // We add a delay (relativeWindowSize / 2) for cleaning up state.
    // No timers will be triggered here.
    testHarness.setProcessingTime(13)
    assertEquals(4, testHarness.numKeyedStateEntries())
    assertEquals(2, testHarness.numProcessingTimeTimers())

    // Trigger the timer registered by the left row (key = 1) with timestamp = 1
    // (1 + 10 + 2 + 0 + 1 = 14).
    // The left row (key = 1) with timestamp = 3 will removed here.
    testHarness.setProcessingTime(14)
    assertEquals(2, testHarness.numKeyedStateEntries())
    assertEquals(1, testHarness.numProcessingTimeTimers())

    // Clean up the left row (key = 2) with timestamp = 2.
    testHarness.setProcessingTime(16)
    assertEquals(0, testHarness.numKeyedStateEntries())
    assertEquals(0, testHarness.numProcessingTimeTimers())
    val result = testHarness.getOutput

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    expectedOutput.add(new StreamRecord(
      CRow(Row.of(2L: JLong, "2a2", 2L: JLong, "2b7"), true), 7))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1L: JLong, "1a3", 1L: JLong, "1b12"), true), 12))

    verify(expectedOutput, result, new RowResultSortComparator())

    testHarness.close()
  }

  /** a.c1 >= b.rowtime - 10 and a.rowtime <= b.rowtime + 20 **/
  @Test
  def testRowTimeJoinWithCommonBounds() {

    val joinProcessFunc = new RowTimeBoundedStreamInnerJoin(
      -10, 20, 0, rowType, rowType, "TestJoinFunction", funcCode, 0, 0)

    val operator: KeyedCoProcessOperator[String, CRow, CRow, CRow] =
      new KeyedCoProcessOperator[String, CRow, CRow, CRow](joinProcessFunc)
    val testHarness: KeyedTwoInputStreamOperatorTestHarness[String, CRow, CRow, CRow] =
      new KeyedTwoInputStreamOperatorTestHarness[String, CRow, CRow, CRow](
        operator,
        new TupleRowKeySelector[String](1),
        new TupleRowKeySelector[String](1),
        Types.STRING,
        1, 1, 0)

    testHarness.open()

    testHarness.processWatermark1(new Watermark(1))
    testHarness.processWatermark2(new Watermark(1))

    // Test late data.
    testHarness.processElement1(new StreamRecord[CRow](
      CRow(Row.of(1L: JLong, "k1"), true), 0))

    // Though (1L, "k1") is actually late, it will also be cached.
    assertEquals(1, testHarness.numEventTimeTimers())

    testHarness.processElement1(new StreamRecord[CRow](
      CRow(Row.of(2L: JLong, "k1"), true), 0))
    testHarness.processElement2(new StreamRecord[CRow](
      CRow(Row.of(2L: JLong, "k1"), true), 0))

    assertEquals(2, testHarness.numEventTimeTimers())
    assertEquals(4, testHarness.numKeyedStateEntries())

    testHarness.processElement1(new StreamRecord[CRow](
      CRow(Row.of(5L: JLong, "k1"), true), 0))

    testHarness.processElement2(new StreamRecord[CRow](
      CRow(Row.of(15L: JLong, "k1"), true), 0))

    testHarness.processWatermark1(new Watermark(20))
    testHarness.processWatermark2(new Watermark(20))

    assertEquals(4, testHarness.numKeyedStateEntries())

    testHarness.processElement1(new StreamRecord[CRow](
      CRow(Row.of(35L: JLong, "k1"), true), 0))

    // The right rows with timestamp = 2 and 5 will be removed here.
    // The left rows with timestamp = 2 and 15 will be removed here.
    testHarness.processWatermark1(new Watermark(38))
    testHarness.processWatermark2(new Watermark(38))

    testHarness.processElement1(new StreamRecord[CRow](
      CRow(Row.of(40L: JLong, "k2"), true), 0))
    testHarness.processElement2(new StreamRecord[CRow](
      CRow(Row.of(39L: JLong, "k2"), true), 0))

    assertEquals(6, testHarness.numKeyedStateEntries())

    // The right row with timestamp = 35 will be removed here.
    testHarness.processWatermark1(new Watermark(61))
    testHarness.processWatermark2(new Watermark(61))

    assertEquals(4, testHarness.numKeyedStateEntries())

    val expectedOutput = new ConcurrentLinkedQueue[Object]()
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(2L: JLong, "k1", 2L: JLong, "k1"), true), 0))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(5L: JLong, "k1", 2L: JLong, "k1"), true), 0))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(5L: JLong, "k1", 15L: JLong, "k1"), true), 0))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(35L: JLong, "k1", 15L: JLong, "k1"), true), 0))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(40L: JLong, "k2", 39L: JLong, "k2"), true), 0))

    // This result is produced by the late row (1, "k1").
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1L: JLong, "k1", 2L: JLong, "k1"), true), 0))

    val result = testHarness.getOutput
    verify(expectedOutput, result, new RowResultSortComparator())
    testHarness.close()
  }

  /** a.rowtime >= b.rowtime - 10 and a.rowtime <= b.rowtime - 7 **/
  @Test
  def testRowTimeJoinWithNegativeBounds() {

    val joinProcessFunc = new RowTimeBoundedStreamInnerJoin(
      -10, -7, 0, rowType, rowType, "TestJoinFunction", funcCode, 0, 0)

    val operator: KeyedCoProcessOperator[String, CRow, CRow, CRow] =
      new KeyedCoProcessOperator[String, CRow, CRow, CRow](joinProcessFunc)
    val testHarness: KeyedTwoInputStreamOperatorTestHarness[String, CRow, CRow, CRow] =
      new KeyedTwoInputStreamOperatorTestHarness[String, CRow, CRow, CRow](
        operator,
        new TupleRowKeySelector[String](1),
        new TupleRowKeySelector[String](1),
        Types.STRING,
        1, 1, 0)

    testHarness.open()

    testHarness.processWatermark1(new Watermark(1))
    testHarness.processWatermark2(new Watermark(1))

    // This row will not be cached.
    testHarness.processElement2(new StreamRecord[CRow](
      CRow(Row.of(2L: JLong, "k1"), true), 0))

    assertEquals(0, testHarness.numKeyedStateEntries())

    testHarness.processWatermark1(new Watermark(2))
    testHarness.processWatermark2(new Watermark(2))

    testHarness.processElement1(new StreamRecord[CRow](
      CRow(Row.of(3L: JLong, "k1"), true), 0))
    testHarness.processElement2(new StreamRecord[CRow](
      CRow(Row.of(3L: JLong, "k1"), true), 0))

    // Test for -10 boundary (13 - 10 = 3).
    // This row from the right stream will be cached.
    // The clean time for the left stream is 13 - 7 + 1 - 1 = 8
    testHarness.processElement2(new StreamRecord[CRow](
      CRow(Row.of(13L: JLong, "k1"), true), 0))

    // Test for -7 boundary (13 - 7 = 6).
    testHarness.processElement1(new StreamRecord[CRow](
      CRow(Row.of(6L: JLong, "k1"), true), 0))

    assertEquals(4, testHarness.numKeyedStateEntries())

    // Trigger the left timer with timestamp  8.
    // The row with timestamp = 13 will be removed here (13 < 10 + 7).
    testHarness.processWatermark1(new Watermark(10))
    testHarness.processWatermark2(new Watermark(10))

    assertEquals(2, testHarness.numKeyedStateEntries())

    // Clear the states.
    testHarness.processWatermark1(new Watermark(18))
    testHarness.processWatermark2(new Watermark(18))

    assertEquals(0, testHarness.numKeyedStateEntries())

    val expectedOutput = new ConcurrentLinkedQueue[Object]()
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(3L: JLong, "k1", 13L: JLong, "k1"), true), 0))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(6L: JLong, "k1", 13L: JLong, "k1"), true), 0))

    val result = testHarness.getOutput
    verify(expectedOutput, result, new RowResultSortComparator())
    testHarness.close()
  }
}
