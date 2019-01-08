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
import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.operators.join.JoinType
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness
import org.apache.flink.table.api.{StreamQueryConfig, Types}
import org.apache.flink.table.runtime.harness.HarnessTestBase.{RowResultSortComparator, RowResultSortComparatorWithWatermarks, TestStreamQueryConfig, TupleRowKeySelector}
import org.apache.flink.table.runtime.join._
import org.apache.flink.table.runtime.operators.KeyedCoProcessOperatorWithWatermarkDelay
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
import org.apache.flink.types.Row
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test

/**
  * Tests for runtime logic of stream joins.
  */
class JoinHarnessTest extends HarnessTestBase {

  private val queryConfig =
    new TestStreamQueryConfig(Time.milliseconds(2), Time.milliseconds(4))

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

  val funcCodeWithNonEqualPred: String =
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
      |   if(((java.lang.String)in1.getField(1)).compareTo((java.lang.String)in2.getField(1))>0) {
      |      c.collect(out);
      |   }
      |  }
      |
      |  @Override
      |  public void close() throws Exception {}
      |}
    """.stripMargin

  val funcCodeWithNonEqualPred2: String =
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
      |   if(((java.lang.String)in1.getField(1)).compareTo((java.lang.String)in2.getField(1))<0) {
      |      c.collect(out);
      |   }
      |  }
      |
      |  @Override
      |  public void close() throws Exception {}
      |}
    """.stripMargin

  /** a.proctime >= b.proctime - 10 and a.proctime <= b.proctime + 20 **/
  @Test
  def testProcTimeInnerJoinWithCommonBounds() {

    val joinProcessFunc = new ProcTimeBoundedStreamJoin(
      JoinType.INNER, -10, 20, rowType, rowType, "TestJoinFunction", funcCode)

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
      CRow(Row.of(1L: JLong, "1a1"), change = true), 1))
    assertEquals(1, testHarness.numProcessingTimeTimers())
    testHarness.setProcessingTime(2)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(2L: JLong, "2a2"), change = true), 2))

    // timers for key = 1 and key = 2
    assertEquals(2, testHarness.numProcessingTimeTimers())

    testHarness.setProcessingTime(3)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1L: JLong, "1a3"), change = true), 3))
    assertEquals(4, testHarness.numKeyedStateEntries())

    // The number of timers won't increase.
    assertEquals(2, testHarness.numProcessingTimeTimers())

    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1L: JLong, "1b3"), change = true), 3))
    testHarness.setProcessingTime(4)
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(2L: JLong, "2b4"), change = true), 4))

    // The number of states should be doubled.
    assertEquals(8, testHarness.numKeyedStateEntries())
    assertEquals(4, testHarness.numProcessingTimeTimers())

    // Test for -10 boundary (13 - 10 = 3).
    // The left row (key = 1) with timestamp = 1 will be eagerly removed here.
    testHarness.setProcessingTime(13)
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1L: JLong, "1b13"), change = true), 13))

    // Test for +20 boundary (13 + 20 = 33).
    testHarness.setProcessingTime(33)
    assertEquals(4, testHarness.numKeyedStateEntries())
    assertEquals(2, testHarness.numProcessingTimeTimers())
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1L: JLong, "1a33"), change = true), 33))

    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(2L: JLong, "2a33"), change = true), 33))

    // The left row (key = 2) with timestamp = 2 will be eagerly removed here.
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(2L: JLong, "2b33"), change = true), 33))

    val result = testHarness.getOutput

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1L: JLong, "1a1", 1L: JLong, "1b3"), change = true), 3))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1L: JLong, "1a3", 1L: JLong, "1b3"), change = true), 3))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(2L: JLong, "2a2", 2L: JLong, "2b4"), change = true), 4))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1L: JLong, "1a3", 1L: JLong, "1b13"), change = true), 13))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1L: JLong, "1a33", 1L: JLong, "1b13"), change = true), 33))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(2L: JLong, "2a33", 2L: JLong, "2b33"), change = true), 33))

    verify(expectedOutput, result, new RowResultSortComparator())

    testHarness.close()
  }

  /** a.proctime >= b.proctime - 10 and a.proctime <= b.proctime - 5 **/
  @Test
  def testProcTimeInnerJoinWithNegativeBounds() {

    val joinProcessFunc = new ProcTimeBoundedStreamJoin(
      JoinType.INNER, -10, -5, rowType, rowType, "TestJoinFunction", funcCode)

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
      CRow(Row.of(1L: JLong, "1a1"), change = true), 1))
    testHarness.setProcessingTime(2)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(2L: JLong, "2a2"), change = true), 2))
    testHarness.setProcessingTime(3)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1L: JLong, "1a3"), change = true), 3))
    assertEquals(4, testHarness.numKeyedStateEntries())
    assertEquals(2, testHarness.numProcessingTimeTimers())

    // All the right rows will not be cached.
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1L: JLong, "1b3"), change = true), 3))
    assertEquals(4, testHarness.numKeyedStateEntries())
    assertEquals(2, testHarness.numProcessingTimeTimers())

    testHarness.setProcessingTime(7)

    // Meets a.proctime <= b.proctime - 5.
    // This row will only be joined without being cached (7 >= 7 - 5).
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(2L: JLong, "2b7"), change = true), 7))
    assertEquals(4, testHarness.numKeyedStateEntries())
    assertEquals(2, testHarness.numProcessingTimeTimers())

    testHarness.setProcessingTime(12)
    // The left row (key = 1) with timestamp = 1 will be eagerly removed here.
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1L: JLong, "1b12"), change = true), 12))

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
      CRow(Row.of(2L: JLong, "2a2", 2L: JLong, "2b7"), change = true), 7))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1L: JLong, "1a3", 1L: JLong, "1b12"), change = true), 12))

    verify(expectedOutput, result, new RowResultSortComparator())

    testHarness.close()
  }

  /** a.rowtime >= b.rowtime - 10 and a.rowtime <= b.rowtime + 20 **/
  @Test
  def testRowTimeInnerJoinWithCommonBounds() {

    val joinProcessFunc = new RowTimeBoundedStreamJoin(
      JoinType.INNER, -10, 20, 0, rowType, rowType, "TestJoinFunction", funcCode, 0, 0)

    val operator: KeyedCoProcessOperator[String, CRow, CRow, CRow] =
      new KeyedCoProcessOperatorWithWatermarkDelay[String, CRow, CRow, CRow](
        joinProcessFunc,
        joinProcessFunc.getMaxOutputDelay)
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
      CRow(Row.of(1L: JLong, "k1"), change = true), 0))

    // Though (1L, "k1") is actually late, it will also be cached.
    assertEquals(1, testHarness.numEventTimeTimers())

    testHarness.processElement1(new StreamRecord[CRow](
      CRow(Row.of(2L: JLong, "k1"), change = true), 0))
    testHarness.processElement2(new StreamRecord[CRow](
      CRow(Row.of(2L: JLong, "k1"), change = true), 0))

    assertEquals(2, testHarness.numEventTimeTimers())
    assertEquals(4, testHarness.numKeyedStateEntries())

    testHarness.processElement1(new StreamRecord[CRow](
      CRow(Row.of(5L: JLong, "k1"), change = true), 0))

    testHarness.processElement2(new StreamRecord[CRow](
      CRow(Row.of(15L: JLong, "k1"), change = true), 0))

    testHarness.processWatermark1(new Watermark(20))
    testHarness.processWatermark2(new Watermark(20))

    assertEquals(4, testHarness.numKeyedStateEntries())

    testHarness.processElement1(new StreamRecord[CRow](
      CRow(Row.of(35L: JLong, "k1"), change = true), 0))

    // The right rows with timestamp = 2 and 5 will be removed here.
    // The left rows with timestamp = 2 and 15 will be removed here.
    testHarness.processWatermark1(new Watermark(38))
    testHarness.processWatermark2(new Watermark(38))

    testHarness.processElement1(new StreamRecord[CRow](
      CRow(Row.of(40L: JLong, "k2"), change = true), 0))
    testHarness.processElement2(new StreamRecord[CRow](
      CRow(Row.of(39L: JLong, "k2"), change = true), 0))

    assertEquals(6, testHarness.numKeyedStateEntries())

    // The right row with timestamp = 35 will be removed here.
    testHarness.processWatermark1(new Watermark(61))
    testHarness.processWatermark2(new Watermark(61))

    assertEquals(4, testHarness.numKeyedStateEntries())

    val expectedOutput = new ConcurrentLinkedQueue[Object]()
    expectedOutput.add(new Watermark(-19))
    // This result is produced by the late row (1, "k1").
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1L: JLong, "k1", 2L: JLong, "k1"), change = true), 0))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(2L: JLong, "k1", 2L: JLong, "k1"), change = true), 0))
    expectedOutput.add(new StreamRecord(
        CRow(Row.of(5L: JLong, "k1", 2L: JLong, "k1"), change = true), 0))
    expectedOutput.add(new StreamRecord(
        CRow(Row.of(5L: JLong, "k1", 15L: JLong, "k1"), change = true), 0))
    expectedOutput.add(new Watermark(0))
    expectedOutput.add(new StreamRecord(
        CRow(Row.of(35L: JLong, "k1", 15L: JLong, "k1"), change = true), 0))
    expectedOutput.add(new Watermark(18))
    expectedOutput.add(new StreamRecord(
        CRow(Row.of(40L: JLong, "k2", 39L: JLong, "k2"), change = true), 0))
    expectedOutput.add(new Watermark(41))

    val result = testHarness.getOutput
    verify(
      expectedOutput,
      result,
      new RowResultSortComparatorWithWatermarks(),
      checkWaterMark = true)
    testHarness.close()
  }

  /** a.rowtime >= b.rowtime - 10 and a.rowtime <= b.rowtime - 7 **/
  @Test
  def testRowTimeInnerJoinWithNegativeBounds() {

    val joinProcessFunc = new RowTimeBoundedStreamJoin(
      JoinType.INNER, -10, -7, 0, rowType, rowType, "TestJoinFunction", funcCode, 0, 0)

    val operator: KeyedCoProcessOperator[String, CRow, CRow, CRow] =
      new KeyedCoProcessOperatorWithWatermarkDelay[String, CRow, CRow, CRow](
        joinProcessFunc,
        joinProcessFunc.getMaxOutputDelay)
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
      CRow(Row.of(2L: JLong, "k1"), change = true), 0))

    assertEquals(0, testHarness.numKeyedStateEntries())

    testHarness.processWatermark1(new Watermark(2))
    testHarness.processWatermark2(new Watermark(2))

    testHarness.processElement1(new StreamRecord[CRow](
      CRow(Row.of(3L: JLong, "k1"), change = true), 0))
    testHarness.processElement2(new StreamRecord[CRow](
      CRow(Row.of(3L: JLong, "k1"), change = true), 0))

    // Test for -10 boundary (13 - 10 = 3).
    // This row from the right stream will be cached.
    // The clean time for the left stream is 13 - 7 + 1 - 1 = 8
    testHarness.processElement2(new StreamRecord[CRow](
      CRow(Row.of(13L: JLong, "k1"), change = true), 0))

    // Test for -7 boundary (13 - 7 = 6).
    testHarness.processElement1(new StreamRecord[CRow](
      CRow(Row.of(6L: JLong, "k1"), change = true), 0))

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
    expectedOutput.add(new Watermark(-9))
    expectedOutput.add(new Watermark(-8))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(3L: JLong, "k1", 13L: JLong, "k1"), change = true), 0))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(6L: JLong, "k1", 13L: JLong, "k1"), change = true), 0))
    expectedOutput.add(new Watermark(0))
    expectedOutput.add(new Watermark(8))

    val result = testHarness.getOutput
    verify(
      expectedOutput,
      result,
      new RowResultSortComparatorWithWatermarks(),
      checkWaterMark = true)
    testHarness.close()
  }

  /** a.rowtime >= b.rowtime - 5 and a.rowtime <= b.rowtime + 9 **/
  @Test
  def testRowTimeLeftOuterJoin() {

    val joinProcessFunc = new RowTimeBoundedStreamJoin(
      JoinType.LEFT_OUTER, -5, 9, 0, rowType, rowType, "TestJoinFunction", funcCode, 0, 0)

    val operator: KeyedCoProcessOperator[String, CRow, CRow, CRow] =
      new KeyedCoProcessOperatorWithWatermarkDelay[String, CRow, CRow, CRow](
        joinProcessFunc,
        joinProcessFunc.getMaxOutputDelay)
    val testHarness: KeyedTwoInputStreamOperatorTestHarness[String, CRow, CRow, CRow] =
      new KeyedTwoInputStreamOperatorTestHarness[String, CRow, CRow, CRow](
        operator,
        new TupleRowKeySelector[String](1),
        new TupleRowKeySelector[String](1),
        Types.STRING,
        1, 1, 0)

    testHarness.open()

    testHarness.processElement1(new StreamRecord[CRow](
      CRow(Row.of(1L: JLong, "k1"), change = true), 0))
    testHarness.processElement2(new StreamRecord[CRow](
      CRow(Row.of(1L: JLong, "k2"), change = true), 0))

    assertEquals(2, testHarness.numEventTimeTimers())
    assertEquals(4, testHarness.numKeyedStateEntries())

    // The left row with timestamp = 1 will be padded and removed (14=1+5+1+((5+9)/2)).
    testHarness.processWatermark1(new Watermark(14))
    testHarness.processWatermark2(new Watermark(14))

    assertEquals(1, testHarness.numEventTimeTimers())
    assertEquals(2, testHarness.numKeyedStateEntries())

    // The right row with timestamp = 1 will be removed (18=1+9+1+((5+9)/2)).
    testHarness.processWatermark1(new Watermark(18))
    testHarness.processWatermark2(new Watermark(18))

    assertEquals(0, testHarness.numEventTimeTimers())
    assertEquals(0, testHarness.numKeyedStateEntries())

    testHarness.processElement1(new StreamRecord[CRow](
      CRow(Row.of(2L: JLong, "k1"), change = true), 0))
    testHarness.processElement2(new StreamRecord[CRow](
      CRow(Row.of(2L: JLong, "k2"), change = true), 0))

    // The late rows with timestamp = 2 will not be cached, but a null padding result for the left
    // row will be emitted.
    assertEquals(0, testHarness.numKeyedStateEntries())
    assertEquals(0, testHarness.numEventTimeTimers())

    // Make sure the common (inner) join can be performed.
    testHarness.processElement1(new StreamRecord[CRow](
      CRow(Row.of(19L: JLong, "k1"), change = true), 0))
    testHarness.processElement1(new StreamRecord[CRow](
      CRow(Row.of(20L: JLong, "k1"), change = true), 0))
    testHarness.processElement2(new StreamRecord[CRow](
      CRow(Row.of(26L: JLong, "k1"), change = true), 0))
    testHarness.processElement2(new StreamRecord[CRow](
      CRow(Row.of(25L: JLong, "k1"), change = true), 0))
    testHarness.processElement1(new StreamRecord[CRow](
      CRow(Row.of(21L: JLong, "k1"), change = true), 0))

    testHarness.processElement2(new StreamRecord[CRow](
      CRow(Row.of(39L: JLong, "k2"), change = true), 0))
    testHarness.processElement2(new StreamRecord[CRow](
      CRow(Row.of(40L: JLong, "k2"), change = true), 0))
    testHarness.processElement1(new StreamRecord[CRow](
      CRow(Row.of(50L: JLong, "k2"), change = true), 0))
    testHarness.processElement1(new StreamRecord[CRow](
      CRow(Row.of(49L: JLong, "k2"), change = true), 0))
    testHarness.processElement2(new StreamRecord[CRow](
      CRow(Row.of(41L: JLong, "k2"), change = true), 0))

    testHarness.processWatermark1(new Watermark(100))
    testHarness.processWatermark2(new Watermark(100))

    val expectedOutput = new ConcurrentLinkedQueue[Object]()
    // The timestamp 14 is set with the triggered timer.
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1L: JLong, "k1", null: JLong, null: String), change = true), 14))
    expectedOutput.add(new Watermark(5))
    expectedOutput.add(new Watermark(9))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(2L: JLong, "k1", null: JLong, null: String), change = true), 0))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(20L: JLong, "k1", 25L: JLong, "k1": String), change = true), 0))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(21L: JLong, "k1", 25L: JLong, "k1": String), change = true), 0))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(21L: JLong, "k1", 26L: JLong, "k1": String), change = true), 0))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(49L: JLong, "k2", 40L: JLong, "k2": String), change = true), 0))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(49L: JLong, "k2", 41L: JLong, "k2": String), change = true), 0))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(50L: JLong, "k2", 41L: JLong, "k2": String), change = true), 0))
    // The timestamp 32 is set with the triggered timer.
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(19L: JLong, "k1", null: JLong, null: String), change = true), 32))
    expectedOutput.add(new Watermark(91))


    val result = testHarness.getOutput
    verify(
      expectedOutput,
      result,
      new RowResultSortComparatorWithWatermarks(),
      checkWaterMark = true)
    testHarness.close()
  }

  /** a.rowtime >= b.rowtime - 5 and a.rowtime <= b.rowtime + 9 **/
  @Test
  def testRowTimeRightOuterJoin() {

    val joinProcessFunc = new RowTimeBoundedStreamJoin(
      JoinType.RIGHT_OUTER, -5, 9, 0, rowType, rowType, "TestJoinFunction", funcCode, 0, 0)

    val operator: KeyedCoProcessOperator[String, CRow, CRow, CRow] =
      new KeyedCoProcessOperatorWithWatermarkDelay[String, CRow, CRow, CRow](
        joinProcessFunc,
        joinProcessFunc.getMaxOutputDelay)
    val testHarness: KeyedTwoInputStreamOperatorTestHarness[String, CRow, CRow, CRow] =
      new KeyedTwoInputStreamOperatorTestHarness[String, CRow, CRow, CRow](
        operator,
        new TupleRowKeySelector[String](1),
        new TupleRowKeySelector[String](1),
        Types.STRING,
        1, 1, 0)

    testHarness.open()

    testHarness.processElement1(new StreamRecord[CRow](
      CRow(Row.of(1L: JLong, "k1"), change = true), 0))
    testHarness.processElement2(new StreamRecord[CRow](
      CRow(Row.of(1L: JLong, "k2"), change = true), 0))

    assertEquals(2, testHarness.numEventTimeTimers())
    assertEquals(4, testHarness.numKeyedStateEntries())

    // The left row with timestamp = 1 will be removed (14=1+5+1+((5+9)/2)).
    testHarness.processWatermark1(new Watermark(14))
    testHarness.processWatermark2(new Watermark(14))

    assertEquals(1, testHarness.numEventTimeTimers())
    assertEquals(2, testHarness.numKeyedStateEntries())

    // The right row with timestamp = 1 will be padded and removed (18=1+9+1+((5+9)/2)).
    testHarness.processWatermark1(new Watermark(18))
    testHarness.processWatermark2(new Watermark(18))

    assertEquals(0, testHarness.numEventTimeTimers())
    assertEquals(0, testHarness.numKeyedStateEntries())

    testHarness.processElement1(new StreamRecord[CRow](
      CRow(Row.of(2L: JLong, "k1"), change = true), 0))
    testHarness.processElement2(new StreamRecord[CRow](
      CRow(Row.of(2L: JLong, "k2"), change = true), 0))

    // The late rows with timestamp = 2 will not be cached, but a null padding result for the right
    // row will be emitted.
    assertEquals(0, testHarness.numKeyedStateEntries())
    assertEquals(0, testHarness.numEventTimeTimers())

    // Make sure the common (inner) join can be performed.
    testHarness.processElement1(new StreamRecord[CRow](
      CRow(Row.of(19L: JLong, "k1"), change = true), 0))
    testHarness.processElement1(new StreamRecord[CRow](
      CRow(Row.of(20L: JLong, "k1"), change = true), 0))
    testHarness.processElement2(new StreamRecord[CRow](
      CRow(Row.of(26L: JLong, "k1"), change = true), 0))
    testHarness.processElement2(new StreamRecord[CRow](
      CRow(Row.of(25L: JLong, "k1"), change = true), 0))
    testHarness.processElement1(new StreamRecord[CRow](
      CRow(Row.of(21L: JLong, "k1"), change = true), 0))

    testHarness.processElement2(new StreamRecord[CRow](
      CRow(Row.of(39L: JLong, "k2"), change = true), 0))
    testHarness.processElement2(new StreamRecord[CRow](
      CRow(Row.of(40L: JLong, "k2"), change = true), 0))
    testHarness.processElement1(new StreamRecord[CRow](
      CRow(Row.of(50L: JLong, "k2"), change = true), 0))
    testHarness.processElement1(new StreamRecord[CRow](
      CRow(Row.of(49L: JLong, "k2"), change = true), 0))
    testHarness.processElement2(new StreamRecord[CRow](
      CRow(Row.of(41L: JLong, "k2"), change = true), 0))

    testHarness.processWatermark1(new Watermark(100))
    testHarness.processWatermark2(new Watermark(100))

    val expectedOutput = new ConcurrentLinkedQueue[Object]()
    expectedOutput.add(new Watermark(5))
    // The timestamp 18 is set with the triggered timer.
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(null: JLong, null: String, 1L: JLong, "k2": String), change = true), 18))
    expectedOutput.add(new Watermark(9))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(null: JLong, null: String, 2L: JLong, "k2": String), change = true), 0))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(20L: JLong, "k1", 25L: JLong, "k1": String), change = true), 0))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(21L: JLong, "k1", 25L: JLong, "k1": String), change = true), 0))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(21L: JLong, "k1", 26L: JLong, "k1": String), change = true), 0))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(49L: JLong, "k2", 40L: JLong, "k2": String), change = true), 0))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(49L: JLong, "k2", 41L: JLong, "k2": String), change = true), 0))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(50L: JLong, "k2", 41L: JLong, "k2": String), change = true), 0))
    // The timestamp 56 is set with the triggered timer.
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(null: JLong, null: String, 39L: JLong, "k2": String), change = true), 56))
    expectedOutput.add(new Watermark(91))

    val result = testHarness.getOutput
    verify(
      expectedOutput,
      result,
      new RowResultSortComparatorWithWatermarks(),
      checkWaterMark = true)
    testHarness.close()
  }

  /** a.rowtime >= b.rowtime - 5 and a.rowtime <= b.rowtime + 9 **/
  @Test
  def testRowTimeFullOuterJoin() {

    val joinProcessFunc = new RowTimeBoundedStreamJoin(
      JoinType.FULL_OUTER, -5, 9, 0, rowType, rowType, "TestJoinFunction", funcCode, 0, 0)

    val operator: KeyedCoProcessOperator[String, CRow, CRow, CRow] =
      new KeyedCoProcessOperatorWithWatermarkDelay[String, CRow, CRow, CRow](
        joinProcessFunc,
        joinProcessFunc.getMaxOutputDelay)
    val testHarness: KeyedTwoInputStreamOperatorTestHarness[String, CRow, CRow, CRow] =
      new KeyedTwoInputStreamOperatorTestHarness[String, CRow, CRow, CRow](
        operator,
        new TupleRowKeySelector[String](1),
        new TupleRowKeySelector[String](1),
        Types.STRING,
        1, 1, 0)

    testHarness.open()

    testHarness.processElement1(new StreamRecord[CRow](
      CRow(Row.of(1L: JLong, "k1"), change = true), 0))
    testHarness.processElement2(new StreamRecord[CRow](
      CRow(Row.of(1L: JLong, "k2"), change = true), 0))

    assertEquals(2, testHarness.numEventTimeTimers())
    assertEquals(4, testHarness.numKeyedStateEntries())

    // The left row with timestamp = 1 will be padded and removed (14=1+5+1+((5+9)/2)).
    testHarness.processWatermark1(new Watermark(14))
    testHarness.processWatermark2(new Watermark(14))

    assertEquals(1, testHarness.numEventTimeTimers())
    assertEquals(2, testHarness.numKeyedStateEntries())

    // The right row with timestamp = 1 will be padded and removed (18=1+9+1+((5+9)/2)).
    testHarness.processWatermark1(new Watermark(18))
    testHarness.processWatermark2(new Watermark(18))

    assertEquals(0, testHarness.numEventTimeTimers())
    assertEquals(0, testHarness.numKeyedStateEntries())

    testHarness.processElement1(new StreamRecord[CRow](
      CRow(Row.of(2L: JLong, "k1"), change = true), 0))
    testHarness.processElement2(new StreamRecord[CRow](
      CRow(Row.of(2L: JLong, "k2"), change = true), 0))

    // The late rows with timestamp = 2 will not be cached, but a null padding result for the right
    // row will be emitted.
    assertEquals(0, testHarness.numKeyedStateEntries())
    assertEquals(0, testHarness.numEventTimeTimers())

    // Make sure the common (inner) join can be performed.
    testHarness.processElement1(new StreamRecord[CRow](
      CRow(Row.of(19L: JLong, "k1"), change = true), 0))
    testHarness.processElement1(new StreamRecord[CRow](
      CRow(Row.of(20L: JLong, "k1"), change = true), 0))
    testHarness.processElement2(new StreamRecord[CRow](
      CRow(Row.of(26L: JLong, "k1"), change = true), 0))
    testHarness.processElement2(new StreamRecord[CRow](
      CRow(Row.of(25L: JLong, "k1"), change = true), 0))
    testHarness.processElement1(new StreamRecord[CRow](
      CRow(Row.of(21L: JLong, "k1"), change = true), 0))

    testHarness.processElement2(new StreamRecord[CRow](
      CRow(Row.of(39L: JLong, "k2"), change = true), 0))
    testHarness.processElement2(new StreamRecord[CRow](
      CRow(Row.of(40L: JLong, "k2"), change = true), 0))
    testHarness.processElement1(new StreamRecord[CRow](
      CRow(Row.of(50L: JLong, "k2"), change = true), 0))
    testHarness.processElement1(new StreamRecord[CRow](
      CRow(Row.of(49L: JLong, "k2"), change = true), 0))
    testHarness.processElement2(new StreamRecord[CRow](
      CRow(Row.of(41L: JLong, "k2"), change = true), 0))

    testHarness.processWatermark1(new Watermark(100))
    testHarness.processWatermark2(new Watermark(100))

    val expectedOutput = new ConcurrentLinkedQueue[Object]()
    // The timestamp 14 is set with the triggered timer.
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1L: JLong, "k1", null: JLong, null: String), change = true), 14))
    expectedOutput.add(new Watermark(5))
    // The timestamp 18 is set with the triggered timer.
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(null: JLong, null: String, 1L: JLong, "k2": String), change = true), 18))
    expectedOutput.add(new Watermark(9))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(2L: JLong, "k1", null: JLong, null: String), change = true), 0))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(null: JLong, null: String, 2L: JLong, "k2": String), change = true), 0))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(20L: JLong, "k1", 25L: JLong, "k1": String), change = true), 0))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(21L: JLong, "k1", 25L: JLong, "k1": String), change = true), 0))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(21L: JLong, "k1", 26L: JLong, "k1": String), change = true), 0))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(49L: JLong, "k2", 40L: JLong, "k2": String), change = true), 0))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(49L: JLong, "k2", 41L: JLong, "k2": String), change = true), 0))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(50L: JLong, "k2", 41L: JLong, "k2": String), change = true), 0))
    // The timestamp 32 is set with the triggered timer.
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(19L: JLong, "k1", null: JLong, null: String), change = true), 32))
    // The timestamp 56 is set with the triggered timer.
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(null: JLong, null: String, 39L: JLong, "k2": String), change = true), 56))
    expectedOutput.add(new Watermark(91))

    val result = testHarness.getOutput
    verify(
      expectedOutput,
      result,
      new RowResultSortComparatorWithWatermarks(),
      checkWaterMark = true)
    testHarness.close()
  }

  @Test
  def testNonWindowInnerJoin() {

    val joinReturnType = CRowTypeInfo(new RowTypeInfo(
      Array[TypeInformation[_]](
        INT_TYPE_INFO,
        STRING_TYPE_INFO,
        INT_TYPE_INFO,
        STRING_TYPE_INFO),
      Array("a", "b", "c", "d")))

    val joinProcessFunc = new NonWindowInnerJoin(
      rowType,
      rowType,
      joinReturnType,
      "TestJoinFunction",
      funcCode,
      queryConfig)

    val operator: KeyedCoProcessOperator[Integer, CRow, CRow, CRow] =
      new KeyedCoProcessOperator[Integer, CRow, CRow, CRow](joinProcessFunc)
    val testHarness: KeyedTwoInputStreamOperatorTestHarness[Integer, CRow, CRow, CRow] =
      new KeyedTwoInputStreamOperatorTestHarness[Integer, CRow, CRow, CRow](
        operator,
        new TupleRowKeySelector[Integer](0),
        new TupleRowKeySelector[Integer](0),
        BasicTypeInfo.INT_TYPE_INFO,
        1, 1, 0)

    testHarness.open()

    // left stream input
    testHarness.setProcessingTime(1)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa"), change = true)))
    assertEquals(1, testHarness.numProcessingTimeTimers())
    assertEquals(2, testHarness.numKeyedStateEntries())
    testHarness.setProcessingTime(2)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa"), change = true)))
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(2: JInt, "bbb"), change = true)))
    assertEquals(2, testHarness.numProcessingTimeTimers())
    assertEquals(4, testHarness.numKeyedStateEntries())
    testHarness.setProcessingTime(3)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa"), change = true)))
    assertEquals(4, testHarness.numKeyedStateEntries())
    assertEquals(2, testHarness.numProcessingTimeTimers())

    // right stream input and output normally
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1: JInt, "Hi1"), change = true)))
    assertEquals(6, testHarness.numKeyedStateEntries())
    assertEquals(3, testHarness.numProcessingTimeTimers())
    testHarness.setProcessingTime(4)
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(2: JInt, "Hello1"), change = true)))
    assertEquals(8, testHarness.numKeyedStateEntries())
    assertEquals(4, testHarness.numProcessingTimeTimers())

    // expired left stream record with key value of 1
    testHarness.setProcessingTime(5)
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1: JInt, "Hi2"), change = true)))
    assertEquals(6, testHarness.numKeyedStateEntries())
    assertEquals(3, testHarness.numProcessingTimeTimers())

    // expired all left stream record
    testHarness.setProcessingTime(6)
    assertEquals(4, testHarness.numKeyedStateEntries())
    assertEquals(2, testHarness.numProcessingTimeTimers())

    // expired right stream record with key value of 2
    testHarness.setProcessingTime(8)
    assertEquals(2, testHarness.numKeyedStateEntries())
    assertEquals(1, testHarness.numProcessingTimeTimers())

    testHarness.setProcessingTime(10)
    assertTrue(testHarness.numKeyedStateEntries() > 0)
    // expired all right stream record
    testHarness.setProcessingTime(11)
    assertEquals(0, testHarness.numKeyedStateEntries())
    assertEquals(0, testHarness.numProcessingTimeTimers())

    val result = testHarness.getOutput

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa", 1: JInt, "Hi1"), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa", 1: JInt, "Hi1"), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa", 1: JInt, "Hi1"), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(2: JInt, "bbb", 2: JInt, "Hello1"), change = true)))

    verify(expectedOutput, result, new RowResultSortComparator())

    testHarness.close()
  }

  @Test
  def testNonWindowInnerJoinWithRetract() {

    val joinReturnType = CRowTypeInfo(new RowTypeInfo(
      Array[TypeInformation[_]](
        INT_TYPE_INFO,
        STRING_TYPE_INFO,
        INT_TYPE_INFO,
        STRING_TYPE_INFO),
      Array("a", "b", "c", "d")))

    val joinProcessFunc = new NonWindowInnerJoin(
      rowType,
      rowType,
      joinReturnType,
      "TestJoinFunction",
      funcCode,
      queryConfig)

    val operator: KeyedCoProcessOperator[Integer, CRow, CRow, CRow] =
      new KeyedCoProcessOperator[Integer, CRow, CRow, CRow](joinProcessFunc)
    val testHarness: KeyedTwoInputStreamOperatorTestHarness[Integer, CRow, CRow, CRow] =
      new KeyedTwoInputStreamOperatorTestHarness[Integer, CRow, CRow, CRow](
        operator,
        new TupleRowKeySelector[Integer](0),
        new TupleRowKeySelector[Integer](0),
        BasicTypeInfo.INT_TYPE_INFO,
        1, 1, 0)

    testHarness.open()

    // left stream input
    testHarness.setProcessingTime(1)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa"), change = true)))
    assertEquals(1, testHarness.numProcessingTimeTimers())
    assertEquals(2, testHarness.numKeyedStateEntries())
    testHarness.setProcessingTime(2)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa"), change = true)))
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(2: JInt, "bbb"), change = true)))
    assertEquals(2, testHarness.numProcessingTimeTimers())
    assertEquals(4, testHarness.numKeyedStateEntries())
    testHarness.setProcessingTime(3)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa"), change = false)))
    assertEquals(4, testHarness.numKeyedStateEntries())
    assertEquals(2, testHarness.numProcessingTimeTimers())

    // right stream input and output normally
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1: JInt, "Hi1"), change = true)))
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1: JInt, "Hi1"), change = false)))
    assertEquals(5, testHarness.numKeyedStateEntries())
    assertEquals(3, testHarness.numProcessingTimeTimers())
    testHarness.setProcessingTime(4)
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(2: JInt, "Hello1"), change = true)))
    assertEquals(7, testHarness.numKeyedStateEntries())
    assertEquals(4, testHarness.numProcessingTimeTimers())

    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa"), change = false)))
    // expired left stream record with key value of 1
    testHarness.setProcessingTime(5)
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1: JInt, "Hi2"), change = true)))
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1: JInt, "Hi2"), change = false)))
    assertEquals(5, testHarness.numKeyedStateEntries())
    assertEquals(3, testHarness.numProcessingTimeTimers())

    // expired all left stream record
    testHarness.setProcessingTime(6)
    assertEquals(3, testHarness.numKeyedStateEntries())
    assertEquals(2, testHarness.numProcessingTimeTimers())

    // expired right stream record with key value of 2
    testHarness.setProcessingTime(8)
    assertEquals(0, testHarness.numKeyedStateEntries())
    assertEquals(0, testHarness.numProcessingTimeTimers())

    val result = testHarness.getOutput

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa", 1: JInt, "Hi1"), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa", 1: JInt, "Hi1"), change = false)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(2: JInt, "bbb", 2: JInt, "Hello1"), change = true)))

    verify(expectedOutput, result, new RowResultSortComparator())

    testHarness.close()
  }

  @Test
  def testNonWindowLeftJoinWithoutNonEqualPred() {

    val joinReturnType = CRowTypeInfo(new RowTypeInfo(
      Array[TypeInformation[_]](
        INT_TYPE_INFO,
        STRING_TYPE_INFO,
        INT_TYPE_INFO,
        STRING_TYPE_INFO),
      Array("a", "b", "c", "d")))

    val joinProcessFunc = new NonWindowLeftRightJoin(
      rowType,
      rowType,
      joinReturnType,
      "TestJoinFunction",
      funcCode,
      true,
      queryConfig)

    val operator: KeyedCoProcessOperator[Integer, CRow, CRow, CRow] =
      new KeyedCoProcessOperator[Integer, CRow, CRow, CRow](joinProcessFunc)
    val testHarness: KeyedTwoInputStreamOperatorTestHarness[Integer, CRow, CRow, CRow] =
      new KeyedTwoInputStreamOperatorTestHarness[Integer, CRow, CRow, CRow](
        operator,
        new TupleRowKeySelector[Integer](0),
        new TupleRowKeySelector[Integer](0),
        BasicTypeInfo.INT_TYPE_INFO,
        1, 1, 0)

    testHarness.open()

    // left stream input
    testHarness.setProcessingTime(1)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa"), change = true)))
    assertEquals(1, testHarness.numProcessingTimeTimers())
    assertEquals(2, testHarness.numKeyedStateEntries())
    testHarness.setProcessingTime(2)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa"), change = true)))
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(2: JInt, "bbb"), change = true)))
    assertEquals(2, testHarness.numProcessingTimeTimers())
    assertEquals(4, testHarness.numKeyedStateEntries())
    testHarness.setProcessingTime(3)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa"), change = false)))
    assertEquals(4, testHarness.numKeyedStateEntries())
    assertEquals(2, testHarness.numProcessingTimeTimers())

    // right stream input and output normally
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1: JInt, "Hi1"), change = true)))
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1: JInt, "Hi1"), change = false)))
    assertEquals(5, testHarness.numKeyedStateEntries())
    assertEquals(3, testHarness.numProcessingTimeTimers())
    testHarness.setProcessingTime(4)
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(2: JInt, "Hello1"), change = true)))
    assertEquals(7, testHarness.numKeyedStateEntries())
    assertEquals(4, testHarness.numProcessingTimeTimers())

    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa"), change = false)))
    // expired left stream record with key value of 1
    testHarness.setProcessingTime(5)
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1: JInt, "Hi2"), change = true)))
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1: JInt, "Hi2"), change = false)))
    assertEquals(5, testHarness.numKeyedStateEntries())
    assertEquals(3, testHarness.numProcessingTimeTimers())

    // expired all left stream record
    testHarness.setProcessingTime(6)
    assertEquals(3, testHarness.numKeyedStateEntries())
    assertEquals(2, testHarness.numProcessingTimeTimers())

    // expired right stream record with key value of 2
    testHarness.setProcessingTime(8)
    assertEquals(0, testHarness.numKeyedStateEntries())
    assertEquals(0, testHarness.numProcessingTimeTimers())

    val result = testHarness.getOutput

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa", null: JInt, null), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa", null: JInt, null), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(2: JInt, "bbb", null: JInt, null), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa", null: JInt, null), change = false)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa", null: JInt, null), change = false)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa", 1: JInt, "Hi1"), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa", 1: JInt, "Hi1"), change = false)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa", null: JInt, null), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(2: JInt, "bbb", null: JInt, null), change = false)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(2: JInt, "bbb", 2: JInt, "Hello1"), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa", null: JInt, null), change = false)))

    verify(expectedOutput, result, new RowResultSortComparator())

    testHarness.close()
  }

  @Test
  def testNonWindowLeftJoinWithNonEqualPred() {

    val joinReturnType = CRowTypeInfo(new RowTypeInfo(
      Array[TypeInformation[_]](
        INT_TYPE_INFO,
        STRING_TYPE_INFO,
        INT_TYPE_INFO,
        STRING_TYPE_INFO),
      Array("a", "b", "c", "d")))

    val joinProcessFunc = new NonWindowLeftRightJoinWithNonEquiPredicates(
      rowType,
      rowType,
      joinReturnType,
      "TestJoinFunction",
      funcCodeWithNonEqualPred,
      true,
      queryConfig)

    val operator: KeyedCoProcessOperator[Integer, CRow, CRow, CRow] =
      new KeyedCoProcessOperator[Integer, CRow, CRow, CRow](joinProcessFunc)
    val testHarness: KeyedTwoInputStreamOperatorTestHarness[Integer, CRow, CRow, CRow] =
      new KeyedTwoInputStreamOperatorTestHarness[Integer, CRow, CRow, CRow](
        operator,
        new TupleRowKeySelector[Integer](0),
        new TupleRowKeySelector[Integer](0),
        BasicTypeInfo.INT_TYPE_INFO,
        1, 1, 0)

    testHarness.open()

    // left stream input
    testHarness.setProcessingTime(1)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa"), change = true)))
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa"), change = false)))
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1: JInt, "bbb"), change = true)))
    assertEquals(1, testHarness.numProcessingTimeTimers())
    // 1 left timer(5), 1 left key(1), 1 join cnt
    assertEquals(3, testHarness.numKeyedStateEntries())
    testHarness.setProcessingTime(2)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa"), change = true)))
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(2: JInt, "bbb"), change = true)))
    assertEquals(2, testHarness.numProcessingTimeTimers())
    // 2 left timer(5,6), 2 left key(1,2), 2 join cnt
    assertEquals(6, testHarness.numKeyedStateEntries())
    testHarness.setProcessingTime(3)

    // right stream input and output normally
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1: JInt, "Hi1"), change = true)))
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1: JInt, "bbb"), change = false)))
    // 2 left timer(5,6), 2 left keys(1,2), 2 join cnt, 1 right timer(7), 1 right key(1)
    assertEquals(8, testHarness.numKeyedStateEntries())
    assertEquals(3, testHarness.numProcessingTimeTimers())
    testHarness.setProcessingTime(4)
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(2: JInt, "ccc"), change = true)))
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(2: JInt, "Hello"), change = true)))
    // 2 left timer(5,6), 2 left keys(1,2), 2 join cnt, 2 right timer(7,8), 2 right key(1,2)
    assertEquals(10, testHarness.numKeyedStateEntries())
    assertEquals(4, testHarness.numProcessingTimeTimers())

    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa"), change = false)))
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1: JInt, "Hi2"), change = true)))
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1: JInt, "Hi2"), change = false)))
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1: JInt, "Hi1"), change = false)))
    // expired left stream record with key value of 1
    testHarness.setProcessingTime(5)
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1: JInt, "Hi3"), change = true)))
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1: JInt, "Hi3"), change = false)))
    // 1 left timer(6), 1 left keys(2), 1 join cnt, 2 right timer(7,8), 1 right key(2)
    assertEquals(6, testHarness.numKeyedStateEntries())
    assertEquals(3, testHarness.numProcessingTimeTimers())

    // expired all left stream record
    testHarness.setProcessingTime(6)
    assertEquals(3, testHarness.numKeyedStateEntries())
    assertEquals(2, testHarness.numProcessingTimeTimers())

    // expired right stream record with key value of 2
    testHarness.setProcessingTime(8)
    assertEquals(0, testHarness.numKeyedStateEntries())
    assertEquals(0, testHarness.numProcessingTimeTimers())

    val result = testHarness.getOutput

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa", null: JInt, null), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa", null: JInt, null), change = false)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "bbb", null: JInt, null), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa", null: JInt, null), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(2: JInt, "bbb", null: JInt, null), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "bbb", null: JInt, null), change = false)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa", null: JInt, null), change = false)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa", 1: JInt, "Hi1"), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "bbb", 1: JInt, "Hi1"), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(2: JInt, "bbb", null: JInt, null), change = false)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(2: JInt, "bbb", 2: JInt, "Hello"), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa", 1: JInt, "Hi1"), change = false)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "bbb", 1: JInt, "Hi2"), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "bbb", 1: JInt, "Hi2"), change = false)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "bbb", 1: JInt, "Hi1"), change = false)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "bbb", null: JInt, null), change = true)))
    verify(expectedOutput, result, new RowResultSortComparator())

    testHarness.close()
  }

  @Test
  def testNonWindowRightJoinWithoutNonEqualPred() {

    val joinReturnType = CRowTypeInfo(new RowTypeInfo(
      Array[TypeInformation[_]](
        INT_TYPE_INFO,
        STRING_TYPE_INFO,
        INT_TYPE_INFO,
        STRING_TYPE_INFO),
      Array("a", "b", "c", "d")))

    val joinProcessFunc = new NonWindowLeftRightJoin(
      rowType,
      rowType,
      joinReturnType,
      "TestJoinFunction",
      funcCode,
      false,
      queryConfig)

    val operator: KeyedCoProcessOperator[Integer, CRow, CRow, CRow] =
      new KeyedCoProcessOperator[Integer, CRow, CRow, CRow](joinProcessFunc)
    val testHarness: KeyedTwoInputStreamOperatorTestHarness[Integer, CRow, CRow, CRow] =
      new KeyedTwoInputStreamOperatorTestHarness[Integer, CRow, CRow, CRow](
        operator,
        new TupleRowKeySelector[Integer](0),
        new TupleRowKeySelector[Integer](0),
        BasicTypeInfo.INT_TYPE_INFO,
        1, 1, 0)

    testHarness.open()

    // right stream input
    testHarness.setProcessingTime(1)
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa"), change = true)))
    assertEquals(1, testHarness.numProcessingTimeTimers())
    assertEquals(2, testHarness.numKeyedStateEntries())
    testHarness.setProcessingTime(2)
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa"), change = true)))
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(2: JInt, "bbb"), change = true)))
    assertEquals(2, testHarness.numProcessingTimeTimers())
    assertEquals(4, testHarness.numKeyedStateEntries())
    testHarness.setProcessingTime(3)
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa"), change = false)))
    assertEquals(4, testHarness.numKeyedStateEntries())
    assertEquals(2, testHarness.numProcessingTimeTimers())

    // left stream input and output normally
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1: JInt, "Hi1"), change = true)))
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1: JInt, "Hi1"), change = false)))
    assertEquals(5, testHarness.numKeyedStateEntries())
    assertEquals(3, testHarness.numProcessingTimeTimers())
    testHarness.setProcessingTime(4)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(2: JInt, "Hello1"), change = true)))
    assertEquals(7, testHarness.numKeyedStateEntries())
    assertEquals(4, testHarness.numProcessingTimeTimers())

    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa"), change = false)))
    // expired right stream record with key value of 1
    testHarness.setProcessingTime(5)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1: JInt, "Hi2"), change = true)))
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1: JInt, "Hi2"), change = false)))
    assertEquals(5, testHarness.numKeyedStateEntries())
    assertEquals(3, testHarness.numProcessingTimeTimers())

    // expired all right stream record
    testHarness.setProcessingTime(6)
    assertEquals(3, testHarness.numKeyedStateEntries())
    assertEquals(2, testHarness.numProcessingTimeTimers())

    // expired left stream record with key value of 2
    testHarness.setProcessingTime(8)
    assertEquals(0, testHarness.numKeyedStateEntries())
    assertEquals(0, testHarness.numProcessingTimeTimers())

    val result = testHarness.getOutput

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    expectedOutput.add(new StreamRecord(
      CRow(Row.of(null: JInt, null, 1: JInt, "aaa"), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(null: JInt, null, 1: JInt, "aaa"), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(null: JInt, null, 2: JInt, "bbb"), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(null: JInt, null, 1: JInt, "aaa"), change = false)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(null: JInt, null, 1: JInt, "aaa"), change = false)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "Hi1", 1: JInt, "aaa"), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "Hi1", 1: JInt, "aaa"), change = false)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(null: JInt, null, 1: JInt, "aaa"), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(null: JInt, null, 2: JInt, "bbb"), change = false)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(2: JInt, "Hello1", 2: JInt, "bbb"), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(null: JInt, null, 1: JInt, "aaa"), change = false)))

    verify(expectedOutput, result, new RowResultSortComparator())

    testHarness.close()
  }

  @Test
  def testNonWindowRightJoinWithNonEqualPred() {

    val joinReturnType = CRowTypeInfo(new RowTypeInfo(
      Array[TypeInformation[_]](
        INT_TYPE_INFO,
        STRING_TYPE_INFO,
        INT_TYPE_INFO,
        STRING_TYPE_INFO),
      Array("a", "b", "c", "d")))

    val joinProcessFunc = new NonWindowLeftRightJoinWithNonEquiPredicates(
      rowType,
      rowType,
      joinReturnType,
      "TestJoinFunction",
      funcCodeWithNonEqualPred2,
      false,
      queryConfig)

    val operator: KeyedCoProcessOperator[Integer, CRow, CRow, CRow] =
      new KeyedCoProcessOperator[Integer, CRow, CRow, CRow](joinProcessFunc)
    val testHarness: KeyedTwoInputStreamOperatorTestHarness[Integer, CRow, CRow, CRow] =
      new KeyedTwoInputStreamOperatorTestHarness[Integer, CRow, CRow, CRow](
        operator,
        new TupleRowKeySelector[Integer](0),
        new TupleRowKeySelector[Integer](0),
        BasicTypeInfo.INT_TYPE_INFO,
        1, 1, 0)

    testHarness.open()

    // right stream input
    testHarness.setProcessingTime(1)
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa"), change = true)))
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa"), change = false)))
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1: JInt, "bbb"), change = true)))
    assertEquals(1, testHarness.numProcessingTimeTimers())
    // 1 right timer(5), 1 right key(1), 1 join cnt
    assertEquals(3, testHarness.numKeyedStateEntries())
    testHarness.setProcessingTime(2)
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa"), change = true)))
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(2: JInt, "bbb"), change = true)))
    assertEquals(2, testHarness.numProcessingTimeTimers())
    // 2 right timer(5,6), 2 right key(1,2), 2 join cnt
    assertEquals(6, testHarness.numKeyedStateEntries())
    testHarness.setProcessingTime(3)

    // left stream input and output normally
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1: JInt, "Hi1"), change = true)))
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1: JInt, "bbb"), change = false)))
    // 2 right timer(5,6), 2 right keys(1,2), 2 join cnt, 1 left timer(7), 1 left key(1)
    assertEquals(8, testHarness.numKeyedStateEntries())
    assertEquals(3, testHarness.numProcessingTimeTimers())
    testHarness.setProcessingTime(4)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(2: JInt, "ccc"), change = true)))
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(2: JInt, "Hello"), change = true)))
    // 2 right timer(5,6), 2 right keys(1,2), 2 join cnt, 2 left timer(7,8), 2 left key(1,2)
    assertEquals(10, testHarness.numKeyedStateEntries())
    assertEquals(4, testHarness.numProcessingTimeTimers())

    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa"), change = false)))
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1: JInt, "Hi2"), change = true)))
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1: JInt, "Hi2"), change = false)))
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1: JInt, "Hi1"), change = false)))
    // expired right stream record with key value of 1
    testHarness.setProcessingTime(5)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1: JInt, "Hi3"), change = true)))
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1: JInt, "Hi3"), change = false)))
    // 1 right timer(6), 1 right keys(2), 1 join cnt, 2 left timer(7,8), 1 left key(2)
    assertEquals(6, testHarness.numKeyedStateEntries())
    assertEquals(3, testHarness.numProcessingTimeTimers())

    // expired all right stream record
    testHarness.setProcessingTime(6)
    assertEquals(3, testHarness.numKeyedStateEntries())
    assertEquals(2, testHarness.numProcessingTimeTimers())

    // expired left stream record with key value of 2
    testHarness.setProcessingTime(8)
    assertEquals(0, testHarness.numKeyedStateEntries())
    assertEquals(0, testHarness.numProcessingTimeTimers())

    val result = testHarness.getOutput

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    expectedOutput.add(new StreamRecord(
      CRow(Row.of(null: JInt, null, 1: JInt, "aaa"), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(null: JInt, null, 1: JInt, "aaa"), change = false)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(null: JInt, null, 1: JInt, "bbb"), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(null: JInt, null, 1: JInt, "aaa"), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(null: JInt, null, 2: JInt, "bbb"), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(null: JInt, null, 1: JInt, "bbb"), change = false)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(null: JInt, null, 1: JInt, "aaa"), change = false)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "Hi1", 1: JInt, "aaa"), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "Hi1", 1: JInt, "bbb"), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(null: JInt, null, 2: JInt, "bbb"), change = false)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(2: JInt, "Hello", 2: JInt, "bbb"), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "Hi1", 1: JInt, "aaa"), change = false)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "Hi2", 1: JInt, "bbb"), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "Hi2", 1: JInt, "bbb"), change = false)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "Hi1", 1: JInt, "bbb"), change = false)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(null: JInt, null, 1: JInt, "bbb"), change = true)))
    verify(expectedOutput, result, new RowResultSortComparator())

    testHarness.close()
  }

  @Test
  def testNonWindowFullJoinWithoutNonEqualPred() {

    val joinReturnType = CRowTypeInfo(new RowTypeInfo(
      Array[TypeInformation[_]](
        INT_TYPE_INFO,
        STRING_TYPE_INFO,
        INT_TYPE_INFO,
        STRING_TYPE_INFO),
      Array("a", "b", "c", "d")))

    val joinProcessFunc = new NonWindowFullJoin(
      rowType,
      rowType,
      joinReturnType,
      "TestJoinFunction",
      funcCode,
      queryConfig)

    val operator: KeyedCoProcessOperator[Integer, CRow, CRow, CRow] =
      new KeyedCoProcessOperator[Integer, CRow, CRow, CRow](joinProcessFunc)
    val testHarness: KeyedTwoInputStreamOperatorTestHarness[Integer, CRow, CRow, CRow] =
      new KeyedTwoInputStreamOperatorTestHarness[Integer, CRow, CRow, CRow](
        operator,
        new TupleRowKeySelector[Integer](0),
        new TupleRowKeySelector[Integer](0),
        BasicTypeInfo.INT_TYPE_INFO,
        1, 1, 0)

    testHarness.open()

    // left stream input
    testHarness.setProcessingTime(1)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1: JInt, "bbb"), change = true)))
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1: JInt, "ccc"), change = true)))
    assertEquals(1, testHarness.numProcessingTimeTimers())
    // 1 left timer(5), 1 left key(1)
    assertEquals(2, testHarness.numKeyedStateEntries())

    testHarness.setProcessingTime(2)
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(2: JInt, "bbb"), change = true)))
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(2: JInt, "ccc"), change = true)))
    assertEquals(2, testHarness.numProcessingTimeTimers())
    // 1 left timer(5), 1 left key(1)
    // 1 right timer(6), 1 right key(1)
    assertEquals(4, testHarness.numKeyedStateEntries())

    testHarness.setProcessingTime(3)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(2: JInt, "aaa"), change = true)))
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(2: JInt, "ddd"), change = true)))
    assertEquals(3, testHarness.numProcessingTimeTimers())
    // 2 left timer(5,7), 2 left key(1,2)
    // 1 right timer(6), 1 right key(1)
    assertEquals(6, testHarness.numKeyedStateEntries())
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa"), change = true)))
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1: JInt, "ddd"), change = true)))
    assertEquals(4, testHarness.numProcessingTimeTimers())
    // 2 left timer(5,7), 2 left key(1,2)
    // 2 right timer(6,7), 2 right key(1,2)
    assertEquals(8, testHarness.numKeyedStateEntries())

    testHarness.setProcessingTime(4)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(2: JInt, "aaa"), change = false)))
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(2: JInt, "ddd"), change = false)))
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa"), change = false)))
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1: JInt, "ddd"), change = false)))
    assertEquals(4, testHarness.numProcessingTimeTimers())
    // 2 left timer(5,7), 1 left key(1)
    // 2 right timer(6,7), 1 right key(2)
    assertEquals(6, testHarness.numKeyedStateEntries())

    testHarness.setProcessingTime(5)
    assertEquals(3, testHarness.numProcessingTimeTimers())
    // 1 left timer(7)
    // 2 right timer(6,7), 1 right key(2)
    assertEquals(4, testHarness.numKeyedStateEntries())

    testHarness.setProcessingTime(6)
    assertEquals(2, testHarness.numProcessingTimeTimers())
    // 1 left timer(7)
    // 2 right timer(7)
    assertEquals(2, testHarness.numKeyedStateEntries())

    testHarness.setProcessingTime(7)
    assertEquals(0, testHarness.numProcessingTimeTimers())
    assertEquals(0, testHarness.numKeyedStateEntries())

    testHarness.setProcessingTime(8)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1: JInt, "bbb"), change = true)))
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(2: JInt, "bbb"), change = true)))

    val result = testHarness.getOutput
    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    // processing time 1
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "bbb", null: JInt, null), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "ccc", null: JInt, null), change = true)))
    // processing time 2
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(null: JInt, null, 2: JInt, "bbb"), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(null: JInt, null, 2: JInt, "ccc"), change = true)))
    // processing time 3
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(null: JInt, null, 2: JInt, "bbb"), change = false)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(null: JInt, null, 2: JInt, "ccc"), change = false)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(2: JInt, "aaa", 2: JInt, "bbb"), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(2: JInt, "aaa", 2: JInt, "ccc"), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(2: JInt, "ddd", 2: JInt, "bbb"), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(2: JInt, "ddd", 2: JInt, "ccc"), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "bbb", null: JInt, null), change = false)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "ccc", null: JInt, null), change = false)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "bbb", 1: JInt, "aaa"), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "ccc", 1: JInt, "aaa"), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "bbb", 1: JInt, "ddd"), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "ccc", 1: JInt, "ddd"), change = true)))
    // processing time 4
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(2: JInt, "aaa", 2: JInt, "bbb"), change = false)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(2: JInt, "aaa", 2: JInt, "ccc"), change = false)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(2: JInt, "ddd", 2: JInt, "bbb"), change = false)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(2: JInt, "ddd", 2: JInt, "ccc"), change = false)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(null: JInt, null, 2: JInt, "bbb"), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(null: JInt, null, 2: JInt, "ccc"), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "bbb", 1: JInt, "aaa"), change = false)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "ccc", 1: JInt, "aaa"), change = false)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "bbb", 1: JInt, "ddd"), change = false)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "ccc", 1: JInt, "ddd"), change = false)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "bbb", null: JInt, null), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "ccc", null: JInt, null), change = true)))
    // processing time 8
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "bbb", null: JInt, null), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(null: JInt, null, 2: JInt, "bbb"), change = true)))

    verify(expectedOutput, result, new RowResultSortComparator())
    testHarness.close()
  }

  @Test
  def testNonWindowFullJoinWithNonEqualPred() {

    val joinReturnType = CRowTypeInfo(new RowTypeInfo(
      Array[TypeInformation[_]](
        INT_TYPE_INFO,
        STRING_TYPE_INFO,
        INT_TYPE_INFO,
        STRING_TYPE_INFO),
      Array("a", "b", "c", "d")))

    val joinProcessFunc = new NonWindowFullJoinWithNonEquiPredicates(
      rowType,
      rowType,
      joinReturnType,
      "TestJoinFunction",
      funcCodeWithNonEqualPred2,
      queryConfig)

    val operator: KeyedCoProcessOperator[Integer, CRow, CRow, CRow] =
      new KeyedCoProcessOperator[Integer, CRow, CRow, CRow](joinProcessFunc)
    val testHarness: KeyedTwoInputStreamOperatorTestHarness[Integer, CRow, CRow, CRow] =
      new KeyedTwoInputStreamOperatorTestHarness[Integer, CRow, CRow, CRow](
        operator,
        new TupleRowKeySelector[Integer](0),
        new TupleRowKeySelector[Integer](0),
        BasicTypeInfo.INT_TYPE_INFO,
        1, 1, 0)

    testHarness.open()

    // left stream input
    testHarness.setProcessingTime(1)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1: JInt, "bbb"), change = true)))
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1: JInt, "ccc"), change = true)))
    assertEquals(1, testHarness.numProcessingTimeTimers())
    // 1 left timer(5), 1 left key(1), 1 left joincnt key(1)
    assertEquals(3, testHarness.numKeyedStateEntries())

    testHarness.setProcessingTime(2)
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(2: JInt, "bbb"), change = true)))
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(2: JInt, "ccc"), change = true)))
    assertEquals(2, testHarness.numProcessingTimeTimers())
    // 1 left timer(5), 1 left key(1), 1 left joincnt key(1)
    // 1 right timer(6), 1 right key(1), 1 right joincnt key(1)
    assertEquals(6, testHarness.numKeyedStateEntries())

    testHarness.setProcessingTime(3)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(2: JInt, "aaa"), change = true)))
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(2: JInt, "ddd"), change = true)))
    assertEquals(3, testHarness.numProcessingTimeTimers())
    // 2 left timer(5,7), 2 left key(1,2), 2 left joincnt key(1,2)
    // 1 right timer(6), 1 right key(1), 1 right joincnt key(1)
    assertEquals(9, testHarness.numKeyedStateEntries())
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa"), change = true)))
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1: JInt, "ddd"), change = true)))
    assertEquals(4, testHarness.numProcessingTimeTimers())
    // 2 left timer(5,7), 2 left key(1,2), 2 left joincnt key(1,2)
    // 2 right timer(6,7), 2 right key(1,2), 2 right joincnt key(1,2)
    assertEquals(12, testHarness.numKeyedStateEntries())

    testHarness.setProcessingTime(4)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(2: JInt, "aaa"), change = false)))
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1: JInt, "ddd"), change = false)))
    assertEquals(4, testHarness.numProcessingTimeTimers())
    // 2 left timer(5,7), 2 left key(1,2), 2 left joincnt key(1,2)
    // 2 right timer(6,7), 2 right key(1,2), 2 right joincnt key(1,2)
    assertEquals(12, testHarness.numKeyedStateEntries())

    testHarness.setProcessingTime(5)
    assertEquals(3, testHarness.numProcessingTimeTimers())
    // 1 left timer(7), 1 left key(2), 1 left joincnt key(2)
    // 2 right timer(6,7), 2 right key(1,2), 2 right joincnt key(1,2)
    assertEquals(9, testHarness.numKeyedStateEntries())

    testHarness.setProcessingTime(6)
    assertEquals(2, testHarness.numProcessingTimeTimers())
    // 1 left timer(7), 1 left key(2), 1 left joincnt key(2)
    // 1 right timer(7), 1 right key(2), 1 right joincnt key(2)
    assertEquals(6, testHarness.numKeyedStateEntries())

    testHarness.setProcessingTime(7)
    assertEquals(0, testHarness.numProcessingTimeTimers())
    assertEquals(0, testHarness.numKeyedStateEntries())

    testHarness.setProcessingTime(8)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1: JInt, "bbb"), change = true)))
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(2: JInt, "bbb"), change = true)))

    val result = testHarness.getOutput
    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    // processing time 1
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "bbb", null: JInt, null), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "ccc", null: JInt, null), change = true)))
    // processing time 2
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(null: JInt, null, 2: JInt, "bbb"), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(null: JInt, null, 2: JInt, "ccc"), change = true)))
    // processing time 3
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(null: JInt, null, 2: JInt, "bbb"), change = false)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(null: JInt, null, 2: JInt, "ccc"), change = false)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(2: JInt, "aaa", 2: JInt, "bbb"), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(2: JInt, "aaa", 2: JInt, "ccc"), change = true)))
    // can not find matched row due to NonEquiJoinPred
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(2: JInt, "ddd", null: JInt, null), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "bbb", null: JInt, null), change = false)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "ccc", null: JInt, null), change = false)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "bbb", 1: JInt, "ddd"), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "ccc", 1: JInt, "ddd"), change = true)))
    // can not find matched row due to NonEquiJoinPred
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(null: JInt, null, 1: JInt, "aaa"), change = true)))
    // processing time 4
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(2: JInt, "aaa", 2: JInt, "bbb"), change = false)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(2: JInt, "aaa", 2: JInt, "ccc"), change = false)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(null: JInt, null, 2: JInt, "bbb"), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(null: JInt, null, 2: JInt, "ccc"), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "bbb", 1: JInt, "ddd"), change = false)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "ccc", 1: JInt, "ddd"), change = false)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "bbb", null: JInt, null), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "ccc", null: JInt, null), change = true)))
    // processing time 8
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "bbb", null: JInt, null), change = true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(null: JInt, null, 2: JInt, "bbb"), change = true)))

    verify(expectedOutput, result, new RowResultSortComparator())
    testHarness.close()
  }
}
