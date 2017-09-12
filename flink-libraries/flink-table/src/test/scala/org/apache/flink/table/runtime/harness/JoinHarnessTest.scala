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

import java.util.concurrent.ConcurrentLinkedQueue
import java.lang.{Long => JLong}

import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness
import org.apache.flink.table.runtime.harness.HarnessTestBase.{RowResultSortComparator, TupleRowKeySelector}
import org.apache.flink.table.runtime.join.{ProcTimeBoundedStreamInnerJoin, ProcTimeWindowInnerJoin, RowTimeBoundedStreamInnerJoin}
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.types.Row
import org.junit.Test
import org.junit.Assert.{assertEquals, assertTrue}

class JoinHarnessTest extends HarnessTestBase{

  private val rT = new RowTypeInfo(Array[TypeInformation[_]](
    LONG_TYPE_INFO,
    STRING_TYPE_INFO),
    Array("a", "b"))


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
  def testNormalProcTimeJoin() {

    val joinProcessFunc = new ProcTimeWindowInnerJoin(-10, 20, rT, rT, "TestJoinFunction", funcCode)

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
      CRow(Row.of(1L: JLong, "aaa"), true), 1))
    assertEquals(1, testHarness.numProcessingTimeTimers())
    testHarness.setProcessingTime(2)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(2L: JLong, "bbb"), true), 2))
    assertEquals(2, testHarness.numProcessingTimeTimers())
    testHarness.setProcessingTime(3)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1L: JLong, "aaa2"), true), 3))
    assertEquals(4, testHarness.numKeyedStateEntries())
    assertEquals(2, testHarness.numProcessingTimeTimers())

    // right stream input and output normally
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1L: JLong, "Hi1"), true), 3))
    testHarness.setProcessingTime(4)
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(2L: JLong, "Hello1"), true), 4))
    assertEquals(8, testHarness.numKeyedStateEntries())
    assertEquals(4, testHarness.numProcessingTimeTimers())

    // expired left stream record at timestamp 1
    testHarness.setProcessingTime(12)
    assertEquals(8, testHarness.numKeyedStateEntries())
    assertEquals(4, testHarness.numProcessingTimeTimers())
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1L: JLong, "Hi2"), true), 12))

    // expired right stream record at timestamp 4 and all left stream
    testHarness.setProcessingTime(25)
    assertEquals(2, testHarness.numKeyedStateEntries())
    assertEquals(1, testHarness.numProcessingTimeTimers())
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1L: JLong, "aaa3"), true), 25))
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(2L: JLong, "bbb2"), true), 25))
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(2L: JLong, "Hello2"), true), 25))

    testHarness.setProcessingTime(45)
    assertTrue(testHarness.numKeyedStateEntries() > 0)
    testHarness.setProcessingTime(46)
    assertEquals(0, testHarness.numKeyedStateEntries())
    val result = testHarness.getOutput

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1L: JLong, "aaa", 1L: JLong, "Hi1"), true), 3))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1L: JLong, "aaa2", 1L: JLong, "Hi1"), true), 3))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(2L: JLong, "bbb", 2L: JLong, "Hello1"), true), 4))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1L: JLong, "aaa2", 1L: JLong, "Hi2"), true), 12))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1L: JLong, "aaa3", 1L: JLong, "Hi2"), true), 25))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(2L: JLong, "bbb2", 2L: JLong, "Hello2"), true), 25))

    verify(expectedOutput, result, new RowResultSortComparator())

    testHarness.close()
  }

  /** a.proctime >= b.proctime - 10 and a.proctime <= b.proctime - 5 **/
  @Test
  def testProcTimeJoinSingleNeedStore() {

    val joinProcessFunc = new ProcTimeWindowInnerJoin(-10, -5, rT, rT, "TestJoinFunction", funcCode)

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

    testHarness.setProcessingTime(1)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1L: JLong, "aaa1"), true), 1))
    testHarness.setProcessingTime(2)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(2L: JLong, "aaa2"), true), 2))
    testHarness.setProcessingTime(3)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1L: JLong, "aaa3"), true), 3))
    assertEquals(4, testHarness.numKeyedStateEntries())
    assertEquals(2, testHarness.numProcessingTimeTimers())

    // Do not store b elements
    // not meet a.proctime <= b.proctime - 5
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1L: JLong, "bbb3"), true), 3))
    assertEquals(4, testHarness.numKeyedStateEntries())
    assertEquals(2, testHarness.numProcessingTimeTimers())

    // meet a.proctime <= b.proctime - 5
    testHarness.setProcessingTime(7)
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(2L: JLong, "bbb7"), true), 7))
    assertEquals(4, testHarness.numKeyedStateEntries())
    assertEquals(2, testHarness.numProcessingTimeTimers())

    // expire record of stream a at timestamp 1
    testHarness.setProcessingTime(12)
    assertEquals(4, testHarness.numKeyedStateEntries())
    assertEquals(2, testHarness.numProcessingTimeTimers())
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1L: JLong, "bbb12"), true), 12))

    testHarness.setProcessingTime(13)
    assertEquals(2, testHarness.numKeyedStateEntries())
    assertEquals(1, testHarness.numProcessingTimeTimers())

    // state must be cleaned after the window timer interval has passed without new rows.
    testHarness.setProcessingTime(23)
    assertEquals(0, testHarness.numKeyedStateEntries())
    assertEquals(0, testHarness.numProcessingTimeTimers())
    val result = testHarness.getOutput

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    expectedOutput.add(new StreamRecord(
      CRow(Row.of(2L: JLong, "aaa2", 2L: JLong, "bbb7"), true), 7))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1L: JLong, "aaa3", 1L: JLong, "bbb12"), true), 12))

    verify(expectedOutput, result, new RowResultSortComparator())

    testHarness.close()
  }

  /** a.proctime >= b.proctime - 10 and a.proctime <= b.proctime + 20 **/
  @Test
  def testNewNormalProcTimeJoin() {

    val joinProcessFunc = new ProcTimeBoundedStreamInnerJoin(
      -10, 20, 0, rT, rT, "TestJoinFunction", funcCode)

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
      CRow(Row.of(1L: JLong, "aaa"), true), 1))
    assertEquals(1, testHarness.numProcessingTimeTimers())
    testHarness.setProcessingTime(2)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(2L: JLong, "bbb"), true), 2))
    assertEquals(2, testHarness.numProcessingTimeTimers())
    testHarness.setProcessingTime(3)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1L: JLong, "aaa2"), true), 3))
    assertEquals(4, testHarness.numKeyedStateEntries())
    assertEquals(2, testHarness.numProcessingTimeTimers())

    // right stream input and output normally
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1L: JLong, "Hi1"), true), 3))
    testHarness.setProcessingTime(4)
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(2L: JLong, "Hello1"), true), 4))
    assertEquals(8, testHarness.numKeyedStateEntries())
    assertEquals(4, testHarness.numProcessingTimeTimers())

    // expired left stream record at timestamp 1
    testHarness.setProcessingTime(12)
    assertEquals(8, testHarness.numKeyedStateEntries())
    assertEquals(4, testHarness.numProcessingTimeTimers())
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1L: JLong, "Hi2"), true), 12))

    // expired right stream record at timestamp 4 and all left stream
    testHarness.setProcessingTime(25)
    assertEquals(8, testHarness.numKeyedStateEntries())
    assertEquals(4, testHarness.numProcessingTimeTimers())
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1L: JLong, "aaa3"), true), 25))
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(2L: JLong, "bbb2"), true), 25))
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(2L: JLong, "Hello2"), true), 25))

    testHarness.setProcessingTime(45)
    assertTrue(testHarness.numKeyedStateEntries() > 0)
    testHarness.setProcessingTime(46)
    assertEquals(2, testHarness.numKeyedStateEntries())
    val result = testHarness.getOutput

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1L: JLong, "aaa", 1L: JLong, "Hi1"), true), 3))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1L: JLong, "aaa2", 1L: JLong, "Hi1"), true), 3))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(2L: JLong, "bbb", 2L: JLong, "Hello1"), true), 4))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1L: JLong, "aaa2", 1L: JLong, "Hi2"), true), 12))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1L: JLong, "aaa3", 1L: JLong, "Hi2"), true), 25))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(2L: JLong, "bbb2", 2L: JLong, "Hello2"), true), 25))

    verify(expectedOutput, result, new RowResultSortComparator())

    testHarness.close()
  }

  /** a.proctime >= b.proctime - 10 and a.proctime <= b.proctime - 5 **/
  @Test
  def testNewProcTimeJoinSingleNeedStore() {

    val joinProcessFunc = new ProcTimeBoundedStreamInnerJoin(
      -10, -5, 0, rT, rT, "TestJoinFunction", funcCode)

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

    testHarness.setProcessingTime(1)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1L: JLong, "aaa1"), true), 1))
    testHarness.setProcessingTime(2)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(2L: JLong, "aaa2"), true), 2))
    testHarness.setProcessingTime(3)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1L: JLong, "aaa3"), true), 3))
    assertEquals(4, testHarness.numKeyedStateEntries())
    assertEquals(2, testHarness.numProcessingTimeTimers())

    // all the records should be stored
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1L: JLong, "bbb3"), true), 3))
    assertEquals(6, testHarness.numKeyedStateEntries())
    assertEquals(3, testHarness.numProcessingTimeTimers())

    // meet a.proctime <= b.proctime - 5
    testHarness.setProcessingTime(7)
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(2L: JLong, "bbb7"), true), 7))
    assertEquals(6, testHarness.numKeyedStateEntries())
    assertEquals(3, testHarness.numProcessingTimeTimers())

    // expire record of stream a at timestamp 1
    testHarness.setProcessingTime(12)
    assertEquals(4, testHarness.numKeyedStateEntries())
    assertEquals(2, testHarness.numProcessingTimeTimers())
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1L: JLong, "bbb12"), true), 12))

    testHarness.setProcessingTime(13)
    assertEquals(4, testHarness.numKeyedStateEntries())
    assertEquals(2, testHarness.numProcessingTimeTimers())

    // state must be cleaned after the window timer interval has passed without new rows.
    testHarness.setProcessingTime(23)
    assertEquals(0, testHarness.numKeyedStateEntries())
    assertEquals(0, testHarness.numProcessingTimeTimers())
    val result = testHarness.getOutput

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    expectedOutput.add(new StreamRecord(
      CRow(Row.of(2L: JLong, "aaa2", 2L: JLong, "bbb7"), true), 7))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1L: JLong, "aaa3", 1L: JLong, "bbb12"), true), 12))

    verify(expectedOutput, result, new RowResultSortComparator())

    testHarness.close()
  }

  /** a.c1 >= b.rowtime - 10 and a.rowtime <= b.rowtime + 20 **/
  @Test
  def testCommonRowTimeJoin() {

    val joinProcessFunc = new RowTimeBoundedStreamInnerJoin(
      -10, 20, 0, rT, rT, "TestJoinFunction", funcCode, 0, 0)

    val operator: KeyedCoProcessOperator[String, CRow, CRow, CRow] =
      new KeyedCoProcessOperator[String, CRow, CRow, CRow](joinProcessFunc)
    val testHarness: KeyedTwoInputStreamOperatorTestHarness[String, CRow, CRow, CRow] =
      new KeyedTwoInputStreamOperatorTestHarness[String, CRow, CRow, CRow](
        operator,
        new TupleRowKeySelector[String](1),
        new TupleRowKeySelector[String](1),
        BasicTypeInfo.STRING_TYPE_INFO,
        1, 1, 0)

    testHarness.open()

    // Advance
    testHarness.processWatermark1(new Watermark(1))
    testHarness.processWatermark2(new Watermark(1))

    // Test late data
    testHarness.processElement1(new StreamRecord[CRow](
      CRow(Row.of(1L: JLong, "k1"), true), 0))

    assertEquals(0, testHarness.numEventTimeTimers())

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

    testHarness.processWatermark1(new Watermark(38))
    testHarness.processWatermark2(new Watermark(38))

    testHarness.processElement1(new StreamRecord[CRow](
      CRow(Row.of(40L: JLong, "k2"), true), 0))
    testHarness.processElement2(new StreamRecord[CRow](
      CRow(Row.of(39L: JLong, "k2"), true), 0))

    assertEquals(6, testHarness.numKeyedStateEntries())

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

    val result = testHarness.getOutput
    verify(expectedOutput, result, new RowResultSortComparator())
    testHarness.close()
  }

  /** a.rowtime >= b.rowtime - 10 and a.rowtime <= b.rowtime - 7 **/
  @Test
  def testNegativeRowTimeJoin() {

    val joinProcessFunc = new RowTimeBoundedStreamInnerJoin(
      -10, -7, 0, rT, rT, "TestJoinFunction", funcCode, 0, 0)

    val operator: KeyedCoProcessOperator[String, CRow, CRow, CRow] =
      new KeyedCoProcessOperator[String, CRow, CRow, CRow](joinProcessFunc)
    val testHarness: KeyedTwoInputStreamOperatorTestHarness[String, CRow, CRow, CRow] =
      new KeyedTwoInputStreamOperatorTestHarness[String, CRow, CRow, CRow](
        operator,
        new TupleRowKeySelector[String](1),
        new TupleRowKeySelector[String](1),
        BasicTypeInfo.STRING_TYPE_INFO,
        1, 1, 0)

    testHarness.open()

    // Advance
    testHarness.processWatermark1(new Watermark(1))
    testHarness.processWatermark2(new Watermark(1))

    testHarness.processElement2(new StreamRecord[CRow](
      CRow(Row.of(2L: JLong, "k1"), true), 0))

    testHarness.processWatermark1(new Watermark(2))
    testHarness.processWatermark2(new Watermark(2))

    assertEquals(0, testHarness.numKeyedStateEntries())

    testHarness.processElement1(new StreamRecord[CRow](
      CRow(Row.of(3L: JLong, "k1"), true), 0))
    testHarness.processElement2(new StreamRecord[CRow](
      CRow(Row.of(3L: JLong, "k1"), true), 0))

    testHarness.processElement2(new StreamRecord[CRow](
      CRow(Row.of(13L: JLong, "k1"), true), 0))
    testHarness.processElement1(new StreamRecord[CRow](
      CRow(Row.of(6L: JLong, "k1"), true), 0))

    assertEquals(4, testHarness.numKeyedStateEntries())

    testHarness.processWatermark1(new Watermark(10))
    testHarness.processWatermark2(new Watermark(10))

    assertEquals(2, testHarness.numKeyedStateEntries())

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
