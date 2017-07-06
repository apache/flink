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
import java.lang.{Integer => JInt}

import org.apache.flink.api.common.functions.FlatJoinFunction
import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.util.{KeyedTwoInputStreamOperatorTestHarness, TwoInputStreamOperatorTestHarness}
import org.apache.flink.table.codegen.GeneratedFunction
import org.apache.flink.table.runtime.harness.HarnessTestBase.{RowResultSortComparator, TupleRowKeySelector}
import org.apache.flink.table.runtime.join.ProcTimeWindowInnerJoin
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.types.Row
import org.junit.Test


class JoinHarnessTest extends HarnessTestBase{

  private val rT = new RowTypeInfo(Array[TypeInformation[_]](
    INT_TYPE_INFO,
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
       1,1,0)

    testHarness.open()

    // left stream input
    testHarness.setProcessingTime(1)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa"), true), 1))
    assert(testHarness.numProcessingTimeTimers() == 1)
    testHarness.setProcessingTime(2)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(2: JInt, "bbb"), true), 2))
    assert(testHarness.numProcessingTimeTimers() == 2)
    testHarness.setProcessingTime(3)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa2"), true), 3))
    assert(testHarness.numKeyedStateEntries() == 4)
    assert(testHarness.numProcessingTimeTimers() == 2)

    // right stream input and output normally
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1: JInt, "Hi1"), true), 3))
    testHarness.setProcessingTime(4)
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(2: JInt, "Hello1"), true), 4))
    assert(testHarness.numKeyedStateEntries() == 8)
    assert(testHarness.numProcessingTimeTimers() == 4)

    // expired left stream record at timestamp 1
    testHarness.setProcessingTime(12)
    assert(testHarness.numKeyedStateEntries() == 8)
    assert(testHarness.numProcessingTimeTimers() == 4)
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1: JInt, "Hi2"), true), 12))

    // expired right stream record at timestamp 4 and all left stream
    testHarness.setProcessingTime(25)
    assert(testHarness.numKeyedStateEntries() == 2)
    assert(testHarness.numProcessingTimeTimers() == 1)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa3"), true), 25))
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(2: JInt, "bbb2"), true), 25))
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(2: JInt, "Hello2"), true), 25))

    testHarness.setProcessingTime(45)
    assert(testHarness.numKeyedStateEntries() > 0)
    testHarness.setProcessingTime(46)
    assert(testHarness.numKeyedStateEntries() == 0)
    val result = testHarness.getOutput

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa", 1: JInt, "Hi1"), true), 3))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa2", 1: JInt, "Hi1"), true), 3))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(2: JInt, "bbb", 2: JInt, "Hello1"), true), 4))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa2", 1: JInt, "Hi2"), true), 12))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa3", 1: JInt, "Hi2"), true), 25))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(2: JInt, "bbb2", 2: JInt, "Hello2"), true), 25))

    verify(expectedOutput, result, new RowResultSortComparator(6))

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
        1,1,0)

    testHarness.open()

    testHarness.setProcessingTime(1)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa1"), true), 1))
    testHarness.setProcessingTime(2)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(2: JInt, "aaa2"), true), 2))
    testHarness.setProcessingTime(3)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa3"), true), 3))
    assert(testHarness.numKeyedStateEntries() == 4)
    assert(testHarness.numProcessingTimeTimers() == 2)

    // Do not store b elements
    // not meet a.proctime <= b.proctime - 5
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1: JInt, "bbb3"), true), 3))
    assert(testHarness.numKeyedStateEntries() == 4)
    assert(testHarness.numProcessingTimeTimers() == 2)

    // meet a.proctime <= b.proctime - 5
    testHarness.setProcessingTime(7)
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(2: JInt, "bbb7"), true), 7))
    assert(testHarness.numKeyedStateEntries() == 4)
    assert(testHarness.numProcessingTimeTimers() == 2)

    // expire record of stream a at timestamp 1
    testHarness.setProcessingTime(12)
    assert(testHarness.numKeyedStateEntries() == 4)
    assert(testHarness.numProcessingTimeTimers() == 2)
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1: JInt, "bbb12"), true), 12))

    testHarness.setProcessingTime(13)
    assert(testHarness.numKeyedStateEntries() == 2)
    assert(testHarness.numProcessingTimeTimers() == 1)

    testHarness.setProcessingTime(14)
    assert(testHarness.numKeyedStateEntries() == 0)
    assert(testHarness.numProcessingTimeTimers() == 0)
    val result = testHarness.getOutput

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    expectedOutput.add(new StreamRecord(
      CRow(Row.of(2: JInt, "aaa2", 2: JInt, "bbb7"), true), 7))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa3", 1: JInt, "bbb12"), true), 12))

    verify(expectedOutput, result, new RowResultSortComparator(6))

    testHarness.close()
  }

}
