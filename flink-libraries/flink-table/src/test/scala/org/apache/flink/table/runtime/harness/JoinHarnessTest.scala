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
import org.apache.flink.table.runtime.join.ProcTimeInnerJoin
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

  @Test
  def testProcTimeJoin() {

    val joinProcessFunc = new ProcTimeInnerJoin(10, 20, rT, rT, "TestJoinFunction", funcCode)

    val operator: KeyedCoProcessOperator[Integer, CRow, CRow, CRow] =
      new KeyedCoProcessOperator[Integer, CRow, CRow, CRow](joinProcessFunc)
    val testHarness: TwoInputStreamOperatorTestHarness[CRow, CRow, CRow] =
      new KeyedTwoInputStreamOperatorTestHarness[Integer, CRow, CRow, CRow](
       operator,
       new TupleRowKeySelector[Integer](0),
       new TupleRowKeySelector[Integer](0),
       BasicTypeInfo.INT_TYPE_INFO,
       1,1,0)

    testHarness.open()

    testHarness.setProcessingTime(1)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa"), true), 1))
    testHarness.setProcessingTime(2)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(2: JInt, "bbb"), true), 2))
    testHarness.setProcessingTime(3)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa2"), true), 3))

    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1: JInt, "Hi1"), true), 3))
    testHarness.setProcessingTime(4)
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(2: JInt, "Hello1"), true), 4))

    testHarness.setProcessingTime(12)
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1: JInt, "Hi2"), true), 12))

    testHarness.setProcessingTime(25)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa3"), true), 25))
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(2: JInt, "bbb2"), true), 25))
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(2: JInt, "Hello2"), true), 25))
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

  @Test
  def testProcTimeJoin2() {

    val joinProcessFunc = new ProcTimeInnerJoin(0, 10, rT, rT, "TestJoinFunction", funcCode)

    val operator: KeyedCoProcessOperator[Integer, CRow, CRow, CRow] =
      new KeyedCoProcessOperator[Integer, CRow, CRow, CRow](joinProcessFunc)
    val testHarness: TwoInputStreamOperatorTestHarness[CRow, CRow, CRow] =
      new KeyedTwoInputStreamOperatorTestHarness[Integer, CRow, CRow, CRow](
        operator,
        new TupleRowKeySelector[Integer](0),
        new TupleRowKeySelector[Integer](0),
        BasicTypeInfo.INT_TYPE_INFO,
        1,1,0)

    testHarness.open()

    testHarness.setProcessingTime(1)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa"), true), 1))
    testHarness.setProcessingTime(2)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(2: JInt, "bbb"), true), 2))
    testHarness.setProcessingTime(3)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa2"), true), 3))

    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(1: JInt, "Hi1"), true), 3))
    testHarness.setProcessingTime(4)
    testHarness.processElement2(new StreamRecord(
      CRow(Row.of(2: JInt, "Hello1"), true), 4))

    testHarness.setProcessingTime(10)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa3"), true), 10))
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(2: JInt, "bbb2"), true), 10))

    testHarness.setProcessingTime(15)
    testHarness.processElement1(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa4"), true), 15))
    val result = testHarness.getOutput

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    expectedOutput.add(new StreamRecord(
      CRow(Row.of(1: JInt, "aaa3", 1: JInt, "Hi1"), true), 10))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(2: JInt, "bbb2", 2: JInt, "Hello1"), true), 10))

    verify(expectedOutput, result, new RowResultSortComparator(6))

    testHarness.close()
  }

}
