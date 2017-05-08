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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.operators.KeyedProcessOperator
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.table.codegen.GeneratedAggregationsFunction
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.table.functions.aggfunctions.{LongMaxWithRetractAggFunction, LongMinWithRetractAggFunction}
import org.apache.flink.table.runtime.aggregate._
import org.apache.flink.table.runtime.harness.HarnessTestBase._
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
import org.apache.flink.types.Row
import org.junit.Test

class OverWindowHarnessTest extends HarnessTestBase{

  private val rT = new RowTypeInfo(Array[TypeInformation[_]](
    INT_TYPE_INFO,
    LONG_TYPE_INFO,
    INT_TYPE_INFO,
    STRING_TYPE_INFO,
    LONG_TYPE_INFO),
    Array("a", "b", "c", "d", "e"))

  private val cRT = new CRowTypeInfo(rT)

  private val aggregates =
    Array(new LongMinWithRetractAggFunction,
      new LongMaxWithRetractAggFunction).asInstanceOf[Array[AggregateFunction[_, _]]]
  private val aggregationStateType: RowTypeInfo = AggregateUtil.createAccumulatorRowType(aggregates)

  val funcCode: String =
    """
      |public class BoundedOverAggregateHelper
      |  extends org.apache.flink.table.runtime.aggregate.GeneratedAggregations {
      |
      |  transient org.apache.flink.table.functions.aggfunctions.LongMinWithRetractAggFunction
      |    fmin = null;
      |
      |  transient org.apache.flink.table.functions.aggfunctions.LongMaxWithRetractAggFunction
      |    fmax = null;
      |
      |  public BoundedOverAggregateHelper() throws Exception {
      |
      |    fmin = (org.apache.flink.table.functions.aggfunctions.LongMinWithRetractAggFunction)
      |    org.apache.flink.table.functions.utils.UserDefinedFunctionUtils
      |    .deserialize("rO0ABXNyAEtvcmcuYXBhY2hlLmZsaW5rLnRhYmxlLmZ1bmN0aW9ucy5hZ2dmdW5jdGlvbn" +
      |    "MuTG9uZ01pbldpdGhSZXRyYWN0QWdnRnVuY3Rpb26oIdX_DaMPxQIAAHhyAEdvcmcuYXBhY2hlLmZsaW5rL" +
      |    "nRhYmxlLmZ1bmN0aW9ucy5hZ2dmdW5jdGlvbnMuTWluV2l0aFJldHJhY3RBZ2dGdW5jdGlvbq_ZGuzxtA_S" +
      |    "AgABTAADb3JkdAAVTHNjYWxhL21hdGgvT3JkZXJpbmc7eHIAMm9yZy5hcGFjaGUuZmxpbmsudGFibGUuZnV" +
      |    "uY3Rpb25zLkFnZ3JlZ2F0ZUZ1bmN0aW9uTcYVPtJjNfwCAAB4cgA0b3JnLmFwYWNoZS5mbGluay50YWJsZS" +
      |    "5mdW5jdGlvbnMuVXNlckRlZmluZWRGdW5jdGlvbi0B91QxuAyTAgAAeHBzcgAZc2NhbGEubWF0aC5PcmRlc" +
      |    "mluZyRMb25nJOda0iCPo2ukAgAAeHA");
      |
      |    fmax = (org.apache.flink.table.functions.aggfunctions.LongMaxWithRetractAggFunction)
      |    org.apache.flink.table.functions.utils.UserDefinedFunctionUtils
      |    .deserialize("rO0ABXNyAEtvcmcuYXBhY2hlLmZsaW5rLnRhYmxlLmZ1bmN0aW9ucy5hZ2dmdW5jdGlvbn" +
      |    "MuTG9uZ01heFdpdGhSZXRyYWN0QWdnRnVuY3Rpb25RmsI8azNGXwIAAHhyAEdvcmcuYXBhY2hlLmZsaW5rL" +
      |    "nRhYmxlLmZ1bmN0aW9ucy5hZ2dmdW5jdGlvbnMuTWF4V2l0aFJldHJhY3RBZ2dGdW5jdGlvbvnwowlX0_Qf" +
      |    "AgABTAADb3JkdAAVTHNjYWxhL21hdGgvT3JkZXJpbmc7eHIAMm9yZy5hcGFjaGUuZmxpbmsudGFibGUuZnV" +
      |    "uY3Rpb25zLkFnZ3JlZ2F0ZUZ1bmN0aW9uTcYVPtJjNfwCAAB4cgA0b3JnLmFwYWNoZS5mbGluay50YWJsZS" +
      |    "5mdW5jdGlvbnMuVXNlckRlZmluZWRGdW5jdGlvbi0B91QxuAyTAgAAeHBzcgAZc2NhbGEubWF0aC5PcmRlc" +
      |    "mluZyRMb25nJOda0iCPo2ukAgAAeHA");
      |  }
      |
      |  public void setAggregationResults(
      |    org.apache.flink.types.Row accs,
      |    org.apache.flink.types.Row output) {
      |
      |    org.apache.flink.table.functions.AggregateFunction baseClass0 =
      |      (org.apache.flink.table.functions.AggregateFunction) fmin;
      |    output.setField(5, baseClass0.getValue(
      |      (org.apache.flink.table.functions.aggfunctions.MinWithRetractAccumulator)
      |      accs.getField(0)));
      |
      |    org.apache.flink.table.functions.AggregateFunction baseClass1 =
      |      (org.apache.flink.table.functions.AggregateFunction) fmax;
      |    output.setField(6, baseClass1.getValue(
      |      (org.apache.flink.table.functions.aggfunctions.MaxWithRetractAccumulator)
      |      accs.getField(1)));
      |  }
      |
      |  public void accumulate(
      |    org.apache.flink.types.Row accs,
      |    org.apache.flink.types.Row input) {
      |
      |    fmin.accumulate(
      |      ((org.apache.flink.table.functions.aggfunctions.MinWithRetractAccumulator)
      |      accs.getField(0)),
      |      (java.lang.Long) input.getField(4));
      |
      |    fmax.accumulate(
      |      ((org.apache.flink.table.functions.aggfunctions.MaxWithRetractAccumulator)
      |      accs.getField(1)),
      |      (java.lang.Long) input.getField(4));
      |  }
      |
      |  public void retract(
      |    org.apache.flink.types.Row accs,
      |    org.apache.flink.types.Row input) {
      |
      |    fmin.retract(
      |      ((org.apache.flink.table.functions.aggfunctions.MinWithRetractAccumulator)
      |      accs.getField(0)),
      |      (java.lang.Long) input.getField(4));
      |
      |    fmax.retract(
      |      ((org.apache.flink.table.functions.aggfunctions.MaxWithRetractAccumulator)
      |      accs.getField(1)),
      |      (java.lang.Long) input.getField(4));
      |  }
      |
      |  public org.apache.flink.types.Row createAccumulators() {
      |
      |    org.apache.flink.types.Row accs = new org.apache.flink.types.Row(2);
      |
      |    accs.setField(
      |      0,
      |      fmin.createAccumulator());
      |
      |    accs.setField(
      |      1,
      |      fmax.createAccumulator());
      |
      |      return accs;
      |  }
      |
      |  public void setForwardedFields(
      |    org.apache.flink.types.Row input,
      |    org.apache.flink.types.Row output) {
      |
      |    output.setField(0, input.getField(0));
      |    output.setField(1, input.getField(1));
      |    output.setField(2, input.getField(2));
      |    output.setField(3, input.getField(3));
      |    output.setField(4, input.getField(4));
      |  }
      |
      |  public org.apache.flink.types.Row createOutputRow() {
      |    return new org.apache.flink.types.Row(7);
      |  }
      |
      |/*******  This test does not use the following methods  *******/
      |  public org.apache.flink.types.Row mergeAccumulatorsPair(
      |    org.apache.flink.types.Row a,
      |    org.apache.flink.types.Row b) {
      |    return null;
      |  }
      |
      |  public void resetAccumulator(org.apache.flink.types.Row accs) {
      |  }
      |
      |  public void setConstantFlags(org.apache.flink.types.Row output) {
      |  }
      |}
    """.stripMargin


  private val funcName = "BoundedOverAggregateHelper"

  private val genAggFunction = GeneratedAggregationsFunction(funcName, funcCode)


  @Test
  def testProcTimeBoundedRowsOver(): Unit = {

    val processFunction = new KeyedProcessOperator[String, CRow, CRow](
      new ProcTimeBoundedRowsOver(
        genAggFunction,
        2,
        aggregationStateType,
        cRT))

    val testHarness =
      createHarnessTester(processFunction,new TupleRowKeySelector[Integer](0),BasicTypeInfo
        .INT_TYPE_INFO)

    testHarness.open()

    testHarness.setProcessingTime(1)

    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 1L: JLong), true), 1))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 10L: JLong), true), 1))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 2L: JLong), true), 1))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 3L: JLong), true), 1))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 20L: JLong), true), 1))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 4L: JLong), true), 1))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 5L: JLong), true), 1))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 6L: JLong), true), 1))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 30L: JLong), true), 1))

    testHarness.setProcessingTime(2)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 7L: JLong), true), 2))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 8L: JLong), true), 2))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 9L: JLong), true), 2))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 10L: JLong), true), 2))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 40L: JLong), true), 2))

    val result = testHarness.getOutput

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    expectedOutput.add(new StreamRecord(
      CRow(
        Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 1L: JLong, 1L: JLong, 1L: JLong), true), 1))
    expectedOutput.add(new StreamRecord(
      CRow(
        Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 10L: JLong, 10L: JLong, 10L: JLong), true), 1))
    expectedOutput.add(new StreamRecord(
      CRow(
        Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 2L: JLong, 1L: JLong, 2L: JLong), true), 1))
    expectedOutput.add(new StreamRecord(
      CRow(
        Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 3L: JLong, 2L: JLong, 3L: JLong), true), 1))
    expectedOutput.add(new StreamRecord(
      CRow(
        Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 20L: JLong, 10L: JLong, 20L: JLong), true), 1))
    expectedOutput.add(new StreamRecord(
      CRow(
        Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 4L: JLong, 3L: JLong, 4L: JLong), true), 1))
    expectedOutput.add(new StreamRecord(
      CRow(
        Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 5L: JLong, 4L: JLong, 5L: JLong), true), 1))
    expectedOutput.add(new StreamRecord(
      CRow(
        Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 6L: JLong, 5L: JLong, 6L: JLong), true), 1))
    expectedOutput.add(new StreamRecord(
      CRow(
        Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 30L: JLong, 20L: JLong, 30L: JLong), true), 1))
    expectedOutput.add(new StreamRecord(
      CRow(
        Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 7L: JLong, 6L: JLong, 7L: JLong), true), 2))
    expectedOutput.add(new StreamRecord(
      CRow(
        Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 8L: JLong, 7L: JLong, 8L: JLong), true), 2))
    expectedOutput.add(new StreamRecord(
      CRow(
        Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 9L: JLong, 8L: JLong, 9L: JLong), true), 2))
    expectedOutput.add(new StreamRecord(
      CRow(
        Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 10L: JLong, 9L: JLong, 10L: JLong), true), 2))
    expectedOutput.add(new StreamRecord(
      CRow(
        Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 40L: JLong, 30L: JLong, 40L: JLong), true), 2))

    verify(expectedOutput, result, new RowResultSortComparator(6))

    testHarness.close()
  }

  /**
    * NOTE: all elements at the same proc timestamp have the same value per key
    */
  @Test
  def testProcTimeBoundedRangeOver(): Unit = {

    val processFunction = new KeyedProcessOperator[String, CRow, CRow](
      new ProcTimeBoundedRangeOver(
        genAggFunction,
        1000,
        aggregationStateType,
        cRT))

    val testHarness =
      createHarnessTester(
        processFunction,
        new TupleRowKeySelector[Integer](0),
        BasicTypeInfo.INT_TYPE_INFO)

    testHarness.open()

    testHarness.setProcessingTime(3)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 1L: JLong), true), 0))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 10L: JLong), true), 0))

    testHarness.setProcessingTime(4)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 2L: JLong), true), 0))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 3L: JLong), true), 0))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 20L: JLong), true), 0))

    testHarness.setProcessingTime(5)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 4L: JLong), true), 0))

    testHarness.setProcessingTime(6)

    testHarness.setProcessingTime(1002)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 5L: JLong), true), 0))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 6L: JLong), true), 0))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 30L: JLong), true), 0))

    testHarness.setProcessingTime(1003)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 7L: JLong), true), 0))

    testHarness.setProcessingTime(1004)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 8L: JLong), true), 0))

    testHarness.setProcessingTime(1005)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 9L: JLong), true), 0))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 10L: JLong), true), 0))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 40L: JLong), true), 0))

    testHarness.setProcessingTime(1006)

    val result = testHarness.getOutput

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    // all elements at the same proc timestamp have the same value per key
    expectedOutput.add(new StreamRecord(
      CRow(
        Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 1L: JLong, 1L: JLong, 1L: JLong), true), 4))
    expectedOutput.add(new StreamRecord(
      CRow(
        Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 10L: JLong, 10L: JLong, 10L: JLong), true), 4))
    expectedOutput.add(new StreamRecord(
      CRow(
        Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 2L: JLong, 1L: JLong, 3L: JLong), true), 5))
    expectedOutput.add(new StreamRecord(
      CRow(
        Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 3L: JLong, 1L: JLong, 3L: JLong), true), 5))
    expectedOutput.add(new StreamRecord(
      CRow(
        Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 20L: JLong, 10L: JLong, 20L: JLong), true), 5))
    expectedOutput.add(new StreamRecord(
      CRow(
        Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 4L: JLong, 1L: JLong, 4L: JLong), true), 6))
    expectedOutput.add(new StreamRecord(
      CRow(
        Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 5L: JLong, 1L: JLong, 6L: JLong), true), 1003))
    expectedOutput.add(new StreamRecord(
      CRow(
        Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 6L: JLong, 1L: JLong, 6L: JLong), true), 1003))
    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 30L: JLong, 10L: JLong, 30L: JLong), true), 1003))
    expectedOutput.add(new StreamRecord(
      CRow(
        Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 7L: JLong, 1L: JLong, 7L: JLong), true), 1004))
    expectedOutput.add(new StreamRecord(
      CRow(
        Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 8L: JLong, 2L: JLong, 8L: JLong), true), 1005))
    expectedOutput.add(new StreamRecord(
      CRow(
        Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 9L: JLong, 4L: JLong, 10L: JLong), true), 1006))
    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 10L: JLong, 4L: JLong, 10L: JLong), true), 1006))
    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 40L: JLong, 30L: JLong, 40L: JLong), true), 1006))

    verify(expectedOutput, result, new RowResultSortComparator(6))

    testHarness.close()
  }

  @Test
  def testProcTimeUnboundedOver(): Unit = {

    val processFunction = new KeyedProcessOperator[String, CRow, CRow](
      new ProcTimeUnboundedPartitionedOver(
        genAggFunction,
        aggregationStateType))

    val testHarness =
      createHarnessTester(
        processFunction,
        new TupleRowKeySelector[Integer](0),
        BasicTypeInfo.INT_TYPE_INFO)

    testHarness.open()

    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 1L: JLong), true), 0))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 10L: JLong), true), 0))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 2L: JLong), true), 0))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 3L: JLong), true), 0))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 20L: JLong), true), 0))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 4L: JLong), true), 0))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 5L: JLong), true), 0))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 6L: JLong), true), 0))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 30L: JLong), true), 0))

    testHarness.setProcessingTime(1003)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 7L: JLong), true), 1003))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 8L: JLong), true), 1003))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 9L: JLong), true), 1003))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 10L: JLong), true), 1003))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 40L: JLong), true), 1003))

    val result = testHarness.getOutput

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    expectedOutput.add(new StreamRecord(
      CRow(
        Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 1L: JLong, 1L: JLong, 1L: JLong), true), 0))
    expectedOutput.add(new StreamRecord(
      CRow(
        Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 10L: JLong, 10L: JLong, 10L: JLong), true), 0))
    expectedOutput.add(new StreamRecord(
      CRow(
        Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 2L: JLong, 1L: JLong, 2L: JLong), true), 0))
    expectedOutput.add(new StreamRecord(
      CRow(
        Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 3L: JLong, 1L: JLong, 3L: JLong), true), 0))
    expectedOutput.add(new StreamRecord(
      CRow(
        Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 20L: JLong, 10L: JLong, 20L: JLong), true), 0))
    expectedOutput.add(new StreamRecord(
      CRow(
        Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 4L: JLong, 1L: JLong, 4L: JLong), true), 0))
    expectedOutput.add(new StreamRecord(
      CRow(
        Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 5L: JLong, 1L: JLong, 5L: JLong), true), 0))
    expectedOutput.add(new StreamRecord(
      CRow(
        Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 6L: JLong, 1L: JLong, 6L: JLong), true), 0))
    expectedOutput.add(new StreamRecord(
      CRow(
        Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 30L: JLong, 10L: JLong, 30L: JLong), true), 0))
    expectedOutput.add(new StreamRecord(
      CRow(
        Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 7L: JLong, 1L: JLong, 7L: JLong), true), 1003))
    expectedOutput.add(new StreamRecord(
      CRow(
        Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 8L: JLong, 1L: JLong, 8L: JLong), true), 1003))
    expectedOutput.add(new StreamRecord(
      CRow(
        Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 9L: JLong, 1L: JLong, 9L: JLong), true), 1003))
    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 10L: JLong, 1L: JLong, 10L: JLong), true), 1003))
    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 40L: JLong, 10L: JLong, 40L: JLong), true), 1003))

    verify(expectedOutput, result, new RowResultSortComparator(6))
    testHarness.close()
  }

  /**
    * all elements at the same row-time have the same value per key
    */
  @Test
  def testRowTimeBoundedRangeOver(): Unit = {

    val processFunction = new KeyedProcessOperator[String, CRow, CRow](
      new RowTimeBoundedRangeOver(
        genAggFunction,
        aggregationStateType,
        cRT,
        4000))

    val testHarness =
      createHarnessTester(
        processFunction,
        new TupleRowKeySelector[String](3),
        BasicTypeInfo.STRING_TYPE_INFO)

    testHarness.open()

    testHarness.processWatermark(1)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 1L: JLong), true), 2))

    testHarness.processWatermark(2)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 10L: JLong), true), 3))

    testHarness.processWatermark(4000)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 2L: JLong), true), 4001))

    testHarness.processWatermark(4001)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 3L: JLong), true), 4002))

    testHarness.processWatermark(4002)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 0L: JLong, 0: JInt, "aaa", 4L: JLong), true), 4003))

    testHarness.processWatermark(4800)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(2: JInt, 11L: JLong, 1: JInt, "bbb", 25L: JLong), true), 4801))

    testHarness.processWatermark(6500)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 5L: JLong), true), 6501))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 6L: JLong), true), 6501))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 30L: JLong), true), 6501))

    testHarness.processWatermark(7000)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 7L: JLong), true), 7001))

    testHarness.processWatermark(8000)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 8L: JLong), true), 8001))

    testHarness.processWatermark(12000)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 9L: JLong), true), 12001))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 10L: JLong), true), 12001))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 40L: JLong), true), 12001))

    testHarness.processWatermark(19000)

    val result = testHarness.getOutput

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    // all elements at the same row-time have the same value per key
    expectedOutput.add(new StreamRecord(
      CRow(
        Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 1L: JLong, 1L: JLong, 1L: JLong), true), 2))
    expectedOutput.add(new StreamRecord(
      CRow(
        Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 10L: JLong, 10L: JLong, 10L: JLong), true), 3))
    expectedOutput.add(new StreamRecord(
      CRow(
        Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 2L: JLong, 1L: JLong, 2L: JLong), true), 4001))
    expectedOutput.add(new StreamRecord(
      CRow(
        Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 3L: JLong, 1L: JLong, 3L: JLong), true), 4002))
    expectedOutput.add(new StreamRecord(
      CRow(
        Row.of(1: JInt, 0L: JLong, 0: JInt, "aaa", 4L: JLong, 2L: JLong, 4L: JLong), true), 4003))
    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(2: JInt, 11L: JLong, 1: JInt, "bbb", 25L: JLong, 25L: JLong, 25L: JLong), true), 4801))
    expectedOutput.add(new StreamRecord(
      CRow(
        Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 5L: JLong, 2L: JLong, 6L: JLong), true), 6501))
    expectedOutput.add(new StreamRecord(
      CRow(
        Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 6L: JLong, 2L: JLong, 6L: JLong), true), 6501))
    expectedOutput.add(new StreamRecord(
      CRow(
        Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 7L: JLong, 2L: JLong, 7L: JLong), true), 7001))
    expectedOutput.add(new StreamRecord(
      CRow(
        Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 8L: JLong, 2L: JLong, 8L: JLong), true), 8001))
    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 30L: JLong, 25L: JLong, 30L: JLong), true), 6501))
    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 9L: JLong, 8L: JLong, 10L: JLong), true), 12001))
    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 10L: JLong, 8L: JLong, 10L: JLong), true), 12001))
    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 40L: JLong, 40L: JLong, 40L: JLong), true), 12001))

    verify(expectedOutput, result, new RowResultSortComparator(6))
    testHarness.close()
  }

  @Test
  def testRowTimeBoundedRowsOver(): Unit = {

    val processFunction = new KeyedProcessOperator[String, CRow, CRow](
      new RowTimeBoundedRowsOver(
        genAggFunction,
        aggregationStateType,
        cRT,
        3))

    val testHarness =
      createHarnessTester(
        processFunction,
        new TupleRowKeySelector[String](3),
        BasicTypeInfo.STRING_TYPE_INFO)

    testHarness.open()

    testHarness.processWatermark(800)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 1L: JLong), true), 801))

    testHarness.processWatermark(2500)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 10L: JLong), true), 2501))

    testHarness.processWatermark(4000)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 2L: JLong), true), 4001))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 3L: JLong), true), 4001))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 20L: JLong), true), 4001))

    testHarness.processWatermark(4800)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 4L: JLong), true), 4801))

    testHarness.processWatermark(6500)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 5L: JLong), true), 6501))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 6L: JLong), true), 6501))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 30L: JLong), true), 6501))

    testHarness.processWatermark(7000)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 7L: JLong), true), 7001))

    testHarness.processWatermark(8000)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 8L: JLong), true), 8001))

    testHarness.processWatermark(12000)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 9L: JLong), true), 12001))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 10L: JLong), true), 12001))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 40L: JLong), true), 12001))

    testHarness.processWatermark(19000)

    val result = testHarness.getOutput

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 1L: JLong, 1L: JLong, 1L: JLong), true), 801))
    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 10L: JLong, 10L: JLong, 10L: JLong), true), 2501))
    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 2L: JLong, 1L: JLong, 2L: JLong), true), 4001))
    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 3L: JLong, 1L: JLong, 3L: JLong), true), 4001))
    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 20L: JLong, 10L: JLong, 20L: JLong), true), 4001))
    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 4L: JLong, 2L: JLong, 4L: JLong), true), 4801))
    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 5L: JLong, 3L: JLong, 5L: JLong), true), 6501))
    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 6L: JLong, 4L: JLong, 6L: JLong), true), 6501))
    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 30L: JLong, 10L: JLong, 30L: JLong), true), 6501))
    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 7L: JLong, 5L: JLong, 7L: JLong), true), 7001))
    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 8L: JLong, 6L: JLong, 8L: JLong), true), 8001))
    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 9L: JLong, 7L: JLong, 9L: JLong), true), 12001))
    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 10L: JLong, 8L: JLong, 10L: JLong), true), 12001))
    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 40L: JLong, 20L: JLong, 40L: JLong), true), 12001))

    verify(expectedOutput, result, new RowResultSortComparator(6))
    testHarness.close()
  }

  /**
    * all elements at the same row-time have the same value per key
    */
  @Test
  def testRowTimeUnboundedRangeOver(): Unit = {

    val processFunction = new KeyedProcessOperator[String, CRow, CRow](
      new RowTimeUnboundedRangeOver(
        genAggFunction,
        aggregationStateType,
        cRT))

    val testHarness =
      createHarnessTester(
        processFunction,
        new TupleRowKeySelector[String](3),
        BasicTypeInfo.STRING_TYPE_INFO)

    testHarness.open()

    testHarness.processWatermark(800)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 1L: JLong), true), 801))

    testHarness.processWatermark(2500)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 10L: JLong), true), 2501))

    testHarness.processWatermark(4000)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 2L: JLong), true), 4001))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 3L: JLong), true), 4001))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 20L: JLong), true), 4001))

    testHarness.processWatermark(4800)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 4L: JLong), true), 4801))

    testHarness.processWatermark(6500)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 5L: JLong), true), 6501))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 6L: JLong), true), 6501))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 30L: JLong), true), 6501))

    testHarness.processWatermark(7000)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 7L: JLong), true), 7001))

    testHarness.processWatermark(8000)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 8L: JLong), true), 8001))

    testHarness.processWatermark(12000)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 9L: JLong), true), 12001))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 10L: JLong), true), 12001))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 40L: JLong), true), 12001))

    testHarness.processWatermark(19000)

    val result = testHarness.getOutput

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    // all elements at the same row-time have the same value per key
    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 1L: JLong, 1L: JLong, 1L: JLong), true), 801))
    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 10L: JLong, 10L: JLong, 10L: JLong), true), 2501))
    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 2L: JLong, 1L: JLong, 3L: JLong), true), 4001))
    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 3L: JLong, 1L: JLong, 3L: JLong), true), 4001))
    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 20L: JLong, 10L: JLong, 20L: JLong), true), 4001))
    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 4L: JLong, 1L: JLong, 4L: JLong), true), 4801))
    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 5L: JLong, 1L: JLong, 6L: JLong), true), 6501))
    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 6L: JLong, 1L: JLong, 6L: JLong), true), 6501))
    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 30L: JLong, 10L: JLong, 30L: JLong), true), 6501))
    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 7L: JLong, 1L: JLong, 7L: JLong), true), 7001))
    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 8L: JLong, 1L: JLong, 8L: JLong), true), 8001))
    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 9L: JLong, 1L: JLong, 10L: JLong), true), 12001))
    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 10L: JLong, 1L: JLong, 10L: JLong), true), 12001))
    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 40L: JLong, 10L: JLong, 40L: JLong), true), 12001))

    verify(expectedOutput, result, new RowResultSortComparator(6))
    testHarness.close()
  }

  @Test
  def testRowTimeUnboundedRowsOver(): Unit = {

    val processFunction = new KeyedProcessOperator[String, CRow, CRow](
      new RowTimeUnboundedRowsOver(
        genAggFunction,
        aggregationStateType,
        cRT))

    val testHarness =
      createHarnessTester(
        processFunction,
        new TupleRowKeySelector[String](3),
        BasicTypeInfo.STRING_TYPE_INFO)

    testHarness.open()

    testHarness.processWatermark(800)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 1L: JLong), true), 801))

    testHarness.processWatermark(2500)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 10L: JLong), true), 2501))

    testHarness.processWatermark(4000)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 2L: JLong), true), 4001))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 3L: JLong), true), 4001))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 20L: JLong), true), 4001))

    testHarness.processWatermark(4800)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 4L: JLong), true), 4801))

    testHarness.processWatermark(6500)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 5L: JLong), true), 6501))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 6L: JLong), true), 6501))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 30L: JLong), true), 6501))

    testHarness.processWatermark(7000)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 7L: JLong), true), 7001))

    testHarness.processWatermark(8000)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 8L: JLong), true), 8001))

    testHarness.processWatermark(12000)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 9L: JLong), true), 12001))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 10L: JLong), true), 12001))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 40L: JLong), true), 12001))

    testHarness.processWatermark(19000)

    val result = testHarness.getOutput

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 1L: JLong, 1L: JLong, 1L: JLong), true), 801))
    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 10L: JLong, 10L: JLong, 10L: JLong), true), 2501))
    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 2L: JLong, 1L: JLong, 2L: JLong), true), 4001))
    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 3L: JLong, 1L: JLong, 3L: JLong), true), 4001))
    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 20L: JLong, 10L: JLong, 20L: JLong), true), 4001))
    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 4L: JLong, 1L: JLong, 4L: JLong), true), 4801))
    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 5L: JLong, 1L: JLong, 5L: JLong), true), 6501))
    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 6L: JLong, 1L: JLong, 6L: JLong), true), 6501))
    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 30L: JLong, 10L: JLong, 30L: JLong), true), 6501))
    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 7L: JLong, 1L: JLong, 7L: JLong), true), 7001))
    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 8L: JLong, 1L: JLong, 8L: JLong), true), 8001))
    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 9L: JLong, 1L: JLong, 9L: JLong), true), 12001))
    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 10L: JLong, 1L: JLong, 10L: JLong), true), 12001))
    expectedOutput.add(new StreamRecord(
      CRow(
      Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 40L: JLong, 10L: JLong, 40L: JLong), true), 12001))

    verify(expectedOutput, result, new RowResultSortComparator(6))
    testHarness.close()
  }
}
