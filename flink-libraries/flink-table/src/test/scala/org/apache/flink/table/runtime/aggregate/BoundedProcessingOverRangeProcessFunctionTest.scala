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
package org.apache.flink.table.runtime.aggregate

import java.util.Comparator
import java.util.concurrent.ConcurrentLinkedQueue
import java.lang.{Integer => JInt, Long => JLong}

import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.operators.KeyedProcessOperator
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.util.{KeyedOneInputStreamOperatorTestHarness, TestHarnessUtil}
import org.apache.flink.table.codegen.GeneratedAggregationsFunction
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.table.functions.aggfunctions.{LongMaxWithRetractAggFunction, LongMinWithRetractAggFunction}
import org.apache.flink.table.runtime.aggregate.BoundedProcessingOverRangeProcessFunctionTest._
import org.apache.flink.types.Row
import org.junit.Test

class BoundedProcessingOverRangeProcessFunctionTest {

  @Test
  def testProcTimePartitionedOverRange(): Unit = {

    val rT =  new RowTypeInfo(Array[TypeInformation[_]](
      INT_TYPE_INFO,
      LONG_TYPE_INFO,
      INT_TYPE_INFO,
      STRING_TYPE_INFO,
      LONG_TYPE_INFO),
      Array("a", "b", "c", "d", "e"))

    val aggregates =
      Array(new LongMinWithRetractAggFunction,
            new LongMaxWithRetractAggFunction).asInstanceOf[Array[AggregateFunction[_]]]
    val aggregationStateType: RowTypeInfo = AggregateUtil.createAccumulatorRowType(aggregates)

    val funcCode =
      """
        |public class BoundedOverAggregateHelper$33
        |  extends org.apache.flink.table.runtime.aggregate.GeneratedAggregations {
        |
        |transient org.apache.flink.table.functions.aggfunctions.LongMinWithRetractAggFunction
        |  fmin = null;
        |
        |transient org.apache.flink.table.functions.aggfunctions.LongMaxWithRetractAggFunction
        |  fmax = null;
        |
        |  public BoundedOverAggregateHelper$33() throws Exception {
        |
        |  fmin = (org.apache.flink.table.functions.aggfunctions.LongMinWithRetractAggFunction)
        |  org.apache.flink.table.functions.utils.UserDefinedFunctionUtils
        |    .deserialize("rO0ABXNyAEtvcmcuYXBhY2hlLmZsaW5rLnRhYmxlLmZ1bmN0aW9ucy5hZ2dmdW5jdGlvbn" +
        |    "MuTG9uZ01pbldpdGhSZXRyYWN0QWdnRnVuY3Rpb26oIdX_DaMPxQIAAHhyAEdvcmcuYXBhY2hlLmZsaW5rL" +
        |    "nRhYmxlLmZ1bmN0aW9ucy5hZ2dmdW5jdGlvbnMuTWluV2l0aFJldHJhY3RBZ2dGdW5jdGlvbkDcXxs1apkP" +
        |    "AgABTAADb3JkdAAVTHNjYWxhL21hdGgvT3JkZXJpbmc7eHIAMm9yZy5hcGFjaGUuZmxpbmsudGFibGUuZnV" +
        |    "uY3Rpb25zLkFnZ3JlZ2F0ZUZ1bmN0aW9uSa3YqbzCL3QCAAB4cgA0b3JnLmFwYWNoZS5mbGluay50YWJsZS" +
        |    "5mdW5jdGlvbnMuVXNlckRlZmluZWRGdW5jdGlvbi0B91QxuAyTAgAAeHBzcgAZc2NhbGEubWF0aC5PcmRlc" +
        |    "mluZyRMb25nJOda0iCPo2ukAgAAeHA");
        |
        |  fmax = (org.apache.flink.table.functions.aggfunctions.LongMaxWithRetractAggFunction)
        |  org.apache.flink.table.functions.utils.UserDefinedFunctionUtils
        |    .deserialize("rO0ABXNyAEtvcmcuYXBhY2hlLmZsaW5rLnRhYmxlLmZ1bmN0aW9ucy5hZ2dmdW5jdGlvbn" +
        |    "MuTG9uZ01heFdpdGhSZXRyYWN0QWdnRnVuY3Rpb25RmsI8azNGXwIAAHhyAEdvcmcuYXBhY2hlLmZsaW5rL" +
        |    "nRhYmxlLmZ1bmN0aW9ucy5hZ2dmdW5jdGlvbnMuTWF4V2l0aFJldHJhY3RBZ2dGdW5jdGlvbu4_w_gPePlO" +
        |    "AgABTAADb3JkdAAVTHNjYWxhL21hdGgvT3JkZXJpbmc7eHIAMm9yZy5hcGFjaGUuZmxpbmsudGFibGUuZnV" +
        |    "uY3Rpb25zLkFnZ3JlZ2F0ZUZ1bmN0aW9uSa3YqbzCL3QCAAB4cgA0b3JnLmFwYWNoZS5mbGluay50YWJsZS" +
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
        |      (org.apache.flink.table.functions.Accumulator) accs.getField(0)));
        |
        |    org.apache.flink.table.functions.AggregateFunction baseClass1 =
        |      (org.apache.flink.table.functions.AggregateFunction) fmax;
        |    output.setField(6, baseClass1.getValue(
        |      (org.apache.flink.table.functions.Accumulator) accs.getField(1)));
        |  }
        |
        |  public void accumulate(
        |    org.apache.flink.types.Row accs,
        |    org.apache.flink.types.Row input) {
        |
        |    fmin.accumulate(
        |      ((org.apache.flink.table.functions.Accumulator) accs.getField(0)),
        |      (java.lang.Long) input.getField(4));
        |
        |    fmax.accumulate(
        |      ((org.apache.flink.table.functions.Accumulator) accs.getField(1)),
        |      (java.lang.Long) input.getField(4));
        |  }
        |
        |  public void retract(
        |    org.apache.flink.types.Row accs,
        |    org.apache.flink.types.Row input) {
        |
        |    fmin.retract(
        |      ((org.apache.flink.table.functions.Accumulator) accs.getField(0)),
        |      (java.lang.Long) input.getField(4));
        |
        |    fmax.retract(
        |      ((org.apache.flink.table.functions.Accumulator) accs.getField(1)),
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
        |}
      """.stripMargin

    val funcName = "BoundedOverAggregateHelper$33"

    val genAggFunction = GeneratedAggregationsFunction(funcName, funcCode)
    val processFunction = new KeyedProcessOperator[String, Row, Row](
      new ProcTimeBoundedRangeOver(
        genAggFunction,
        1000,
        aggregationStateType,
        rT))

    val testHarness = new KeyedOneInputStreamOperatorTestHarness[JInt, Row, Row](
      processFunction,
      new TupleRowSelector(0),
      BasicTypeInfo.INT_TYPE_INFO)

    testHarness.open()

    // Time = 3
    testHarness.setProcessingTime(3)
    // key = 1
    testHarness.processElement(new StreamRecord(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 1L: JLong), 0))
    // key = 2
    testHarness.processElement(new StreamRecord(
      Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 10L: JLong), 0))

    // Time = 4
    testHarness.setProcessingTime(4)
    // key = 1
    testHarness.processElement(new StreamRecord(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 2L: JLong), 0))
    testHarness.processElement(new StreamRecord(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 3L: JLong), 0))
    // key = 2
    testHarness.processElement(new StreamRecord(
      Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 20L: JLong), 0))

    // Time = 5
    testHarness.setProcessingTime(5)
    testHarness.processElement(new StreamRecord(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 4L: JLong), 0))

    // Time = 6
    testHarness.setProcessingTime(6)

    // Time = 1002
    testHarness.setProcessingTime(1002)
    // key = 1
    testHarness.processElement(new StreamRecord(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 5L: JLong), 0))
    testHarness.processElement(new StreamRecord(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 6L: JLong), 0))
    // key = 2
    testHarness.processElement(new StreamRecord(
      Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 30L: JLong), 0))

    // Time = 1003
    testHarness.setProcessingTime(1003)
    testHarness.processElement(new StreamRecord(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 7L: JLong), 0))

    // Time = 1004
    testHarness.setProcessingTime(1004)
    testHarness.processElement(new StreamRecord(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 8L: JLong), 0))

    // Time = 1005
    testHarness.setProcessingTime(1005)
    // key = 1
    testHarness.processElement(new StreamRecord(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 9L: JLong), 0))
    testHarness.processElement(new StreamRecord(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 10L: JLong), 0))
    // key = 2
    testHarness.processElement(new StreamRecord(
      Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 40L: JLong), 0))

    testHarness.setProcessingTime(1006)

    val result = testHarness.getOutput

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    // all elements at the same proc timestamp have the same value
    expectedOutput.add(new StreamRecord(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 1L: JLong, 1L: JLong, 1L: JLong), 4))
    expectedOutput.add(new StreamRecord(
      Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 10L: JLong, 10L: JLong, 10L: JLong), 4))
    expectedOutput.add(new StreamRecord(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 2L: JLong, 1L: JLong, 3L: JLong), 5))
    expectedOutput.add(new StreamRecord(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 3L: JLong, 1L: JLong, 3L: JLong), 5))
    expectedOutput.add(new StreamRecord(
      Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 20L: JLong, 10L: JLong, 20L: JLong), 5))
    expectedOutput.add(new StreamRecord(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 4L: JLong, 1L: JLong, 4L: JLong), 6))
    expectedOutput.add(new StreamRecord(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 5L: JLong, 1L: JLong, 6L: JLong), 1003))
    expectedOutput.add(new StreamRecord(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 6L: JLong, 1L: JLong, 6L: JLong), 1003))
    expectedOutput.add(new StreamRecord(
      Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 30L: JLong, 10L: JLong, 30L: JLong), 1003))
    expectedOutput.add(new StreamRecord(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 7L: JLong, 1L: JLong, 7L: JLong), 1004))
    expectedOutput.add(new StreamRecord(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 8L: JLong, 2L: JLong, 8L: JLong), 1005))
    expectedOutput.add(new StreamRecord(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 9L: JLong, 4L: JLong, 10L: JLong), 1006))
    expectedOutput.add(new StreamRecord(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 10L: JLong, 4L: JLong, 10L: JLong), 1006))
    expectedOutput.add(new StreamRecord(
      Row.of(2: JInt, 0L: JLong, 0: JInt, "bbb", 40L: JLong, 30L: JLong, 40L: JLong), 1006))

    TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.",
        expectedOutput, result, new RowResultSortComparator(6))

    testHarness.close()

  }
}

object BoundedProcessingOverRangeProcessFunctionTest {

/**
 * Return 0 for equal Rows and non zero for different rows
 */
class RowResultSortComparator(indexCounter: Int) extends Comparator[Object] with Serializable {

    override def compare(o1: Object, o2: Object):Int = {

      if (o1.isInstanceOf[Watermark] || o2.isInstanceOf[Watermark]) {
        // watermark is not expected
         -1
       } else {
        val row1 = o1.asInstanceOf[StreamRecord[Row]].getValue
        val row2 = o2.asInstanceOf[StreamRecord[Row]].getValue
        row1.toString.compareTo(row2.toString)
      }
   }
}

/**
 * Simple test class that returns a specified field as the selector function
 */
class TupleRowSelector(
    private val selectorField:Int) extends KeySelector[Row, Integer] {

  override def getKey(value: Row): Integer = {
    value.getField(selectorField).asInstanceOf[Integer]
  }
}

}
