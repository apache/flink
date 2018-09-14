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

import java.util.{Comparator, Queue => JQueue}

import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.BasicTypeInfo.{INT_TYPE_INFO, LONG_TYPE_INFO, STRING_TYPE_INFO}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.operators.OneInputStreamOperator
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.util.{KeyedOneInputStreamOperatorTestHarness, TestHarnessUtil}
import org.apache.flink.table.api.StreamQueryConfig
import org.apache.flink.table.codegen.GeneratedAggregationsFunction
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils
import org.apache.flink.table.functions.aggfunctions.{IntSumWithRetractAggFunction, LongMaxWithRetractAggFunction, LongMinWithRetractAggFunction}
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils.getAccumulatorTypeOfAggregateFunction
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}

class HarnessTestBase {

  val longMinWithRetractAggFunction: String =
    UserDefinedFunctionUtils.serialize(new LongMinWithRetractAggFunction)

  val longMaxWithRetractAggFunction: String =
    UserDefinedFunctionUtils.serialize(new LongMaxWithRetractAggFunction)

  val intSumWithRetractAggFunction: String =
    UserDefinedFunctionUtils.serialize(new IntSumWithRetractAggFunction)

  protected val MinMaxRowType = new RowTypeInfo(Array[TypeInformation[_]](
    LONG_TYPE_INFO,
    STRING_TYPE_INFO,
    LONG_TYPE_INFO),
    Array("rowtime", "a", "b"))

  protected val SumRowType = new RowTypeInfo(Array[TypeInformation[_]](
    LONG_TYPE_INFO,
    INT_TYPE_INFO,
    STRING_TYPE_INFO),
    Array("a", "b", "c"))

  protected val minMaxCRowType = new CRowTypeInfo(MinMaxRowType)
  protected val sumCRowType = new CRowTypeInfo(SumRowType)

  protected val minMaxAggregates: Array[AggregateFunction[_, _]] =
    Array(new LongMinWithRetractAggFunction,
          new LongMaxWithRetractAggFunction).asInstanceOf[Array[AggregateFunction[_, _]]]

  protected val sumAggregates: Array[AggregateFunction[_, _]] =
    Array(new IntSumWithRetractAggFunction).asInstanceOf[Array[AggregateFunction[_, _]]]

  protected val minMaxAggregationStateType: RowTypeInfo =
    new RowTypeInfo(minMaxAggregates.map(getAccumulatorTypeOfAggregateFunction(_)): _*)

  protected val sumAggregationStateType: RowTypeInfo =
    new RowTypeInfo(sumAggregates.map(getAccumulatorTypeOfAggregateFunction(_)): _*)

  val minMaxCode: String =
    s"""
      |public class MinMaxAggregateHelper
      |  extends org.apache.flink.table.runtime.aggregate.GeneratedAggregations {
      |
      |  transient org.apache.flink.table.functions.aggfunctions.LongMinWithRetractAggFunction
      |    fmin = null;
      |
      |  transient org.apache.flink.table.functions.aggfunctions.LongMaxWithRetractAggFunction
      |    fmax = null;
      |
      |  public MinMaxAggregateHelper() throws Exception {
      |
      |    fmin = (org.apache.flink.table.functions.aggfunctions.LongMinWithRetractAggFunction)
      |    org.apache.flink.table.functions.utils.UserDefinedFunctionUtils
      |    .deserialize("$longMinWithRetractAggFunction");
      |
      |    fmax = (org.apache.flink.table.functions.aggfunctions.LongMaxWithRetractAggFunction)
      |    org.apache.flink.table.functions.utils.UserDefinedFunctionUtils
      |    .deserialize("$longMaxWithRetractAggFunction");
      |  }
      |
      |  public void setAggregationResults(
      |    org.apache.flink.types.Row accs,
      |    org.apache.flink.types.Row output) {
      |
      |    org.apache.flink.table.functions.AggregateFunction baseClass0 =
      |      (org.apache.flink.table.functions.AggregateFunction) fmin;
      |    output.setField(3, baseClass0.getValue(
      |      (org.apache.flink.table.functions.aggfunctions.MinWithRetractAccumulator)
      |      accs.getField(0)));
      |
      |    org.apache.flink.table.functions.AggregateFunction baseClass1 =
      |      (org.apache.flink.table.functions.AggregateFunction) fmax;
      |    output.setField(4, baseClass1.getValue(
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
      |      (java.lang.Long) input.getField(2));
      |
      |    fmax.accumulate(
      |      ((org.apache.flink.table.functions.aggfunctions.MaxWithRetractAccumulator)
      |      accs.getField(1)),
      |      (java.lang.Long) input.getField(2));
      |  }
      |
      |  public void retract(
      |    org.apache.flink.types.Row accs,
      |    org.apache.flink.types.Row input) {
      |
      |    fmin.retract(
      |      ((org.apache.flink.table.functions.aggfunctions.MinWithRetractAccumulator)
      |      accs.getField(0)),
      |      (java.lang.Long) input.getField(2));
      |
      |    fmax.retract(
      |      ((org.apache.flink.table.functions.aggfunctions.MaxWithRetractAccumulator)
      |      accs.getField(1)),
      |      (java.lang.Long) input.getField(2));
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
      |  }
      |
      |  public org.apache.flink.types.Row createOutputRow() {
      |    return new org.apache.flink.types.Row(5);
      |  }
      |
      |  public void open(org.apache.flink.api.common.functions.RuntimeContext ctx) {
      |  }
      |
      |  public void cleanup() {
      |  }
      |
      |  public void close() {
      |  }
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

  val sumAggCode: String =
    s"""
      |public final class SumAggregationHelper
      |  extends org.apache.flink.table.runtime.aggregate.GeneratedAggregations {
      |
      |
      |transient org.apache.flink.table.functions.aggfunctions.IntSumWithRetractAggFunction
      |sum = null;
      |private final org.apache.flink.table.runtime.aggregate.SingleElementIterable<org.apache
      |    .flink.table.functions.aggfunctions.SumWithRetractAccumulator> accIt0 =
      |      new org.apache.flink.table.runtime.aggregate.SingleElementIterable<org.apache.flink
      |      .table
      |      .functions.aggfunctions.SumWithRetractAccumulator>();
      |
      |  public SumAggregationHelper() throws Exception {
      |
      |sum = (org.apache.flink.table.functions.aggfunctions.IntSumWithRetractAggFunction)
      |org.apache.flink.table.functions.utils.UserDefinedFunctionUtils
      |.deserialize("$intSumWithRetractAggFunction");
      |}
      |
      |  public final void setAggregationResults(
      |    org.apache.flink.types.Row accs,
      |    org.apache.flink.types.Row output) {
      |
      |    org.apache.flink.table.functions.AggregateFunction baseClass0 =
      |      (org.apache.flink.table.functions.AggregateFunction)
      |      sum;
      |
      |    output.setField(
      |      1,
      |      baseClass0.getValue((org.apache.flink.table.functions.aggfunctions
      |      .SumWithRetractAccumulator) accs.getField(0)));
      |  }
      |
      |  public final void accumulate(
      |    org.apache.flink.types.Row accs,
      |    org.apache.flink.types.Row input) {
      |
      |    sum.accumulate(
      |      ((org.apache.flink.table.functions.aggfunctions.SumWithRetractAccumulator) accs
      |      .getField
      |      (0)),
      |      (java.lang.Integer) input.getField(1));
      |  }
      |
      |
      |  public final void retract(
      |    org.apache.flink.types.Row accs,
      |    org.apache.flink.types.Row input) {
      |  }
      |
      |  public final org.apache.flink.types.Row createAccumulators()
      |     {
      |
      |      org.apache.flink.types.Row accs =
      |          new org.apache.flink.types.Row(1);
      |
      |    accs.setField(
      |      0,
      |      sum.createAccumulator());
      |
      |      return accs;
      |  }
      |
      |  public final void setForwardedFields(
      |    org.apache.flink.types.Row input,
      |    org.apache.flink.types.Row output)
      |     {
      |
      |    output.setField(
      |      0,
      |      input.getField(0));
      |  }
      |
      |  public final void setConstantFlags(org.apache.flink.types.Row output)
      |     {
      |
      |  }
      |
      |  public final org.apache.flink.types.Row createOutputRow() {
      |    return new org.apache.flink.types.Row(2);
      |  }
      |
      |
      |  public final org.apache.flink.types.Row mergeAccumulatorsPair(
      |    org.apache.flink.types.Row a,
      |    org.apache.flink.types.Row b)
      |            {
      |
      |      return a;
      |
      |  }
      |
      |  public final void resetAccumulator(
      |    org.apache.flink.types.Row accs) {
      |  }
      |
      |  public void open(org.apache.flink.api.common.functions.RuntimeContext ctx) {
      |  }
      |
      |  public void cleanup() {
      |  }
      |
      |  public void close() {
      |  }
      |}
      |""".stripMargin


  protected val minMaxFuncName = "MinMaxAggregateHelper"
  protected val sumFuncName = "SumAggregationHelper"

  protected val genMinMaxAggFunction = GeneratedAggregationsFunction(minMaxFuncName, minMaxCode)
  protected val genSumAggFunction = GeneratedAggregationsFunction(sumFuncName, sumAggCode)

  def createHarnessTester[IN, OUT, KEY](
    operator: OneInputStreamOperator[IN, OUT],
    keySelector: KeySelector[IN, KEY],
    keyType: TypeInformation[KEY]): KeyedOneInputStreamOperatorTestHarness[KEY, IN, OUT] = {
    new KeyedOneInputStreamOperatorTestHarness[KEY, IN, OUT](operator, keySelector, keyType)
  }

  def verify(
    expected: JQueue[Object],
    actual: JQueue[Object],
    comparator: Comparator[Object],
    checkWaterMark: Boolean = false): Unit = {
    if (!checkWaterMark) {
      val it = actual.iterator()
      while (it.hasNext) {
        val data = it.next()
        if (data.isInstanceOf[Watermark]) {
          actual.remove(data)
        }
      }
    }
    TestHarnessUtil.assertOutputEqualsSorted("Verify Error...", expected, actual, comparator)
  }
}

object HarnessTestBase {

  /**
    * Return 0 for equal Rows and non zero for different rows
    */
  class RowResultSortComparator() extends Comparator[Object] with Serializable {

    override def compare(o1: Object, o2: Object): Int = {

      if (o1.isInstanceOf[Watermark] || o2.isInstanceOf[Watermark]) {
        // watermark is not expected
        -1
      } else {
        val row1 = o1.asInstanceOf[StreamRecord[CRow]].getValue
        val row2 = o2.asInstanceOf[StreamRecord[CRow]].getValue
        row1.toString.compareTo(row2.toString)
      }
    }
  }

  /**
    * Return 0 for equal Rows and non zero for different rows
    */
  class RowResultSortComparatorWithWatermarks()
    extends Comparator[Object] with Serializable {

    override def compare(o1: Object, o2: Object): Int = {

      (o1, o2) match {
        case (w1: Watermark, w2: Watermark) =>
          w1.getTimestamp.compareTo(w2.getTimestamp)
        case (r1: StreamRecord[CRow], r2: StreamRecord[CRow]) =>
          r1.getValue.toString.compareTo(r2.getValue.toString)
        case (_: Watermark, _: StreamRecord[CRow]) => -1
        case (_: StreamRecord[CRow], _: Watermark) => 1
        case _ => -1
      }
    }
  }

  /**
    * Tuple row key selector that returns a specified field as the selector function
    */
  class TupleRowKeySelector[T](
    private val selectorField: Int) extends KeySelector[CRow, T] {

    override def getKey(value: CRow): T = {
      value.row.getField(selectorField).asInstanceOf[T]
    }
  }

  /**
    * Test class used to test min and max retention time.
    */
  class TestStreamQueryConfig(min: Time, max: Time) extends StreamQueryConfig {
    override def getMinIdleStateRetentionTime: Long = min.toMilliseconds
    override def getMaxIdleStateRetentionTime: Long = max.toMilliseconds
  }
}
