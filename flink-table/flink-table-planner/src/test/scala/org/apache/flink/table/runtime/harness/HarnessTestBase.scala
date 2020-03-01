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

import java.lang.reflect.Field
import java.util.{Comparator, Queue => JQueue}

import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.BasicTypeInfo.{LONG_TYPE_INFO, STRING_TYPE_INFO}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.dag.Transformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.operators.{AbstractUdfStreamOperator, OneInputStreamOperator}
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.transformations._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.util.{KeyedOneInputStreamOperatorTestHarness, OneInputStreamOperatorTestHarness, TestHarnessUtil}
import org.apache.flink.table.api.StreamQueryConfig
import org.apache.flink.table.api.dataview.DataView
import org.apache.flink.table.codegen.GeneratedAggregationsFunction
import org.apache.flink.table.functions.aggfunctions.{CountAggFunction, IntSumWithRetractAggFunction, LongMaxWithRetractAggFunction, LongMinWithRetractAggFunction}
import org.apache.flink.table.functions.{AggregateFunction, UserDefinedFunction, UserDefinedFunctionHelper}
import org.apache.flink.table.runtime.aggregate.GeneratedAggregations
import org.apache.flink.table.runtime.harness.HarnessTestBase.{RowResultSortComparator, RowResultSortComparatorWithWatermarks}
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
import org.apache.flink.table.runtime.utils.StreamingWithStateTestBase
import org.apache.flink.table.utils.EncodingUtils

import _root_.scala.collection.JavaConversions._

class HarnessTestBase extends StreamingWithStateTestBase {

  val longMinWithRetractAggFunction: String =
    EncodingUtils.encodeObjectToString(new LongMinWithRetractAggFunction)

  val longMaxWithRetractAggFunction: String =
    EncodingUtils.encodeObjectToString(new LongMaxWithRetractAggFunction)

  val intSumWithRetractAggFunction: String =
    EncodingUtils.encodeObjectToString(new IntSumWithRetractAggFunction)

  val distinctCountAggFunction: String =
    EncodingUtils.encodeObjectToString(new CountAggFunction())

  protected val MinMaxRowType = new RowTypeInfo(Array[TypeInformation[_]](
    LONG_TYPE_INFO,
    STRING_TYPE_INFO,
    LONG_TYPE_INFO),
    Array("rowtime", "a", "b"))

  protected val minMaxCRowType = new CRowTypeInfo(MinMaxRowType)

  protected val minMaxAggregates: Array[AggregateFunction[_, _]] =
    Array(new LongMinWithRetractAggFunction,
          new LongMaxWithRetractAggFunction).asInstanceOf[Array[AggregateFunction[_, _]]]

  protected val sumAggregates: Array[AggregateFunction[_, _]] =
    Array(new IntSumWithRetractAggFunction).asInstanceOf[Array[AggregateFunction[_, _]]]

  protected val distinctCountAggregates: Array[AggregateFunction[_, _]] =
    Array(new CountAggFunction).asInstanceOf[Array[AggregateFunction[_, _]]]

  protected val minMaxAggregationStateType: RowTypeInfo =
    new RowTypeInfo(minMaxAggregates
      .map(UserDefinedFunctionHelper.getAccumulatorTypeOfAggregateFunction(_)): _*)

  protected val sumAggregationStateType: RowTypeInfo =
    new RowTypeInfo(sumAggregates
      .map(UserDefinedFunctionHelper.getAccumulatorTypeOfAggregateFunction(_)): _*)

  protected val minMaxFuncName = "MinMaxAggregateHelper"
  protected val sumFuncName = "SumAggregationHelper"

  val minMaxCode: String =
    s"""
      |public class $minMaxFuncName
      |  extends org.apache.flink.table.runtime.aggregate.GeneratedAggregations {
      |
      |  transient org.apache.flink.table.functions.aggfunctions.LongMinWithRetractAggFunction
      |    fmin = null;
      |
      |  transient org.apache.flink.table.functions.aggfunctions.LongMaxWithRetractAggFunction
      |    fmax = null;
      |
      |  public $minMaxFuncName() throws Exception {
      |
      |    fmin = (org.apache.flink.table.functions.aggfunctions.LongMinWithRetractAggFunction)
      |    ${classOf[EncodingUtils].getCanonicalName}.decodeStringToObject(
      |      "$longMinWithRetractAggFunction",
      |      ${classOf[UserDefinedFunction].getCanonicalName}.class);
      |
      |    fmax = (org.apache.flink.table.functions.aggfunctions.LongMaxWithRetractAggFunction)
      |    ${classOf[EncodingUtils].getCanonicalName}.decodeStringToObject(
      |      "$longMaxWithRetractAggFunction",
      |      ${classOf[UserDefinedFunction].getCanonicalName}.class);
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
      |public final class $sumFuncName
      |  extends org.apache.flink.table.runtime.aggregate.GeneratedAggregations {
      |
      |
      |  transient org.apache.flink.table.functions.aggfunctions.IntSumWithRetractAggFunction
      |  sum = null;
      |  private final org.apache.flink.table.runtime.aggregate.SingleElementIterable<org.apache
      |    .flink.table.functions.aggfunctions.SumWithRetractAccumulator> accIt0 =
      |      new org.apache.flink.table.runtime.aggregate.SingleElementIterable<org.apache.flink
      |      .table
      |      .functions.aggfunctions.SumWithRetractAccumulator>();
      |
      |  public $sumFuncName() throws Exception {
      |
      |    sum = (org.apache.flink.table.functions.aggfunctions.IntSumWithRetractAggFunction)
      |      ${classOf[EncodingUtils].getCanonicalName}.decodeStringToObject(
      |        "$intSumWithRetractAggFunction",
      |        ${classOf[UserDefinedFunction].getCanonicalName}.class);
      |  }
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
      |
      |    sum.retract(
      |      ((org.apache.flink.table.functions.aggfunctions.SumWithRetractAccumulator) accs
      |      .getField
      |      (0)),
      |      (java.lang.Integer) input.getField(1));
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

  protected val genMinMaxAggFunction = GeneratedAggregationsFunction(minMaxFuncName, minMaxCode)
  protected val genSumAggFunction = GeneratedAggregationsFunction(sumFuncName, sumAggCode)

  def createHarnessTester[KEY, IN, OUT](
      dataStream: DataStream[_],
      prefixOperatorName: String)
  : KeyedOneInputStreamOperatorTestHarness[KEY, IN, OUT] = {

    val transformation = extractExpectedTransformation(
      dataStream.javaStream.getTransformation,
      prefixOperatorName).asInstanceOf[OneInputTransformation[_, _]]
    if (transformation == null) {
      throw new Exception("Can not find the expected transformation")
    }

    val processOperator = transformation.getOperator.asInstanceOf[OneInputStreamOperator[IN, OUT]]
    val keySelector = transformation.getStateKeySelector.asInstanceOf[KeySelector[IN, KEY]]
    val keyType = transformation.getStateKeyType.asInstanceOf[TypeInformation[KEY]]

    createHarnessTester(processOperator, keySelector, keyType)
      .asInstanceOf[KeyedOneInputStreamOperatorTestHarness[KEY, IN, OUT]]
  }

  private def extractExpectedTransformation(
      transformation: Transformation[_],
      prefixOperatorName: String): Transformation[_] = {
    def extractFromInputs(inputs: Transformation[_]*): Transformation[_] = {
      for (input <- inputs) {
        val t = extractExpectedTransformation(input, prefixOperatorName)
        if (t != null) {
          return t
        }
      }
      null
    }

    transformation match {
      case one: OneInputTransformation[_, _] =>
        if (one.getName.startsWith(prefixOperatorName)) {
          one
        } else {
          extractExpectedTransformation(one.getInput, prefixOperatorName)
        }
      case union: UnionTransformation[_] => extractFromInputs(union.getInputs.toSeq: _*)
      case p: PartitionTransformation[_] => extractFromInputs(p.getInput)
      case _: SourceTransformation[_] => null
      case _ => throw new UnsupportedOperationException("This should not happen.")
    }
  }

  def getState(
      operator: AbstractUdfStreamOperator[_, _],
      funcName: String,
      funcClass: Class[_],
      stateFieldName: String): DataView = {
    val function = funcClass.getDeclaredField(funcName)
    function.setAccessible(true)
    val generatedAggregation =
      function.get(operator.getUserFunction).asInstanceOf[GeneratedAggregations]
    val cls = generatedAggregation.getClass
    val stateField = cls.getDeclaredField(stateFieldName)
    stateField.setAccessible(true)
    stateField.get(generatedAggregation).asInstanceOf[DataView]
  }

  def getGeneratedAggregationFields(
      operator: AbstractUdfStreamOperator[_, _],
      funcName: String,
      funcClass: Class[_]): Array[Field] = {
    val function = funcClass.getDeclaredField(funcName)
    function.setAccessible(true)
    val generatedAggregation =
      function.get(operator.getUserFunction).asInstanceOf[GeneratedAggregations]
    val cls = generatedAggregation.getClass
    val fields = cls.getDeclaredFields
    fields.foreach(_.setAccessible(true))
    fields
  }

  def createHarnessTester[IN, OUT, KEY](
    operator: OneInputStreamOperator[IN, OUT],
    keySelector: KeySelector[IN, KEY],
    keyType: TypeInformation[KEY]): KeyedOneInputStreamOperatorTestHarness[KEY, IN, OUT] = {
    new KeyedOneInputStreamOperatorTestHarness[KEY, IN, OUT](operator, keySelector, keyType)
  }

  def getOperator(testHarness: OneInputStreamOperatorTestHarness[_, _])
  : AbstractUdfStreamOperator[_, _] =
    testHarness.getOneInputOperator.asInstanceOf[AbstractUdfStreamOperator[_, _]]

  def verify(expected: JQueue[Object], actual: JQueue[Object]): Unit = {
    verify(expected, actual, new RowResultSortComparator)
  }

  def verifyWithWatermarks(expected: JQueue[Object], actual: JQueue[Object]): Unit = {
    verify(expected, actual, new RowResultSortComparatorWithWatermarks, true)
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
