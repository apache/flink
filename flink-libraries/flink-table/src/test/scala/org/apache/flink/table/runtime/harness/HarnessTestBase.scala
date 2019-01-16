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
import java.lang.{Boolean => JBoolean}
import java.util.{Comparator, Queue => JQueue}

import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.datastream.{DataStream => JDataStream}
import org.apache.flink.streaming.api.operators.{AbstractStreamOperator, AbstractUdfStreamOperator, OneInputStreamOperator, TwoInputStreamOperator}
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.transformations._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.util._
import org.apache.flink.table.api.dataview.DataView
import org.apache.flink.table.api.{StreamQueryConfig, Table}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.runtime.aggregate.GeneratedAggregations
import org.apache.flink.table.runtime.harness.HarnessTestBase.{RowResultSortComparator, RowResultSortComparatorWithWatermarks}
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.table.runtime.utils.StreamingWithStateTestBase

import _root_.scala.collection.JavaConversions._

class HarnessTestBase extends StreamingWithStateTestBase {

  def createHarnessTester[KEY, IN, OUT](
      dataStream: DataStream[_],
      prefixOperatorName: String)
  : KeyedOneInputStreamOperatorTestHarness[KEY, IN, OUT] = {
    createHarnessTester(dataStream.javaStream, prefixOperatorName)
  }

  def createHarnessTester[KEY, IN, OUT](
      dataStream: JDataStream[_],
      prefixOperatorName: String)
  : KeyedOneInputStreamOperatorTestHarness[KEY, IN, OUT] = {

    val transformation = extractExpectedTransformation(
      dataStream.getTransformation,
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

  def createHarnessTester[KEY, IN1, IN2, OUT](
      dataStream: DataStream[_],
      prefixOperatorName: String)
  : KeyedTwoInputStreamOperatorTestHarness[KEY, IN1, IN2, OUT] = {
    createHarnessTester(dataStream.javaStream, prefixOperatorName)
  }

  def createHarnessTester[KEY, IN1, IN2, OUT](
      dataStream: JDataStream[_],
      prefixOperatorName: String)
  : KeyedTwoInputStreamOperatorTestHarness[KEY, IN1, IN2, OUT] = {

    val transformation = extractExpectedTransformation(
      dataStream.getTransformation,
      prefixOperatorName).asInstanceOf[TwoInputTransformation[_, _, _]]
    if (transformation == null) {
      throw new Exception("Can not find the expected transformation")
    }

    val processOperator =
      transformation.getOperator.asInstanceOf[TwoInputStreamOperator[IN1, IN2, OUT]]
    val keySelector1 = transformation.getStateKeySelector1.asInstanceOf[KeySelector[IN1, KEY]]
    val keySelector2 = transformation.getStateKeySelector2.asInstanceOf[KeySelector[IN2, KEY]]
    val keyType = transformation.getStateKeyType.asInstanceOf[TypeInformation[KEY]]

    createHarnessTester(processOperator, keySelector1, keySelector2, keyType)
      .asInstanceOf[KeyedTwoInputStreamOperatorTestHarness[KEY, IN1, IN2, OUT]]
  }

  private def extractExpectedTransformation(
      transformation: StreamTransformation[_],
      prefixOperatorName: String): StreamTransformation[_] = {
    def extractFromInputs(inputs: StreamTransformation[_]*): StreamTransformation[_] = {
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
      case two: TwoInputTransformation[_, _, _] =>
        if (two.getName.startsWith(prefixOperatorName)) {
          two
        } else {
          extractFromInputs(two.getInput1, two.getInput2)
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

  def createHarnessTester[IN1, IN2, OUT, KEY](
      operator: TwoInputStreamOperator[IN1, IN2, OUT],
      keySelector1: KeySelector[IN1, KEY],
      keySelector2: KeySelector[IN2, KEY],
      keyType: TypeInformation[KEY])
      : KeyedTwoInputStreamOperatorTestHarness[KEY, IN1, IN2, OUT] = {
    new KeyedTwoInputStreamOperatorTestHarness[KEY, IN1, IN2, OUT](
      operator, keySelector1, keySelector2, keyType)
  }

  def getOperator(testHarness: OneInputStreamOperatorTestHarness[_, _])
      : AbstractUdfStreamOperator[_, _] = {
    val operatorField = classOf[OneInputStreamOperatorTestHarness[_, _]]
      .getDeclaredField("oneInputOperator")
    operatorField.setAccessible(true)
    operatorField.get(testHarness).asInstanceOf[AbstractUdfStreamOperator[_, _]]
  }

  def getOperator(testHarness: TwoInputStreamOperatorTestHarness[_, _, _])
      : AbstractStreamOperator[_] = {
    val operatorField = classOf[TwoInputStreamOperatorTestHarness[_, _, _]]
      .getDeclaredField("twoInputOperator")
    operatorField.setAccessible(true)
    operatorField.get(testHarness).asInstanceOf[AbstractStreamOperator[_]]
  }

  def toUpsertStream[T: TypeInformation](
      tEnv: StreamTableEnvironment,
      table: Table,
      queryConfig: StreamQueryConfig): JDataStream[(JBoolean, T)] = {
    val returnType = createTypeInformation[(JBoolean, T)]
    val translateMethod = classOf[org.apache.flink.table.api.StreamTableEnvironment]
      .getDeclaredMethod(
        "translate",
        classOf[Table],
        classOf[StreamQueryConfig],
        JBoolean.TYPE,              // updatesAsRetraction
        JBoolean.TYPE,              // withChangeFlag
        classOf[TypeInformation[_]])
    translateMethod.setAccessible(true)

    translateMethod.invoke(
      tEnv, table, queryConfig, JBoolean.FALSE, JBoolean.TRUE, returnType)
      .asInstanceOf[JDataStream[(JBoolean, T)]]
  }

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
