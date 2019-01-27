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

import java.util
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.{Comparator, Queue => JQueue}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.configuration.{CheckpointingOptions, Configuration}
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.operators.{OneInputStreamOperator, TwoInputStreamOperator}
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.util.{KeyedOneInputStreamOperatorTestHarness, KeyedTwoInputStreamOperatorTestHarness, TestHarnessUtil}
import org.apache.flink.table.api.types.{DataTypes, TypeConverters}
import org.apache.flink.table.dataformat.util.BaseRowUtil
import org.apache.flink.table.dataformat.{BaseRow, GenericRow, JoinedRow}
import org.apache.flink.table.runtime.utils.StreamingTestBase
import org.apache.flink.table.runtime.utils.StreamingWithStateTestBase.{HEAP_BACKEND, ROCKSDB_BACKEND, StateBackendMode}
import org.apache.flink.table.typeutils.{BaseRowTypeInfo, TypeUtils}

import org.junit.runners.Parameterized

import scala.collection.JavaConversions._

class HarnessTestBase(mode: StateBackendMode) extends StreamingTestBase {

  protected def getStateBackend: StateBackend = {
    mode match {
      case HEAP_BACKEND =>
        val conf = new Configuration()
        conf.setBoolean(CheckpointingOptions.ASYNC_SNAPSHOTS, true)
        new MemoryStateBackend().configure(conf)

      case ROCKSDB_BACKEND =>
        new RocksDBStateBackend("file://" + tempFolder.newFolder().getAbsoluteFile)
    }
  }

  def createHarnessTester[IN, OUT, KEY](
      operator: OneInputStreamOperator[IN, OUT],
      keySelector: KeySelector[IN, KEY],
      keyType: TypeInformation[KEY]): KeyedOneInputStreamOperatorTestHarness[KEY, IN, OUT] = {
    val harness = new KeyedOneInputStreamOperatorTestHarness[KEY, IN, OUT](
      operator,
      keySelector,
      keyType)
    harness.setStateBackend(getStateBackend)
    harness
  }

  def createHarnessTester(
      ds: DataStream[_],
      prefixOperatorName: String)
  : KeyedOneInputStreamOperatorTestHarness[BaseRow, BaseRow, BaseRow] = {

    val transformation = extractExpectedTransformation(
      ds.javaStream.getTransformation,
      prefixOperatorName)
    val processOperator = transformation.getOperator
      .asInstanceOf[OneInputStreamOperator[Any, Any]]
    val keySelector = transformation.getStateKeySelector.asInstanceOf[KeySelector[Any, Any]]
    val keyType = transformation.getStateKeyType.asInstanceOf[TypeInformation[Any]]

    createHarnessTester(processOperator, keySelector, keyType)
      .asInstanceOf[KeyedOneInputStreamOperatorTestHarness[BaseRow, BaseRow, BaseRow]]
  }

  def createTwoInputHarnessTester[IN1, IN2, OUT, K](
    operator: TwoInputStreamOperator[IN1, IN2, OUT],
    leftKeySelector: KeySelector[IN1, K],
    rightKeySelector: KeySelector[IN2, K])
  : KeyedTwoInputStreamOperatorTestHarness[K, IN1, IN2, OUT] = {
    val testHarness =
      new KeyedTwoInputStreamOperatorTestHarness(
        operator,
        leftKeySelector,
        rightKeySelector,
        rightKeySelector.asInstanceOf[ResultTypeQueryable[K]].getProducedType,
        1, 1, 0)
    testHarness.setStateBackend(getStateBackend)
    testHarness
  }

//  def createTwoInputHarnessTester[IN1, IN2, OUT, K](
//    operator: TwoInputStreamOperator[IN1, IN2, OUT],
//    leftKeySelector: KeySelector[IN1, K],
//    rightKeySelector: KeySelector[IN2, K],
//    typeSerializer1: TypeSerializer[IN1],
//    typeSerializer2: TypeSerializer[IN2])
//  : KeyedTwoInputStreamOperatorTestHarness[K, IN1, IN2, OUT] = {
//    val testHarness =
//      new KeyedTwoInputStreamOperatorTestHarness(
//        operator,
//        leftKeySelector,
//        rightKeySelector,
//        rightKeySelector.asInstanceOf[ResultTypeQueryable[K]].getProducedType,
//        1, 1, 0)
//    operator.setup
//    testHarness.setStateBackend(getStateBackend)
//    testHarness
//  }

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

  def verify(
    expected: JQueue[Object],
    actual: JQueue[Object]): Unit = {

    TestHarnessUtil.assertOutputEquals("Verify Error...", expected, actual)
  }

  def removeWatermark(queue: JQueue[Object]): JQueue[Object] = {
    val it = queue.iterator()
    while (it.hasNext) {
      val data = it.next()
      if (data.isInstanceOf[Watermark]) {
        it.remove()
      }
    }
    queue
  }

  private def extractExpectedTransformation(
      t: StreamTransformation[_],
      prefixOperatorName: String): OneInputTransformation[_, _] = {
    t match {
      case one: OneInputTransformation[_, _] =>
        if (one.getName.startsWith(prefixOperatorName)) {
          one
        } else {
          extractExpectedTransformation(one.getInput, prefixOperatorName)
        }
      case _ => throw new Exception("Can not find the expected transformation")
    }
  }

  def dropWatermarks(elements: Array[AnyRef]): util.Collection[AnyRef] = {
    elements.filter(e => !e.isInstanceOf[Watermark]).toList
  }

  def convertStreamRecordToGenericRow(
    output: ConcurrentLinkedQueue[AnyRef], joinTypes: BaseRowTypeInfo)
  : ConcurrentLinkedQueue[Object] = {
    val outputList = new ConcurrentLinkedQueue[Object]
    val iter = output.iterator()
    val typeSerializers = joinTypes.getFieldTypes
        .map(TypeConverters.createInternalTypeFromTypeInfo).map(DataTypes.createInternalSerializer)
    while (iter.hasNext) {
      val element = iter.next()
      if (element.isInstanceOf[StreamRecord[BaseRow]]) {
        val row = element.asInstanceOf[StreamRecord[BaseRow]].getValue
        outputList.add(BaseRowUtil.toGenericRow(row, joinTypes.getFieldTypes, typeSerializers))
      }
    }
    outputList
  }

  def hOf(header: Byte, objects: Object*): GenericRow = {
    val row = GenericRow.of(objects: _*)
    row.setHeader(header)
    row
  }

  def jhOf(header: Byte, baseRow1: BaseRow, baseRow2: BaseRow): JoinedRow = {
    val joinedRow = new JoinedRow(baseRow1, baseRow2)
    joinedRow.setHeader(header)
    joinedRow
  }
}

object HarnessTestBase {

  @Parameterized.Parameters(name = "StateBackend={0}")
  def parameters(): util.Collection[Array[java.lang.Object]] = {
    Seq[Array[AnyRef]](Array(HEAP_BACKEND), Array(ROCKSDB_BACKEND))
  }

  /**
    * Return 0 for equal Rows and non zero for different rows
    */
  class BaseRowResultSortComparator() extends Comparator[Object] with Serializable {

    override def compare(o1: Object, o2: Object): Int = {

      if (o1.isInstanceOf[Watermark] || o2.isInstanceOf[Watermark]) {
        // watermark is not expected
        -1
      } else {
        val row1 = o1.asInstanceOf[StreamRecord[BaseRow]].getValue
        val row2 = o2.asInstanceOf[StreamRecord[BaseRow]].getValue
        row1.toString.compareTo(row2.toString)
      }
    }
  }
}
