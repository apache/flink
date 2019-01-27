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
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.util.TestHarnessUtil
import org.apache.flink.table.api.types.{DataTypes, TypeConverters}
import org.apache.flink.table.dataformat.{BaseRow, BinaryRow, BinaryRowWriter, GenericRow}
import org.apache.flink.table.plan.util.SortUtil
import org.apache.flink.table.runtime.aggregate.SorterHelper
import org.apache.flink.table.runtime.harness.SortProcessOperatorHarnessTest._
import org.apache.flink.table.runtime.sort.{ProcTimeSortOperator, RowTimeSortOperator}
import org.apache.flink.table.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.typeutils._

import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

@RunWith(classOf[Parameterized])
class SortProcessOperatorHarnessTest(mode: StateBackendMode) extends HarnessTestBase(mode) {

  @Test
  def testSortProcTimeHarnessPartitioned(): Unit = {
    val rT = new BaseRowTypeInfo(
      Array[TypeInformation[_]](
        INT_TYPE_INFO,
        LONG_TYPE_INFO,
        INT_TYPE_INFO,
        STRING_TYPE_INFO,
        LONG_TYPE_INFO),
      Array("a", "b", "c", "d", "e"))

    val outputSerializer = new BaseRowSerializer(classOf[GenericRow], rT.getFieldTypes: _*)

    val indexes = Array(1, 2)
    val booleanOrders = Array(true, false)

    val nullsIsLast = SortUtil.getNullDefaultOrders(booleanOrders)

    val generatedSorter = SorterHelper.createSorter(
      rT.getFieldTypes.map(
        TypeConverters.createInternalTypeFromTypeInfo), indexes, booleanOrders, nullsIsLast)

    val sortOperator = new ProcTimeSortOperator(
      rT.asInstanceOf[BaseRowTypeInfo],
      generatedSorter,
      1 * 1024 * 1024)

    val testHarness = createHarnessTester(
      sortOperator,
      new TupleRowSelector(0),
      BasicTypeInfo.INT_TYPE_INFO)

    testHarness.setup(outputSerializer.asInstanceOf[TypeSerializer[BaseRow]])
    testHarness.open()
    testHarness.setProcessingTime(3)

      // timestamp is ignored in processing time
    testHarness.processElement(new StreamRecord(
      newRow(1: JInt, 11L: JLong, 1: JInt, "aaa", 11L: JLong)))
    testHarness.processElement(new StreamRecord(
      newRow(1: JInt, 12L: JLong, 1: JInt, "aaa", 11L: JLong)))
    testHarness.processElement(new StreamRecord(
      newRow(1: JInt, 12L: JLong, 2: JInt, "aaa", 11L: JLong)))
    testHarness.processElement(new StreamRecord(
      newRow(1: JInt, 12L: JLong, 0: JInt, "aaa", 11L: JLong)))
    testHarness.processElement(new StreamRecord(
      newRow(1: JInt, 10L: JLong, 0: JInt, "aaa", 11L: JLong)))

    //move the timestamp to ensure the execution
    testHarness.setProcessingTime(1005)

    testHarness.processElement(new StreamRecord(
      newRow(1: JInt, 1L: JLong, 0: JInt, "aaa", 11L: JLong)))
    testHarness.processElement(new StreamRecord(
      newRow(1: JInt, 3L: JLong, 0: JInt, "aaa", 11L: JLong)))
    testHarness.processElement(new StreamRecord(
      newRow(1: JInt, 2L: JLong, 0: JInt, "aaa", 11L: JLong)))

    testHarness.setProcessingTime(1008)

    val result = testHarness.getOutput

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    // all elements at the same proc timestamp have the same value
    // elements should be sorted ascending on field 1 and descending on field 2
    // (10,0) (11,1) (12,2) (12,1) (12,0)
    // (1,0) (2,0)

    expectedOutput.add(new StreamRecord(
      newRow(1: JInt, 10L: JLong, 0: JInt, "aaa", 11L: JLong)))
    expectedOutput.add(new StreamRecord(
      newRow(1: JInt, 11L: JLong, 1: JInt, "aaa", 11L: JLong)))
    expectedOutput.add(new StreamRecord(
      newRow(1: JInt, 12L: JLong, 2: JInt, "aaa", 11L: JLong)))
    expectedOutput.add(new StreamRecord(
      newRow(1: JInt, 12L: JLong, 1: JInt, "aaa", 11L: JLong)))
    expectedOutput.add(new StreamRecord(
      newRow(1: JInt, 12L: JLong, 0: JInt, "aaa", 11L: JLong)))

    expectedOutput.add(new StreamRecord(
      newRow(1: JInt, 1L: JLong, 0: JInt, "aaa", 11L: JLong)))
    expectedOutput.add(new StreamRecord(
        newRow(1: JInt, 2L: JLong, 0: JInt, "aaa", 11L: JLong)))
    expectedOutput.add(new StreamRecord(
      newRow(1: JInt, 3L: JLong, 0: JInt, "aaa", 11L: JLong)))

    TestHarnessUtil.assertOutputEquals("Output was not correctly sorted.", expectedOutput, result)

    testHarness.getEnvironment.getIOManager.shutdown()
    testHarness.getEnvironment.getMemoryManager.shutdown()
    testHarness.close()
  }

  @Test
  def testSortRowTimeHarnessPartitioned(): Unit = {

    val rT = new BaseRowTypeInfo(
      Array[TypeInformation[_]](
        INT_TYPE_INFO,
        LONG_TYPE_INFO,
        INT_TYPE_INFO,
        STRING_TYPE_INFO,
        TimeIndicatorTypeInfo.ROWTIME_INDICATOR),
      Array("a", "b", "c", "d", "e"))

    val outputSerializer = new BaseRowSerializer(classOf[GenericRow], rT.getFieldTypes: _*)

    val indexes = Array(1, 2)
    val booleanOrders = Array(true, false)
    val nullsIsLast = SortUtil.getNullDefaultOrders(booleanOrders)

    val generatedSorter = SorterHelper.createSorter(
      rT.getFieldTypes.map(
        TypeConverters.createInternalTypeFromTypeInfo), indexes, booleanOrders, nullsIsLast)

    val processOperator = new RowTimeSortOperator(
      rT.asInstanceOf[BaseRowTypeInfo],
      generatedSorter,
      4,
      1 * 1024 * 1024)

    val testHarness = createHarnessTester(
     processOperator,
      new TupleRowSelector(0),
      BasicTypeInfo.INT_TYPE_INFO)

    testHarness.setup(outputSerializer.asInstanceOf[TypeSerializer[BaseRow]])

    testHarness.open()

    testHarness.setTimeCharacteristic(TimeCharacteristic.EventTime)
    testHarness.processWatermark(3)

      // timestamp is ignored in processing time
    testHarness.processElement(new StreamRecord(
      newRow(1: JInt, 11L: JLong, 1: JInt, "aaa", 1001L: JLong)))
    testHarness.processElement(new StreamRecord(
      newRow(1: JInt, 12L: JLong, 1: JInt, "aaa", 2002L: JLong)))
    testHarness.processElement(new StreamRecord(
      newRow(1: JInt, 13L: JLong, 2: JInt, "aaa", 2002L: JLong)))
    testHarness.processElement(new StreamRecord(
      newRow(1: JInt, 12L: JLong, 3: JInt, "aaa", 2002L: JLong)))
    testHarness.processElement(new StreamRecord(
      newRow(1: JInt, 14L: JLong, 0: JInt, "aaa", 2002L: JLong)))
    testHarness.processElement(new StreamRecord(
      newRow(1: JInt, 12L: JLong, 3: JInt, "aaa", 2004L: JLong)))
    testHarness.processElement(new StreamRecord(
      newRow(1: JInt, 10L: JLong, 0: JInt, "aaa", 2006L: JLong)))

    // move watermark forward
    testHarness.processWatermark(2007)

    testHarness.processElement(new StreamRecord(
      newRow(1: JInt, 20L: JLong, 1: JInt, "aaa", 2008L: JLong)))
    testHarness.processElement(new StreamRecord(
      newRow(1: JInt, 14L: JLong, 0: JInt, "aaa", 2002L: JLong))) // too late
    testHarness.processElement(new StreamRecord(
      newRow(1: JInt, 12L: JLong, 3: JInt, "aaa", 2019L: JLong))) // too early
    testHarness.processElement(new StreamRecord(
      newRow(1: JInt, 20L: JLong, 2: JInt, "aaa", 2008L: JLong)))
    testHarness.processElement(new StreamRecord(
      newRow(1: JInt, 10L: JLong, 0: JInt, "aaa", 2010L: JLong)))
    testHarness.processElement(new StreamRecord(
      newRow(1: JInt, 19L: JLong, 0: JInt, "aaa", 2008L: JLong)))

    // move watermark forward
    testHarness.processWatermark(2012)

    val result = testHarness.getOutput

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    // all elements at the same proc timestamp have the same value
    // elements should be sorted ascending on field 1 and descending on field 2
    // (10,0) (11,1) (12,2) (12,1) (12,0)
    expectedOutput.add(new Watermark(3))
    expectedOutput.add(new StreamRecord(
      newRow(1: JInt, 11L: JLong, 1: JInt, "aaa", 1001L: JLong)))
    expectedOutput.add(new StreamRecord(
      newRow(1: JInt, 12L: JLong, 3: JInt, "aaa", 2002L: JLong)))
    expectedOutput.add(new StreamRecord(
      newRow(1: JInt, 12L: JLong, 1: JInt, "aaa", 2002L: JLong)))
    expectedOutput.add(new StreamRecord(
      newRow(1: JInt, 13L: JLong, 2: JInt, "aaa", 2002L: JLong)))
    expectedOutput.add(new StreamRecord(
      newRow(1: JInt, 14L: JLong, 0: JInt, "aaa", 2002L: JLong)))
    expectedOutput.add(new StreamRecord(
      newRow(1: JInt, 12L: JLong, 3: JInt, "aaa", 2004L: JLong)))
    expectedOutput.add(new StreamRecord(
      newRow(1: JInt, 10L: JLong, 0: JInt, "aaa", 2006L: JLong)))
    expectedOutput.add(new Watermark(2007))

    expectedOutput.add(new StreamRecord(
      newRow(1: JInt, 19L: JLong, 0: JInt, "aaa", 2008L: JLong)))
    expectedOutput.add(new StreamRecord(
      newRow(1: JInt, 20L: JLong, 2: JInt, "aaa", 2008L: JLong)))
    expectedOutput.add(new StreamRecord(
      newRow(1: JInt, 20L: JLong, 1: JInt, "aaa", 2008L: JLong)))
    expectedOutput.add(new StreamRecord(
      newRow(1: JInt, 10L: JLong, 0: JInt, "aaa", 2010L: JLong)))

    expectedOutput.add(new Watermark(2012))

    TestHarnessUtil.assertOutputEquals("Output was not correctly sorted.", expectedOutput, result)

    testHarness.getEnvironment.getIOManager.shutdown()
    testHarness.getEnvironment.getMemoryManager.shutdown()
    testHarness.close()

  }
}

object SortProcessOperatorHarnessTest {

  /**
   * Simple test class that returns a specified field as the selector function
   */
  class TupleRowSelector(private val selectorField: Int) extends KeySelector[BaseRow, Integer] {

    override def getKey(value: BaseRow): Integer = {
      value.getInt(selectorField)
    }
  }

  def newRow(f0: Int, f1: Long, f2: Int, f3: String, f4: Long): BinaryRow = {
    val row = new BinaryRow(5)
    val writer = new BinaryRowWriter(row)
    writer.writeInt(0, f0)
    writer.writeLong(1, f1)
    writer.writeInt(2, f2)
    writer.writeString(3, f3)
    writer.writeLong(4, f4)
    writer.complete()
    row
  }
}
