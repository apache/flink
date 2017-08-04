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

import java.lang.{Long => JLong}
import java.util.concurrent.ConcurrentLinkedQueue

import org.apache.calcite.runtime.SqlFunctions.{internalToTimestamp => toTS}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.streaming.api.operators.KeyedProcessOperator
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.table.api.{StreamQueryConfig, Types}
import org.apache.flink.table.runtime.aggregate._
import org.apache.flink.table.runtime.harness.HarnessTestBase._
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.types.Row
import org.junit.Test

class OverWindowHarnessTest extends HarnessTestBase{

  protected var queryConfig =
    new StreamQueryConfig().withIdleStateRetentionTime(Time.seconds(2), Time.seconds(3))

  @Test
  def testProcTimeBoundedRowsOver(): Unit = {

    val processFunction = new KeyedProcessOperator[String, CRow, CRow](
      new ProcTimeBoundedRowsOver(
        genMinMaxAggFunction,
        2,
        minMaxAggregationStateType,
        minMaxCRowType,
        queryConfig))

    val testHarness =
      createHarnessTester(
        processFunction,
        new TupleRowKeySelector[String](1),
        Types.STRING)

    testHarness.open()

    // register cleanup timer with 3001
    testHarness.setProcessingTime(1)

    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(1), "aaa", 1L: JLong), true)))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(1), "bbb", 10L: JLong), true)))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(1), "aaa", 2L: JLong), true)))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(1), "aaa", 3L: JLong), true)))

    // register cleanup timer with 4100
    testHarness.setProcessingTime(1100)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(1), "bbb", 20L: JLong), true)))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(1), "aaa", 4L: JLong), true)))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(1), "aaa", 5L: JLong), true)))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(1), "aaa", 6L: JLong), true)))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(1), "bbb", 30L: JLong), true)))

    // register cleanup timer with 6001
    testHarness.setProcessingTime(3001)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(2), "aaa", 7L: JLong), true)))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(2), "aaa", 8L: JLong), true)))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(2), "aaa", 9L: JLong), true)))

    // trigger cleanup timer and register cleanup timer with 9002
    testHarness.setProcessingTime(6002)
    testHarness.processElement(new StreamRecord(
        CRow(Row.of(toTS(2), "aaa", 10L: JLong), true)))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(2), "bbb", 40L: JLong), true)))

    val result = testHarness.getOutput

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(1), "aaa", 1L: JLong, 1L: JLong, 1L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(1), "bbb", 10L: JLong, 10L: JLong, 10L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(1), "aaa", 2L: JLong, 1L: JLong, 2L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(1), "aaa", 3L: JLong, 2L: JLong, 3L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(1), "bbb", 20L: JLong, 10L: JLong, 20L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(1), "aaa", 4L: JLong, 3L: JLong, 4L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(1), "aaa", 5L: JLong, 4L: JLong, 5L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(1), "aaa", 6L: JLong, 5L: JLong, 6L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(1), "bbb", 30L: JLong, 20L: JLong, 30L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(2), "aaa", 7L: JLong, 6L: JLong, 7L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(2), "aaa", 8L: JLong, 7L: JLong, 8L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(2), "aaa", 9L: JLong, 8L: JLong, 9L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(2), "aaa", 10L: JLong, 10L: JLong, 10L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(2), "bbb", 40L: JLong, 40L: JLong, 40L: JLong), true)))

    verify(expectedOutput, result, new RowResultSortComparator())

    testHarness.close()
  }

  /**
    * NOTE: all elements at the same proc timestamp have the same value per key
    */
  @Test
  def testProcTimeBoundedRangeOver(): Unit = {

    val processFunction = new KeyedProcessOperator[String, CRow, CRow](
      new ProcTimeBoundedRangeOver(
        genMinMaxAggFunction,
        4000,
        minMaxAggregationStateType,
        minMaxCRowType,
        queryConfig))

    val testHarness =
      createHarnessTester(
        processFunction,
        new TupleRowKeySelector[String](1),
        Types.STRING)

    testHarness.open()

    // register cleanup timer with 3003
    testHarness.setProcessingTime(3)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(0), "aaa", 1L: JLong), true)))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(0), "bbb", 10L: JLong), true)))

    testHarness.setProcessingTime(4)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(0), "aaa", 2L: JLong), true)))

    // trigger cleanup timer and register cleanup timer with 6003
    testHarness.setProcessingTime(3003)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(0), "aaa", 3L: JLong), true)))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(0), "bbb", 20L: JLong), true)))

    testHarness.setProcessingTime(5)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(0), "aaa", 4L: JLong), true)))

    // register cleanup timer with 9002
    testHarness.setProcessingTime(6002)

    testHarness.setProcessingTime(7002)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(0), "aaa", 5L: JLong), true)))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(0), "aaa", 6L: JLong), true)))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(0), "bbb", 30L: JLong), true)))

    // register cleanup timer with 14002
    testHarness.setProcessingTime(11002)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(0), "aaa", 7L: JLong), true)))

    testHarness.setProcessingTime(11004)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(0), "aaa", 8L: JLong), true)))

    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(0), "aaa", 9L: JLong), true)))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(0), "aaa", 10L: JLong), true)))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(0), "bbb", 40L: JLong), true)))

    testHarness.setProcessingTime(11006)

    val result = testHarness.getOutput

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    // all elements at the same proc timestamp have the same value per key
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(0), "aaa", 1L: JLong, 1L: JLong, 1L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(0), "bbb", 10L: JLong, 10L: JLong, 10L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(0), "aaa", 2L: JLong, 1L: JLong, 2L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(0), "aaa", 3L: JLong, 3L: JLong, 4L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(0), "bbb", 20L: JLong, 20L: JLong, 20L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(0), "aaa", 4L: JLong, 4L: JLong, 4L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(0), "aaa", 5L: JLong, 5L: JLong, 6L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(0), "aaa", 6L: JLong, 5L: JLong, 6L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(0), "bbb", 30L: JLong, 30L: JLong, 30L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(0), "aaa", 7L: JLong, 7L: JLong, 7L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(0), "aaa", 8L: JLong, 7L: JLong, 10L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(0), "aaa", 9L: JLong, 7L: JLong, 10L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(0), "aaa", 10L: JLong, 7L: JLong, 10L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(0), "bbb", 40L: JLong, 40L: JLong, 40L: JLong), true)))

    verify(expectedOutput, result, new RowResultSortComparator())

    testHarness.close()
  }

  @Test
  def testProcTimeUnboundedOver(): Unit = {

    val processFunction = new KeyedProcessOperator[String, CRow, CRow](
      new ProcTimeUnboundedOver(
        genMinMaxAggFunction,
        minMaxAggregationStateType,
        queryConfig))

    val testHarness =
      createHarnessTester(
        processFunction,
        new TupleRowKeySelector[String](1),
        Types.STRING)

    testHarness.open()

    // register cleanup timer with 4003
    testHarness.setProcessingTime(1003)

    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(0), "aaa", 1L: JLong), true)))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(0), "bbb", 10L: JLong), true)))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(0), "aaa", 2L: JLong), true)))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(0), "aaa", 3L: JLong), true)))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(0), "bbb", 20L: JLong), true)))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(0), "aaa", 4L: JLong), true)))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(0), "aaa", 5L: JLong), true)))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(0), "aaa", 6L: JLong), true)))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(0), "bbb", 30L: JLong), true)))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(0), "aaa", 7L: JLong), true)))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(0), "aaa", 8L: JLong), true)))

    // trigger cleanup timer and register cleanup timer with 8003
    testHarness.setProcessingTime(5003)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(0), "aaa", 9L: JLong), true)))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(0), "aaa", 10L: JLong), true)))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(0), "bbb", 40L: JLong), true)))

    val result = testHarness.getOutput

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(0), "aaa", 1L: JLong, 1L: JLong, 1L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(0), "bbb", 10L: JLong, 10L: JLong, 10L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(0), "aaa", 2L: JLong, 1L: JLong, 2L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(0), "aaa", 3L: JLong, 1L: JLong, 3L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(0), "bbb", 20L: JLong, 10L: JLong, 20L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(0), "aaa", 4L: JLong, 1L: JLong, 4L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(0), "aaa", 5L: JLong, 1L: JLong, 5L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(0), "aaa", 6L: JLong, 1L: JLong, 6L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(0), "bbb", 30L: JLong, 10L: JLong, 30L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(0), "aaa", 7L: JLong, 1L: JLong, 7L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(0), "aaa", 8L: JLong, 1L: JLong, 8L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(0), "aaa", 9L: JLong, 9L: JLong, 9L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(0), "aaa", 10L: JLong, 9L: JLong, 10L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(0), "bbb", 40L: JLong, 40L: JLong, 40L: JLong), true)))

    verify(expectedOutput, result, new RowResultSortComparator())
    testHarness.close()
  }

  /**
    * all elements at the same row-time have the same value per key
    */
  @Test
  def testRowTimeBoundedRangeOver(): Unit = {

    val processFunction = new KeyedProcessOperator[String, CRow, CRow](
      new RowTimeBoundedRangeOver(
        genMinMaxAggFunction,
        minMaxAggregationStateType,
        minMaxCRowType,
        4000,
        0,
        new StreamQueryConfig().withIdleStateRetentionTime(Time.seconds(1), Time.seconds(2))))

    val testHarness =
      createHarnessTester(
        processFunction,
        new TupleRowKeySelector[String](1),
        BasicTypeInfo.STRING_TYPE_INFO)

    testHarness.open()

    testHarness.processWatermark(1)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(2), "aaa", 1L: JLong), true)))

    testHarness.processWatermark(2)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(3), "bbb", 10L: JLong), true)))

    testHarness.processWatermark(4000)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(4001), "aaa", 2L: JLong), true)))

    testHarness.processWatermark(4001)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(4002), "aaa", 3L: JLong), true)))

    testHarness.processWatermark(4002)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(4003), "aaa", 4L: JLong), true)))

    testHarness.processWatermark(4800)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(4801), "bbb", 25L: JLong), true)))

    testHarness.processWatermark(6500)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(6501), "aaa", 5L: JLong), true)))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(6501), "aaa", 6L: JLong), true)))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(6501), "bbb", 30L: JLong), true)))

    testHarness.processWatermark(7000)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(7001), "aaa", 7L: JLong), true)))

    testHarness.processWatermark(8000)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(8001), "aaa", 8L: JLong), true)))

    testHarness.processWatermark(12000)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(12001), "aaa", 9L: JLong), true)))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(12001), "aaa", 10L: JLong), true)))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(12001), "bbb", 40L: JLong), true)))

    testHarness.processWatermark(19000)

    // test cleanup
    testHarness.setProcessingTime(1000)
    testHarness.processWatermark(20000)

    // check that state is removed after max retention time
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(20001), "ccc", 1L: JLong), true))) // clean-up 3000
    testHarness.setProcessingTime(2500)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(20002), "ccc", 2L: JLong), true))) // clean-up 4500
    testHarness.processWatermark(20010) // compute output

    assert(testHarness.numKeyedStateEntries() > 0) // check that we have state
    testHarness.setProcessingTime(4499)
    assert(testHarness.numKeyedStateEntries() > 0) // check that we have state
    testHarness.setProcessingTime(4500)
    val x = testHarness.numKeyedStateEntries()
    assert(testHarness.numKeyedStateEntries() == 0) // check that all state is gone

    // check that state is only removed if all data was processed
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(20011), "ccc", 3L: JLong), true))) // clean-up 6500

    assert(testHarness.numKeyedStateEntries() > 0) // check that we have state
    testHarness.setProcessingTime(6500) // clean-up attempt but rescheduled to 8500
    assert(testHarness.numKeyedStateEntries() > 0) // check that we have state

    testHarness.processWatermark(20020) // schedule emission

    assert(testHarness.numKeyedStateEntries() > 0) // check that we have state
    testHarness.setProcessingTime(8499) // clean-up
    assert(testHarness.numKeyedStateEntries() > 0) // check that we have state
    testHarness.setProcessingTime(8500) // clean-up
    assert(testHarness.numKeyedStateEntries() == 0) // check that all state is gone

    val result = testHarness.getOutput

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    // all elements at the same row-time have the same value per key
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(2), "aaa", 1L: JLong, 1L: JLong, 1L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(3), "bbb", 10L: JLong, 10L: JLong, 10L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(4001), "aaa", 2L: JLong, 1L: JLong, 2L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(4002), "aaa", 3L: JLong, 1L: JLong, 3L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(4003), "aaa", 4L: JLong, 2L: JLong, 4L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(4801), "bbb", 25L: JLong, 25L: JLong, 25L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(6501), "aaa", 5L: JLong, 2L: JLong, 6L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(6501), "aaa", 6L: JLong, 2L: JLong, 6L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(7001), "aaa", 7L: JLong, 2L: JLong, 7L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(8001), "aaa", 8L: JLong, 2L: JLong, 8L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(6501), "bbb", 30L: JLong, 25L: JLong, 30L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(12001), "aaa", 9L: JLong, 8L: JLong, 10L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(12001), "aaa", 10L: JLong, 8L: JLong, 10L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(12001), "bbb", 40L: JLong, 40L: JLong, 40L: JLong), true)))

    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(20001), "ccc", 1L: JLong, 1L: JLong, 1L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(20002), "ccc", 2L: JLong, 1L: JLong, 2L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(20011), "ccc", 3L: JLong, 3L: JLong, 3L: JLong), true)))

    verify(expectedOutput, result, new RowResultSortComparator())
    testHarness.close()
  }

  @Test
  def testRowTimeBoundedRowsOver(): Unit = {

    val processFunction = new KeyedProcessOperator[String, CRow, CRow](
      new RowTimeBoundedRowsOver(
        genMinMaxAggFunction,
        minMaxAggregationStateType,
        minMaxCRowType,
        3,
        0,
        new StreamQueryConfig().withIdleStateRetentionTime(Time.seconds(1), Time.seconds(2))))

    val testHarness =
      createHarnessTester(
        processFunction,
        new TupleRowKeySelector[String](1),
        BasicTypeInfo.STRING_TYPE_INFO)

    testHarness.open()

    testHarness.processWatermark(800)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(801), "aaa", 1L: JLong), true)))

    testHarness.processWatermark(2500)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(2501), "bbb", 10L: JLong), true)))

    testHarness.processWatermark(4000)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(4001), "aaa", 2L: JLong), true)))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(4001), "aaa", 3L: JLong), true)))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(4001), "bbb", 20L: JLong), true)))

    testHarness.processWatermark(4800)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(4801), "aaa", 4L: JLong), true)))

    testHarness.processWatermark(6500)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(6501), "aaa", 5L: JLong), true)))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(6501), "aaa", 6L: JLong), true)))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(6501), "bbb", 30L: JLong), true)))

    testHarness.processWatermark(7000)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(7001), "aaa", 7L: JLong), true)))

    testHarness.processWatermark(8000)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(8001), "aaa", 8L: JLong), true)))

    testHarness.processWatermark(12000)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(12001), "aaa", 9L: JLong), true)))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(12001), "aaa", 10L: JLong), true)))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(12001), "bbb", 40L: JLong), true)))

    testHarness.processWatermark(19000)

    // test cleanup
    testHarness.setProcessingTime(1000)
    testHarness.processWatermark(20000)

    // check that state is removed after max retention time
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(20001), "ccc", 1L: JLong), true))) // clean-up 3000
    testHarness.setProcessingTime(2500)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(20002), "ccc", 2L: JLong), true))) // clean-up 4500
    testHarness.processWatermark(20010) // compute output

    assert(testHarness.numKeyedStateEntries() > 0) // check that we have state
    testHarness.setProcessingTime(4499)
    assert(testHarness.numKeyedStateEntries() > 0) // check that we have state
    testHarness.setProcessingTime(4500)
    assert(testHarness.numKeyedStateEntries() == 0) // check that all state is gone

    // check that state is only removed if all data was processed
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(20011), "ccc", 3L: JLong), true))) // clean-up 6500

    assert(testHarness.numKeyedStateEntries() > 0) // check that we have state
    testHarness.setProcessingTime(6500) // clean-up attempt but rescheduled to 8500
    assert(testHarness.numKeyedStateEntries() > 0) // check that we have state

    testHarness.processWatermark(20020) // schedule emission

    assert(testHarness.numKeyedStateEntries() > 0) // check that we have state
    testHarness.setProcessingTime(8499) // clean-up
    assert(testHarness.numKeyedStateEntries() > 0) // check that we have state
    testHarness.setProcessingTime(8500) // clean-up
    assert(testHarness.numKeyedStateEntries() == 0) // check that all state is gone


    val result = testHarness.getOutput

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(801), "aaa", 1L: JLong, 1L: JLong, 1L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(2501), "bbb", 10L: JLong, 10L: JLong, 10L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(4001), "aaa", 2L: JLong, 1L: JLong, 2L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(4001), "aaa", 3L: JLong, 1L: JLong, 3L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(4001), "bbb", 20L: JLong, 10L: JLong, 20L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(4801), "aaa", 4L: JLong, 2L: JLong, 4L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(6501), "aaa", 5L: JLong, 3L: JLong, 5L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(6501), "aaa", 6L: JLong, 4L: JLong, 6L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(6501), "bbb", 30L: JLong, 10L: JLong, 30L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(7001), "aaa", 7L: JLong, 5L: JLong, 7L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(8001), "aaa", 8L: JLong, 6L: JLong, 8L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(12001), "aaa", 9L: JLong, 7L: JLong, 9L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(12001), "aaa", 10L: JLong, 8L: JLong, 10L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(12001), "bbb", 40L: JLong, 20L: JLong, 40L: JLong), true)))

    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(20001), "ccc", 1L: JLong, 1L: JLong, 1L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(20002), "ccc", 2L: JLong, 1L: JLong, 2L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(20011), "ccc", 3L: JLong, 3L: JLong, 3L: JLong), true)))

    verify(expectedOutput, result, new RowResultSortComparator())
    testHarness.close()
  }

  /**
    * all elements at the same row-time have the same value per key
    */
  @Test
  def testRowTimeUnboundedRangeOver(): Unit = {

    val processFunction = new KeyedProcessOperator[String, CRow, CRow](
      new RowTimeUnboundedRangeOver(
        genMinMaxAggFunction,
        minMaxAggregationStateType,
        minMaxCRowType,
        0,
        new StreamQueryConfig().withIdleStateRetentionTime(Time.seconds(1), Time.seconds(2))))

    val testHarness =
      createHarnessTester(
        processFunction,
        new TupleRowKeySelector[String](1),
        BasicTypeInfo.STRING_TYPE_INFO)

    testHarness.open()

    testHarness.setProcessingTime(1000)
    testHarness.processWatermark(800)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(801), "aaa", 1L: JLong), true)))

    testHarness.processWatermark(2500)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(2501), "bbb", 10L: JLong), true)))

    testHarness.processWatermark(4000)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(4001), "aaa", 2L: JLong), true)))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(4001), "aaa", 3L: JLong), true)))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(4001), "bbb", 20L: JLong), true)))

    testHarness.processWatermark(4800)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(4801), "aaa", 4L: JLong), true)))

    testHarness.processWatermark(6500)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(6501), "aaa", 5L: JLong), true)))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(6501), "aaa", 6L: JLong), true)))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(6501), "bbb", 30L: JLong), true)))

    testHarness.processWatermark(7000)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(7001), "aaa", 7L: JLong), true)))

    testHarness.processWatermark(8000)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(8001), "aaa", 8L: JLong), true)))

    testHarness.processWatermark(12000)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(12001), "aaa", 9L: JLong), true)))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(12001), "aaa", 10L: JLong), true)))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(12001), "bbb", 40L: JLong), true)))

    testHarness.processWatermark(19000)

    // test cleanup
    assert(testHarness.numKeyedStateEntries() > 0)
    testHarness.setProcessingTime(2999) // clean up timer is 3000, so nothing should happen
    assert(testHarness.numKeyedStateEntries() > 0)
    testHarness.setProcessingTime(3000) // clean up is triggered
    assert(testHarness.numKeyedStateEntries() == 0)

    testHarness.processWatermark(20000)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(20001), "ccc", 1L: JLong), true))) // clean-up 5000
    testHarness.setProcessingTime(2500)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(20002), "ccc", 2L: JLong), true))) // clean-up 5000

    assert(testHarness.numKeyedStateEntries() > 0)
    testHarness.setProcessingTime(5000) // does not clean up, because data left. New timer 7000
    testHarness.processWatermark(20010) // compute output

    assert(testHarness.numKeyedStateEntries() > 0)
    testHarness.setProcessingTime(6999) // clean up timer is 3000, so nothing should happen
    assert(testHarness.numKeyedStateEntries() > 0)
    testHarness.setProcessingTime(7000) // clean up is triggered
    assert(testHarness.numKeyedStateEntries() == 0)

    val result = testHarness.getOutput

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    // all elements at the same row-time have the same value per key
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(801), "aaa", 1L: JLong, 1L: JLong, 1L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(2501), "bbb", 10L: JLong, 10L: JLong, 10L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(4001), "aaa", 2L: JLong, 1L: JLong, 3L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(4001), "aaa", 3L: JLong, 1L: JLong, 3L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(4001), "bbb", 20L: JLong, 10L: JLong, 20L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(4801), "aaa", 4L: JLong, 1L: JLong, 4L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(6501), "aaa", 5L: JLong, 1L: JLong, 6L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(6501), "aaa", 6L: JLong, 1L: JLong, 6L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(6501), "bbb", 30L: JLong, 10L: JLong, 30L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(7001), "aaa", 7L: JLong, 1L: JLong, 7L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(8001), "aaa", 8L: JLong, 1L: JLong, 8L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(12001), "aaa", 9L: JLong, 1L: JLong, 10L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(12001), "aaa", 10L: JLong, 1L: JLong, 10L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(12001), "bbb", 40L: JLong, 10L: JLong, 40L: JLong), true)))

    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(20001), "ccc", 1L: JLong, 1L: JLong, 1L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(20002), "ccc", 2L: JLong, 1L: JLong, 2L: JLong), true)))

    verify(expectedOutput, result, new RowResultSortComparator())
    testHarness.close()
  }

  @Test
  def testRowTimeUnboundedRowsOver(): Unit = {

    val processFunction = new KeyedProcessOperator[String, CRow, CRow](
      new RowTimeUnboundedRowsOver(
        genMinMaxAggFunction,
        minMaxAggregationStateType,
        minMaxCRowType,
        0,
        new StreamQueryConfig().withIdleStateRetentionTime(Time.seconds(1), Time.seconds(2))))

    val testHarness =
      createHarnessTester(
        processFunction,
        new TupleRowKeySelector[String](1),
        BasicTypeInfo.STRING_TYPE_INFO)

    testHarness.open()

    testHarness.setProcessingTime(1000)
    testHarness.processWatermark(800)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(801), "aaa", 1L: JLong), true)))

    testHarness.processWatermark(2500)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(2501), "bbb", 10L: JLong), true)))

    testHarness.processWatermark(4000)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(4001), "aaa", 2L: JLong), true)))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(4001), "aaa", 3L: JLong), true)))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(4001), "bbb", 20L: JLong), true)))

    testHarness.processWatermark(4800)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(4801), "aaa", 4L: JLong), true)))

    testHarness.processWatermark(6500)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(6501), "aaa", 5L: JLong), true)))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(6501), "aaa", 6L: JLong), true)))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(6501), "bbb", 30L: JLong), true)))

    testHarness.processWatermark(7000)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(7001), "aaa", 7L: JLong), true)))

    testHarness.processWatermark(8000)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(8001), "aaa", 8L: JLong), true)))

    testHarness.processWatermark(12000)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(12001), "aaa", 9L: JLong), true)))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(12001), "aaa", 10L: JLong), true)))
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(12001), "bbb", 40L: JLong), true)))

    testHarness.processWatermark(19000)

    // test cleanup
    assert(testHarness.numKeyedStateEntries() > 0)
    testHarness.setProcessingTime(2999) // clean up timer is 3000, so nothing should happen
    assert(testHarness.numKeyedStateEntries() > 0)
    testHarness.setProcessingTime(3000) // clean up is triggered
    assert(testHarness.numKeyedStateEntries() == 0)

    testHarness.processWatermark(20000)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(20001), "ccc", 1L: JLong), true))) // clean-up 5000
    testHarness.setProcessingTime(2500)
    testHarness.processElement(new StreamRecord(
      CRow(Row.of(toTS(20002), "ccc", 2L: JLong), true))) // clean-up 5000

    assert(testHarness.numKeyedStateEntries() > 0)
    testHarness.setProcessingTime(5000) // does not clean up, because data left. New timer 7000
    testHarness.processWatermark(20010) // compute output

    assert(testHarness.numKeyedStateEntries() > 0)
    testHarness.setProcessingTime(6999) // clean up timer is 3000, so nothing should happen
    assert(testHarness.numKeyedStateEntries() > 0)
    testHarness.setProcessingTime(7000) // clean up is triggered
    assert(testHarness.numKeyedStateEntries() == 0)

    val result = testHarness.getOutput

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(801), "aaa", 1L: JLong, 1L: JLong, 1L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(2501), "bbb", 10L: JLong, 10L: JLong, 10L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(4001), "aaa", 2L: JLong, 1L: JLong, 2L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(4001), "aaa", 3L: JLong, 1L: JLong, 3L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(4001), "bbb", 20L: JLong, 10L: JLong, 20L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(4801), "aaa", 4L: JLong, 1L: JLong, 4L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(6501), "aaa", 5L: JLong, 1L: JLong, 5L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(6501), "aaa", 6L: JLong, 1L: JLong, 6L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(6501), "bbb", 30L: JLong, 10L: JLong, 30L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(7001), "aaa", 7L: JLong, 1L: JLong, 7L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(8001), "aaa", 8L: JLong, 1L: JLong, 8L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(12001), "aaa", 9L: JLong, 1L: JLong, 9L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(12001), "aaa", 10L: JLong, 1L: JLong, 10L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(12001), "bbb", 40L: JLong, 10L: JLong, 40L: JLong), true)))

    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(20001), "ccc", 1L: JLong, 1L: JLong, 1L: JLong), true)))
    expectedOutput.add(new StreamRecord(
      CRow(Row.of(toTS(20002), "ccc", 2L: JLong, 1L: JLong, 2L: JLong), true)))

    verify(expectedOutput, result, new RowResultSortComparator())
    testHarness.close()
  }
}
