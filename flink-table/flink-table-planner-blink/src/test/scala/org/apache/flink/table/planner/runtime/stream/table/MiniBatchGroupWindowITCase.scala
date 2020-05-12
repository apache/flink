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
package org.apache.flink.table.planner.runtime.stream.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{Slide, Tumble}
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedAggFunctions.{CountDistinct, WeightedAvg}
import org.apache.flink.table.planner.runtime.utils.StreamingWithMiniBatchTestBase.MiniBatchMode
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.planner.runtime.utils.TimeTestUtil.TimestampAndWatermarkWithOffset
import org.apache.flink.table.planner.runtime.utils.{StreamingWithMiniBatchTestBase, TestingAppendSink}
import org.apache.flink.table.planner.utils.CountAggFunction
import org.apache.flink.types.Row

import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import java.math.BigDecimal

/**
  * Integrate tests for group window aggregate in SQL API.
  *
  * NOTE: All the cases in this file should support minibatch window, if not, please move to
  * [[GroupWindowITCase]].
  *
  * For the requirements which windows support minibatch, please see
  * [[org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecGroupWindowAggregate]].
  */
@RunWith(classOf[Parameterized])
class MiniBatchGroupWindowITCase(miniBatch: MiniBatchMode, mode: StateBackendMode)
  extends StreamingWithMiniBatchTestBase(miniBatch, mode) {

  val data = List(
    (1L, 1, "Hi"),
    (2L, 2, "Hello"),
    (4L, 2, "Hello"),
    (8L, 3, "Hello world"),
    (16L, 3, "Hello world"))

  val data2 = List(
    (1L, 1, 1d, 1f, new BigDecimal("1"), "Hi"),
    (2L, 2, 2d, 2f, new BigDecimal("2"), "Hallo"),
    (3L, 2, 2d, 2f, new BigDecimal("2"), "Hello"),
    (4L, 5, 5d, 5f, new BigDecimal("5"), "Hello"),
    (7L, 3, 3d, 3f, new BigDecimal("3"), "Hello"),
    (8L, 3, 3d, 3f, new BigDecimal("3"), "Hello world"),
    (16L, 4, 4d, 4f, new BigDecimal("4"), "Hello world"),
    (32L, 4, 4d, 4f, new BigDecimal("4"), null.asInstanceOf[String]))

  @Test
  def testEventTimeTumblingWindow(): Unit = {
    val stream = failingDataSource(data)
      .assignTimestampsAndWatermarks(new TimestampAndWatermarkWithOffset[(Long, Int, String)](0L))
    val table = stream.toTable(tEnv, 'long, 'int, 'string, 'rowtime.rowtime)
    val countFun = new CountAggFunction
    val weightAvgFun = new WeightedAvg
    val countDistinct = new CountDistinct

    val windowedTable = table
      .window(Tumble over 5.milli on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('string, countFun('string), 'int.avg, weightAvgFun('long, 'int),
              weightAvgFun('int, 'int), 'int.min, 'int.max, 'int.sum, 'w.start, 'w.end,
              countDistinct('long))

    val sink = new TestingAppendSink
    windowedTable.toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "Hello world,1,3,16,3,3,3,3,1970-01-01T00:00:00.015,1970-01-01T00:00:00.020,1",
      "Hello world,1,3,8,3,3,3,3,1970-01-01T00:00:00.005,1970-01-01T00:00:00.010,1",
      s"Hello,2,2,3,2,2,2,4,1970-01-01T00:00,1970-01-01T00:00:00.005,2",
      "Hi,1,1,1,1,1,1,1,1970-01-01T00:00,1970-01-01T00:00:00.005,1")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }


  @Test
  def testEventTimeSlidingGroupWindowOverTimeNonOverlappingFullPane(): Unit = {
    // please keep this test in sync with the DataSet variant
    val stream = failingDataSource(data2)
      .assignTimestampsAndWatermarks(
        new TimestampAndWatermarkWithOffset[(Long, Int, Double, Float, BigDecimal, String)](0L))
    val table = stream.toTable(tEnv, 'rowtime.rowtime, 'int, 'double, 'float, 'bigdec, 'string)

    val windowedTable = table
      .window(Slide over 5.milli every 10.milli on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count, 'w.start, 'w.end)

    val sink = new TestingAppendSink
    windowedTable.toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "Hallo,1,1970-01-01T00:00,1970-01-01T00:00:00.005",
      "Hello,2,1970-01-01T00:00,1970-01-01T00:00:00.005",
      "Hi,1,1970-01-01T00:00,1970-01-01T00:00:00.005",
      "null,1,1970-01-01T00:00:00.030,1970-01-01T00:00:00.035")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testEventTimeSlidingGroupWindowOverTimeOverlappingFullPane(): Unit = {
    // please keep this test in sync with the DataSet variant
    val stream = failingDataSource(data2)
      .assignTimestampsAndWatermarks(
        new TimestampAndWatermarkWithOffset[(Long, Int, Double, Float, BigDecimal, String)](0L))
    val table = stream.toTable(tEnv, 'rowtime.rowtime, 'int, 'double, 'float, 'bigdec, 'string)

    val windowedTable = table
      .window(Slide over 10.milli every 5.milli on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count, 'w.start, 'w.end)

    val sink = new TestingAppendSink
    windowedTable.toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "Hallo,1,1969-12-31T23:59:59.995,1970-01-01T00:00:00.005",
      "Hallo,1,1970-01-01T00:00,1970-01-01T00:00:00.010",
      "Hello world,1,1970-01-01T00:00,1970-01-01T00:00:00.010",
      "Hello world,1,1970-01-01T00:00:00.005,1970-01-01T00:00:00.015",
      "Hello world,1,1970-01-01T00:00:00.010,1970-01-01T00:00:00.020",
      "Hello world,1,1970-01-01T00:00:00.015,1970-01-01T00:00:00.025",
      "Hello,1,1970-01-01T00:00:00.005,1970-01-01T00:00:00.015",
      "Hello,2,1969-12-31T23:59:59.995,1970-01-01T00:00:00.005",
      "Hello,3,1970-01-01T00:00,1970-01-01T00:00:00.010",
      "Hi,1,1969-12-31T23:59:59.995,1970-01-01T00:00:00.005",
      "Hi,1,1970-01-01T00:00,1970-01-01T00:00:00.010",
      "null,1,1970-01-01T00:00:00.025,1970-01-01T00:00:00.035",
      "null,1,1970-01-01T00:00:00.030,1970-01-01T00:00:00.040")
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }

}
