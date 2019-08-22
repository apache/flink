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

import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{Session, Slide, Tumble}
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedAggFunctions.{CountDistinct, CountDistinctWithMerge, WeightedAvg, WeightedAvgWithMerge}
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.planner.runtime.utils.TimeTestUtil.TimestampAndWatermarkWithOffset
import org.apache.flink.table.planner.runtime.utils.{StreamingWithStateTestBase, TestingAppendSink}
import org.apache.flink.table.planner.utils.CountAggFunction
import org.apache.flink.types.Row

import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import java.math.BigDecimal

/**
  * We only test some aggregations until better testing of constructed DataStream
  * programs is possible.
  */
@RunWith(classOf[Parameterized])
class GroupWindowITCase(mode: StateBackendMode)
  extends StreamingWithStateTestBase(mode) {

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
  def testProcessingTimeSlidingGroupWindowOverCount(): Unit = {
    tEnv.getConfig.setIdleStateRetentionTime(Time.hours(1), Time.hours(2))
    val stream = failingDataSource(data)
    val table = stream.toTable(tEnv, 'long, 'int, 'string, 'proctime.proctime)

    val countFun = new CountAggFunction
    val weightAvgFun = new WeightedAvg
    val countDistinct = new CountDistinct

    val windowedTable = table
      .window(Slide over 2.rows every 1.rows on 'proctime as 'w)
      .groupBy('w, 'string)
      .select('string, countFun('int), 'int.avg,
        weightAvgFun('long, 'int), weightAvgFun('int, 'int),
        countDistinct('long))

    val sink = new TestingAppendSink
    windowedTable.toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(s"Hello world,2,3,12,3,2", s"Hello,2,2,3,2,2")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testEventTimeSessionGroupWindowOverTime(): Unit = {
    //To verify the "merge" functionality, we create this test with the following characteristics:
    // 1. set the Parallelism to 1, and have the test data out of order
    // 2. create a waterMark with 10ms offset to delay the window emission by 10ms
    val sessionWindowTestData = List(
      (1L, 1, "Hello"),
      (2L, 2, "Hello"),
      (8L, 8, "Hello"),
      (9L, 9, "Hello World"),
      (4L, 4, "Hello"),
      (16L, 16, "Hello"))

    val countFun = new CountAggFunction
    val weightAvgFun = new WeightedAvgWithMerge
    val countDistinct = new CountDistinctWithMerge

    val stream = failingDataSource(sessionWindowTestData)
      .assignTimestampsAndWatermarks(new TimestampAndWatermarkWithOffset[(Long, Int, String)](10L))
    val table = stream.toTable(tEnv, 'long, 'int, 'string, 'rowtime.rowtime)

    val windowedTable = table
      .window(Session withGap 5.milli on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('string, 'w.end, countFun('int), 'int.avg,
        weightAvgFun('long, 'int), weightAvgFun('int, 'int),
        countDistinct('long))

    val sink = new TestingAppendSink
    windowedTable.toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "Hello World,1970-01-01T00:00:00.014,1,9,9,9,1",
      "Hello,1970-01-01T00:00:00.021,1,16,16,16,1",
      s"Hello,1970-01-01T00:00:00.013,4,3,5,5,4")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testAllProcessingTimeTumblingGroupWindowOverCount(): Unit = {
    tEnv.getConfig.setIdleStateRetentionTime(Time.hours(1), Time.hours(2))
    val stream = failingDataSource(data)
    val table = stream.toTable(tEnv, 'long, 'int, 'string, 'proctime.proctime)
    val countFun = new CountAggFunction
    val weightAvgFun = new WeightedAvg
    val countDistinct = new CountDistinct

    val windowedTable = table
      .window(Tumble over 2.rows on 'proctime as 'w)
      .groupBy('w)
      .select(countFun('string), 'int.avg,
        weightAvgFun('long, 'int), weightAvgFun('int, 'int),
        countDistinct('long)
      )

    val sink = new TestingAppendSink
    windowedTable.toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(s"2,1,1,1,2", s"2,2,6,2,2")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testGroupWindowWithoutKeyInProjection(): Unit = {
    val data = List(
      (1L, 1, "Hi", 1, 1),
      (2L, 2, "Hello", 2, 2),
      (4L, 2, "Hello", 2, 2),
      (8L, 3, "Hello world", 3, 3),
      (16L, 3, "Hello world", 3, 3))

    val stream = failingDataSource(data)
    val table = stream.toTable(tEnv, 'long, 'int, 'string, 'int2, 'int3, 'proctime.proctime)

    val weightAvgFun = new WeightedAvg
    val countDistinct = new CountDistinct

    val windowedTable = table
      .window(Slide over 2.rows every 1.rows on 'proctime as 'w)
      .groupBy('w, 'int2, 'int3, 'string)
      .select(weightAvgFun('long, 'int), countDistinct('long))

    val sink = new TestingAppendSink
    windowedTable.toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq("12,2", "3,2")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  // ----------------------------------------------------------------------------------------------
  // Sliding windows
  // ----------------------------------------------------------------------------------------------

  @Test
  def testAllEventTimeSlidingGroupWindowOverTime(): Unit = {
    // please keep this test in sync with the DataSet variant
    val stream = failingDataSource(data2)
      .assignTimestampsAndWatermarks(
        new TimestampAndWatermarkWithOffset[(Long, Int, Double, Float, BigDecimal, String)](0L))
    val table = stream.toTable(tEnv, 'rowtime.rowtime, 'int, 'double, 'float, 'bigdec, 'string)

    val windowedTable = table
      .window(Slide over 5.milli every 2.milli on 'rowtime as 'w)
      .groupBy('w)
      .select('int.count, 'w.start, 'w.end, 'w.rowtime)

    val sink = new TestingAppendSink
    windowedTable.toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "1,1970-01-01T00:00:00.008,1970-01-01T00:00:00.013,1970-01-01T00:00:00.012",
      "1,1970-01-01T00:00:00.012,1970-01-01T00:00:00.017,1970-01-01T00:00:00.016",
      "1,1970-01-01T00:00:00.014,1970-01-01T00:00:00.019,1970-01-01T00:00:00.018",
      "1,1970-01-01T00:00:00.016,1970-01-01T00:00:00.021,1970-01-01T00:00:00.020",
      "2,1969-12-31T23:59:59.998,1970-01-01T00:00:00.003,1970-01-01T00:00:00.002",
      "2,1970-01-01T00:00:00.006,1970-01-01T00:00:00.011,1970-01-01T00:00:00.010",
      "3,1970-01-01T00:00:00.002,1970-01-01T00:00:00.007,1970-01-01T00:00:00.006",
      "3,1970-01-01T00:00:00.004,1970-01-01T00:00:00.009,1970-01-01T00:00:00.008",
      "4,1970-01-01T00:00,1970-01-01T00:00:00.005,1970-01-01T00:00:00.004",
      "1,1970-01-01T00:00:00.028,1970-01-01T00:00:00.033,1970-01-01T00:00:00.032",
      "1,1970-01-01T00:00:00.030,1970-01-01T00:00:00.035,1970-01-01T00:00:00.034",
      "1,1970-01-01T00:00:00.032,1970-01-01T00:00:00.037,1970-01-01T00:00:00.036")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testEventTimeSlidingGroupWindowOverTimeOverlappingSplitPane(): Unit = {
    // please keep this test in sync with the DataSet variant
    val stream = failingDataSource(data2)
      .assignTimestampsAndWatermarks(
        new TimestampAndWatermarkWithOffset[(Long, Int, Double, Float, BigDecimal, String)](0L))
    val table = stream.toTable(tEnv, 'rowtime.rowtime, 'int, 'double, 'float, 'bigdec, 'string)

    val windowedTable = table
      .window(Slide over 5.milli every 4.milli on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count, 'w.start, 'w.end)

    val sink = new TestingAppendSink
    windowedTable.toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "Hallo,1,1970-01-01T00:00,1970-01-01T00:00:00.005",
      "Hello world,1,1970-01-01T00:00:00.004,1970-01-01T00:00:00.009",
      "Hello world,1,1970-01-01T00:00:00.008,1970-01-01T00:00:00.013",
      "Hello world,1,1970-01-01T00:00:00.012,1970-01-01T00:00:00.017",
      "Hello world,1,1970-01-01T00:00:00.016,1970-01-01T00:00:00.021",
      "Hello,2,1970-01-01T00:00,1970-01-01T00:00:00.005",
      "Hello,2,1970-01-01T00:00:00.004,1970-01-01T00:00:00.009",
      "Hi,1,1970-01-01T00:00,1970-01-01T00:00:00.005",
      "null,1,1970-01-01T00:00:00.028,1970-01-01T00:00:00.033",
      "null,1,1970-01-01T00:00:00.032,1970-01-01T00:00:00.037")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testEventTimeSlidingGroupWindowOverTimeNonOverlappingSplitPane(): Unit = {
    // please keep this test in sync with the DataSet variant
    val stream = failingDataSource(data2)
      .assignTimestampsAndWatermarks(
        new TimestampAndWatermarkWithOffset[(Long, Int, Double, Float, BigDecimal, String)](0L))
    val table = stream.toTable(tEnv, 'rowtime.rowtime, 'int, 'double, 'float, 'bigdec, 'string)

    val windowedTable = table
      .window(Slide over 3.milli every 10.milli on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count, 'w.start, 'w.end)

    val sink = new TestingAppendSink
    windowedTable.toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "Hallo,1,1970-01-01T00:00,1970-01-01T00:00:00.003",
      "Hi,1,1970-01-01T00:00,1970-01-01T00:00:00.003",
      "null,1,1970-01-01T00:00:00.030,1970-01-01T00:00:00.033")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testEventTimeGroupWindowWithoutExplicitTimeField(): Unit = {
    val stream = failingDataSource(data2)
      .assignTimestampsAndWatermarks(
        new TimestampAndWatermarkWithOffset[(Long, Int, Double, Float, BigDecimal, String)](0L))
      .map(t => (t._2, t._6))
    val table = stream.toTable(tEnv, 'int, 'string, 'rowtime.rowtime)

    val windowedTable = table
      .window(Slide over 3.milli every 10.milli on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count, 'w.start, 'w.end)

    val sink = new TestingAppendSink
    windowedTable.toAppendStream[Row].addSink(sink)
    env.execute()
    val expected = Seq(
      "Hallo,1,1970-01-01T00:00,1970-01-01T00:00:00.003",
      "Hi,1,1970-01-01T00:00,1970-01-01T00:00:00.003",
      "null,1,1970-01-01T00:00:00.030,1970-01-01T00:00:00.033")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }
}
