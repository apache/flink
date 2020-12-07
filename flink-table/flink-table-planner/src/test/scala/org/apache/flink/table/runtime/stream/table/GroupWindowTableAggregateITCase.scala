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

package org.apache.flink.table.runtime.stream.table

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.runtime.stream.table.GroupWindowITCase._
import org.apache.flink.table.runtime.utils.{StreamITCase, StreamTestData}
import org.apache.flink.table.utils.Top3
import org.apache.flink.test.util.AbstractTestBase
import org.apache.flink.types.Row

import org.junit.Assert._
import org.junit.{Before, Test}

import java.math.BigDecimal

/**
  * We only test some aggregations until better testing of constructed DataStream
  * programs is possible.
  */
class GroupWindowTableAggregateITCase extends AbstractTestBase {

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

  @Before
  def setup(): Unit = {
    StreamITCase.clear
  }

  @Test
  def testProcessingTimeSlidingGroupWindowOverCount(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    val top3 = new Top3
    val stream = StreamTestData.get3TupleDataStream(env)
    val table = stream.toTable(tEnv, 'int, 'long, 'string, 'proctime.proctime)

    val windowedTable = table
      .window(Slide over 4.rows every 2.rows on 'proctime as 'w)
      .groupBy('w, 'long)
      .flatAggregate(top3('int) as ('x, 'y))
      .select('long, 'x, 'y)

    val results = windowedTable.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = Seq("2,2,2", "2,3,3", "3,4,4", "3,5,5", "4,7,7", "4,8,8", "4,8,8", "4,9,9",
      "4,10,10", "5,11,11", "5,12,12", "5,12,12", "5,13,13", "5,14,14", "6,16,16", "6,17,17",
      "6,17,17", "6,18,18", "6,19,19", "6,19,19", "6,20,20", "6,21,21")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testEventTimeSessionGroupWindowOverTime(): Unit = {
    //To verify the "merge" functionality, we create this test with the following characteristics:
    // 1. set the Parallelism to 1, and have the test data out of order
    // 2. create a waterMark with 10ms offset to delay the window emission by 10ms
    val sessionWindowTestdata = List(
      (1L, 1, "Hello"),
      (2L, 2, "Hello"),
      (8L, 8, "Hello"),
      (9L, 9, "Hello World"),
      (4L, 4, "Hello"),
      (16L, 16, "Hello"))

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    val stream = env
      .fromCollection(sessionWindowTestdata)
      .assignTimestampsAndWatermarks(
   new TimestampAndWatermarkWithOffset[(Long, Int, String)](10L))
    val table = stream.toTable(tEnv, 'long, 'int, 'string, 'rowtime.rowtime)

    val top3 = new Top3
    val windowedTable = table
      .window(Session withGap 5.milli on 'rowtime as 'w)
      .groupBy('w, 'string)
      .flatAggregate(top3('int))
      .select('string, 'f0, 'f1)

    val results = windowedTable.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = Seq("Hello,2,2", "Hello,4,4", "Hello,8,8", "Hello World,9,9", "Hello,16,16")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testAllProcessingTimeTumblingGroupWindowOverCount(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    val stream = StreamTestData.get3TupleDataStream(env)
    val table = stream.toTable(tEnv, 'int, 'long, 'string, 'proctime.proctime)
    val top3 = new Top3

    val windowedTable = table
      .window(Tumble over 7.rows on 'proctime as 'w)
      .groupBy('w)
      .flatAggregate(top3('int))
      .select('f0, 'f1)

    val results = windowedTable.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = Seq("5,5", "6,6", "7,7", "12,12", "13,13", "14,14", "19,19", "20,20", "21,21")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testEventTimeTumblingWindow(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    val stream = StreamTestData.get3TupleDataStream(env)
      .assignTimestampsAndWatermarks(new TimestampAndWatermarkWithOffset[(Int, Long, String)](0L))
    val table = stream.toTable(tEnv, 'int, 'long, 'string, 'rowtime.rowtime)

    val top3 = new Top3
    val windowedTable = table
      .window(Tumble over 10.milli on 'rowtime as 'w)
      .groupBy('w, 'long)
      .flatAggregate(top3('int) as ('x, 'y))
      .select('w.start, 'w.end, 'long, 'x, 'y + 1)

    val results = windowedTable.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = Seq(
      "1970-01-01 00:00:00.0,1970-01-01 00:00:00.01,1,1,2",
      "1970-01-01 00:00:00.0,1970-01-01 00:00:00.01,2,2,3",
      "1970-01-01 00:00:00.0,1970-01-01 00:00:00.01,2,3,4",
      "1970-01-01 00:00:00.0,1970-01-01 00:00:00.01,3,4,5",
      "1970-01-01 00:00:00.0,1970-01-01 00:00:00.01,3,5,6",
      "1970-01-01 00:00:00.0,1970-01-01 00:00:00.01,3,6,7",
      "1970-01-01 00:00:00.0,1970-01-01 00:00:00.01,4,7,8",
      "1970-01-01 00:00:00.0,1970-01-01 00:00:00.01,4,8,9",
      "1970-01-01 00:00:00.0,1970-01-01 00:00:00.01,4,9,10",
      "1970-01-01 00:00:00.01,1970-01-01 00:00:00.02,4,10,11",
      "1970-01-01 00:00:00.01,1970-01-01 00:00:00.02,5,13,14",
      "1970-01-01 00:00:00.01,1970-01-01 00:00:00.02,5,14,15",
      "1970-01-01 00:00:00.01,1970-01-01 00:00:00.02,5,15,16",
      "1970-01-01 00:00:00.01,1970-01-01 00:00:00.02,6,17,18",
      "1970-01-01 00:00:00.01,1970-01-01 00:00:00.02,6,18,19",
      "1970-01-01 00:00:00.01,1970-01-01 00:00:00.02,6,19,20",
      "1970-01-01 00:00:00.02,1970-01-01 00:00:00.03,6,21,22",
      "1970-01-01 00:00:00.02,1970-01-01 00:00:00.03,6,20,21")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testGroupWindowWithoutKeyInProjection(): Unit = {
    val data = List(
      (1L, 1, "Hi", 1, 1),
      (2L, 2, "Hello", 2, 2),
      (4L, 2, "Hello", 2, 2),
      (8L, 3, "Hello world", 3, 3),
      (16L, 3, "Hello world", 3, 3))

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    val stream = env.fromCollection(data)
    val table = stream.toTable(tEnv, 'long, 'int, 'string, 'int2, 'int3, 'proctime.proctime)

    val top3 = new Top3
    val windowedTable = table
      .window(Slide over 2.rows every 1.rows on 'proctime as 'w)
      .groupBy('w, 'int2, 'int3, 'string)
      .flatAggregate(top3('int))
      .select('f0, 'f1)

    val results = windowedTable.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = Seq("1,1", "2,2", "2,2", "2,2", "3,3", "3,3", "3,3")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  // ----------------------------------------------------------------------------------------------
  // Sliding windows
  // ----------------------------------------------------------------------------------------------

  @Test
  def testAllEventTimeSlidingGroupWindowOverTime(): Unit = {
    // please keep this test in sync with the DataSet variant
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    val stream = env
      .fromCollection(data2)
      .assignTimestampsAndWatermarks(
        new TimestampAndWatermarkWithOffset[(Long, Int, Double, Float, BigDecimal, String)](0L))
    val table = stream.toTable(tEnv, 'long.rowtime, 'int, 'double, 'float, 'bigdec, 'string)

    val top3 = new Top3
    val windowedTable = table
      .window(Slide over 5.milli every 2.milli on 'long as 'w)
      .groupBy('w)
      .flatAggregate(top3('int))
      .select('f0, 'f1, 'w.start, 'w.end, 'w.rowtime)

    val results = windowedTable.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = Seq(
      "1,1,1969-12-31 23:59:59.998,1970-01-01 00:00:00.003,1970-01-01 00:00:00.002",
      "2,2,1969-12-31 23:59:59.998,1970-01-01 00:00:00.003,1970-01-01 00:00:00.002",
      "2,2,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005,1970-01-01 00:00:00.004",
      "5,5,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005,1970-01-01 00:00:00.004",
      "2,2,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005,1970-01-01 00:00:00.004",
      "2,2,1970-01-01 00:00:00.002,1970-01-01 00:00:00.007,1970-01-01 00:00:00.006",
      "2,2,1970-01-01 00:00:00.002,1970-01-01 00:00:00.007,1970-01-01 00:00:00.006",
      "5,5,1970-01-01 00:00:00.002,1970-01-01 00:00:00.007,1970-01-01 00:00:00.006",
      "3,3,1970-01-01 00:00:00.004,1970-01-01 00:00:00.009,1970-01-01 00:00:00.008",
      "3,3,1970-01-01 00:00:00.004,1970-01-01 00:00:00.009,1970-01-01 00:00:00.008",
      "5,5,1970-01-01 00:00:00.004,1970-01-01 00:00:00.009,1970-01-01 00:00:00.008",
      "3,3,1970-01-01 00:00:00.006,1970-01-01 00:00:00.011,1970-01-01 00:00:00.01",
      "3,3,1970-01-01 00:00:00.006,1970-01-01 00:00:00.011,1970-01-01 00:00:00.01",
      "3,3,1970-01-01 00:00:00.008,1970-01-01 00:00:00.013,1970-01-01 00:00:00.012",
      "4,4,1970-01-01 00:00:00.012,1970-01-01 00:00:00.017,1970-01-01 00:00:00.016",
      "4,4,1970-01-01 00:00:00.014,1970-01-01 00:00:00.019,1970-01-01 00:00:00.018",
      "4,4,1970-01-01 00:00:00.016,1970-01-01 00:00:00.021,1970-01-01 00:00:00.02",
      "4,4,1970-01-01 00:00:00.028,1970-01-01 00:00:00.033,1970-01-01 00:00:00.032",
      "4,4,1970-01-01 00:00:00.03,1970-01-01 00:00:00.035,1970-01-01 00:00:00.034",
      "4,4,1970-01-01 00:00:00.032,1970-01-01 00:00:00.037,1970-01-01 00:00:00.036")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testEventTimeSlidingGroupWindowOverTimeOverlappingFullPane(): Unit = {
    // please keep this test in sync with the DataSet variant
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    val stream = env
      .fromCollection(data2)
      .assignTimestampsAndWatermarks(
        new TimestampAndWatermarkWithOffset[(Long, Int, Double, Float, BigDecimal, String)](0L))
    val table = stream.toTable(tEnv, 'long.rowtime, 'int, 'double, 'float, 'bigdec, 'string)

    val top3 = new Top3
    val windowedTable = table
      .window(Slide over 10.milli every 5.milli on 'long as 'w)
      .groupBy('w, 'string)
      .flatAggregate(top3('int))
      .select('string, 'f0, 'f1, 'w.start, 'w.end)

    val results = windowedTable.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = Seq(
      "Hallo,2,2,1969-12-31 23:59:59.995,1970-01-01 00:00:00.005",
      "Hallo,2,2,1970-01-01 00:00:00.0,1970-01-01 00:00:00.01",
      "Hello world,3,3,1970-01-01 00:00:00.0,1970-01-01 00:00:00.01",
      "Hello world,3,3,1970-01-01 00:00:00.005,1970-01-01 00:00:00.015",
      "Hello world,4,4,1970-01-01 00:00:00.01,1970-01-01 00:00:00.02",
      "Hello world,4,4,1970-01-01 00:00:00.015,1970-01-01 00:00:00.025",
      "Hello,2,2,1969-12-31 23:59:59.995,1970-01-01 00:00:00.005",
      "Hello,5,5,1969-12-31 23:59:59.995,1970-01-01 00:00:00.005",
      "Hello,2,2,1970-01-01 00:00:00.0,1970-01-01 00:00:00.01",
      "Hello,3,3,1970-01-01 00:00:00.0,1970-01-01 00:00:00.01",
      "Hello,5,5,1970-01-01 00:00:00.0,1970-01-01 00:00:00.01",
      "Hello,3,3,1970-01-01 00:00:00.005,1970-01-01 00:00:00.015",
      "Hi,1,1,1969-12-31 23:59:59.995,1970-01-01 00:00:00.005",
      "Hi,1,1,1970-01-01 00:00:00.0,1970-01-01 00:00:00.01",
      "null,4,4,1970-01-01 00:00:00.025,1970-01-01 00:00:00.035",
      "null,4,4,1970-01-01 00:00:00.03,1970-01-01 00:00:00.04")

    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testEventTimeSlidingGroupWindowOverTimeOverlappingSplitPane(): Unit = {
    // please keep this test in sync with the DataSet variant
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    val top3 = new Top3
    val stream = env
      .fromCollection(data2)
      .assignTimestampsAndWatermarks(
        new TimestampAndWatermarkWithOffset[(Long, Int, Double, Float, BigDecimal, String)](0L))
    val table = stream.toTable(tEnv, 'long.rowtime, 'int, 'double, 'float, 'bigdec, 'string)

    val windowedTable = table
      .window(Slide over 5.milli every 4.milli on 'long as 'w)
      .groupBy('w, 'string)
      .flatAggregate(top3('int))
      .select('string, 'f0, 'f1, 'w.start, 'w.end)

    val results = windowedTable.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = Seq(
      "Hello,2,2,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005",
      "Hello,5,5,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005",
      "Hallo,2,2,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005",
      "Hello world,3,3,1970-01-01 00:00:00.004,1970-01-01 00:00:00.009",
      "Hello world,3,3,1970-01-01 00:00:00.008,1970-01-01 00:00:00.013",
      "Hello,3,3,1970-01-01 00:00:00.004,1970-01-01 00:00:00.009",
      "Hi,1,1,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005",
      "Hello,5,5,1970-01-01 00:00:00.004,1970-01-01 00:00:00.009",
      "Hello world,4,4,1970-01-01 00:00:00.012,1970-01-01 00:00:00.017",
      "null,4,4,1970-01-01 00:00:00.028,1970-01-01 00:00:00.033",
      "Hello world,4,4,1970-01-01 00:00:00.016,1970-01-01 00:00:00.021",
      "null,4,4,1970-01-01 00:00:00.032,1970-01-01 00:00:00.037")

    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testEventTimeSlidingGroupWindowOverTimeNonOverlappingFullPane(): Unit = {
    // please keep this test in sync with the DataSet variant
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    val stream = env
      .fromCollection(data2)
      .assignTimestampsAndWatermarks(
        new TimestampAndWatermarkWithOffset[(Long, Int, Double, Float, BigDecimal, String)](0L))
    val table = stream.toTable(tEnv, 'long.rowtime, 'int, 'double, 'float, 'bigdec, 'string)

    val top3 = new Top3
    val windowedTable = table
      .window(Slide over 5.milli every 10.milli on 'long as 'w)
      .groupBy('w, 'string)
      .flatAggregate(top3('int))
      .select('string, 'f0, 'f1, 'w.start, 'w.end)

    val results = windowedTable.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = Seq(
      "Hallo,2,2,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005",
      "Hello,2,2,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005",
      "Hello,5,5,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005",
      "Hi,1,1,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005",
      "null,4,4,1970-01-01 00:00:00.03,1970-01-01 00:00:00.035")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testEventTimeSlidingGroupWindowOverTimeNonOverlappingSplitPane(): Unit = {
    // please keep this test in sync with the DataSet variant
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    val stream = env
      .fromCollection(data2)
      .assignTimestampsAndWatermarks(
        new TimestampAndWatermarkWithOffset[(Long, Int, Double, Float, BigDecimal, String)](0L))
    val table = stream.toTable(tEnv, 'long.rowtime, 'int, 'double, 'float, 'bigdec, 'string)

    val top3 = new Top3
    val windowedTable = table
      .window(Slide over 3.milli every 10.milli on 'long as 'w)
      .groupBy('w, 'string)
      .flatAggregate(top3('int))
      .select('string, 'f0, 'f1, 'w.start, 'w.end)

    val results = windowedTable.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = Seq(
      "null,4,4,1970-01-01 00:00:00.03,1970-01-01 00:00:00.033",
      "Hallo,2,2,1970-01-01 00:00:00.0,1970-01-01 00:00:00.003",
      "Hi,1,1,1970-01-01 00:00:00.0,1970-01-01 00:00:00.003")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testEventTimeGroupWindowWithoutExplicitTimeField(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    val stream = env
      .fromCollection(data2)
      .assignTimestampsAndWatermarks(
        new TimestampAndWatermarkWithOffset[(Long, Int, Double, Float, BigDecimal, String)](0L))
      .map(t => (t._2, t._6))
    val table = stream.toTable(tEnv, 'int, 'string, 'rowtime.rowtime)

    val top3 = new Top3
    val windowedTable = table
      .window(Slide over 3.milli every 10.milli on 'rowtime as 'w)
      .groupBy('w, 'string)
      .flatAggregate(top3('int))
      .select('string, 'f0, 'f1, 'w.start, 'w.end)

    val results = windowedTable.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()
    val expected = Seq(
      "Hallo,2,2,1970-01-01 00:00:00.0,1970-01-01 00:00:00.003",
      "Hi,1,1,1970-01-01 00:00:00.0,1970-01-01 00:00:00.003",
      "null,4,4,1970-01-01 00:00:00.03,1970-01-01 00:00:00.033")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }
}
