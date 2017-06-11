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

package org.apache.flink.table.runtime.datastream

import java.math.BigDecimal

import org.apache.flink.api.scala._
import org.apache.flink.types.Row
import org.apache.flink.table.api.scala.stream.utils.StreamITCase
import org.apache.flink.table.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.runtime.datastream.DataStreamAggregateITCase.TimestampWithEqualWatermark
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable

class DataStreamAggregateITCase extends StreamingMultipleProgramsTestBase {

  val data = List(
    (1L, 1, 1d, 1f, new BigDecimal("1"), "Hi"),
    (2L, 2, 2d, 2f, new BigDecimal("2"), "Hallo"),
    (3L, 2, 2d, 2f, new BigDecimal("2"), "Hello"),
    (4L, 5, 5d, 5f, new BigDecimal("5"), "Hello"),
    (7L, 3, 3d, 3f, new BigDecimal("3"), "Hello"),
    (8L, 3, 3d, 3f, new BigDecimal("3"), "Hello world"),
    (16L, 4, 4d, 4f, new BigDecimal("4"), "Hello world"))

  // ----------------------------------------------------------------------------------------------
  // Sliding windows
  // ----------------------------------------------------------------------------------------------

  @Test
  def testAllEventTimeSlidingGroupWindowOverTime(): Unit = {
    // please keep this test in sync with the DataSet variant
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())
    val table = stream.toTable(tEnv, 'long.rowtime, 'int, 'double, 'float, 'bigdec, 'string)

    val windowedTable = table
      .window(Slide over 5.milli every 2.milli on 'long as 'w)
      .groupBy('w)
      .select('int.count, 'w.start, 'w.end)

    val results = windowedTable.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = Seq(
      "1,1970-01-01 00:00:00.008,1970-01-01 00:00:00.013",
      "1,1970-01-01 00:00:00.012,1970-01-01 00:00:00.017",
      "1,1970-01-01 00:00:00.014,1970-01-01 00:00:00.019",
      "1,1970-01-01 00:00:00.016,1970-01-01 00:00:00.021",
      "2,1969-12-31 23:59:59.998,1970-01-01 00:00:00.003",
      "2,1970-01-01 00:00:00.006,1970-01-01 00:00:00.011",
      "3,1970-01-01 00:00:00.002,1970-01-01 00:00:00.007",
      "3,1970-01-01 00:00:00.004,1970-01-01 00:00:00.009",
      "4,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testEventTimeSlidingGroupWindowOverTimeOverlappingFullPane(): Unit = {
    // please keep this test in sync with the DataSet variant
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())
    val table = stream.toTable(tEnv, 'long.rowtime, 'int, 'double, 'float, 'bigdec, 'string)

    val windowedTable = table
      .window(Slide over 10.milli every 5.milli on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count, 'w.start, 'w.end)

    val results = windowedTable.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = Seq(
      "Hallo,1,1969-12-31 23:59:59.995,1970-01-01 00:00:00.005",
      "Hallo,1,1970-01-01 00:00:00.0,1970-01-01 00:00:00.01",
      "Hello world,1,1970-01-01 00:00:00.0,1970-01-01 00:00:00.01",
      "Hello world,1,1970-01-01 00:00:00.005,1970-01-01 00:00:00.015",
      "Hello world,1,1970-01-01 00:00:00.01,1970-01-01 00:00:00.02",
      "Hello world,1,1970-01-01 00:00:00.015,1970-01-01 00:00:00.025",
      "Hello,1,1970-01-01 00:00:00.005,1970-01-01 00:00:00.015",
      "Hello,2,1969-12-31 23:59:59.995,1970-01-01 00:00:00.005",
      "Hello,3,1970-01-01 00:00:00.0,1970-01-01 00:00:00.01",
      "Hi,1,1969-12-31 23:59:59.995,1970-01-01 00:00:00.005",
      "Hi,1,1970-01-01 00:00:00.0,1970-01-01 00:00:00.01")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testEventTimeSlidingGroupWindowOverTimeOverlappingSplitPane(): Unit = {
    // please keep this test in sync with the DataSet variant
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())
    val table = stream.toTable(tEnv, 'long.rowtime, 'int, 'double, 'float, 'bigdec, 'string)

    val windowedTable = table
      .window(Slide over 5.milli every 4.milli on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count, 'w.start, 'w.end)

    val results = windowedTable.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = Seq(
      "Hallo,1,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005",
      "Hello world,1,1970-01-01 00:00:00.004,1970-01-01 00:00:00.009",
      "Hello world,1,1970-01-01 00:00:00.008,1970-01-01 00:00:00.013",
      "Hello world,1,1970-01-01 00:00:00.012,1970-01-01 00:00:00.017",
      "Hello world,1,1970-01-01 00:00:00.016,1970-01-01 00:00:00.021",
      "Hello,2,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005",
      "Hello,2,1970-01-01 00:00:00.004,1970-01-01 00:00:00.009",
      "Hi,1,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testEventTimeSlidingGroupWindowOverTimeNonOverlappingFullPane(): Unit = {
    // please keep this test in sync with the DataSet variant
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())
    val table = stream.toTable(tEnv, 'long.rowtime, 'int, 'double, 'float, 'bigdec, 'string)

    val windowedTable = table
      .window(Slide over 5.milli every 10.milli on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count, 'w.start, 'w.end)

    val results = windowedTable.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = Seq(
      "Hallo,1,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005",
      "Hello,2,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005",
      "Hi,1,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testEventTimeSlidingGroupWindowOverTimeNonOverlappingSplitPane(): Unit = {
    // please keep this test in sync with the DataSet variant
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())
    val table = stream.toTable(tEnv, 'long.rowtime, 'int, 'double, 'float, 'bigdec, 'string)

    val windowedTable = table
      .window(Slide over 3.milli every 10.milli on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count, 'w.start, 'w.end)

    val results = windowedTable.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = Seq(
      "Hallo,1,1970-01-01 00:00:00.0,1970-01-01 00:00:00.003",
      "Hi,1,1970-01-01 00:00:00.0,1970-01-01 00:00:00.003")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testEventTimeGroupWindowWithoutExplicitTimeField(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())
      .map(t => (t._2, t._6))
    val table = stream.toTable(tEnv, 'int, 'string, 'rowtime.rowtime)

    val windowedTable = table
      .window(Slide over 3.milli every 10.milli on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count, 'w.start, 'w.end)

    val results = windowedTable.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()
    val expected = Seq(
      "Hallo,1,1970-01-01 00:00:00.0,1970-01-01 00:00:00.003",
      "Hi,1,1970-01-01 00:00:00.0,1970-01-01 00:00:00.003")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }
}

object DataStreamAggregateITCase {
  class TimestampWithEqualWatermark
  extends AssignerWithPunctuatedWatermarks[(Long, Int, Double, Float, BigDecimal, String)] {

    override def checkAndGetNextWatermark(
        lastElement: (Long, Int, Double, Float, BigDecimal, String),
        extractedTimestamp: Long)
      : Watermark = {
      new Watermark(extractedTimestamp)
    }

    override def extractTimestamp(
        element: (Long, Int, Double, Float, BigDecimal, String),
        previousElementTimestamp: Long): Long = {
      element._1
    }
  }
}
