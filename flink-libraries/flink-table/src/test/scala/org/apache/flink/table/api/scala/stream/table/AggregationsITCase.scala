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

package org.apache.flink.table.api.scala.stream.table

import org.apache.flink.api.scala._
import org.apache.flink.types.Row
import org.apache.flink.table.api.scala.stream.table.GroupWindowITCase.TimestampWithEqualWatermark
import org.apache.flink.table.api.scala.stream.utils.StreamITCase
import org.apache.flink.table.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.apache.flink.table.api.TableEnvironment
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable

/**
  * We only test some aggregations until better testing of constructed DataStream
  * programs is possible.
  */
class AggregationsITCase extends StreamingMultipleProgramsTestBase {

  val data = List(
    (1L, 1, "Hi"),
    (2L, 2, "Hello"),
    (4L, 2, "Hello"),
    (8L, 3, "Hello world"),
    (16L, 3, "Hello world"))

  @Test
  def testProcessingTimeSlidingGroupWindowOverCount(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env.fromCollection(data)
    val table = stream.toTable(tEnv, 'long, 'int, 'string)

    val windowedTable = table
      .groupBy('string)
      .window(Slide over 2.rows every 1.rows)
      .select('string, 'int.count, 'int.avg)

    val results = windowedTable.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = Seq("Hello world,1,3", "Hello world,2,3", "Hello,1,2", "Hello,2,2", "Hi,1,1")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testEventTimeSessionGroupWindowOverTime(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())
    val table = stream.toTable(tEnv, 'long, 'int, 'string)

    val windowedTable = table
      .groupBy('string)
      .window(Session withGap 7.milli on 'rowtime)
      .select('string, 'int.count)

    val results = windowedTable.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = Seq("Hello world,1", "Hello world,1", "Hello,2", "Hi,1")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testAllProcessingTimeTumblingGroupWindowOverCount(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env.fromCollection(data)
    val table = stream.toTable(tEnv, 'long, 'int, 'string)

    val windowedTable = table
      .window(Tumble over 2.rows)
      .select('int.count)

    val results = windowedTable.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = Seq("2", "2")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testEventTimeTumblingWindow(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())
    val table = stream.toTable(tEnv, 'long, 'int, 'string)

    val windowedTable = table
      .groupBy('string)
      .window(Tumble over 5.milli on 'rowtime as 'w)
      .select('string, 'int.count, 'int.avg, 'w.start, 'w.end)

    val results = windowedTable.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = Seq(
      "Hello world,1,3,1970-01-01 00:00:00.005,1970-01-01 00:00:00.01",
      "Hello world,1,3,1970-01-01 00:00:00.015,1970-01-01 00:00:00.02",
      "Hello,2,2,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005",
      "Hi,1,1,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testEventTimeSlidingWindow(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())
    val table = stream.toTable(tEnv, 'long, 'int, 'string)

    val windowedTable = table
      .groupBy('string)
      .window(Slide over 10.milli every 5.milli on 'rowtime as 'w)
      .select('string, 'int.count, 'w.start, 'w.end, 'w.start)

    val results = windowedTable.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = Seq(
      "Hello world,1,1970-01-01 00:00:00.0,1970-01-01 00:00:00.01,1970-01-01 00:00:00.0",
      "Hello world,1,1970-01-01 00:00:00.005,1970-01-01 00:00:00.015,1970-01-01 00:00:00.005",
      "Hello world,1,1970-01-01 00:00:00.01,1970-01-01 00:00:00.02,1970-01-01 00:00:00.01",
      "Hello world,1,1970-01-01 00:00:00.015,1970-01-01 00:00:00.025,1970-01-01 00:00:00.015",
      "Hello,2,1969-12-31 23:59:59.995,1970-01-01 00:00:00.005,1969-12-31 23:59:59.995",
      "Hello,2,1970-01-01 00:00:00.0,1970-01-01 00:00:00.01,1970-01-01 00:00:00.0",
      "Hi,1,1969-12-31 23:59:59.995,1970-01-01 00:00:00.005,1969-12-31 23:59:59.995",
      "Hi,1,1970-01-01 00:00:00.0,1970-01-01 00:00:00.01,1970-01-01 00:00:00.0")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }
}

object GroupWindowITCase {
  class TimestampWithEqualWatermark extends AssignerWithPunctuatedWatermarks[(Long, Int, String)] {

    override def checkAndGetNextWatermark(
        lastElement: (Long, Int, String),
        extractedTimestamp: Long)
      : Watermark = {
      new Watermark(extractedTimestamp)
    }

    override def extractTimestamp(
        element: (Long, Int, String),
        previousElementTimestamp: Long): Long = {
      element._1
    }
  }
}
