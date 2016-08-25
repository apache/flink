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

package org.apache.flink.api.scala.stream.table

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.stream.table.GroupWindowITCase.TimestampWithEqualWatermark
import org.apache.flink.api.scala.stream.utils.StreamITCase
import org.apache.flink.api.scala.table._
import org.apache.flink.api.table.{Row, _}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable

class GroupWindowITCase extends StreamingMultipleProgramsTestBase {

  @Test(expected = classOf[ValidationException])
  def testInvalidBatchWindow(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    val data = new mutable.MutableList[(Long, Int, String)]
    val stream = env.fromCollection(data)
    val table = stream.toTable(tEnv, 'long, 'int, 'string)

    table
      .groupBy('string)
      .window(Session withGap 10.rows as 'string)
  }

  @Test(expected = classOf[TableException])
  def testInvalidRowtime1(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    val data = new mutable.MutableList[(Long, Int, String)]
    val stream = env.fromCollection(data)
    val table = stream.toTable(tEnv, 'rowtime, 'int, 'string)

    table
      .groupBy('string)
      .window(Tumble over 50.milli)
      .select('string, 'int.count)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidRowtime2(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    val data = new mutable.MutableList[(Long, Int, String)]
    val stream = env.fromCollection(data)
    val table = stream.toTable(tEnv, 'long, 'int, 'string)

    table
      .groupBy('string)
      .window(Tumble over 50.milli)
      .select('string, 'int.count as 'rowtime)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidRowtime3(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    val data = new mutable.MutableList[(Long, Int, String)]
    val stream = env.fromCollection(data)
    val table = stream.toTable(tEnv, 'long, 'int, 'string)

    table.as('rowtime, 'myint, 'mystring)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidRowtime4(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    val data = new mutable.MutableList[(Long, Int, String)]
    val stream = env.fromCollection(data)
    val table = stream.toTable(tEnv, 'long, 'int, 'string)

    table
      .groupBy('string)
      .window(Tumble over 50.milli on 'string)
      .select('string, 'int.count)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidTumblingSize(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    val data = new mutable.MutableList[(Long, Int, String)]
    val stream = env.fromCollection(data)
    val table = stream.toTable(tEnv, 'long, 'int, 'string)

    table
      .groupBy('string)
      .window(Tumble over "WRONG")
      .select('string, 'int.count)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidSlidingSize(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    val data = new mutable.MutableList[(Long, Int, String)]
    val stream = env.fromCollection(data)
    val table = stream.toTable(tEnv, 'long, 'int, 'string)

    table
      .groupBy('string)
      .window(Slide over "WRONG" every "WRONG")
      .select('string, 'int.count)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidSlidingSlide(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    val data = new mutable.MutableList[(Long, Int, String)]
    val stream = env.fromCollection(data)
    val table = stream.toTable(tEnv, 'long, 'int, 'string)

    table
      .groupBy('string)
      .window(Slide over 12.rows every "WRONG")
      .select('string, 'int.count)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidSessionGap(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    val data = new mutable.MutableList[(Long, Int, String)]
    val stream = env.fromCollection(data)
    val table = stream.toTable(tEnv, 'long, 'int, 'string)

    table
      .groupBy('string)
      .window(Session withGap 10.rows)
      .select('string, 'int.count)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidWindowAlias1(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    val data = new mutable.MutableList[(Long, Int, String)]
    val stream = env.fromCollection(data)
    val table = stream.toTable(tEnv, 'long, 'int, 'string)

    table
      .groupBy('string)
      .window(Session withGap 10.rows as 1 + 1)
      .select('string, 'int.count)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidWindowAlias2(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    val data = new mutable.MutableList[(Long, Int, String)]
    val stream = env.fromCollection(data)
    val table = stream.toTable(tEnv, 'long, 'int, 'string)

    table
      .groupBy('string)
      .window(Session withGap 10.rows as 'string)
      .select('string, 'int.count)
  }

  @Test
  def testProcessingTimeTumblingGroupWindowOverTime(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val data = new mutable.MutableList[(Long, Int, String)]
    data.+=((1L, 1, "Hi"))
    data.+=((2L, 2, "Hello"))
    data.+=((4L, 2, "Hello"))
    data.+=((8L, 3, "Hello world"))
    data.+=((16L, 3, "Hello world"))
    val stream = env.fromCollection(data)

    val table = stream.toTable(tEnv, 'long, 'int, 'string)

    val windowedTable = table
      .groupBy('string)
      .window(Tumble over 50.milli)
      .select('string, 'int.count)

    // we only test if validation is successful here since processing time is non-deterministic
    windowedTable.toDataStream[Row]
  }

  @Test
  def testProcessingTimeTumblingGroupWindowOverCount(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val data = new mutable.MutableList[(Long, Int, String)]
    data.+=((1L, 1, "Hi"))
    data.+=((2L, 2, "Hello"))
    data.+=((4L, 2, "Hello"))
    data.+=((8L, 3, "Hello world"))
    data.+=((16L, 3, "Hello world"))
    val stream = env.fromCollection(data)

    val table = stream.toTable(tEnv, 'long, 'int, 'string)

    val windowedTable = table
      .groupBy('string)
      .window(Tumble over 2.rows)
      .select('string, 'int.count)

    val results = windowedTable.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = Seq("Hello world,2", "Hello,2")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testEventTimeTumblingGroupWindowOverTime(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val data = new mutable.MutableList[(Long, Int, String)]
    data.+=((1L, 1, "Hi"))
    data.+=((2L, 2, "Hello"))
    data.+=((4L, 2, "Hello"))
    data.+=((8L, 3, "Hello world"))
    data.+=((16L, 3, "Hello world"))
    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())

    val table = stream.toTable(tEnv, 'long, 'int, 'string)

    val windowedTable = table
      .groupBy('string)
      .window(Tumble over 5.milli on 'rowtime)
      .select('string, 'int.count)

    val results = windowedTable.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = Seq("Hello world,1", "Hello world,1", "Hello,2", "Hi,1")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testEventTimeTumblingGroupWindowOverCount(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val data = new mutable.MutableList[(Long, Int, String)]
    data.+=((1L, 1, "Hi"))
    data.+=((2L, 2, "Hello"))
    data.+=((4L, 2, "Hello"))
    data.+=((8L, 3, "Hello world"))
    data.+=((16L, 3, "Hello world"))
    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())

    val table = stream.toTable(tEnv, 'long, 'int, 'string)

    val windowedTable = table
      .groupBy('string)
      .window(Tumble over 2.rows on 'rowtime)
      .select('string, 'int.count)

    val results = windowedTable.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = Seq("Hello world,2", "Hello,2")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testProcessingTimeSlidingGroupWindowOverTime(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val data = new mutable.MutableList[(Long, Int, String)]
    data.+=((1L, 1, "Hi"))
    data.+=((2L, 2, "Hello"))
    data.+=((4L, 2, "Hello"))
    data.+=((8L, 3, "Hello world"))
    data.+=((16L, 3, "Hello world"))
    val stream = env.fromCollection(data)

    val table = stream.toTable(tEnv, 'long, 'int, 'string)

    val windowedTable = table
      .groupBy('string)
      .window(Slide over 50.milli every 50.milli)
      .select('string, 'int.count)

    // we only test if validation is successful here since processing time is non-deterministic
    windowedTable.toDataStream[Row]
  }

  @Test
  def testProcessingTimeSlidingGroupWindowOverCount(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val data = new mutable.MutableList[(Long, Int, String)]
    data.+=((1L, 1, "Hi"))
    data.+=((2L, 2, "Hello"))
    data.+=((4L, 2, "Hello"))
    data.+=((8L, 3, "Hello world"))
    data.+=((16L, 3, "Hello world"))
    val stream = env.fromCollection(data)

    val table = stream.toTable(tEnv, 'long, 'int, 'string)

    val windowedTable = table
      .groupBy('string)
      .window(Slide over 2.rows every 1.rows)
      .select('string, 'int.count)

    val results = windowedTable.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = Seq("Hello world,1", "Hello world,2", "Hello,1", "Hello,2", "Hi,1")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testEventTimeSlidingGroupWindowOverTime(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val data = new mutable.MutableList[(Long, Int, String)]
    data.+=((1L, 1, "Hi"))
    data.+=((2L, 2, "Hello"))
    data.+=((4L, 2, "Hello"))
    data.+=((8L, 3, "Hello world"))
    data.+=((16L, 3, "Hello world"))
    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())

    val table = stream.toTable(tEnv, 'long, 'int, 'string)

    val windowedTable = table
      .groupBy('string)
      .window(Slide over 8.milli every 10.milli on 'rowtime)
      .select('string, 'int.count)

    val results = windowedTable.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = Seq("Hello world,1", "Hello,2", "Hi,1")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testEventTimeSlidingGroupWindowOverCount(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val data = new mutable.MutableList[(Long, Int, String)]
    data.+=((1L, 1, "Hi"))
    data.+=((2L, 2, "Hello"))
    data.+=((4L, 2, "Hello"))
    data.+=((8L, 3, "Hello world"))
    data.+=((16L, 3, "Hello world"))
    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())

    val table = stream.toTable(tEnv, 'long, 'int, 'string)

    val windowedTable = table
      .groupBy('string)
      .window(Slide over 2.rows every 1.rows on 'rowtime)
      .select('string, 'int.count)

    val results = windowedTable.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = Seq("Hello world,1", "Hello world,2", "Hello,1", "Hello,2", "Hi,1")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testEventTimeSessionGroupWindowOverTime(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val data = new mutable.MutableList[(Long, Int, String)]
    data.+=((1L, 1, "Hi"))
    data.+=((2L, 2, "Hello"))
    data.+=((4L, 2, "Hello"))
    data.+=((8L, 3, "Hello world"))
    data.+=((16L, 3, "Hello world"))
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
  def testAllProcessingTimeTumblingGroupWindowOverTime(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val data = new mutable.MutableList[(Long, Int, String)]
    data.+=((1L, 1, "Hi"))
    data.+=((2L, 2, "Hello"))
    data.+=((4L, 2, "Hello"))
    data.+=((8L, 3, "Hello world"))
    data.+=((16L, 3, "Hello world"))
    val stream = env.fromCollection(data)

    val table = stream.toTable(tEnv, 'long, 'int, 'string)

    val windowedTable = table
      .groupBy('string)
      .window(Tumble over 50.milli)
      .select('string, 'int.count)

    // we only test if validation is successful here since processing time is non-deterministic
    windowedTable.toDataStream[Row]
  }

  @Test
  def testAllProcessingTimeTumblingGroupWindowOverCount(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val data = new mutable.MutableList[(Long, Int, String)]
    data.+=((1L, 1, "Hi"))
    data.+=((2L, 2, "Hello"))
    data.+=((4L, 2, "Hello"))
    data.+=((8L, 3, "Hello world"))
    data.+=((16L, 3, "Hello world"))
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
  def testAllEventTimeTumblingGroupWindowOverTime(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val data = new mutable.MutableList[(Long, Int, String)]
    data.+=((1L, 1, "Hi"))
    data.+=((2L, 2, "Hello"))
    data.+=((4L, 2, "Hello"))
    data.+=((8L, 3, "Hello world"))
    data.+=((16L, 3, "Hello world"))
    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())

    val table = stream.toTable(tEnv, 'long, 'int, 'string)

    val windowedTable = table
      .window(Tumble over 5.milli on 'rowtime)
      .select('int.count)

    val results = windowedTable.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = Seq("1", "1", "3")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testAllEventTimeGroupWindowOverRow(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val data = new mutable.MutableList[(Long, Int, String)]
    data.+=((1L, 1, "Hi"))
    data.+=((2L, 2, "Hello"))
    data.+=((4L, 2, "Hello"))
    data.+=((8L, 3, "Hello world"))
    data.+=((16L, 3, "Hello world"))
    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())

    val table = stream.toTable(tEnv, 'long, 'int, 'string)

    val windowedTable = table
      .window(Tumble over 2.rows on 'rowtime)
      .select('int.count)

    val results = windowedTable.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = Seq("2", "2")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }


  @Test
  def testAllProcessingTimeSlidingGroupWindowOverTime(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val data = new mutable.MutableList[(Long, Int, String)]
    data.+=((1L, 1, "Hi"))
    data.+=((2L, 2, "Hello"))
    data.+=((4L, 2, "Hello"))
    data.+=((8L, 3, "Hello world"))
    data.+=((16L, 3, "Hello world"))
    val stream = env.fromCollection(data)

    val table = stream.toTable(tEnv, 'long, 'int, 'string)

    val windowedTable = table
      .window(Slide over 50.milli every 50.milli)
      .select('int.count)

    // we only test if validation is successful here since processing time is non-deterministic
    windowedTable.toDataStream[Row]
  }

  @Test
  def testAllProcessingTimeSlidingGroupWindowOverCount(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val data = new mutable.MutableList[(Long, Int, String)]
    data.+=((1L, 1, "Hi"))
    data.+=((2L, 2, "Hello"))
    data.+=((4L, 2, "Hello"))
    data.+=((8L, 3, "Hello world"))
    data.+=((16L, 3, "Hello world"))
    val stream = env.fromCollection(data)

    val table = stream.toTable(tEnv, 'long, 'int, 'string)

    val windowedTable = table
      .window(Slide over 2.rows every 1.rows)
      .select('int.count)

    val results = windowedTable.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = Seq("1", "2", "2", "2", "2")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testAllEventTimeSlidingGroupWindowOverTime(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val data = new mutable.MutableList[(Long, Int, String)]
    data.+=((1L, 1, "Hi"))
    data.+=((2L, 2, "Hello"))
    data.+=((4L, 2, "Hello"))
    data.+=((8L, 3, "Hello world"))
    data.+=((16L, 3, "Hello world"))
    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())

    val table = stream.toTable(tEnv, 'long, 'int, 'string)

    val windowedTable = table
      .window(Slide over 8.milli every 10.milli on 'rowtime)
      .select('int.count)

    val results = windowedTable.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = Seq("1", "3")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testAllEventTimeSlidingGroupWindowOverCount(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val data = new mutable.MutableList[(Long, Int, String)]
    data.+=((1L, 1, "Hi"))
    data.+=((2L, 2, "Hello"))
    data.+=((4L, 2, "Hello"))
    data.+=((8L, 3, "Hello world"))
    data.+=((16L, 3, "Hello world"))
    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())

    val table = stream.toTable(tEnv, 'long, 'int, 'string)

    val windowedTable = table
      .window(Slide over 2.rows every 1.rows on 'rowtime)
      .select('int.count)

    val results = windowedTable.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = Seq("1", "2", "2", "2", "2")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testAllEventTimeSessionGroupWindowOverTime(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val data = new mutable.MutableList[(Long, Int, String)]
    data.+=((1L, 1, "Hi"))
    data.+=((2L, 2, "Hello"))
    data.+=((4L, 2, "Hello"))
    data.+=((8L, 3, "Hello world"))
    data.+=((16L, 3, "Hello world"))
    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())

    val table = stream.toTable(tEnv, 'long, 'int, 'string)

    val windowedTable = table
      .window(Session withGap 7.milli on 'rowtime)
      .select('int.count)

    val results = windowedTable.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = Seq("1", "4")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testWindowStartEnd(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val data = new mutable.MutableList[(Long, Int, String)]
    data.+=((1L, 1, "Hi"))
    data.+=((2L, 2, "Hello"))
    data.+=((4L, 2, "Hello"))
    data.+=((8L, 3, "Hello world"))
    data.+=((16L, 3, "Hello world"))
    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())

    val table = stream.toTable(tEnv, 'long, 'int, 'string)

    val windowedTable = table
      .groupBy('string)
      .window(Tumble over 5.milli on 'rowtime as 'w)
      .select('string, 'int.count, 'w.start, 'w.end)

    val results = windowedTable.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = Seq(
      "Hello world,1,1970-01-01 00:00:00.005,1970-01-01 00:00:00.01",
      "Hello world,1,1970-01-01 00:00:00.015,1970-01-01 00:00:00.02",
      "Hello,2,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005",
      "Hi,1,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005")
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
