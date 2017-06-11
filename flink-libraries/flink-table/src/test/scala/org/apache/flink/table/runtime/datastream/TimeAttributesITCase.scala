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
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.stream.utils.StreamITCase
import org.apache.flink.table.api.{TableEnvironment, TableException, Types, ValidationException}
import org.apache.flink.table.calcite.RelTimeIndicatorConverterTest.TableFunc
import org.apache.flink.table.expressions.TimeIntervalUnit
import org.apache.flink.table.runtime.datastream.TimeAttributesITCase.TimestampWithEqualWatermark
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable

/**
  * Tests for access and materialization of time attributes.
  */
class TimeAttributesITCase extends StreamingMultipleProgramsTestBase {

  val data = List(
    (1L, 1, 1d, 1f, new BigDecimal("1"), "Hi"),
    (2L, 2, 2d, 2f, new BigDecimal("2"), "Hallo"),
    (3L, 2, 2d, 2f, new BigDecimal("2"), "Hello"),
    (4L, 5, 5d, 5f, new BigDecimal("5"), "Hello"),
    (7L, 3, 3d, 3f, new BigDecimal("3"), "Hello"),
    (8L, 3, 3d, 3f, new BigDecimal("3"), "Hello world"),
    (16L, 4, 4d, 4f, new BigDecimal("4"), "Hello world"))

  @Test(expected = classOf[TableException])
  def testInvalidTimeCharacteristic(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())
    stream.toTable(tEnv, 'rowtime.rowtime, 'int, 'double, 'float, 'bigdec, 'string)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidUseOfRowtime(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())
    stream.toTable(tEnv, 'rowtime.rowtime, 'int, 'double, 'float, 'bigdec, 'string)
      .select('rowtime.rowtime)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidUseOfRowtime2(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())
    stream
      .toTable(tEnv, 'rowtime.rowtime, 'int, 'double, 'float, 'bigdec, 'string)
      .window(Tumble over 2.millis on 'rowtime as 'w)
      .groupBy('w)
      .select('w.end.rowtime, 'int.count as 'int) // no rowtime on non-window reference
  }

  @Test
  def testCalcMaterialization(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())
    val table = stream.toTable(tEnv, 'rowtime.rowtime, 'int, 'double, 'float, 'bigdec, 'string)

    val t = table.select('rowtime.cast(Types.STRING))

    val results = t.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = Seq(
      "1970-01-01 00:00:00.001",
      "1970-01-01 00:00:00.002",
      "1970-01-01 00:00:00.003",
      "1970-01-01 00:00:00.004",
      "1970-01-01 00:00:00.007",
      "1970-01-01 00:00:00.008",
      "1970-01-01 00:00:00.016")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testCalcMaterialization2(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())
    val table = stream.toTable(tEnv, 'rowtime.rowtime, 'int, 'double, 'float, 'bigdec, 'string)

    val t = table
      .filter('rowtime.cast(Types.LONG) > 4)
      .select('rowtime, 'rowtime.floor(TimeIntervalUnit.DAY), 'rowtime.ceil(TimeIntervalUnit.DAY))

    val results = t.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = Seq(
      "1970-01-01 00:00:00.007,1970-01-01 00:00:00.0,1970-01-02 00:00:00.0",
      "1970-01-01 00:00:00.008,1970-01-01 00:00:00.0,1970-01-02 00:00:00.0",
      "1970-01-01 00:00:00.016,1970-01-01 00:00:00.0,1970-01-02 00:00:00.0")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testTableFunction(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())
    val table = stream.toTable(
      tEnv, 'rowtime.rowtime, 'int, 'double, 'float, 'bigdec, 'string, 'proctime.proctime)
    val func = new TableFunc

    val t = table.join(func('rowtime, 'proctime, 'string) as 's).select('rowtime, 's)

    val results = t.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = Seq(
      "1970-01-01 00:00:00.001,1trueHi",
      "1970-01-01 00:00:00.002,2trueHallo",
      "1970-01-01 00:00:00.003,3trueHello",
      "1970-01-01 00:00:00.004,4trueHello",
      "1970-01-01 00:00:00.007,7trueHello",
      "1970-01-01 00:00:00.008,8trueHello world",
      "1970-01-01 00:00:00.016,16trueHello world")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testWindowAfterTableFunction(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())
    val table = stream.toTable(
      tEnv, 'rowtime.rowtime, 'int, 'double, 'float, 'bigdec, 'string, 'proctime.proctime)
    val func = new TableFunc

    val t = table
      .join(func('rowtime, 'proctime, 'string) as 's)
      .window(Tumble over 5.millis on 'rowtime as 'w)
      .groupBy('w)
      .select('w.rowtime, 's.count)

    val results = t.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = Seq(
      "1970-01-01 00:00:00.004,4",
      "1970-01-01 00:00:00.009,2",
      "1970-01-01 00:00:00.019,1")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testUnion(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())
    val table = stream.toTable(
      tEnv, 'rowtime.rowtime, 'int, 'double, 'float, 'bigdec, 'string)

    val t = table.unionAll(table).select('rowtime)

    val results = t.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = Seq(
      "1970-01-01 00:00:00.001",
      "1970-01-01 00:00:00.001",
      "1970-01-01 00:00:00.002",
      "1970-01-01 00:00:00.002",
      "1970-01-01 00:00:00.003",
      "1970-01-01 00:00:00.003",
      "1970-01-01 00:00:00.004",
      "1970-01-01 00:00:00.004",
      "1970-01-01 00:00:00.007",
      "1970-01-01 00:00:00.007",
      "1970-01-01 00:00:00.008",
      "1970-01-01 00:00:00.008",
      "1970-01-01 00:00:00.016",
      "1970-01-01 00:00:00.016")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testWindowWithAggregationOnRowtimeSql(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())
    val table = stream.toTable(tEnv, 'rowtime.rowtime, 'int, 'double, 'float, 'bigdec, 'string)
    tEnv.registerTable("MyTable", table)

    val t = tEnv.sql("SELECT COUNT(`rowtime`) FROM MyTable " +
      "GROUP BY TUMBLE(rowtime, INTERVAL '0.003' SECOND)")

    val results = t.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = Seq(
      "1",
      "2",
      "2",
      "2"
    )
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testMultiWindow(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())
    val table = stream.toTable(tEnv, 'rowtime.rowtime, 'int, 'double, 'float, 'bigdec, 'string)

    val t = table
      .window(Tumble over 2.millis on 'rowtime as 'w)
      .groupBy('w)
      .select('w.rowtime as 'rowtime, 'int.count as 'int)
      .window(Tumble over 4.millis on 'rowtime as 'w2)
      .groupBy('w2)
      .select('w2.rowtime, 'w2.end, 'int.count)

    val results = t.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = Seq(
      "1970-01-01 00:00:00.003,1970-01-01 00:00:00.004,2",
      "1970-01-01 00:00:00.007,1970-01-01 00:00:00.008,2",
      "1970-01-01 00:00:00.011,1970-01-01 00:00:00.012,1",
      "1970-01-01 00:00:00.019,1970-01-01 00:00:00.02,1"
    )
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

}

object TimeAttributesITCase {
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
