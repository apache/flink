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
import java.lang.{Integer => JInt, Long => JLong}

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.{StreamExecutionEnvironment => JStreamExecEnv}
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.apache.flink.table.api.java.utils.Pojos.Pojo1
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.stream.utils.StreamITCase
import org.apache.flink.table.api.{TableEnvironment, TableException, Types, ValidationException}
import org.apache.flink.table.calcite.RelTimeIndicatorConverterTest.TableFunc
import org.apache.flink.table.expressions.{ExpressionParser, TimeIntervalUnit}
import org.apache.flink.table.runtime.datastream.TimeAttributesITCase._
import org.apache.flink.table.sources.{DefinedProctimeAttribute, DefinedRowtimeAttribute, StreamTableSource}
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable
import scala.collection.JavaConverters._

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
  def testAtomicType1(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env
      .fromCollection(Seq(1L, 2L, 3L, 4L, 7L, 8L, 16L))
      .assignTimestampsAndWatermarks(new AtomicTimestampWithEqualWatermark())
    val table = stream.toTable(
      tEnv, 'rowtime.rowtime, 'proctime.proctime)

    val t = table
      .where('proctime.cast(Types.LONG) > 0)
      .select('rowtime.cast(Types.STRING))

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
  def testAtomicType2(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env
      .fromCollection(Seq(1L, 2L, 3L, 4L, 7L, 8L, 16L))
      .assignTimestampsAndWatermarks(new AtomicTimestampWithEqualWatermark())
    val table = stream.toTable(
      tEnv, 'l, 'rowtime.rowtime, 'proctime.proctime)

    val t = table
      .where('proctime.cast(Types.LONG) > 0)
      .select('l, 'rowtime.cast(Types.STRING))

    val results = t.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = Seq(
      "1,1970-01-01 00:00:00.001",
      "2,1970-01-01 00:00:00.002",
      "3,1970-01-01 00:00:00.003",
      "4,1970-01-01 00:00:00.004",
      "7,1970-01-01 00:00:00.007",
      "8,1970-01-01 00:00:00.008",
      "16,1970-01-01 00:00:00.016")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
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

  @Test
  def testCalcMaterializationWithPojoType(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())
    val table = stream.toTable(tEnv, 'rowtime.rowtime, 'int, 'double, 'float, 'bigdec, 'string)
    tEnv.registerTable("T1", table)
    val querySql = "select rowtime as ts, string as msg from T1"

    val results = tEnv.sql(querySql).toAppendStream[Pojo1]
    results.addSink(new StreamITCase.StringSink[Pojo1])
    env.execute()

    val expected = Seq(
      "Pojo1{ts=1970-01-01 00:00:00.001, msg='Hi'}",
      "Pojo1{ts=1970-01-01 00:00:00.002, msg='Hallo'}",
      "Pojo1{ts=1970-01-01 00:00:00.003, msg='Hello'}",
      "Pojo1{ts=1970-01-01 00:00:00.004, msg='Hello'}",
      "Pojo1{ts=1970-01-01 00:00:00.007, msg='Hello'}",
      "Pojo1{ts=1970-01-01 00:00:00.008, msg='Hello world'}",
      "Pojo1{ts=1970-01-01 00:00:00.016, msg='Hello world'}")

    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testPojoSupport(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val p1 = new TestPojo
    p1.a = 12
    p1.b = 42L
    p1.c = "Test me."

    val p2 = new TestPojo
    p2.a = 13
    p2.b = 43L
    p2.c = "And me."

    val stream = env
      .fromElements(p1, p2)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermarkPojo)
    // use aliases, swap all attributes, and skip b2
    val table = stream.toTable(tEnv, ('b as 'b).rowtime, 'c as 'c, 'a as 'a)
    // no aliases, no swapping
    val table2 = stream.toTable(tEnv, 'a, 'b.rowtime, 'c)
    // use proctime, no skipping
    val table3 = stream.toTable(tEnv, 'a, 'b.rowtime, 'c, 'b2, 'proctime.proctime)

    // Java expressions

    // use aliases, swap all attributes, and skip b2
    val table4 = stream.toTable(
      tEnv,
      ExpressionParser.parseExpressionList("(b as b).rowtime, c as c, a as a"): _*)
    // no aliases, no swapping
    val table5 = stream.toTable(
      tEnv,
      ExpressionParser.parseExpressionList("a, b.rowtime, c"): _*)

    val t = table.select('b, 'c , 'a)
      .unionAll(table2.select('b, 'c, 'a))
      .unionAll(table3.select('b, 'c, 'a))
      .unionAll(table4.select('b, 'c, 'a))
      .unionAll(table5.select('b, 'c, 'a))

    val results = t.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = Seq(
      "1970-01-01 00:00:00.042,Test me.,12",
      "1970-01-01 00:00:00.042,Test me.,12",
      "1970-01-01 00:00:00.042,Test me.,12",
      "1970-01-01 00:00:00.042,Test me.,12",
      "1970-01-01 00:00:00.042,Test me.,12",
      "1970-01-01 00:00:00.043,And me.,13",
      "1970-01-01 00:00:00.043,And me.,13",
      "1970-01-01 00:00:00.043,And me.,13",
      "1970-01-01 00:00:00.043,And me.,13",
      "1970-01-01 00:00:00.043,And me.,13")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testTableSourceWithTimeIndicators(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    tEnv.registerTableSource("testTable", new TestTableSource)
    StreamITCase.clear

    val result = tEnv
      .scan("testTable")
      .where('a % 2 === 1)
      .select('rowtime, 'a, 'b, 'c)
      .toAppendStream[Row]

    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = Seq(
      "1970-01-01 00:00:01.0,1,A,1000",
      "1970-01-01 00:00:03.0,3,C,3000",
      "1970-01-01 00:00:05.0,5,E,5000")

    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }
}

object TimeAttributesITCase {

  class AtomicTimestampWithEqualWatermark
    extends AssignerWithPunctuatedWatermarks[Long] {

    override def checkAndGetNextWatermark(
        lastElement: Long,
        extractedTimestamp: Long): Watermark = {
      new Watermark(extractedTimestamp)
    }

    override def extractTimestamp(
        element: Long,
        previousElementTimestamp: Long): Long = {
      element
    }
  }

  class TimestampWithEqualWatermark
    extends AssignerWithPunctuatedWatermarks[(Long, Int, Double, Float, BigDecimal, String)] {

    override def checkAndGetNextWatermark(
        lastElement: (Long, Int, Double, Float, BigDecimal, String),
        extractedTimestamp: Long): Watermark = {
      new Watermark(extractedTimestamp)
    }

    override def extractTimestamp(
        element: (Long, Int, Double, Float, BigDecimal, String),
        previousElementTimestamp: Long): Long = {
      element._1
    }
  }

  class TimestampWithEqualWatermarkPojo
    extends AssignerWithPunctuatedWatermarks[TestPojo] {

    override def checkAndGetNextWatermark(
        lastElement: TestPojo,
        extractedTimestamp: Long): Watermark = {
      new Watermark(extractedTimestamp)
    }

    override def extractTimestamp(
        element: TestPojo,
        previousElementTimestamp: Long): Long = {
      element.b
    }
  }

  class TestPojo() {
    var a: Int = _
    var b: Long = _
    var b2: String = "skip me"
    var c: String = _
  }

  class TestTableSource
    extends StreamTableSource[Row]
      with DefinedRowtimeAttribute
      with DefinedProctimeAttribute {

    override def getDataStream(env: JStreamExecEnv): DataStream[Row] = {

      def toRow(i: Int, s: String, l: Long) = Row.of(i.asInstanceOf[JInt], s, l.asInstanceOf[JLong])

      val rows = Seq(
        toRow(1, "A", 1000L),
        toRow(2, "B", 2000L),
        toRow(3, "C", 3000L),
        toRow(4, "D", 4000L),
        toRow(5, "E", 5000L),
        toRow(6, "F", 6000L)
      )

      env
        .fromCollection(rows.asJava).returns(getReturnType)
        .assignTimestampsAndWatermarks(new AscendingTimestampExtractor[Row] {
          override def extractAscendingTimestamp(r: Row): Long = r.getField(2).asInstanceOf[Long]
        })
    }

    override def getRowtimeAttribute: String = "rowtime"

    override def getProctimeAttribute: String = "proctime"

    override def getReturnType: TypeInformation[Row] = {
      new RowTypeInfo(
        Array(Types.INT, Types.STRING, Types.LONG).asInstanceOf[Array[TypeInformation[_]]],
        Array("a", "b", "c")
      )
    }
  }
}
