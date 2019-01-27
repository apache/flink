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

package org.apache.flink.table.runtime.stream

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.{AssignerWithPunctuatedWatermarks, ProcessFunction}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.api.Types
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types.{DataType, DataTypes}
import org.apache.flink.table.dataformat.{BaseRow, BinaryRow, BinaryRowWriter, GenericRow}
import org.apache.flink.table.expressions.{ExpressionParser, TimeIntervalUnit}
import org.apache.flink.table.plan.TimeIndicatorConversionTest.TableFunc
import org.apache.flink.table.runtime.stream.TimeAttributesITCase._
import org.apache.flink.table.runtime.utils.JavaPojos.Pojo1
import org.apache.flink.table.runtime.utils.{StreamingTestBase, TestingAppendPojoSink, TestingAppendSink}
import org.apache.flink.table.sinks.PrintTableSink
import org.apache.flink.table.typeutils.{BaseRowTypeInfo, BinaryRowTypeInfoTest}
import org.apache.flink.table.util.TestTableSourceWithTime
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

import org.junit.Assert._
import org.junit.Test

import java.lang.{Integer => JInt, Long => JLong}
import java.math.BigDecimal
import java.sql.Timestamp
import java.util.TimeZone


import scala.collection.mutable

/**
  * Tests for access and materialization of time attributes.
  */
class TimeAttributesITCase extends StreamingTestBase {

  val data = List(
    (1L, 1, 1d, 1f, new BigDecimal("1"), "Hi"),
    (2L, 2, 2d, 2f, new BigDecimal("2"), "Hallo"),
    (3L, 2, 2d, 2f, new BigDecimal("2"), "Hello"),
    (4L, 5, 5d, 5f, new BigDecimal("5"), "Hello"),
    (7L, 3, 3d, 3f, new BigDecimal("3"), "Hello"),
    (8L, 3, 3d, 3f, new BigDecimal("3"), "Hello world"),
    (16L, 4, 4d, 4f, new BigDecimal("4"), "Hello world"))

  @Test
  def testAtomicType1(): Unit = {
    val sink = new TestingAppendSink

    val stream = env
      .fromCollection(Seq(1L, 2L, 3L, 4L, 7L, 8L, 16L))
      .assignTimestampsAndWatermarks(new AtomicTimestampWithEqualWatermark())
    val table = stream.toTable(
      tEnv, 'rowtime.rowtime, 'proctime.proctime)

    val t = table
      .where('proctime.cast(DataTypes.LONG) > 0)
      .select('rowtime.cast(DataTypes.STRING))

    val results = t.toAppendStream[Row]
    results.addSink(sink)
    env.execute()

    val expected = Seq(
      "1970-01-01 00:00:00.001",
      "1970-01-01 00:00:00.002",
      "1970-01-01 00:00:00.003",
      "1970-01-01 00:00:00.004",
      "1970-01-01 00:00:00.007",
      "1970-01-01 00:00:00.008",
      "1970-01-01 00:00:00.016")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testAtomicType2(): Unit = {
    val sink = new TestingAppendSink

    val stream = env
      .fromCollection(Seq(1L, 2L, 3L, 4L, 7L, 8L, 16L))
      .assignTimestampsAndWatermarks(new AtomicTimestampWithEqualWatermark())
      val table = stream.toTable(
        tEnv, 'l, 'rowtime.rowtime, 'proctime.proctime)

    val t = table
      .where('proctime.cast(DataTypes.LONG) > 0)
      .select('l, 'rowtime.cast(DataTypes.STRING))

    val results = t.toAppendStream[Row]
    results.addSink(sink)
    env.execute()

    val expected = Seq(
      "1,1970-01-01 00:00:00.001",
      "2,1970-01-01 00:00:00.002",
      "3,1970-01-01 00:00:00.003",
      "4,1970-01-01 00:00:00.004",
      "7,1970-01-01 00:00:00.007",
      "8,1970-01-01 00:00:00.008",
      "16,1970-01-01 00:00:00.016")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testCalcMaterialization(): Unit = {
    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())
    val table = stream.toTable(
      tEnv, 'rowtime.rowtime, 'int, 'double, 'float, 'bigdec, 'string, 'proctime.proctime)

    val t = table.select('rowtime.cast(DataTypes.STRING))

    val sink = new TestingAppendSink
    val results = t.toAppendStream[Row]
    results.addSink(sink)
    env.execute()

    val expected = Seq(
      "1970-01-01 00:00:00.001",
      "1970-01-01 00:00:00.002",
      "1970-01-01 00:00:00.003",
      "1970-01-01 00:00:00.004",
      "1970-01-01 00:00:00.007",
      "1970-01-01 00:00:00.008",
      "1970-01-01 00:00:00.016")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testCalcMaterialization2(): Unit = {
    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())
    val table = stream.toTable(tEnv, 'rowtime.rowtime, 'int, 'double, 'float, 'bigdec, 'string)

    val t = table
      .filter('rowtime.cast(DataTypes.DOUBLE) > 0.004)
      .select('rowtime, 'rowtime.floor(TimeIntervalUnit.DAY), 'rowtime.ceil(TimeIntervalUnit.DAY))

    val sink = new TestingAppendSink
    val results = t.toAppendStream[Row]
    results.addSink(sink)
    env.execute()

    val expected = Seq(
      "1970-01-01 00:00:00.007,1970-01-01 00:00:00.0,1970-01-02 00:00:00.0",
      "1970-01-01 00:00:00.008,1970-01-01 00:00:00.0,1970-01-02 00:00:00.0",
      "1970-01-01 00:00:00.016,1970-01-01 00:00:00.0,1970-01-02 00:00:00.0")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testTableSink(): Unit = {
    val sink = new TestingAppendSink
    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())
    stream.toTable(tEnv, 'rowtime.rowtime, 'int, 'double, 'float, 'bigdec, 'string)
      .filter('rowtime.cast(DataTypes.DOUBLE) > 0.004)
      .select('rowtime, 'rowtime.floor(TimeIntervalUnit.DAY), 'rowtime.ceil(TimeIntervalUnit.DAY))
      .addSink(sink)

    env.execute()

    val expected = Seq(
      "1970-01-01 00:00:00.007,1970-01-01 00:00:00.0,1970-01-02 00:00:00.0",
      "1970-01-01 00:00:00.008,1970-01-01 00:00:00.0,1970-01-02 00:00:00.0",
      "1970-01-01 00:00:00.016,1970-01-01 00:00:00.0,1970-01-02 00:00:00.0")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testTableFunction(): Unit = {
    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())
    val table = stream.toTable(
      tEnv, 'rowtime.rowtime, 'int, 'double, 'float, 'bigdec, 'string, 'proctime.proctime)
    val func = new TableFunc

    // we test if this can be executed with any exceptions
    table.join(func('proctime, 'proctime, 'string) as 's).toAppendStream[Row]

    // we test if this can be executed with any exceptions
    table.join(func('rowtime, 'rowtime, 'string) as 's).toAppendStream[Row]

    // we can only test rowtime, not proctime
    val t = table.join(func('rowtime, 'proctime, 'string) as 's).select('rowtime, 's)

    val sink = new TestingAppendSink
    t.toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "1970-01-01 00:00:00.001,1trueHi",
      "1970-01-01 00:00:00.002,2trueHallo",
      "1970-01-01 00:00:00.003,3trueHello",
      "1970-01-01 00:00:00.004,4trueHello",
      "1970-01-01 00:00:00.007,7trueHello",
      "1970-01-01 00:00:00.008,8trueHello world",
      "1970-01-01 00:00:00.016,16trueHello world")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testWindowAfterTableFunction(): Unit = {
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

    val sink = new TestingAppendSink
    val results = t.toAppendStream[Row]
    results.addSink(sink)
    env.execute()

    val expected = Seq(
      "1970-01-01 00:00:00.004,4",
      "1970-01-01 00:00:00.009,2",
      "1970-01-01 00:00:00.019,1")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testUnion(): Unit = {
    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())
    val table = stream.toTable(
      tEnv, 'rowtime.rowtime, 'int, 'double, 'float, 'bigdec, 'string)

    val t = table.unionAll(table).select('rowtime)

    val sink = new TestingAppendSink
    val results = t.toAppendStream[Row]
    results.addSink(sink)
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
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testWindowWithAggregationOnRowtimeSql(): Unit = {
    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())
    val table = stream.toTable(tEnv, 'rowtime.rowtime, 'int, 'double, 'float, 'bigdec, 'string)
    tEnv.registerTable("MyTable", table)

    val t = tEnv.sqlQuery("SELECT COUNT(`rowtime`) FROM MyTable " +
      "GROUP BY TUMBLE(rowtime, INTERVAL '0.003' SECOND)")

    val sink = new TestingAppendSink
    val results = t.toAppendStream[Row]
    results.addSink(sink)
    env.execute()

    val expected = Seq(
      "1",
      "2",
      "2",
      "2"
    )
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testMultiWindow(): Unit = {
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

    val sink = new TestingAppendSink
    val results = t.toAppendStream[Row]
    results.addSink(sink)
    env.execute()

    val expected = Seq(
      "1970-01-01 00:00:00.003,1970-01-01 00:00:00.004,2",
      "1970-01-01 00:00:00.007,1970-01-01 00:00:00.008,2",
      "1970-01-01 00:00:00.011,1970-01-01 00:00:00.012,1",
      "1970-01-01 00:00:00.019,1970-01-01 00:00:00.02,1"
    )
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testMultiWindowSqlNoAggregation(): Unit = {
    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())
    val table = stream.toTable(tEnv, 'rowtime.rowtime, 'int, 'double, 'float, 'bigdec, 'string)

    val window1 = tEnv.sqlQuery(
      s"""SELECT
          TUMBLE_ROWTIME(rowtime, INTERVAL '0.002' SECOND) AS rowtime,
          TUMBLE_END(rowtime, INTERVAL '0.002' SECOND) AS endtime
        FROM $table
        GROUP BY TUMBLE(rowtime, INTERVAL '0.002' SECOND)""")

    val window2 = tEnv.sqlQuery(
      s"""SELECT
          TUMBLE_ROWTIME(rowtime, INTERVAL '0.004' SECOND),
          TUMBLE_END(rowtime, INTERVAL '0.004' SECOND)
        FROM $window1
        GROUP BY TUMBLE(rowtime, INTERVAL '0.004' SECOND)""")

    val sink = new TestingAppendSink
    val results = window2.toAppendStream[Row]
    results.addSink(sink)
    env.execute()

    val expected = Seq(
      "1970-01-01 00:00:00.003,1970-01-01 00:00:00.004",
      "1970-01-01 00:00:00.007,1970-01-01 00:00:00.008",
      "1970-01-01 00:00:00.011,1970-01-01 00:00:00.012",
      "1970-01-01 00:00:00.019,1970-01-01 00:00:00.02"
    )
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testMultiWindowSqlWithAggregation(): Unit = {
    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())
    val table = stream.toTable(tEnv, 'rowtime.rowtime, 'int, 'double, 'float, 'bigdec, 'string)

    val window = tEnv.sqlQuery(
      s"""SELECT
          TUMBLE_ROWTIME(rowtime, INTERVAL '0.004' SECOND),
          TUMBLE_END(rowtime, INTERVAL '0.004' SECOND),
          COUNT(`int`) AS `int`
        FROM (
          SELECT
            COUNT(`int`) AS `int`,
            TUMBLE_ROWTIME(rowtime, INTERVAL '0.002' SECOND) AS `rowtime`
          FROM $table
          GROUP BY TUMBLE(rowtime, INTERVAL '0.002' SECOND)
        )
        GROUP BY TUMBLE(rowtime, INTERVAL '0.004' SECOND)""")

    val sink = new TestingAppendSink
    val results = window.toAppendStream[Row]
    results.addSink(sink)
    env.execute()

    val expected = Seq(
      "1970-01-01 00:00:00.003,1970-01-01 00:00:00.004,2",
      "1970-01-01 00:00:00.007,1970-01-01 00:00:00.008,2",
      "1970-01-01 00:00:00.011,1970-01-01 00:00:00.012,1",
      "1970-01-01 00:00:00.019,1970-01-01 00:00:00.02,1"
    )
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testMultiWindowSqlWithAggregation2(): Unit = {
    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())
    val table = stream.toTable(tEnv, 'rowtime1.rowtime, 'int, 'double, 'float, 'bigdec, 'string)

    val window = tEnv.sqlQuery(
      s"""SELECT
          TUMBLE_ROWTIME(rowtime2, INTERVAL '0.004' SECOND),
          TUMBLE_END(rowtime2, INTERVAL '0.004' SECOND),
          COUNT(`int`) as `int`
        FROM (
          SELECT
            TUMBLE_ROWTIME(rowtime1, INTERVAL '0.002' SECOND) AS rowtime2,
            COUNT(`int`) as `int`
          FROM $table
          GROUP BY TUMBLE(rowtime1, INTERVAL '0.002' SECOND)
        )
        GROUP BY TUMBLE(rowtime2, INTERVAL '0.004' SECOND)""")

    val sink = new TestingAppendSink
    val results = window.toAppendStream[Row]
    results.addSink(sink)
    env.execute()

    val expected = Seq(
      "1970-01-01 00:00:00.003,1970-01-01 00:00:00.004,2",
      "1970-01-01 00:00:00.007,1970-01-01 00:00:00.008,2",
      "1970-01-01 00:00:00.011,1970-01-01 00:00:00.012,1",
      "1970-01-01 00:00:00.019,1970-01-01 00:00:00.02,1"
    )
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testCalcMaterializationWithPojoType(): Unit = {
    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())
    val table = stream.toTable(tEnv, 'rowtime.rowtime, 'int, 'double, 'float, 'bigdec, 'string)
    tEnv.registerTable("T1", table)
    val querySql = "select rowtime as ts, string as msg from T1"

    val results = tEnv.sqlQuery(querySql).toAppendStream[Pojo1]
    val sink = new TestingAppendPojoSink
    results.addSink(sink)
    env.execute()

    val expected = Seq(
      "Pojo1{ts=1970-01-01 00:00:00.001, msg='Hi'}",
      "Pojo1{ts=1970-01-01 00:00:00.002, msg='Hallo'}",
      "Pojo1{ts=1970-01-01 00:00:00.003, msg='Hello'}",
      "Pojo1{ts=1970-01-01 00:00:00.004, msg='Hello'}",
      "Pojo1{ts=1970-01-01 00:00:00.007, msg='Hello'}",
      "Pojo1{ts=1970-01-01 00:00:00.008, msg='Hello world'}",
      "Pojo1{ts=1970-01-01 00:00:00.016, msg='Hello world'}")

    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testPojoSupport(): Unit = {
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

    val sink = new TestingAppendSink
    val results = t.toAppendStream[Row]
    results.addSink(sink)
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
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testTableSourceWithTimeIndicators(): Unit = {
    val rows = Seq(
      Row.of(new JInt(1), "A", new JLong(1000L)),
      Row.of(new JInt(2), "B", new JLong(2000L)),
      Row.of(new JInt(3), "C", new JLong(3000L)),
      Row.of(new JInt(4), "D", new JLong(4000L)),
      Row.of(new JInt(5), "E", new JLong(5000L)),
      Row.of(new JInt(6), "F", new JLong(6000L)))

    val fieldNames = Array("a", "b", "rowtime")
    val schema = new TableSchema(
      fieldNames :+ "proctime",
      Array(DataTypes.INT, DataTypes.STRING, DataTypes.TIMESTAMP, DataTypes.TIMESTAMP))
    val rowType = new RowTypeInfo(
      Array(Types.INT, Types.STRING, Types.LONG).asInstanceOf[Array[TypeInformation[_]]],
      fieldNames)

    val tableSource = new TestTableSourceWithTime(schema, rowType, rows, "rowtime", "proctime")
    tEnv.registerTableSource("testTable", tableSource)

    val result = tEnv
      .scan("testTable")
      .where('a % 2 === 1)
      .select('rowtime, 'a, 'b)
      .toAppendStream[Row]

    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = Seq(
      "1970-01-01 00:00:01.0,1,A",
      "1970-01-01 00:00:03.0,3,C",
      "1970-01-01 00:00:05.0,5,E")

    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testSqlWindowRowtime(): Unit = {
    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())
    val table = stream.toTable(tEnv, 'rowtime.rowtime, 'int, 'double, 'float, 'bigdec, 'string)
    tEnv.registerTable("MyTable", table)

    val t = tEnv.sqlQuery("SELECT TUMBLE_ROWTIME(rowtime, INTERVAL '0.003' SECOND) FROM MyTable " +
      "GROUP BY TUMBLE(rowtime, INTERVAL '0.003' SECOND)")

    val sink = new TestingAppendSink
    val results = t.toAppendStream[Row]
    results.addSink(sink)
    env.execute()

    val expected = Seq(
      "1970-01-01 00:00:00.002",
      "1970-01-01 00:00:00.005",
      "1970-01-01 00:00:00.008",
      "1970-01-01 00:00:00.017"
    )
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testRowtimeWithBaseRowSupport(): Unit = {
    val p1 = new BinaryRow(2)
    val writer = new BinaryRowWriter(p1)
    writer.writeInt(0, 12)
    writer.writeLong(1, 42L)
    writer.complete()

    val p2 = new BinaryRow(2)
    val writer2 = new BinaryRowWriter(p2)
    writer2.writeInt(0, 13)
    writer2.writeLong(1, 43L)
    writer2.complete()

    implicit val typeInfo: TypeInformation[BaseRow] =
      new BaseRowTypeInfo(
        Array(Types.INT, Types.LONG).asInstanceOf[Array[TypeInformation[_]]],
        Array("c", "a")).asInstanceOf[TypeInformation[BaseRow]]
    val stream = env
        .fromElements(p1.asInstanceOf[BaseRow], p2.asInstanceOf[BaseRow])
        .assignTimestampsAndWatermarks(new TimestampWithEqualWatermarkBaseRow(1))
      // use aliases, swap all attributes, and skip b2
    val table = stream.toTable(tEnv, 'd.rowtime, 'c as 'c, 'a as 'a)
    tEnv.registerTable("MyTable", table)

    val t = tEnv.sqlQuery("SELECT TUMBLE_ROWTIME(d, INTERVAL '0.003' SECOND) FROM MyTable " +
      "GROUP BY TUMBLE(d, INTERVAL '0.003' SECOND)")

    val sink = new TestingAppendSink
    val results = t.toAppendStream[Row]
    results.addSink(sink)
    env.execute()

    val expected = Seq("1970-01-01 00:00:00.044")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testRegisterTableWithWatermark(): Unit = {
    val tableName = "MyTable"

    val data = Seq(
      Row.of("Mary", new Timestamp(1000L), new JInt(10)),
      Row.of("Bob", new Timestamp(2000L), new JInt(20)),
      Row.of("Mary", new Timestamp(2000L), new JInt(30)),
      Row.of("Liz", new Timestamp(2001L), new JInt(40)))

    val fieldNames = Array("name", "rtime", "amount")

    val schema = new TableSchema(fieldNames,
      Array(DataTypes.STRING, DataTypes.TIMESTAMP, DataTypes.INT))

    val rowType = new RowTypeInfo(
      Array(Types.STRING, Types.SQL_TIMESTAMP, Types.INT).asInstanceOf[Array[TypeInformation[_]]],
      fieldNames)

    val tableSource = new TestTableSourceWithTime(schema, rowType, data)

    tEnv.getConfig.setSubsectionOptimization(true)
    tEnv.registerTableSource(tableName, tableSource)

    tEnv.registerTableWithWatermark("MyTable2", tEnv.scan(tableName), "rtime", 0)
    tEnv.registerTableSink(
      "MyTable3",
      schema.getColumnNames,
      schema.getTypes.asInstanceOf[Array[DataType]],
      new PrintTableSink(TimeZone.getDefault))
    tEnv.registerTableSink(
      "MyTable4",
      schema.getColumnNames,
      schema.getTypes.asInstanceOf[Array[DataType]],
      new PrintTableSink(TimeZone.getDefault))
    tEnv.sqlUpdate(
      "insert into MyTable3 " +
          "select name, TUMBLE_START(rtime, INTERVAL '1' MINUTE), sum(amount) " +
          "from MyTable2 group by TUMBLE(rtime, INTERVAL '1' MINUTE), name")
    tEnv.sqlUpdate(
      "insert into MyTable4 " +
          "select name, TUMBLE_START(rtime, INTERVAL '2' MINUTE), sum(amount) " +
          "from MyTable2 group by TUMBLE(rtime, INTERVAL '2' MINUTE), name")

    // check the node size of the generated stream graph.
    assertEquals(11, tEnv.generateStreamGraph().getStreamNodes.size())

    val sink = new TestingAppendSink
    tEnv.scan("MyTable2")
      .window(Tumble over 1.second on 'rtime as 'w)
      .groupBy('name, 'w)
      .select('name, 'w.start, 'amount.sum)
      .toAppendStream[Row]
        // append current watermark to each row to verify that original watermarks were preserved
      .process(new ProcessFunction[Row, Row] {
        override def processElement(
          value: Row,
          ctx: ProcessFunction[Row, Row]#Context,
          out: Collector[Row]): Unit = {
        val res = new Row(4)
        res.setField(0, value.getField(0))
        res.setField(1, value.getField(1))
        res.setField(2, value.getField(2))
        res.setField(3, ctx.timerService().currentWatermark())
        out.collect(res)
      }
      }).addSink(sink)
    env.execute()

    val expected = mutable.MutableList(
      "Mary,1970-01-01 00:00:01.0,10,1000",
      "Mary,1970-01-01 00:00:02.0,30,2000",
      "Bob,1970-01-01 00:00:02.0,20,2000",
      "Liz,1970-01-01 00:00:02.0,40,2000")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)

  }

}

object TimeAttributesITCase {

  class AtomicTimestampWithEqualWatermark
    extends AssignerWithPunctuatedWatermarks[Long] {

    override def checkAndGetNextWatermark(
        lastElement: Long,
        extractedTimestamp: Long)
    : Watermark = {
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

  class TimestampWithEqualWatermarkBaseRow(val index: Int)
      extends AssignerWithPunctuatedWatermarks[BaseRow] {

    override def checkAndGetNextWatermark(
        lastElement: BaseRow,
        extractedTimestamp: Long)
    : Watermark = {
      new Watermark(extractedTimestamp)
    }

    override def extractTimestamp(
        element: BaseRow,
        previousElementTimestamp: Long): Long = {
      element.getLong(index)
    }
  }

  class TimestampWithEqualWatermarkPojo
    extends AssignerWithPunctuatedWatermarks[TestPojo] {

    override def checkAndGetNextWatermark(
        lastElement: TestPojo,
        extractedTimestamp: Long)
      : Watermark = {
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
}
