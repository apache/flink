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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{TableException, Tumble, Types}
import org.apache.flink.table.planner.runtime.utils.TestData.{smallTupleData3, tupleData3, tupleData5}
import org.apache.flink.table.planner.runtime.utils.{TestingAppendTableSink, TestingRetractTableSink, TestingUpsertTableSink}
import org.apache.flink.table.planner.utils.{MemoryTableSourceSinkUtil, TableTestUtil}
import org.apache.flink.table.sinks._
import org.apache.flink.test.util.{AbstractTestBase, TestBaseUtils}
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit.Test

import java.io.File
import java.util.TimeZone

import scala.collection.JavaConverters._

class TableSinkITCase extends AbstractTestBase {

  @Test
  def testInsertIntoRegisteredTableSink(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.enableObjectReuse()
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)
    MemoryTableSourceSinkUtil.clear()

    val input = env.fromCollection(tupleData3)
      .assignAscendingTimestamps(r => r._2)
    val fieldNames = Array("d", "e", "t")
    val fieldTypes: Array[TypeInformation[_]] = Array(Types.STRING, Types.SQL_TIMESTAMP, Types.LONG)
    val sink = new MemoryTableSourceSinkUtil.UnsafeMemoryAppendTableSink
    tEnv.registerTableSink("targetTable", sink.configure(fieldNames, fieldTypes))

    input.toTable(tEnv, 'a, 'b, 'c, 't.rowtime)
      .where('a < 3 || 'a > 19)
      .select('c, 't, 'b)
      .insertInto("targetTable")
    env.execute()

    val expected = Seq(
      "Hi,1970-01-01 00:00:00.001,1",
      "Hello,1970-01-01 00:00:00.002,2",
      "Comment#14,1970-01-01 00:00:00.006,6",
      "Comment#15,1970-01-01 00:00:00.006,6").mkString("\n")

    TestBaseUtils.compareResultAsText(MemoryTableSourceSinkUtil.tableData.asJava, expected)
  }

  @Test
  def testStreamTableSink(): Unit = {

    val tmpFile = File.createTempFile("flink-table-sink-test", ".tmp")
    tmpFile.deleteOnExit()
    val path = tmpFile.toURI.toString

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.enableObjectReuse()
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)
    env.setParallelism(4)

    tEnv.registerTableSink(
      "csvSink",
      new CsvTableSink(path).configure(
        Array[String]("c", "b"),
        Array[TypeInformation[_]](Types.STRING, Types.SQL_TIMESTAMP)))

    val input = env.fromCollection(tupleData3)
      .assignAscendingTimestamps(_._2)
      .map(x => x).setParallelism(4) // increase DOP to 4

    input.toTable(tEnv, 'a, 'b.rowtime, 'c)
      .where('a < 5 || 'a > 17)
      .select('c, 'b)
      .insertInto("csvSink")

    env.execute()

    val expected = Seq(
      "Hi,1970-01-01 00:00:00.001",
      "Hello,1970-01-01 00:00:00.002",
      "Hello world,1970-01-01 00:00:00.002",
      "Hello world, how are you?,1970-01-01 00:00:00.003",
      "Comment#12,1970-01-01 00:00:00.006",
      "Comment#13,1970-01-01 00:00:00.006",
      "Comment#14,1970-01-01 00:00:00.006",
      "Comment#15,1970-01-01 00:00:00.006").mkString("\n")

    TestBaseUtils.compareResultsByLinesInMemory(expected, path)
  }

  @Test
  def testAppendSinkOnAppendTable(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.enableObjectReuse()
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)

    val t = env.fromCollection(tupleData3)
        .assignAscendingTimestamps(_._1.toLong)
        .toTable(tEnv, 'id, 'num, 'text, 'rowtime.rowtime)

    val sink = new TestingAppendTableSink(TimeZone.getDefault)
    tEnv.registerTableSink(
      "appendSink",
      sink.configure(
        Array[String]("t", "icnt", "nsum"),
        Array[TypeInformation[_]](Types.SQL_TIMESTAMP, Types.LONG, Types.LONG)))

    t.window(Tumble over 5.millis on 'rowtime as 'w)
      .groupBy('w)
      .select('w.end as 't, 'id.count as 'icnt, 'num.sum as 'nsum)
      .insertInto("appendSink")

    env.execute()

    val result = sink.getAppendResults.sorted
    val expected = List(
      "1970-01-01 00:00:00.005,4,8",
      "1970-01-01 00:00:00.010,5,18",
      "1970-01-01 00:00:00.015,5,24",
      "1970-01-01 00:00:00.020,5,29",
      "1970-01-01 00:00:00.025,2,12")
      .sorted
    assertEquals(expected, result)
  }


  @Test
  def testAppendSinkWithNestedRow(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.enableObjectReuse()
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)

    val t = env.fromCollection(smallTupleData3)
      .toTable(tEnv, 'id, 'num, 'text)
    tEnv.registerTable("src", t)

    val sink = new TestingAppendTableSink()
    tEnv.registerTableSink(
      "appendSink",
      sink.configure(
        Array[String]("t", "item"),
        Array[TypeInformation[_]](Types.INT(), Types.ROW(Types.LONG, Types.STRING()))))

    tEnv.sqlUpdate("INSERT INTO appendSink SELECT id, ROW(num, text) FROM src")

    env.execute()

    val result = sink.getAppendResults.sorted
    val expected = List(
      "1,1,Hi",
      "2,2,Hello",
      "3,2,Hello world").sorted
    assertEquals(expected, result)
  }

  @Test
  def testAppendSinkOnAppendTableForInnerJoin(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.enableObjectReuse()
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)

    val ds1 = env.fromCollection(smallTupleData3).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = env.fromCollection(tupleData5).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val sink = new TestingAppendTableSink
    tEnv.registerTableSink(
      "appendSink",
      sink.configure(
        Array[String]("c", "g"),
        Array[TypeInformation[_]](Types.STRING, Types.STRING)))

    ds1.join(ds2).where('b === 'e)
      .select('c, 'g)
      .insertInto("appendSink")

    env.execute()

    val result = sink.getAppendResults.sorted
    val expected = List("Hi,Hallo", "Hello,Hallo Welt", "Hello world,Hallo Welt").sorted
    assertEquals(expected, result)
  }

  @Test
  def testRetractSinkOnUpdatingTable(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.enableObjectReuse()
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)

    val t = env.fromCollection(tupleData3)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text)

    val sink = new TestingRetractTableSink()
    tEnv.registerTableSink(
      "retractSink",
      sink.configure(
        Array[String]("len", "icnt", "nsum"),
        Array[TypeInformation[_]](Types.INT, Types.LONG, Types.LONG)))

    t.select('id, 'num, 'text.charLength() as 'len)
      .groupBy('len)
      .select('len, 'id.count as 'icnt, 'num.sum as 'nsum)
      .insertInto("retractSink")

    env.execute()

    val retracted = sink.getRetractResults.sorted
    val expected = List(
      "2,1,1",
      "5,1,2",
      "11,1,2",
      "25,1,3",
      "10,7,39",
      "14,1,3",
      "9,9,41").sorted
    assertEquals(expected, retracted)

  }

  @Test
  def testRetractSinkOnAppendTable(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.enableObjectReuse()
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)

    val t = env.fromCollection(tupleData3)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text, 'rowtime.rowtime)

    val sink = new TestingRetractTableSink(TimeZone.getDefault)
    tEnv.registerTableSink(
      "retractSink",
      sink.configure(
        Array[String]("t", "icnt", "nsum"),
        Array[TypeInformation[_]](Types.SQL_TIMESTAMP, Types.LONG, Types.LONG)))

    t.window(Tumble over 5.millis on 'rowtime as 'w)
      .groupBy('w)
      .select('w.end as 't, 'id.count as 'icnt, 'num.sum as 'nsum)
      .insertInto("retractSink")

    env.execute()

    assertFalse(
      "Received retraction messages for append only table",
      sink.getRawResults.exists(_.startsWith("(false,")))

    val retracted = sink.getRetractResults.sorted
    val expected = List(
      "1970-01-01 00:00:00.005,4,8",
      "1970-01-01 00:00:00.010,5,18",
      "1970-01-01 00:00:00.015,5,24",
      "1970-01-01 00:00:00.020,5,29",
      "1970-01-01 00:00:00.025,2,12")
      .sorted
    assertEquals(expected, retracted)

  }

  @Test
  def testUpsertSinkOnUpdatingTableWithFullKey(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.enableObjectReuse()
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)

    val t = env.fromCollection(tupleData3)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text)

    val sink = new TestingUpsertTableSink(Array(0, 2), TimeZone.getDefault)
    sink.expectedKeys = Some(Array("cnt", "cTrue"))
    sink.expectedIsAppendOnly = Some(false)
    tEnv.registerTableSink(
      "upsertSink",
      sink.configure(
        Array[String]("cnt", "lencnt", "cTrue"),
        Array[TypeInformation[_]](Types.LONG, Types.LONG, Types.BOOLEAN)))

    t.select('id, 'num, 'text.charLength() as 'len, ('id > 0) as 'cTrue)
      .groupBy('len, 'cTrue)
      .select('len, 'id.count as 'cnt, 'cTrue)
      .groupBy('cnt, 'cTrue)
      .select('cnt, 'len.count as 'lencnt, 'cTrue)
      .insertInto("upsertSink")

    env.execute()

    assertTrue(
      "Results must include delete messages",
      sink.getRawResults.exists(_.startsWith("(false,")))

    val retracted = sink.getUpsertResults.sorted
    val expected = List(
      "1,5,true",
      "7,1,true",
      "9,1,true").sorted
    assertEquals(expected, retracted)

  }

  @Test
  def testUpsertSinkOnAppendingTableWithFullKey1(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.enableObjectReuse()
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)

    val t = env.fromCollection(tupleData3)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text, 'rowtime.rowtime)

    val sink = new TestingUpsertTableSink(Array(0, 1, 2), TimeZone.getDefault)
    sink.expectedKeys = Some(Array("wend", "num"))
    sink.expectedIsAppendOnly = Some(true)
    tEnv.registerTableSink(
      "upsertSink",
      sink.configure(
        Array[String]("num", "wend", "icnt"),
        Array[TypeInformation[_]](Types.LONG, Types.SQL_TIMESTAMP, Types.LONG)))

    t.window(Tumble over 5.millis on 'rowtime as 'w)
      .groupBy('w, 'num)
      .select('num, 'w.end as 'wend, 'id.count as 'icnt)
      .insertInto("upsertSink")

    env.execute()

    assertFalse(
      "Received retraction messages for append only table",
      sink.getRawResults.exists(_.startsWith("(false,")))

    val retracted = sink.getUpsertResults.sorted
    val expected = List(
      "1,1970-01-01 00:00:00.005,1",
      "2,1970-01-01 00:00:00.005,2",
      "3,1970-01-01 00:00:00.005,1",
      "3,1970-01-01 00:00:00.010,2",
      "4,1970-01-01 00:00:00.010,3",
      "4,1970-01-01 00:00:00.015,1",
      "5,1970-01-01 00:00:00.015,4",
      "5,1970-01-01 00:00:00.020,1",
      "6,1970-01-01 00:00:00.020,4",
      "6,1970-01-01 00:00:00.025,2").sorted
    assertEquals(expected, retracted)
  }

  @Test
  def testUpsertSinkOnAppendingTableWithFullKey2(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.enableObjectReuse()
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)

    val t = env.fromCollection(tupleData3)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text, 'rowtime.rowtime)

    val sink = new TestingUpsertTableSink(Array(0, 1, 2), TimeZone.getDefault)
    sink.expectedKeys = Some(Array("wend", "num"))
    sink.expectedIsAppendOnly = Some(true)
    tEnv.registerTableSink(
      "upsertSink",
      sink.configure(
        Array[String]("wstart", "wend", "num", "icnt"),
        Array[TypeInformation[_]]
          (Types.SQL_TIMESTAMP, Types.SQL_TIMESTAMP, Types.LONG, Types.LONG)))

    t.window(Tumble over 5.millis on 'rowtime as 'w)
      .groupBy('w, 'num)
      .select('w.start as 'wstart, 'w.end as 'wend, 'num, 'id.count as 'icnt)
      .insertInto("upsertSink")

    env.execute()

    assertFalse(
      "Received retraction messages for append only table",
      sink.getRawResults.exists(_.startsWith("(false,")))

    val retracted = sink.getUpsertResults.sorted
    val expected = List(
      "1970-01-01 00:00:00.000,1970-01-01 00:00:00.005,1,1",
      "1970-01-01 00:00:00.000,1970-01-01 00:00:00.005,2,2",
      "1970-01-01 00:00:00.000,1970-01-01 00:00:00.005,3,1",
      "1970-01-01 00:00:00.005,1970-01-01 00:00:00.010,3,2",
      "1970-01-01 00:00:00.005,1970-01-01 00:00:00.010,4,3",
      "1970-01-01 00:00:00.010,1970-01-01 00:00:00.015,4,1",
      "1970-01-01 00:00:00.010,1970-01-01 00:00:00.015,5,4",
      "1970-01-01 00:00:00.015,1970-01-01 00:00:00.020,5,1",
      "1970-01-01 00:00:00.015,1970-01-01 00:00:00.020,6,4",
      "1970-01-01 00:00:00.020,1970-01-01 00:00:00.025,6,2").sorted
    assertEquals(expected, retracted)
  }

  @Test
  def testUpsertSinkOnAppendingTableWithoutFullKey1(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.enableObjectReuse()
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)

    val t = env.fromCollection(tupleData3)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text, 'rowtime.rowtime)

    val sink = new TestingUpsertTableSink(Array(0), TimeZone.getDefault)
    sink.expectedIsAppendOnly = Some(true)
    tEnv.registerTableSink(
      "upsertSink",
      sink.configure(
        Array[String]("wend", "cnt"),
        Array[TypeInformation[_]](Types.SQL_TIMESTAMP, Types.LONG)))

    t.window(Tumble over 5.millis on 'rowtime as 'w)
      .groupBy('w, 'num)
      .select('w.end as 'wend, 'id.count as 'cnt)
      .insertInto("upsertSink")

    env.execute()

    assertFalse(
      "Received retraction messages for append only table",
      sink.getRawResults.exists(_.startsWith("(false,")))

    val retracted = sink.getRawResults.sorted
    val expected = List(
      "(true,1970-01-01 00:00:00.005,1)",
      "(true,1970-01-01 00:00:00.005,2)",
      "(true,1970-01-01 00:00:00.005,1)",
      "(true,1970-01-01 00:00:00.010,2)",
      "(true,1970-01-01 00:00:00.010,3)",
      "(true,1970-01-01 00:00:00.015,1)",
      "(true,1970-01-01 00:00:00.015,4)",
      "(true,1970-01-01 00:00:00.020,1)",
      "(true,1970-01-01 00:00:00.020,4)",
      "(true,1970-01-01 00:00:00.025,2)").sorted
    assertEquals(expected, retracted)
  }

  @Test
  def testUpsertSinkOnAppendingTableWithoutFullKey2(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.enableObjectReuse()
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)

    val t = env.fromCollection(tupleData3)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text, 'rowtime.rowtime)

    val sink = new TestingUpsertTableSink(Array(0), TimeZone.getDefault)
    sink.expectedIsAppendOnly = Some(true)
    tEnv.registerTableSink(
      "upsertSink",
      sink.configure(
        Array[String]("num", "cnt"),
        Array[TypeInformation[_]](Types.LONG, Types.LONG)))

    t.window(Tumble over 5.millis on 'rowtime as 'w)
      .groupBy('w, 'num)
      .select('num, 'id.count as 'cnt)
      .insertInto("upsertSink")

    env.execute()

    assertFalse(
      "Received retraction messages for append only table",
      sink.getRawResults.exists(_.startsWith("(false,")))

    val retracted = sink.getRawResults.sorted
    val expected = List(
      "(true,1,1)",
      "(true,2,2)",
      "(true,3,1)",
      "(true,3,2)",
      "(true,4,3)",
      "(true,4,1)",
      "(true,5,4)",
      "(true,5,1)",
      "(true,6,4)",
      "(true,6,2)").sorted
    assertEquals(expected, retracted)
  }

  @Test(expected = classOf[TableException])
  def testToAppendStreamMultiRowtime(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.enableObjectReuse()
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)

    val t = env.fromCollection(tupleData3)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text, 'rowtime.rowtime)

    val r = t
      .window(Tumble over 5.milli on 'rowtime as 'w)
      .groupBy('num, 'w)
      .select('num, 'w.rowtime, 'w.rowtime as 'rowtime2)

    r.toAppendStream[Row]
  }

  @Test(expected = classOf[TableException])
  def testToRetractStreamMultiRowtime(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.enableObjectReuse()
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)

    val t = env.fromCollection(tupleData3)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text, 'rowtime.rowtime)

    val r = t
      .window(Tumble over 5.milli on 'rowtime as 'w)
      .groupBy('num, 'w)
      .select('num, 'w.rowtime, 'w.rowtime as 'rowtime2)

    r.toRetractStream[Row]
  }
}
