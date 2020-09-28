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
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.internal.TableEnvironmentInternal
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

class LegacyTableSinkITCase extends AbstractTestBase {

  @Test
  def testStreamTableSink(): Unit = {

    val tmpFile = File.createTempFile("flink-table-sink-test", ".tmp")
    tmpFile.delete()
    tmpFile.deleteOnExit()
    val path = tmpFile.toURI.toString

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.enableObjectReuse()

    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)
    env.setParallelism(4)

    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "csvSink",
      new CsvTableSink(path).configure(
        Array[String]("nullableCol", "c", "b"),
        Array[TypeInformation[_]](Types.INT, Types.STRING, Types.SQL_TIMESTAMP)))

    val input = env.fromCollection(tupleData3)
      .assignAscendingTimestamps(_._2)
      .map(x => x).setParallelism(4) // increase DOP to 4

    val table = input.toTable(tEnv, 'a, 'b.rowtime, 'c)
      .where('a < 5 || 'a > 17)
      .select(ifThenElse('a < 4, nullOf(Types.INT()), 'a), 'c, 'b)
    table.executeInsert("csvSink").await()

    val expected = Seq(
      ",Hello world,1970-01-01 00:00:00.002",
      ",Hello,1970-01-01 00:00:00.002",
      ",Hi,1970-01-01 00:00:00.001",
      "18,Comment#12,1970-01-01 00:00:00.006",
      "19,Comment#13,1970-01-01 00:00:00.006",
      "20,Comment#14,1970-01-01 00:00:00.006",
      "21,Comment#15,1970-01-01 00:00:00.006",
      "4,Hello world, how are you?,1970-01-01 00:00:00.003").mkString("\n")

    TestBaseUtils.compareResultsByLinesInMemory(expected, path)
  }

  @Test
  def testAppendSinkOnAppendTable(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.enableObjectReuse()
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)

    val t = env.fromCollection(tupleData3)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text, 'rowtime.rowtime)

    val sink = new TestingAppendTableSink(TimeZone.getDefault)
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "appendSink",
      sink.configure(
        Array[String]("t", "icnt", "nsum"),
        Array[TypeInformation[_]](Types.SQL_TIMESTAMP, Types.LONG, Types.LONG)))

    val table = t.window(Tumble over 5.millis on 'rowtime as 'w)
      .groupBy('w)
      .select('w.end as 't, 'id.count as 'icnt, 'num.sum as 'nsum)
    table.executeInsert("appendSink").await()

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
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "appendSink",
      sink.configure(
        Array[String]("t", "item"),
        Array[TypeInformation[_]](Types.INT(), Types.ROW(Types.LONG, Types.STRING()))))

    tEnv.executeSql("INSERT INTO appendSink SELECT id, ROW(num, text) FROM src").await()

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
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)

    val ds1 = env.fromCollection(smallTupleData3).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = env.fromCollection(tupleData5).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val sink = new TestingAppendTableSink
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "appendSink",
      sink.configure(
        Array[String]("c", "g"),
        Array[TypeInformation[_]](Types.STRING, Types.STRING)))

    val table = ds1.join(ds2).where('b === 'e)
      .select('c, 'g)
    table.executeInsert("appendSink").await()

    val result = sink.getAppendResults.sorted
    val expected = List("Hi,Hallo", "Hello,Hallo Welt", "Hello world,Hallo Welt").sorted
    assertEquals(expected, result)
  }

  @Test
  def testRetractSinkOnUpdatingTable(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.enableObjectReuse()
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)

    val t = env.fromCollection(tupleData3)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text)

    val sink = new TestingRetractTableSink()
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "retractSink",
      sink.configure(
        Array[String]("len", "icnt", "nsum"),
        Array[TypeInformation[_]](Types.INT, Types.LONG, Types.DECIMAL())))

    val table = t.select('id, 'num, 'text.charLength() as 'len)
      .groupBy('len)
      .select('len, 'id.count as 'icnt, 'num.sum as 'nsum)
    table.executeInsert("retractSink").await()

    val retracted = sink.getRetractResults.sorted
    val expected = List(
      "2,1,1.000000000000000000",
      "5,1,2.000000000000000000",
      "11,1,2.000000000000000000",
      "25,1,3.000000000000000000",
      "10,7,39.000000000000000000",
      "14,1,3.000000000000000000",
      "9,9,41.000000000000000000").sorted
    assertEquals(expected, retracted)

  }

  @Test
  def testRetractSinkOnAppendTable(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.enableObjectReuse()
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)

    val t = env.fromCollection(tupleData3)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text, 'rowtime.rowtime)

    val sink = new TestingRetractTableSink(TimeZone.getDefault)
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "retractSink",
      sink.configure(
        Array[String]("t", "icnt", "nsum"),
        Array[TypeInformation[_]](Types.SQL_TIMESTAMP, Types.LONG, Types.LONG)))

    val table = t.window(Tumble over 5.millis on 'rowtime as 'w)
      .groupBy('w)
      .select('w.end as 't, 'id.count as 'icnt, 'num.sum as 'nsum)
    table.executeInsert("retractSink").await()

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
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)

    val t = env.fromCollection(tupleData3)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text)

    val sink = new TestingUpsertTableSink(Array(0, 2), TimeZone.getDefault).configure(
      Array[String]("cnt", "lencnt", "cTrue"),
      Array[TypeInformation[_]](Types.LONG, Types.DECIMAL(), Types.BOOLEAN))
    sink.expectedKeys = Some(Array("cnt", "cTrue"))
    sink.expectedIsAppendOnly = Some(false)
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("upsertSink", sink)

    val table = t.select('id, 'num, 'text.charLength() as 'len, ('id > 0) as 'cTrue)
      .groupBy('len, 'cTrue)
      // test query field name is different with registered sink field name
      .select('len, 'id.count as 'count, 'cTrue)
      .groupBy('count, 'cTrue)
      .select('count, 'len.count as 'lencnt, 'cTrue)
    table.executeInsert("upsertSink").await()

    assertTrue(
      "Results must include delete messages",
      sink.getRawResults.exists(_.startsWith("(false,")))

    val retracted = sink.getUpsertResults.sorted
    val expected = List(
      "1,5.000000000000000000,true",
      "7,1.000000000000000000,true",
      "9,1.000000000000000000,true").sorted
    assertEquals(expected, retracted)

  }

  @Test
  def testUpsertSinkOnAppendingTableWithFullKey1(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.enableObjectReuse()
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)

    val t = env.fromCollection(tupleData3)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text, 'rowtime.rowtime)

    val sink = new TestingUpsertTableSink(Array(0, 1, 2), TimeZone.getDefault).configure(
      Array[String]("num", "wend", "icnt"),
      Array[TypeInformation[_]](Types.LONG, Types.SQL_TIMESTAMP, Types.LONG))
    sink.expectedKeys = Some(Array("wend", "num"))
    sink.expectedIsAppendOnly = Some(true)
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("upsertSink", sink)

    val table = t.window(Tumble over 5.millis on 'rowtime as 'w)
      .groupBy('w, 'num)
      // test query field name is different with registered sink field name
      .select('num, 'w.end as 'window_end, 'id.count as 'icnt)
    table.executeInsert("upsertSink").await()

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
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)

    val t = env.fromCollection(tupleData3)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text, 'rowtime.rowtime)

    val sink = new TestingUpsertTableSink(Array(0, 1, 2), TimeZone.getDefault)
    sink.expectedKeys = Some(Array("wend", "num"))
    sink.expectedIsAppendOnly = Some(true)
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "upsertSink",
      sink.configure(
        Array[String]("wstart", "wend", "num", "icnt"),
        Array[TypeInformation[_]]
          (Types.SQL_TIMESTAMP, Types.SQL_TIMESTAMP, Types.LONG, Types.LONG)))

    val table = t.window(Tumble over 5.millis on 'rowtime as 'w)
      .groupBy('w, 'num)
      .select('w.start as 'wstart, 'w.end as 'wend, 'num, 'id.count as 'icnt)
    table.executeInsert("upsertSink").await()

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
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)

    val t = env.fromCollection(tupleData3)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text, 'rowtime.rowtime)

    val sink = new TestingUpsertTableSink(Array(0), TimeZone.getDefault)
    sink.expectedIsAppendOnly = Some(true)
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "upsertSink",
      sink.configure(
        Array[String]("wend", "cnt"),
        Array[TypeInformation[_]](Types.SQL_TIMESTAMP, Types.LONG)))

    val table = t.window(Tumble over 5.millis on 'rowtime as 'w)
      .groupBy('w, 'num)
      .select('w.end as 'wend, 'id.count as 'cnt)
    table.executeInsert("upsertSink").await()

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
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)

    val t = env.fromCollection(tupleData3)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text, 'rowtime.rowtime)

    val sink = new TestingUpsertTableSink(Array(0), TimeZone.getDefault)
    sink.expectedIsAppendOnly = Some(true)
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "upsertSink",
      sink.configure(
        Array[String]("num", "cnt"),
        Array[TypeInformation[_]](Types.LONG, Types.LONG)))

    val table = t.window(Tumble over 5.millis on 'rowtime as 'w)
      .groupBy('w, 'num)
      .select('num, 'id.count as 'cnt)
    table.executeInsert("upsertSink").await()

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

  @Test
  def testUpsertSinkWithFilter(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.enableObjectReuse()

    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)
    env.setParallelism(4)

    val t = env.fromCollection(tupleData3)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text)

    val sink = new TestingUpsertTableSink(Array(0))
    sink.expectedIsAppendOnly = Some(false)
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "upsertSink",
      sink.configure(
        Array[String]("num", "cnt"),
        Array[TypeInformation[_]](Types.LONG, Types.LONG)))

    // num, cnt
    //   1, 1
    //   2, 2
    //   3, 3
    //   4, 4
    //   5, 5
    //   6, 6

    val table = t.groupBy('num)
      .select('num, 'id.count as 'cnt)
      .where('cnt <= 3)
    table.executeInsert("upsertSink").await()

    val expectedWithFilter = List("1,1", "2,2", "3,3")
    assertEquals(expectedWithFilter.sorted, sink.getUpsertResults.sorted)
  }

  @Test(expected = classOf[TableException])
  def testToAppendStreamMultiRowtime(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.enableObjectReuse()
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

  @Test
  def testDecimalAppendStreamTableSink(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)

    MemoryTableSourceSinkUtil.clear()

    val schema = TableSchema.builder()
        .field("c", DataTypes.VARCHAR(5))
        .field("b", DataTypes.DECIMAL(10, 0))
        .field("d", DataTypes.CHAR(5))
        .build()

    MemoryTableSourceSinkUtil.createDataTypeAppendStreamTable(
      tEnv, schema, "testSink")

    val table = env.fromCollection(tupleData3)
      .toTable(tEnv, 'a, 'b, 'c)
      .where('a > 20)
      .select("12345", 55.cast(DataTypes.DECIMAL(10, 0)), "12345".cast(DataTypes.CHAR(5)))
    table.executeInsert("testSink").await()

    val results = MemoryTableSourceSinkUtil.tableDataStrings.asJava
    val expected = Seq("12345,55,12345").mkString("\n")

    TestBaseUtils.compareResultAsText(results, expected)
  }
}
