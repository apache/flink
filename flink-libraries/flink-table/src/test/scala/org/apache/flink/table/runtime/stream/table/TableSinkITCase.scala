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

import java.io.File
import java.lang.{Boolean => JBool}

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{TableEnvironment, TableException, Types}
import org.apache.flink.table.runtime.utils.{StreamITCase, StreamTestData}
import org.apache.flink.table.sinks._
import org.apache.flink.table.utils.MemoryTableSinkUtil
import org.apache.flink.test.util.{AbstractTestBase, TestBaseUtils}
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._
import scala.collection.mutable

class TableSinkITCase extends AbstractTestBase {

  @Test
  def testInsertIntoRegisteredTableSink(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.enableObjectReuse()
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val tEnv = TableEnvironment.getTableEnvironment(env)
    MemoryTableSinkUtil.clear

    val input = StreamTestData.get3TupleDataStream(env)
      .assignAscendingTimestamps(r => r._2)
    val fieldNames = Array("d", "e", "t")
    val fieldTypes: Array[TypeInformation[_]] = Array(Types.STRING, Types.SQL_TIMESTAMP, Types.LONG)
    val sink = new MemoryTableSinkUtil.UnsafeMemoryAppendTableSink
    tEnv.registerTableSink("targetTable", fieldNames, fieldTypes, sink)

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

    TestBaseUtils.compareResultAsText(MemoryTableSinkUtil.results.asJava, expected)
  }

  @Test
  def testStreamTableSink(): Unit = {

    val tmpFile = File.createTempFile("flink-table-sink-test", ".tmp")
    tmpFile.deleteOnExit()
    val path = tmpFile.toURI.toString

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.enableObjectReuse()
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setParallelism(4)

    val input = StreamTestData.get3TupleDataStream(env)
      .assignAscendingTimestamps(_._2)
      .map(x => x).setParallelism(4) // increase DOP to 4

    input.toTable(tEnv, 'a, 'b.rowtime, 'c)
      .where('a < 5 || 'a > 17)
      .select('c, 'b)
      .writeToSink(new CsvTableSink(path))

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
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val t = StreamTestData.get3TupleDataStream(env)
        .assignAscendingTimestamps(_._1.toLong)
        .toTable(tEnv, 'id, 'num, 'text, 'rowtime.rowtime)

    t.window(Tumble over 5.millis on 'rowtime as 'w)
      .groupBy('w)
      .select('w.end, 'id.count, 'num.sum)
      .writeToSink(new TestAppendSink)

    env.execute()

    val result = RowCollector.getAndClearValues.map(_.f1.toString).sorted
    val expected = List(
      "1970-01-01 00:00:00.005,4,8",
      "1970-01-01 00:00:00.01,5,18",
      "1970-01-01 00:00:00.015,5,24",
      "1970-01-01 00:00:00.02,5,29",
      "1970-01-01 00:00:00.025,2,12")
      .sorted
    assertEquals(expected, result)
  }

  @Test
  def testRetractSinkOnUpdatingTable(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.enableObjectReuse()
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val t = StreamTestData.get3TupleDataStream(env)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text)

    t.select('id, 'num, 'text.charLength() as 'len)
      .groupBy('len)
      .select('len, 'id.count, 'num.sum)
      .writeToSink(new TestRetractSink)

    env.execute()
    val results = RowCollector.getAndClearValues

    val retracted = RowCollector.retractResults(results).sorted
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
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val t = StreamTestData.get3TupleDataStream(env)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text, 'rowtime.rowtime)

    t.window(Tumble over 5.millis on 'rowtime as 'w)
      .groupBy('w)
      .select('w.end, 'id.count, 'num.sum)
      .writeToSink(new TestRetractSink)

    env.execute()
    val results = RowCollector.getAndClearValues

    assertFalse(
      "Received retraction messages for append only table",
      results.exists(!_.f0))

    val retracted = RowCollector.retractResults(results).sorted
    val expected = List(
      "1970-01-01 00:00:00.005,4,8",
      "1970-01-01 00:00:00.01,5,18",
      "1970-01-01 00:00:00.015,5,24",
      "1970-01-01 00:00:00.02,5,29",
      "1970-01-01 00:00:00.025,2,12")
      .sorted
    assertEquals(expected, retracted)

  }

  @Test
  def testUpsertSinkOnUpdatingTableWithFullKey(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.enableObjectReuse()
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val t = StreamTestData.get3TupleDataStream(env)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text)

    t.select('id, 'num, 'text.charLength() as 'len, ('id > 0) as 'cTrue)
      .groupBy('len, 'cTrue)
      .select('len, 'id.count as 'cnt, 'cTrue)
      .groupBy('cnt, 'cTrue)
      .select('cnt, 'len.count, 'cTrue)
      .writeToSink(new TestUpsertSink(Array("cnt", "cTrue"), false))

    env.execute()
    val results = RowCollector.getAndClearValues

    assertTrue(
      "Results must include delete messages",
      results.exists(_.f0 == false)
    )

    val retracted = RowCollector.upsertResults(results, Array(0, 2)).sorted
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
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val t = StreamTestData.get3TupleDataStream(env)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text, 'rowtime.rowtime)

    t.window(Tumble over 5.millis on 'rowtime as 'w)
      .groupBy('w, 'num)
      .select('num, 'w.end as 'wend, 'id.count)
      .writeToSink(new TestUpsertSink(Array("wend", "num"), true))

    env.execute()
    val results = RowCollector.getAndClearValues

    assertFalse(
      "Received retraction messages for append only table",
      results.exists(!_.f0))

    val retracted = RowCollector.upsertResults(results, Array(0, 1, 2)).sorted
    val expected = List(
      "1,1970-01-01 00:00:00.005,1",
      "2,1970-01-01 00:00:00.005,2",
      "3,1970-01-01 00:00:00.005,1",
      "3,1970-01-01 00:00:00.01,2",
      "4,1970-01-01 00:00:00.01,3",
      "4,1970-01-01 00:00:00.015,1",
      "5,1970-01-01 00:00:00.015,4",
      "5,1970-01-01 00:00:00.02,1",
      "6,1970-01-01 00:00:00.02,4",
      "6,1970-01-01 00:00:00.025,2").sorted
    assertEquals(expected, retracted)
  }

  @Test
  def testUpsertSinkOnAppendingTableWithFullKey2(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.enableObjectReuse()
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val t = StreamTestData.get3TupleDataStream(env)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text, 'rowtime.rowtime)

    t.window(Tumble over 5.millis on 'rowtime as 'w)
      .groupBy('w, 'num)
      .select('w.start as 'wstart, 'w.end as 'wend, 'num, 'id.count)
      .writeToSink(new TestUpsertSink(Array("wstart", "wend", "num"), true))

    env.execute()
    val results = RowCollector.getAndClearValues

    assertFalse(
      "Received retraction messages for append only table",
      results.exists(!_.f0))

    val retracted = RowCollector.upsertResults(results, Array(0, 1, 2)).sorted
    val expected = List(
      "1970-01-01 00:00:00.0,1970-01-01 00:00:00.005,1,1",
      "1970-01-01 00:00:00.0,1970-01-01 00:00:00.005,2,2",
      "1970-01-01 00:00:00.0,1970-01-01 00:00:00.005,3,1",
      "1970-01-01 00:00:00.005,1970-01-01 00:00:00.01,3,2",
      "1970-01-01 00:00:00.005,1970-01-01 00:00:00.01,4,3",
      "1970-01-01 00:00:00.01,1970-01-01 00:00:00.015,4,1",
      "1970-01-01 00:00:00.01,1970-01-01 00:00:00.015,5,4",
      "1970-01-01 00:00:00.015,1970-01-01 00:00:00.02,5,1",
      "1970-01-01 00:00:00.015,1970-01-01 00:00:00.02,6,4",
      "1970-01-01 00:00:00.02,1970-01-01 00:00:00.025,6,2").sorted
    assertEquals(expected, retracted)
  }

  @Test
  def testUpsertSinkOnAppendingTableWithoutFullKey1(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.enableObjectReuse()
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val t = StreamTestData.get3TupleDataStream(env)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text, 'rowtime.rowtime)

    t.window(Tumble over 5.millis on 'rowtime as 'w)
      .groupBy('w, 'num)
      .select('w.end as 'wend, 'id.count as 'cnt)
      .writeToSink(new TestUpsertSink(null, true))

    env.execute()
    val results = RowCollector.getAndClearValues

    assertFalse(
      "Received retraction messages for append only table",
      results.exists(!_.f0))

    val retracted = results.map(_.f1.toString).sorted
    val expected = List(
      "1970-01-01 00:00:00.005,1",
      "1970-01-01 00:00:00.005,2",
      "1970-01-01 00:00:00.005,1",
      "1970-01-01 00:00:00.01,2",
      "1970-01-01 00:00:00.01,3",
      "1970-01-01 00:00:00.015,1",
      "1970-01-01 00:00:00.015,4",
      "1970-01-01 00:00:00.02,1",
      "1970-01-01 00:00:00.02,4",
      "1970-01-01 00:00:00.025,2").sorted
    assertEquals(expected, retracted)
  }

  @Test
  def testUpsertSinkOnAppendingTableWithoutFullKey2(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.enableObjectReuse()
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val t = StreamTestData.get3TupleDataStream(env)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text, 'rowtime.rowtime)

    t.window(Tumble over 5.millis on 'rowtime as 'w)
      .groupBy('w, 'num)
      .select('num, 'id.count as 'cnt)
      .writeToSink(new TestUpsertSink(null, true))

    env.execute()
    val results = RowCollector.getAndClearValues

    assertFalse(
      "Received retraction messages for append only table",
      results.exists(!_.f0))

    val retracted = results.map(_.f1.toString).sorted
    val expected = List(
      "1,1",
      "2,2",
      "3,1",
      "3,2",
      "4,3",
      "4,1",
      "5,4",
      "5,1",
      "6,4",
      "6,2").sorted
    assertEquals(expected, retracted)
  }

  @Test
  def testToAppendStreamRowtime(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.enableObjectReuse()
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val t = StreamTestData.get3TupleDataStream(env)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text, 'rowtime.rowtime)

    val r = t
      .window(Tumble over 5.milli on 'rowtime as 'w)
      .groupBy('num, 'w)
      .select('num, 'w.rowtime, 'w.rowtime.cast(Types.LONG))

    r.toAppendStream[Row]
      .process(new ProcessFunction[Row, Row] {
        override def processElement(
          row: Row,
          ctx: ProcessFunction[Row, Row]#Context,
          out: Collector[Row]): Unit = {

          val rowTS: Long = row.getField(2).asInstanceOf[Long]
          if (ctx.timestamp() == rowTS) {
            out.collect(row)
          }
        }
      }).addSink(new StreamITCase.StringSink[Row])

    env.execute()

    val expected = List(
      "1,1970-01-01 00:00:00.004,4",
      "2,1970-01-01 00:00:00.004,4",
      "3,1970-01-01 00:00:00.004,4",
      "3,1970-01-01 00:00:00.009,9",
      "4,1970-01-01 00:00:00.009,9",
      "4,1970-01-01 00:00:00.014,14",
      "5,1970-01-01 00:00:00.014,14",
      "5,1970-01-01 00:00:00.019,19",
      "6,1970-01-01 00:00:00.019,19",
      "6,1970-01-01 00:00:00.024,24")

    assertEquals(expected, StreamITCase.testResults.sorted)
  }

  @Test
  def testToRetractStreamRowtime(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val t = StreamTestData.get3TupleDataStream(env)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text, 'rowtime.rowtime)

    val r = t
      .window(Tumble over 5.milli on 'rowtime as 'w)
      .groupBy('num, 'w)
      .select('num, 'w.rowtime, 'w.rowtime.cast(Types.LONG))

    r.toRetractStream[Row]
      .process(new ProcessFunction[(Boolean, Row), Row] {
        override def processElement(
          row: (Boolean, Row),
          ctx: ProcessFunction[(Boolean, Row), Row]#Context,
          out: Collector[Row]): Unit = {

          val rowTs = row._2.getField(2).asInstanceOf[Long]
          if (ctx.timestamp() == rowTs) {
            out.collect(row._2)
          }
        }
      }).addSink(new StreamITCase.StringSink[Row])

    env.execute()

    val expected = List(
      "1,1970-01-01 00:00:00.004,4",
      "2,1970-01-01 00:00:00.004,4",
      "3,1970-01-01 00:00:00.004,4",
      "3,1970-01-01 00:00:00.009,9",
      "4,1970-01-01 00:00:00.009,9",
      "4,1970-01-01 00:00:00.014,14",
      "5,1970-01-01 00:00:00.014,14",
      "5,1970-01-01 00:00:00.019,19",
      "6,1970-01-01 00:00:00.019,19",
      "6,1970-01-01 00:00:00.024,24")

    assertEquals(expected, StreamITCase.testResults.sorted)
  }

  @Test(expected = classOf[TableException])
  def testToAppendStreamMultiRowtime(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.enableObjectReuse()
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val t = StreamTestData.get3TupleDataStream(env)
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
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val t = StreamTestData.get3TupleDataStream(env)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text, 'rowtime.rowtime)

    val r = t
      .window(Tumble over 5.milli on 'rowtime as 'w)
      .groupBy('num, 'w)
      .select('num, 'w.rowtime, 'w.rowtime as 'rowtime2)

    r.toRetractStream[Row]
  }
}

private[flink] class TestAppendSink extends AppendStreamTableSink[Row] {

  var fNames: Array[String] = _
  var fTypes: Array[TypeInformation[_]] = _

  override def emitDataStream(s: DataStream[Row]): Unit = {
    s.map(
      new MapFunction[Row, JTuple2[JBool, Row]] {
        override def map(value: Row): JTuple2[JBool, Row] = new JTuple2(true, value)
      })
      .addSink(new RowSink)
  }

  override def getOutputType: TypeInformation[Row] = new RowTypeInfo(fTypes, fNames)

  override def getFieldNames: Array[String] = fNames

  override def getFieldTypes: Array[TypeInformation[_]] = fTypes

  override def configure(
    fieldNames: Array[String],
    fieldTypes: Array[TypeInformation[_]]): TableSink[Row] = {
    val copy = new TestAppendSink
    copy.fNames = fieldNames
    copy.fTypes = fieldTypes
    copy
  }
}

private[flink] class TestRetractSink extends RetractStreamTableSink[Row] {

  var fNames: Array[String] = _
  var fTypes: Array[TypeInformation[_]] = _

  override def emitDataStream(s: DataStream[JTuple2[JBool, Row]]): Unit = {
    s.addSink(new RowSink)
  }

  override def getRecordType: TypeInformation[Row] = new RowTypeInfo(fTypes, fNames)

  override def getFieldNames: Array[String] = fNames

  override def getFieldTypes: Array[TypeInformation[_]] = fTypes

  override def configure(
      fieldNames: Array[String],
      fieldTypes: Array[TypeInformation[_]]): TableSink[JTuple2[JBool, Row]] = {
    val copy = new TestRetractSink
    copy.fNames = fieldNames
    copy.fTypes = fieldTypes
    copy
  }

}

private[flink] class TestUpsertSink(
    expectedKeys: Array[String],
    expectedIsAppendOnly: Boolean)
  extends UpsertStreamTableSink[Row] {

  var fNames: Array[String] = _
  var fTypes: Array[TypeInformation[_]] = _

  override def setKeyFields(keys: Array[String]): Unit =
    if (keys != null) {
      assertEquals("Provided key fields do not match expected keys",
        expectedKeys.sorted.mkString(","),
        keys.sorted.mkString(","))
    } else {
      assertNull("Provided key fields should not be null.", expectedKeys)
    }

  override def setIsAppendOnly(isAppendOnly: JBool): Unit =
    assertEquals(
      "Provided isAppendOnly does not match expected isAppendOnly",
      expectedIsAppendOnly,
      isAppendOnly)

  override def getRecordType: TypeInformation[Row] = new RowTypeInfo(fTypes, fNames)

  override def emitDataStream(s: DataStream[JTuple2[JBool, Row]]): Unit = {
    s.addSink(new RowSink)
  }

  override def getFieldNames: Array[String] = fNames

  override def getFieldTypes: Array[TypeInformation[_]] = fTypes

  override def configure(
      fieldNames: Array[String],
      fieldTypes: Array[TypeInformation[_]]): TableSink[JTuple2[JBool, Row]] = {
    val copy = new TestUpsertSink(expectedKeys, expectedIsAppendOnly)
    copy.fNames = fieldNames
    copy.fTypes = fieldTypes
    copy
  }
}

class RowSink extends SinkFunction[JTuple2[JBool, Row]] {
  override def invoke(value: JTuple2[JBool, Row]): Unit = RowCollector.addValue(value)
}

object RowCollector {
  private val sink: mutable.ArrayBuffer[JTuple2[JBool, Row]] =
    new mutable.ArrayBuffer[JTuple2[JBool, Row]]()

  def addValue(value: JTuple2[JBool, Row]): Unit = {

    // make a copy
    val copy = new JTuple2[JBool, Row](value.f0, Row.copy(value.f1))
    sink.synchronized {
      sink += copy
    }
  }

  def getAndClearValues: List[JTuple2[JBool, Row]] = {
    val out = sink.toList
    sink.clear()
    out
  }

  /** Converts a list of retraction messages into a list of final results. */
  def retractResults(results: List[JTuple2[JBool, Row]]): List[String] = {

    val retracted = results
      .foldLeft(Map[String, Int]()){ (m: Map[String, Int], v: JTuple2[JBool, Row]) =>
        val cnt = m.getOrElse(v.f1.toString, 0)
        if (v.f0) {
          m + (v.f1.toString -> (cnt + 1))
        } else {
          m + (v.f1.toString -> (cnt - 1))
        }
      }.filter{ case (_, c: Int) => c != 0 }

    assertFalse(
      "Received retracted rows which have not been accumulated.",
      retracted.exists{ case (_, c: Int) => c < 0})

    retracted.flatMap { case (r: String, c: Int) => (0 until c).map(_ => r) }.toList
  }

  /** Converts a list of upsert messages into a list of final results. */
  def upsertResults(results: List[JTuple2[JBool, Row]], keys: Array[Int]): List[String] = {

    def getKeys(r: Row): Row = Row.project(r, keys)

    val upserted = results.foldLeft(Map[Row, String]()){ (o: Map[Row, String], r) =>
      val key = getKeys(r.f1)
      if (r.f0) {
        o + (key -> r.f1.toString)
      } else {
        o - key
      }
    }

    upserted.values.toList
  }
}
