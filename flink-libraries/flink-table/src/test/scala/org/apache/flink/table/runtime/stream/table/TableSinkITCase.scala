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
import java.util.TimeZone

import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types.{DataType, DataTypes}
import org.apache.flink.table.runtime.utils.{StreamTestData, StreamingTestBase}
import org.apache.flink.table.sinks.{RetractStreamTableSink, TableSink, UpsertStreamTableSink}
import org.apache.flink.table.sinks.csv.CsvTableSink
import org.apache.flink.table.util.MemoryTableSourceSinkUtil
import org.apache.flink.test.util.TestBaseUtils
import org.apache.flink.types.Row
import org.junit.Assert.{assertEquals, assertFalse, assertNull}
import org.junit.Test

import scala.collection.JavaConverters._
import scala.collection.mutable

class TableSinkITCase extends StreamingTestBase {

  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

  @Test
  def testInsertIntoRegisteredTableSink(): Unit = {
    MemoryTableSourceSinkUtil.clear()

    val input = StreamTestData.get3TupleDataStream(env)
      .assignAscendingTimestamps(r => r._2)
    val fieldNames = Array("d", "e", "t")
    val fieldTypes: Array[DataType] = Array(DataTypes.STRING, DataTypes.TIMESTAMP, DataTypes.LONG)
    val sink = new MemoryTableSourceSinkUtil.UnsafeMemoryAppendTableSink
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

    TestBaseUtils.compareResultAsText(MemoryTableSourceSinkUtil.results.asJava, expected)
  }

  @Test
  def testStreamTableSink(): Unit = {

    val tmpFile = File.createTempFile("flink-table-sink-test", ".tmp")
    tmpFile.deleteOnExit()
    val path = tmpFile.toURI.toString

    val input = StreamTestData.get3TupleDataStream(env)
      .assignAscendingTimestamps(_._2)
      .map(x => x).setParallelism(4) // increase DOP to 4

    input.toTable(tEnv, 'a, 'b.rowtime, 'c)
      .where('a < 5 || 'a > 17)
      .select('c, 'b)
      .writeToSink(
        new CsvTableSink(path, Some(","), None, None, None, Some(WriteMode.OVERWRITE), None, None))

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
}

private[flink] class TestRetractSink extends RetractStreamTableSink[Row] {

  var fNames: Array[String] = _
  var fTypes: Array[DataType] = _

  override def emitDataStream(s: DataStream[JTuple2[JBool, Row]]) = {
    s.addSink(new RowSink)
  }

  override def getRecordType: DataType = DataTypes.createRowType(fTypes, fNames)

  override def getFieldNames: Array[String] = fNames

  override def getFieldTypes: Array[DataType] = fTypes

  override def configure(
    fieldNames: Array[String],
    fieldTypes: Array[DataType]): TableSink[JTuple2[JBool, Row]] = {
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
  var fTypes: Array[DataType] = _

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

  override def getRecordType: DataType = DataTypes.createRowType(fTypes, fNames)

  override def emitDataStream(s: DataStream[JTuple2[JBool, Row]]) = {
    s.addSink(new RowSink)
  }

  override def getFieldNames: Array[String] = fNames

  override def getFieldTypes: Array[DataType] = fTypes

  override def configure(
    fieldNames: Array[String],
    fieldTypes: Array[DataType]): TableSink[JTuple2[JBool, Row]] = {
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
