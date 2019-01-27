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

import org.apache.calcite.runtime.SqlFunctions.{internalToTimestamp => toTimestamp}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.{GenericTypeInfo, RowTypeInfo}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{TableException, TableSchema}
import org.apache.flink.table.sources.StreamTableSource
import org.apache.flink.table.api.types.{DataType, DataTypes, InternalType, TypeConverters}
import org.apache.flink.table.util._
import org.apache.flink.table.api.Types
import org.apache.flink.table.runtime.utils.{CommonTestData, StreamingTestBase, TestingAppendSink}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.sources.wmstrategies.PunctuatedWatermarkAssigner
import org.apache.flink.table.util.{TestFilterableTableSource, TestPartitionableTableSource, TestTableSourceWithTime}
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

import org.junit.Assert._
import org.junit.Test

import java.lang.{Boolean => JBool, Integer => JInt, Long => JLong}
import java.sql.Timestamp

import scala.collection.JavaConverters._
import scala.collection.mutable

class TableSourceITCase extends StreamingTestBase {

  @Test(expected = classOf[TableException])
  def testInvalidDatastreamType(): Unit = {
    val tableSource = new StreamTableSource[Row]() {
      private val fieldNames: Array[String] = Array("name", "id", "value")
      private val fieldTypes: Array[InternalType] =
        Array(DataTypes.STRING, DataTypes.LONG, DataTypes.INT)

      override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = {
        val data = List(Row.of("Mary", new JLong(1L), new JInt(1))).asJava
        // return DataStream[Row] with GenericTypeInfo
        execEnv.fromCollection(data, new GenericTypeInfo[Row](classOf[Row]))
      }
      override def getReturnType: DataType =
        DataTypes.createRowType(fieldTypes.toArray[DataType], fieldNames)
      override def getTableSchema: TableSchema = new TableSchema(fieldNames, fieldTypes)
    }
    tEnv.registerTableSource("T", tableSource)

    val sink = new TestingAppendSink

    tEnv.scan("T")
      .select('value, 'name)
        .toAppendStream[Row]
        .addSink(sink)
    env.execute()

    // test should fail because type info of returned DataStream does not match type return type
    // info.
  }

  @Test
  def testCsvTableSource(): Unit = {

    val csvTable = CommonTestData.getCsvTableSource

    val sink = new TestingAppendSink

    tEnv.registerTableSource("csvTable", csvTable)
    tEnv.scan("csvTable")
      .where('id > 4)
      .select('last, 'score * 2)
      .toAppendStream[Row]
      .addSink(sink)

    env.execute()

    val expected = mutable.MutableList(
      "Williams,69.0",
      "Miller,13.56",
      "Smith,180.2",
      "Williams,4.68")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testCsvTableSourceWithFilterable(): Unit = {
    val tableName = "MyTable"
    val sink = new TestingAppendSink

    tEnv.registerTableSource(tableName, new TestFilterableTableSource)
    tEnv.scan(tableName)
      .where("amount > 4 && price < 9")
      .select("id, name")
      .addSink(sink)

    env.execute()

    val expected = mutable.MutableList(
      "5,Record_5", "6,Record_6", "7,Record_7", "8,Record_8")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testRowtimeTableSource(): Unit = {
    val tableName = "MyTable"

    val data = Seq(
      Row.of("Mary", new Timestamp(1L), new JInt(10)),
      Row.of("Bob", new Timestamp(2L), new JInt(20)),
      Row.of("Mary", new Timestamp(2L), new JInt(30)),
      Row.of("Liz", new Timestamp(2001L), new JInt(40)))

    val fieldNames = Array("name", "rtime", "amount")
    val schema = new TableSchema(fieldNames,
      Array(DataTypes.STRING, DataTypes.TIMESTAMP, DataTypes.INT))
    val rowType = new RowTypeInfo(
      Array(Types.STRING, Types.SQL_TIMESTAMP, Types.INT).asInstanceOf[Array[TypeInformation[_]]],
      fieldNames)

    val watermarkStrategy = new PunctuatedWatermarkAssigner {
      /**
       * Returns the watermark for the current row or null if no watermark should be generated.
       *
       * @param row       The current row.
       * @param timestamp The value of the timestamp attribute for the row.
       * @return The watermark for this row or null if no watermark should be generated.
       */
      override def getWatermark(row: BaseRow, timestamp: Long): Watermark =
        // timestamp in base row is stored in type of long in millisecond.
        new Watermark(row.getLong(1))
    }

    val tableSource = new TestDefinedWMTableSource(
      schema, rowType, data, "rtime", watermarkStrategy)

    tEnv.registerTableSource(tableName, tableSource)

    val sink = new TestingAppendSink
    tEnv.scan(tableName)
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
      "Mary,1970-01-01 00:00:00.0,40,2",
      "Bob,1970-01-01 00:00:00.0,20,2",
      "Liz,1970-01-01 00:00:02.0,40,2001")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }


  @Test
  def testProctimeTableSource(): Unit = {
    val tableName = "MyTable"

    val data = Seq(
      Row.of("Mary", new JLong(1L), new JInt(10)),
      Row.of("Bob", new JLong(2L), new JInt(20)),
      Row.of("Mary", new JLong(2L), new JInt(30)),
      Row.of("Liz", new JLong(2001L), new JInt(40)))

    val fieldNames = Array("name", "rtime", "amount")
    val schema = new TableSchema(
      fieldNames :+ "ptime",
      Array(DataTypes.STRING, DataTypes.LONG, DataTypes.INT, DataTypes.TIMESTAMP))
    val rowType = new RowTypeInfo(
      Array(Types.STRING, Types.LONG, Types.INT).asInstanceOf[Array[TypeInformation[_]]],
      fieldNames)

    val tableSource = new TestTableSourceWithTime(schema, rowType, data, null, "ptime")
    tEnv.registerTableSource(tableName, tableSource)

    val sink = new TestingAppendSink
    tEnv.scan(tableName)
      .where('ptime.cast(DataTypes.LONG) > 0L)
      .select('name, 'amount)
      .addSink(sink)
    env.execute()

    val expected = mutable.MutableList(
      "Mary,10",
      "Bob,20",
      "Mary,30",
      "Liz,40")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testRowtimeProctimeTableSource(): Unit = {
    val tableName = "MyTable"

    val data = Seq(
      Row.of("Mary", new JLong(1L), new JInt(10)),
      Row.of("Bob", new JLong(2L), new JInt(20)),
      Row.of("Mary", new JLong(2L), new JInt(30)),
      Row.of("Liz", new JLong(2001L), new JInt(40)))

    val fieldNames = Array("name", "rtime", "amount")
    val schema = new TableSchema(
      fieldNames :+ "ptime",
      Array(DataTypes.STRING, DataTypes.TIMESTAMP, DataTypes.INT, DataTypes.TIMESTAMP))
    val rowType = new RowTypeInfo(
      Array(Types.STRING, Types.LONG, Types.INT).asInstanceOf[Array[TypeInformation[_]]],
      fieldNames)

    val tableSource = new TestTableSourceWithTime(schema, rowType, data, "rtime", "ptime")
    tEnv.registerTableSource(tableName, tableSource)

    val sink = new TestingAppendSink
    tEnv.scan(tableName)
      .window(Tumble over 1.second on 'rtime as 'w)
      .groupBy('name, 'w)
      .select('name, 'w.start, 'amount.sum)
      .addSink(sink)
    env.execute()

    val expected = mutable.MutableList(
      "Mary,1970-01-01 00:00:00.0,40",
      "Bob,1970-01-01 00:00:00.0,20",
      "Liz,1970-01-01 00:00:02.0,40")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testRowtimeAsTimestampTableSource(): Unit = {
    val tableName = "MyTable"

    val data = Seq(
      Row.of("Mary", toTimestamp(1L), new JInt(10)),
      Row.of("Bob", toTimestamp(2L), new JInt(20)),
      Row.of("Mary", toTimestamp(2L), new JInt(30)),
      Row.of("Liz", toTimestamp(2001L), new JInt(40)))

    val fieldNames = Array("name", "rtime", "amount")
    val schema = new TableSchema(fieldNames,
      Array(DataTypes.STRING, DataTypes.TIMESTAMP, DataTypes.INT))
    val rowType = new RowTypeInfo(
      Array(Types.STRING, Types.SQL_TIMESTAMP, Types.INT).asInstanceOf[Array[TypeInformation[_]]],
      fieldNames)

    val tableSource = new TestTableSourceWithTime(schema, rowType, data, "rtime", null)
    tEnv.registerTableSource(tableName, tableSource)

    val sink = new TestingAppendSink
    tEnv.scan(tableName)
      .window(Tumble over 1.second on 'rtime as 'w)
      .groupBy('name, 'w)
      .select('name, 'w.start, 'amount.sum)
      .addSink(sink)
    env.execute()

    val expected = mutable.MutableList(
      "Mary,1970-01-01 00:00:00.0,40",
      "Bob,1970-01-01 00:00:00.0,20",
      "Liz,1970-01-01 00:00:02.0,40").toList
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }


  @Test
  def testPartitionableTableSourceWithPartitionFields(): Unit = {
    tEnv.registerTableSource("partitionable_table", new TestPartitionableTableSource)

    val sink = new TestingAppendSink
    tEnv.scan("partitionable_table")
      .where('part === "2" || 'part === "1" && 'id > 2)
      .addSink(sink)

    env.execute()

    val expected = mutable.MutableList(
      "3,John,2,part=1#part=2,true",
      "4,nosharp,2,part=1#part=2,true")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testPartitionableTableSourceWithPartitionFieldsAndBoolTypeColumn(): Unit = {

    tEnv.registerTableSource("partitionable_table", new TestPartitionableTableSource)

    val sink = new TestingAppendSink
    tEnv.scan("partitionable_table")
      .where('is_ok && 'part === "2")
      .addSink(sink)

    env.execute()

    val expected = mutable.MutableList(
      "3,John,2,part=2,true",
      "4,nosharp,2,part=2,true")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testPartitionableTableSourceWithPartitionFieldsAndBoolTypeColumn1(): Unit = {

    tEnv.registerTableSource("partitionable_table", new TestPartitionableTableSource)

    val sink = new TestingAppendSink
    tEnv.scan("partitionable_table")
      .where('is_ok === true && 'part === "2")
      .addSink(sink)

    env.execute()

    val expected = mutable.MutableList(
      "3,John,2,part=2,true",
      "4,nosharp,2,part=2,true")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testPartitionPruningRuleNotAppliedWithoutPartitionFields(): Unit = {
    tEnv.registerTableSource("partitionable_table", new TestPartitionableTableSource)

    val sink = new TestingAppendSink
    tEnv.scan("partitionable_table")
      .where('name === "Lucy")
      .addSink(sink)

    env.execute()

    val expected = mutable.MutableList("6,Lucy,3,null,true")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testRowtimeLongTableSource(): Unit = {
    val tableName = "MyTable"

    // StringSink is using UTC.
    val data = Seq(
      "1970-01-01 00:00:00",
      "1970-01-01 00:00:01",
      "1970-01-01 00:00:01",
      "1970-01-01 00:00:02",
      "1970-01-01 00:00:04")

    val schema = new TableSchema(Array("rtime"), Array(DataTypes.TIMESTAMP))
    val returnType = Types.STRING

    val tableSource = new TestTableSourceWithTime(schema, returnType, data, "rtime", null)
    tEnv.registerTableSource(tableName, tableSource)

    val sink = new TestingAppendSink
    tEnv.scan(tableName)
        .window(Tumble over 1.second on 'rtime as 'w)
        .groupBy('w)
        .select('w.start, 1.count)
        .addSink(sink)
    env.execute()

    val expected = Seq(
      "1970-01-01 00:00:00.0,1",
      "1970-01-01 00:00:01.0,2",
      "1970-01-01 00:00:02.0,1",
      "1970-01-01 00:00:04.0,1")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testProctimeStringTableSource(): Unit = {
    val tableName = "MyTable"

    val data = Seq("Mary", "Peter", "Bob", "Liz")

    val schema = new TableSchema(Array("name", "ptime"),
      Array(DataTypes.STRING, DataTypes.TIMESTAMP))
    val returnType = Types.STRING

    val tableSource = new TestTableSourceWithTime(schema, returnType, data, null, "ptime")
    tEnv.registerTableSource(tableName, tableSource)

    val sink = new TestingAppendSink
    tEnv.scan(tableName)
        .where('ptime.cast(DataTypes.LONG) > 1)
        .select('name)
        .addSink(sink)
    env.execute()

    val expected = Seq("Mary", "Peter", "Bob", "Liz")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testRowtimeProctimeLongTableSource(): Unit = {
    val tableName = "MyTable"

    val data = Seq(new JLong(1L), new JLong(2L), new JLong(2L), new JLong(2001L), new JLong(4001L))

    val schema = new TableSchema(
      Array("rtime", "ptime"),
      Array(DataTypes.TIMESTAMP, DataTypes.TIMESTAMP))
    val returnType = Types.LONG

    val tableSource = new TestTableSourceWithTime(schema, returnType, data, "rtime", "ptime")
    tEnv.registerTableSource(tableName, tableSource)

    val sink = new TestingAppendSink
    tEnv.scan(tableName)
        .where('ptime.cast(DataTypes.LONG) > 1)
        .window(Tumble over 1.second on 'rtime as 'w)
        .groupBy('w)
        .select('w.start, 1.count)
        .addSink(sink)
    env.execute()

    val expected = Seq(
      "1970-01-01 00:00:00.0,3",
      "1970-01-01 00:00:02.0,1",
      "1970-01-01 00:00:04.0,1")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testFieldMappingTableSource(): Unit = {
    val tableName = "MyTable"

    val data = Seq(
      Row.of("Mary", new JLong(1L), new JInt(10)),
      Row.of("Bob", new JLong(2L), new JInt(20)),
      Row.of("Mary", new JLong(2L), new JInt(30)),
      Row.of("Liz", new JLong(2001L), new JInt(40)))

    val schema = new TableSchema(
      Array("ptime", "amount", "name", "rtime"),
      Array(DataTypes.TIMESTAMP, DataTypes.INT, DataTypes.STRING, DataTypes.TIMESTAMP))
    val returnType = new RowTypeInfo(Types.STRING, Types.LONG, Types.INT)
    val mapping = Map("amount" -> "f2", "name" -> "f0", "rtime" -> "f1")

    val source = new TestTableSourceWithTime(schema, returnType, data, "rtime", "ptime", mapping)
    tEnv.registerTableSource(tableName, source)

    val sink = new TestingAppendSink
    tEnv.scan(tableName)
        .window(Tumble over 1.second on 'rtime as 'w)
        .groupBy('name, 'w)
        .select('name, 'w.start, 'amount.sum)
        .addSink(sink)
    env.execute()

    val expected = Seq(
      "Mary,1970-01-01 00:00:00.0,40",
      "Bob,1970-01-01 00:00:00.0,20",
      "Liz,1970-01-01 00:00:02.0,40")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testProjectWithoutRowtimeProctime(): Unit = {

    val data = Seq(
      Row.of(new JInt(1), "Mary", new JLong(10L), new JLong(1)),
      Row.of(new JInt(2), "Bob", new JLong(20L), new JLong(2)),
      Row.of(new JInt(3), "Mike", new JLong(30L), new JLong(2)),
      Row.of(new JInt(4), "Liz", new JLong(40L), new JLong(2001)))

    val tableSchema = new TableSchema(
      Array("id", "rtime", "val", "ptime", "name"),
      Array(
        DataTypes.INT, DataTypes.TIMESTAMP, DataTypes.LONG, DataTypes.TIMESTAMP, DataTypes.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.STRING, Types.LONG, Types.LONG)
          .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "name", "val", "rtime"))

    tEnv.registerTableSource(
      "T",
      new TestProjectableTableSource(tableSchema, returnType, data, "rtime", "ptime"))

    val sink = new TestingAppendSink
    tEnv.scan("T")
        .select('name, 'val, 'id)
        .addSink(sink)
    env.execute()

    val expected = Seq(
      "Mary,10,1",
      "Bob,20,2",
      "Mike,30,3",
      "Liz,40,4")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testProjectWithoutProctime(): Unit = {

    val data = Seq(
      Row.of(new JInt(1), "Mary", new JLong(10L), new JLong(1)),
      Row.of(new JInt(2), "Bob", new JLong(20L), new JLong(2)),
      Row.of(new JInt(3), "Mike", new JLong(30L), new JLong(2)),
      Row.of(new JInt(4), "Liz", new JLong(40L), new JLong(2001)))

    val tableSchema = new TableSchema(
      Array("id", "rtime", "val", "ptime", "name"),
      Array(
        DataTypes.INT, DataTypes.TIMESTAMP, DataTypes.LONG, DataTypes.TIMESTAMP, DataTypes.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.STRING, Types.LONG, Types.LONG)
          .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "name", "val", "rtime"))

    tEnv.registerTableSource(
      "T",
      new TestProjectableTableSource(tableSchema, returnType, data, "rtime", "ptime"))

    val sink = new TestingAppendSink
    tEnv.scan("T")
        .select('rtime, 'name, 'id)
        .addSink(sink)
    env.execute()

    val expected = Seq(
      "1970-01-01 00:00:00.001,Mary,1",
      "1970-01-01 00:00:00.002,Bob,2",
      "1970-01-01 00:00:00.002,Mike,3",
      "1970-01-01 00:00:02.001,Liz,4")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testProjectWithoutRowtime(): Unit = {

    val data = Seq(
      Row.of(new JInt(1), "Mary", new JLong(10L), new JLong(1)),
      Row.of(new JInt(2), "Bob", new JLong(20L), new JLong(2)),
      Row.of(new JInt(3), "Mike", new JLong(30L), new JLong(2)),
      Row.of(new JInt(4), "Liz", new JLong(40L), new JLong(2001)))

    val tableSchema = new TableSchema(
      Array("id", "rtime", "val", "ptime", "name"),
      Array(
        DataTypes.INT, DataTypes.TIMESTAMP, DataTypes.LONG, DataTypes.TIMESTAMP, DataTypes.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.STRING, Types.LONG, Types.LONG)
          .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "name", "val", "rtime"))

    tEnv.registerTableSource(
      "T",
      new TestProjectableTableSource(tableSchema, returnType, data, "rtime", "ptime"))

    val sink = new TestingAppendSink
    tEnv.scan("T")
        .filter('ptime.cast(DataTypes.LONG) > 0)
        .select('name, 'id)
        .addSink(sink)
    env.execute()

    val expected = Seq(
      "Mary,1",
      "Bob,2",
      "Mike,3",
      "Liz,4")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  def testProjectOnlyProctime(): Unit = {

    val data = Seq(
      Row.of(new JInt(1), new JLong(1), new JLong(10L), "Mary"),
      Row.of(new JInt(2), new JLong(2L), new JLong(20L), "Bob"),
      Row.of(new JInt(3), new JLong(2L), new JLong(30L), "Mike"),
      Row.of(new JInt(4), new JLong(2001L), new JLong(30L), "Liz"))

    val tableSchema = new TableSchema(
      Array("id", "rtime", "val", "ptime", "name"),
      Array(
        DataTypes.INT, DataTypes.TIMESTAMP, DataTypes.LONG, DataTypes.TIMESTAMP, DataTypes.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.LONG, Types.LONG, Types.STRING)
          .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "rtime", "val", "name"))

    tEnv.registerTableSource(
      "T",
      new TestProjectableTableSource(tableSchema, returnType, data, "rtime", "ptime"))

    val sink = new TestingAppendSink
    tEnv.scan("T")
        .select('ptime > 0)
        .select(1.count)
        .addSink(sink)
    env.execute()

    val expected = Seq("4")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  def testProjectOnlyRowtime(): Unit = {

    val data = Seq(
      Row.of(new JInt(1), new JLong(1), new JLong(10L), "Mary"),
      Row.of(new JInt(2), new JLong(2L), new JLong(20L), "Bob"),
      Row.of(new JInt(3), new JLong(2L), new JLong(30L), "Mike"),
      Row.of(new JInt(4), new JLong(2001L), new JLong(30L), "Liz"))

    val tableSchema = new TableSchema(
      Array("id", "rtime", "val", "ptime", "name"),
      Array(
        DataTypes.INT, DataTypes.TIMESTAMP, DataTypes.LONG, DataTypes.TIMESTAMP, DataTypes.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.LONG, Types.LONG, Types.STRING)
          .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "rtime", "val", "name"))

    tEnv.registerTableSource(
      "T",
      new TestProjectableTableSource(tableSchema, returnType, data, "rtime", "ptime"))

    val sink = new TestingAppendSink
    tEnv.scan("T")
        .select('rtime)
        .addSink(sink)
    env.execute()

    val expected = Seq(
      "1970-01-01 00:00:00.001",
      "1970-01-01 00:00:00.002",
      "1970-01-01 00:00:00.002",
      "1970-01-01 00:00:02.001")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testProjectWithMapping(): Unit = {

    val data = Seq(
      Row.of(new JLong(1), new JInt(1), "Mary", new JLong(10)),
      Row.of(new JLong(2), new JInt(2), "Bob", new JLong(20)),
      Row.of(new JLong(2), new JInt(3), "Mike", new JLong(30)),
      Row.of(new JLong(2001), new JInt(4), "Liz", new JLong(40)))

    val tableSchema = new TableSchema(
      Array("id", "rtime", "val", "ptime", "name"),
      Array(
        DataTypes.INT, DataTypes.TIMESTAMP, DataTypes.LONG, DataTypes.TIMESTAMP, DataTypes.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.LONG, Types.INT, Types.STRING, Types.LONG)
          .asInstanceOf[Array[TypeInformation[_]]],
      Array("p-rtime", "p-id", "p-name", "p-val"))
    val mapping = Map("rtime" -> "p-rtime", "id" -> "p-id", "val" -> "p-val", "name" -> "p-name")

    tEnv.registerTableSource(
      "T",
      new TestProjectableTableSource(tableSchema, returnType, data, "rtime", "ptime", mapping))

    val sink = new TestingAppendSink
    tEnv.scan("T")
        .select('name, 'rtime, 'val)
        .addSink(sink)
    env.execute()

    val expected = Seq(
      "Mary,1970-01-01 00:00:00.001,10",
      "Bob,1970-01-01 00:00:00.002,20",
      "Mike,1970-01-01 00:00:00.002,30",
      "Liz,1970-01-01 00:00:02.001,40")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testNestedProject(): Unit = {

    val data = Seq(
      Row.of(new JLong(1),
        Row.of(
          Row.of("Sarah", new JInt(100)),
          Row.of(new JInt(1000), new JBool(true))
        ),
        Row.of("Peter", new JInt(10000)),
        "Mary"),
      Row.of(new JLong(2),
        Row.of(
          Row.of("Rob", new JInt(200)),
          Row.of(new JInt(2000), new JBool(false))
        ),
        Row.of("Lucy", new JInt(20000)),
        "Bob"),
      Row.of(new JLong(3),
        Row.of(
          Row.of("Mike", new JInt(300)),
          Row.of(new JInt(3000), new JBool(true))
        ),
        Row.of("Betty", new JInt(30000)),
        "Liz"))

    val nested1 = new RowTypeInfo(
      Array(Types.STRING, Types.INT).asInstanceOf[Array[TypeInformation[_]]],
      Array("name", "value")
    )
    val nested2 = new RowTypeInfo(
      Array(Types.INT, Types.BOOLEAN).asInstanceOf[Array[TypeInformation[_]]],
      Array("num", "flag")
    )
    val deepNested = new RowTypeInfo(
      Array(nested1, nested2).asInstanceOf[Array[TypeInformation[_]]],
      Array("nested1", "nested2")
    )
    val tableSchema = new TableSchema(
      Array("id", "deepNested", "nested", "name"),
      Array(DataTypes.LONG, TypeConverters.createInternalTypeFromTypeInfo(deepNested),
        TypeConverters.createInternalTypeFromTypeInfo(nested1), DataTypes.STRING))

    val returnType = new RowTypeInfo(
      Array(Types.LONG, deepNested, nested1, Types.STRING).asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "deepNested", "nested", "name"))

    tEnv.registerTableSource(
      "T",
      new TestNestedProjectableTableSource(tableSchema, returnType, data))

    val sink = new TestingAppendSink
    tEnv
        .scan("T")
        .select('id,
          'deepNested.get("nested1").get("name") as 'nestedName,
          'nested.get("value") as 'nestedValue,
          'deepNested.get("nested2").get("flag") as 'nestedFlag,
          'deepNested.get("nested2").get("num") as 'nestedNum)
        .addSink(sink)
    env.execute()

    val expected = Seq(
      "1,Sarah,10000,true,1000",
      "2,Rob,20000,false,2000",
      "3,Mike,30000,true,3000")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testRowtimeTableSourcePreserveWatermarks(): Unit = {
    val tableName = "MyTable"

    // rows with timestamps and watermarks
    val data = Seq(
      Right(1L),
      Left(5L, Row.of(new JInt(1), new JLong(5), "A")),
      Left(2L, Row.of(new JInt(2), new JLong(1), "B")),
      Right(10L),
      Left(8L, Row.of(new JInt(6), new JLong(8), "C")),
      Right(20L),
      Left(21L, Row.of(new JInt(6), new JLong(21), "D")),
      Right(30L)
    )

    val fieldNames = Array("id", "rtime", "name")
    val schema = new TableSchema(fieldNames,
      Array(DataTypes.INT, DataTypes.TIMESTAMP, DataTypes.STRING))
    val rowType = new RowTypeInfo(
      Array(Types.INT, Types.LONG, Types.STRING).asInstanceOf[Array[TypeInformation[_]]],
      fieldNames)

    val tableSource = new TestPreserveWMTableSource(schema, rowType, data, "rtime")
    tEnv.registerTableSource(tableName, tableSource)

    val sink = new TestingAppendSink
    tEnv.scan(tableName)
        .where('rtime.cast(DataTypes.DOUBLE) > 0.003)
        .select('id, 'name)
        .toAppendStream[Row]
        // append current watermark to each row to verify that original watermarks were preserved
        .process(new ProcessFunction[Row, Row] {
      override def processElement(
          value: Row,
          ctx: ProcessFunction[Row, Row]#Context,
          out: Collector[Row]): Unit = {
        val res = new Row(3)
        res.setField(0, value.getField(0))
        res.setField(1, value.getField(1))
        res.setField(2, ctx.timerService().currentWatermark())
        out.collect(res)
      }
    }).addSink(sink)
    env.execute()

    val expected = Seq("1,A,1", "6,C,10", "6,D,20")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

}
