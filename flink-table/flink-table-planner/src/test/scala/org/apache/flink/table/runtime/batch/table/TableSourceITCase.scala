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

package org.apache.flink.table.runtime.batch.table

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, SqlTimeTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.{GenericTypeInfo, RowTypeInfo}
import org.apache.flink.api.java.{DataSet, ExecutionEnvironment => JExecEnv}
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.internal.TableEnvironmentInternal
import org.apache.flink.table.runtime.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.table.runtime.utils.{CommonTestData, TableProgramsCollectionTestBase}
import org.apache.flink.table.sources.BatchTableSource
import org.apache.flink.table.utils._
import org.apache.flink.test.util.TestBaseUtils
import org.apache.flink.types.Row

import org.apache.calcite.runtime.SqlFunctions.{internalToTimestamp => toTimestamp}
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import java.lang.{Boolean => JBool, Integer => JInt, Long => JLong}
import java.sql.{Date, Time, Timestamp}

import scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class TableSourceITCase(
    configMode: TableConfigMode)
  extends TableProgramsCollectionTestBase(configMode) {

  @Test(expected = classOf[TableException])
  def testInvalidDatastreamType(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)

    val tableSource = new BatchTableSource[Row]() {
      private val fieldNames: Array[String] = Array("name", "id", "value")
      private val fieldTypes: Array[TypeInformation[_]] = Array(Types.STRING, Types.LONG, Types.INT)
        .asInstanceOf[Array[TypeInformation[_]]]

      override def getDataSet(execEnv: JExecEnv): DataSet[Row] = {
        val data = List(Row.of("Mary", new JLong(1L), new JInt(1))).asJava
        // return DataSet[Row] with GenericTypeInfo
        execEnv.fromCollection(data, new GenericTypeInfo[Row](classOf[Row]))
      }
      override def getReturnType: TypeInformation[Row] = new RowTypeInfo(fieldTypes, fieldNames)
      override def getTableSchema: TableSchema = new TableSchema(fieldNames, fieldTypes)
    }
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSourceInternal("T", tableSource)

    tEnv.scan("T")
      .select('value, 'name)
      .collect()

    // test should fail because type info of returned DataSet does not match type return type info.
  }

  @Test
  def testBoundedTableSource(): Unit = {
    val tableName = "MyTable"
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)

    val data = Seq(
      Row.of("Mary", new JLong(1L), new JInt(10)),
      Row.of("Bob", new JLong(2L), new JInt(20)),
      Row.of("Mary", new JLong(2L), new JInt(30)),
      Row.of("Liz", new JLong(2001L), new JInt(40)))

    val fieldNames = Array("name", "rtime", "amount")
    val schema = new TableSchema(fieldNames, Array(Types.STRING, Types.LONG(), Types.INT))
    val rowType = new RowTypeInfo(
      Array(Types.STRING, Types.LONG, Types.INT).asInstanceOf[Array[TypeInformation[_]]],
      fieldNames)

    val tableSource = new TestInputFormatTableSource(schema, rowType, data)
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSourceInternal(tableName, tableSource)

    val results = tEnv.scan(tableName)
      .groupBy('name)
      .select('name, 'amount.sum)
      .collect()

    val expected = Seq(
      "Mary,40",
      "Bob,20",
      "Liz,40").mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testCsvTableSourceWithProjection(): Unit = {
    val csvTable = CommonTestData.getCsvTableSource

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSourceInternal("csvTable", csvTable)

    val results = tEnv
      .scan("csvTable")
      .where('score < 20)
      .select('last, 'id.floor(), 'score * 2)
      .collect()

    val expected = Seq(
      "Smith,1,24.6",
      "Miller,3,15.78",
      "Smith,4,0.24",
      "Miller,6,13.56",
      "Williams,8,4.68").mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testTableSourceWithFilterable(): Unit = {
    val tableName = "MyTable"
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = BatchTableEnvironment.create(env, config)
    tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSourceInternal(
      tableName, TestFilterableTableSource())
    val results = tableEnv
      .scan(tableName)
      .where($"amount" > 4 && $"price" < 9)
      .select($"id", $"name")
      .collect()

    val expected = Seq(
      "5,Record_5", "6,Record_6", "7,Record_7", "8,Record_8").mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testRowtimeRowTableSource(): Unit = {
    val tableName = "MyTable"
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)

    val data = Seq(
      Row.of("Mary", new JLong(1L), new JInt(10)),
      Row.of("Bob", new JLong(2L), new JInt(20)),
      Row.of("Mary", new JLong(2L), new JInt(30)),
      Row.of("Liz", new JLong(2001L), new JInt(40)))

    val fieldNames = Array("name", "rtime", "amount")
    val schema = new TableSchema(fieldNames, Array(Types.STRING, Types.SQL_TIMESTAMP, Types.INT))
    val rowType = new RowTypeInfo(
      Array(Types.STRING, Types.LONG, Types.INT).asInstanceOf[Array[TypeInformation[_]]],
      fieldNames)

    val tableSource = new TestTableSourceWithTime(schema, rowType, data, "rtime", null)
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSourceInternal(tableName, tableSource)

    val results = tEnv.scan(tableName)
      .window(Tumble over 1.second on 'rtime as 'w)
      .groupBy('name, 'w)
      .select('name, 'w.start, 'amount.sum)
      .collect()

    val expected = Seq(
      "Mary,1970-01-01 00:00:00.0,40",
      "Bob,1970-01-01 00:00:00.0,20",
      "Liz,1970-01-01 00:00:02.0,40").mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testProctimeRowTableSource(): Unit = {
    val tableName = "MyTable"
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)

    val data = Seq(
      Row.of("Mary", new JLong(1L), new JInt(10)),
      Row.of("Bob", new JLong(2L), new JInt(20)),
      Row.of("Mary", new JLong(2L), new JInt(30)),
      Row.of("Liz", new JLong(2001L), new JInt(40)))

    val fieldNames = Array("name", "rtime", "amount")
    val schema = new TableSchema(
      fieldNames :+ "ptime",
      Array(Types.STRING, Types.LONG, Types.INT, Types.SQL_TIMESTAMP))
    val rowType = new RowTypeInfo(
      Array(Types.STRING, Types.LONG, Types.INT).asInstanceOf[Array[TypeInformation[_]]],
      fieldNames)

    val tableSource = new TestTableSourceWithTime(schema, rowType, data, null, "ptime")
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSourceInternal(tableName, tableSource)

    val results = tEnv.scan(tableName)
      .where('ptime.cast(Types.LONG) > 0L)
      .select('name, 'amount)
      .collect()

    val expected = Seq(
      "Mary,10",
      "Bob,20",
      "Mary,30",
      "Liz,40").mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testRowtimeProctimeRowTableSource(): Unit = {
    val tableName = "MyTable"
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)

    val data = Seq(
      Row.of("Mary", new JLong(1L), new JInt(10)),
      Row.of("Bob", new JLong(2L), new JInt(20)),
      Row.of("Mary", new JLong(2L), new JInt(30)),
      Row.of("Liz", new JLong(2001L), new JInt(40)))

    val fieldNames = Array("name", "rtime", "amount")
    val schema = new TableSchema(
      fieldNames :+ "ptime",
      Array(Types.STRING, Types.SQL_TIMESTAMP, Types.INT, Types.SQL_TIMESTAMP))
    val rowType = new RowTypeInfo(
      Array(Types.STRING, Types.LONG, Types.INT).asInstanceOf[Array[TypeInformation[_]]],
      fieldNames)

    val tableSource = new TestTableSourceWithTime(schema, rowType, data, "rtime", "ptime")
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSourceInternal(tableName, tableSource)

    val results = tEnv.scan(tableName)
      .window(Tumble over 1.second on 'rtime as 'w)
      .groupBy('name, 'w)
      .select('name, 'w.start, 'amount.sum)
      .collect()

    val expected = Seq(
      "Mary,1970-01-01 00:00:00.0,40",
      "Bob,1970-01-01 00:00:00.0,20",
      "Liz,1970-01-01 00:00:02.0,40").mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testRowtimeAsTimestampRowTableSource(): Unit = {
    val tableName = "MyTable"
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)

    val data = Seq(
      Row.of("Mary", toTimestamp(1L), new JInt(10)),
      Row.of("Bob", toTimestamp(2L), new JInt(20)),
      Row.of("Mary", toTimestamp(2L), new JInt(30)),
      Row.of("Liz", toTimestamp(2001L), new JInt(40)))

    val fieldNames = Array("name", "rtime", "amount")
    val schema = new TableSchema(fieldNames, Array(Types.STRING, Types.SQL_TIMESTAMP, Types.INT))
    val rowType = new RowTypeInfo(
      Array(Types.STRING, Types.SQL_TIMESTAMP, Types.INT).asInstanceOf[Array[TypeInformation[_]]],
      fieldNames)

    val tableSource = new TestTableSourceWithTime(schema, rowType, data, "rtime", null)
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSourceInternal(tableName, tableSource)

    val results = tEnv.scan(tableName)
      .window(Tumble over 1.second on 'rtime as 'w)
      .groupBy('name, 'w)
      .select('name, 'w.start, 'amount.sum)
      .collect()

    val expected = Seq(
      "Mary,1970-01-01 00:00:00.0,40",
      "Bob,1970-01-01 00:00:00.0,20",
      "Liz,1970-01-01 00:00:02.0,40").mkString("\n")
  }

  @Test
  def testTableSourceWithFilterableDate(): Unit = {
    val tableName = "MyTable"
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = BatchTableEnvironment.create(env, config)

    val rowTypeInfo = new RowTypeInfo(
      Array[TypeInformation[_]](BasicTypeInfo.INT_TYPE_INFO, SqlTimeTypeInfo.DATE),
      Array("id", "date_val"))

    val rows = Seq(
      makeRow(23, Date.valueOf("2017-04-23")),
      makeRow(24, Date.valueOf("2017-04-24")),
      makeRow(25, Date.valueOf("2017-04-25")),
      makeRow(26, Date.valueOf("2017-04-26"))
    )

    val query =
      """
        |select id from MyTable
        |where date_val >= DATE '2017-04-24' and date_val < DATE '2017-04-26'
      """.stripMargin
    val tableSource = TestFilterableTableSource(rowTypeInfo, rows, Set("date_val"))
    tableEnv.asInstanceOf[TableEnvironmentInternal]
      .registerTableSourceInternal(tableName, tableSource)
    val results = tableEnv
      .sqlQuery(query)
      .collect()

    val expected = Seq(24, 25).mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testRowtimeLongTableSource(): Unit = {
    val tableName = "MyTable"
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)

    val data = Seq(new JLong(1L), new JLong(2L), new JLong(2L), new JLong(2001L), new JLong(4001L))

    val schema = new TableSchema(Array("rtime"), Array(Types.SQL_TIMESTAMP))
    val returnType = Types.LONG

    val tableSource = new TestTableSourceWithTime(schema, returnType, data, "rtime", null)
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSourceInternal(tableName, tableSource)

    val results = tEnv.scan(tableName)
      .window(Tumble over 1.second on 'rtime as 'w)
      .groupBy('w)
      .select('w.start, 'rtime.count)
      .collect()

    val expected = Seq(
      "1970-01-01 00:00:00.0,3",
      "1970-01-01 00:00:02.0,1",
      "1970-01-01 00:00:04.0,1").mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testProctimeStringTableSource(): Unit = {
    val tableName = "MyTable"
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)

    val data = Seq("Mary", "Peter", "Bob", "Liz")

    val schema = new TableSchema(Array("name", "ptime"), Array(Types.STRING, Types.SQL_TIMESTAMP))
    val returnType = Types.STRING

    val tableSource = new TestTableSourceWithTime(schema, returnType, data, null, "ptime")
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSourceInternal(tableName, tableSource)

    val results = tEnv.scan(tableName)
      .where('ptime.cast(Types.LONG) > 1)
      .select('name)
      .collect()

    val expected = Seq("Mary", "Peter", "Bob", "Liz").mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testRowtimeProctimeLongTableSource(): Unit = {
    val tableName = "MyTable"
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)

    val data = Seq(new JLong(1L), new JLong(2L), new JLong(2L), new JLong(2001L), new JLong(4001L))

    val schema = new TableSchema(
      Array("rtime", "ptime"),
      Array(Types.SQL_TIMESTAMP, Types.SQL_TIMESTAMP))
    val returnType = Types.LONG

    val tableSource = new TestTableSourceWithTime(schema, returnType, data, "rtime", "ptime")
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSourceInternal(tableName, tableSource)

    val results = tEnv.scan(tableName)
      .where('ptime.cast(Types.LONG) > 1)
      .window(Tumble over 1.second on 'rtime as 'w)
      .groupBy('w)
      .select('w.start, 'rtime.count)
      .collect()

    val expected = Seq(
      "1970-01-01 00:00:00.0,3",
      "1970-01-01 00:00:02.0,1",
      "1970-01-01 00:00:04.0,1").mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testFieldMappingTableSource(): Unit = {
    val tableName = "MyTable"
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)

    val data = Seq(
      Row.of("Mary", new JLong(1L), new JInt(10)),
      Row.of("Bob", new JLong(2L), new JInt(20)),
      Row.of("Mary", new JLong(2L), new JInt(30)),
      Row.of("Liz", new JLong(2001L), new JInt(40)))

    val schema = new TableSchema(
      Array("ptime", "amount", "name", "rtime"),
      Array(Types.SQL_TIMESTAMP, Types.INT, Types.STRING, Types.SQL_TIMESTAMP))
    val returnType = new RowTypeInfo(Types.STRING, Types.LONG, Types.INT)
    val mapping = Map("amount" -> "f2", "name" -> "f0", "rtime" -> "f1")

    val source = new TestTableSourceWithTime(schema, returnType, data, "rtime", "ptime", mapping)
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSourceInternal(tableName, source)

    val results = tEnv.scan(tableName)
      .window(Tumble over 1.second on 'rtime as 'w)
      .groupBy('name, 'w)
      .select('name, 'w.start, 'amount.sum)
      .collect()

    val expected = Seq(
      "Mary,1970-01-01 00:00:00.0,40",
      "Bob,1970-01-01 00:00:00.0,20",
      "Liz,1970-01-01 00:00:02.0,40").mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testProjectWithoutRowtimeProctime(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)

    val data = Seq(
      Row.of(new JInt(1), "Mary", new JLong(10L), new JLong(1)),
      Row.of(new JInt(2), "Bob", new JLong(20L), new JLong(2)),
      Row.of(new JInt(3), "Mike", new JLong(30L), new JLong(2)),
      Row.of(new JInt(4), "Liz", new JLong(40L), new JLong(2001)))

    val tableSchema = new TableSchema(
      Array("id", "rtime", "val", "ptime", "name"),
      Array(Types.INT, Types.SQL_TIMESTAMP, Types.LONG, Types.SQL_TIMESTAMP, Types.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.STRING, Types.LONG, Types.LONG)
        .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "name", "val", "rtime"))

    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSourceInternal(
      "T",
      new TestProjectableTableSource(tableSchema, returnType, data, "rtime", "ptime"))

    val results = tEnv.scan("T")
      .select('name, 'val, 'id)
      .collect()

    val expected = Seq(
      "Mary,10,1",
      "Bob,20,2",
      "Mike,30,3",
      "Liz,40,4").mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testProjectWithoutProctime(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)

    val data = Seq(
      Row.of(new JInt(1), "Mary", new JLong(10L), new JLong(1)),
      Row.of(new JInt(2), "Bob", new JLong(20L), new JLong(2)),
      Row.of(new JInt(3), "Mike", new JLong(30L), new JLong(2)),
      Row.of(new JInt(4), "Liz", new JLong(40L), new JLong(2001)))

    val tableSchema = new TableSchema(
      Array("id", "rtime", "val", "ptime", "name"),
      Array(Types.INT, Types.SQL_TIMESTAMP, Types.LONG, Types.SQL_TIMESTAMP, Types.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.STRING, Types.LONG, Types.LONG)
        .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "name", "val", "rtime"))

    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSourceInternal(
      "T",
      new TestProjectableTableSource(tableSchema, returnType, data, "rtime", "ptime"))

    val results = tEnv.scan("T")
      .select('rtime, 'name, 'id)
      .collect()

    val expected = Seq(
      "1970-01-01 00:00:00.001,Mary,1",
      "1970-01-01 00:00:00.002,Bob,2",
      "1970-01-01 00:00:00.002,Mike,3",
      "1970-01-01 00:00:02.001,Liz,4").mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testProjectWithoutRowtime(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)

    val data = Seq(
      Row.of(new JInt(1), "Mary", new JLong(10L), new JLong(1)),
      Row.of(new JInt(2), "Bob", new JLong(20L), new JLong(2)),
      Row.of(new JInt(3), "Mike", new JLong(30L), new JLong(2)),
      Row.of(new JInt(4), "Liz", new JLong(40L), new JLong(2001)))

    val tableSchema = new TableSchema(
      Array("id", "rtime", "val", "ptime", "name"),
      Array(Types.INT, Types.SQL_TIMESTAMP, Types.LONG, Types.SQL_TIMESTAMP, Types.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.STRING, Types.LONG, Types.LONG)
        .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "name", "val", "rtime"))

    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSourceInternal(
      "T",
      new TestProjectableTableSource(tableSchema, returnType, data, "rtime", "ptime"))

    val results = tEnv.scan("T")
      .filter('ptime.cast(Types.LONG) > 0)
      .select('name, 'id)
      .collect()

    val expected = Seq(
      "Mary,1",
      "Bob,2",
      "Mike,3",
      "Liz,4").mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testProjectOnlyProctime(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)

    val data = Seq(
      Row.of(new JInt(1), new JLong(1), new JLong(10L), "Mary"),
      Row.of(new JInt(2), new JLong(2L), new JLong(20L), "Bob"),
      Row.of(new JInt(3), new JLong(2L), new JLong(30L), "Mike"),
      Row.of(new JInt(4), new JLong(2001L), new JLong(30L), "Liz"))

    val tableSchema = new TableSchema(
      Array("id", "rtime", "val", "ptime", "name"),
      Array(Types.INT, Types.SQL_TIMESTAMP, Types.LONG, Types.SQL_TIMESTAMP, Types.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.LONG, Types.LONG, Types.STRING)
        .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "rtime", "val", "name"))

    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSourceInternal(
      "T",
      new TestProjectableTableSource(tableSchema, returnType, data, "rtime", "ptime"))

    val results = tEnv.scan("T")
      .select('ptime.cast(Types.LONG) > 0)
      .select(1.count)
      .collect()

    val expected = Seq("4").mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testProjectOnlyRowtime(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)

    val data = Seq(
      Row.of(new JInt(1), new JLong(1), new JLong(10L), "Mary"),
      Row.of(new JInt(2), new JLong(2L), new JLong(20L), "Bob"),
      Row.of(new JInt(3), new JLong(2L), new JLong(30L), "Mike"),
      Row.of(new JInt(4), new JLong(2001L), new JLong(30L), "Liz"))

    val tableSchema = new TableSchema(
      Array("id", "rtime", "val", "ptime", "name"),
      Array(Types.INT, Types.SQL_TIMESTAMP, Types.LONG, Types.SQL_TIMESTAMP, Types.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.LONG, Types.LONG, Types.STRING)
        .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "rtime", "val", "name"))

    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSourceInternal(
      "T",
      new TestProjectableTableSource(tableSchema, returnType, data, "rtime", "ptime"))

    val results = tEnv.scan("T")
      .select('rtime)
      .collect()

    val expected = Seq(
      "1970-01-01 00:00:00.001",
      "1970-01-01 00:00:00.002",
      "1970-01-01 00:00:00.002",
      "1970-01-01 00:00:02.001").mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testProjectWithMapping(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)

    val data = Seq(
      Row.of(new JLong(1), new JInt(1), "Mary", new JLong(10)),
      Row.of(new JLong(2), new JInt(2), "Bob", new JLong(20)),
      Row.of(new JLong(2), new JInt(3), "Mike", new JLong(30)),
      Row.of(new JLong(2001), new JInt(4), "Liz", new JLong(40)))

    val tableSchema = new TableSchema(
      Array("id", "rtime", "val", "ptime", "name"),
      Array(Types.INT, Types.SQL_TIMESTAMP, Types.LONG, Types.SQL_TIMESTAMP, Types.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.LONG, Types.INT, Types.STRING, Types.LONG)
        .asInstanceOf[Array[TypeInformation[_]]],
      Array("p-rtime", "p-id", "p-name", "p-val"))
    val mapping = Map("rtime" -> "p-rtime", "id" -> "p-id", "val" -> "p-val", "name" -> "p-name")

    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSourceInternal(
      "T",
      new TestProjectableTableSource(tableSchema, returnType, data, "rtime", "ptime", mapping))

    val results = tEnv.scan("T")
      .select('name, 'rtime, 'val)
      .collect()

    val expected = Seq(
      "Mary,1970-01-01 00:00:00.001,10",
      "Bob,1970-01-01 00:00:00.002,20",
      "Mike,1970-01-01 00:00:00.002,30",
      "Liz,1970-01-01 00:00:02.001,40").mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testNestedProject(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)

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
      Array(Types.LONG, deepNested, nested1, Types.STRING))

    val returnType = new RowTypeInfo(
      Array(Types.LONG, deepNested, nested1, Types.STRING).asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "deepNested", "nested", "name"))

    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSourceInternal(
      "T",
      new TestNestedProjectableTableSource(tableSchema, returnType, data))

    val results = tEnv
      .scan("T")
      .select('id,
        'deepNested.get("nested1").get("name") as 'nestedName,
        'nested.get("value") as 'nestedValue,
        'deepNested.get("nested2").get("flag") as 'nestedFlag,
        'deepNested.get("nested2").get("num") as 'nestedNum)
      .collect()

    val expected = Seq(
      "1,Sarah,10000,true,1000",
      "2,Rob,20000,false,2000",
      "3,Mike,30000,true,3000").mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testTableSourceWithFilterableTime(): Unit = {
    val tableName = "MyTable"
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = BatchTableEnvironment.create(env, config)

    val rowTypeInfo = new RowTypeInfo(
      Array[TypeInformation[_]](BasicTypeInfo.INT_TYPE_INFO, SqlTimeTypeInfo.TIME),
      Array("id", "time_val"))

    val rows = Seq(
      makeRow(1, Time.valueOf("7:23:19")),
      makeRow(2, Time.valueOf("11:45:00")),
      makeRow(3, Time.valueOf("11:45:01")),
      makeRow(4, Time.valueOf("12:14:23")),
      makeRow(5, Time.valueOf("13:33:12"))
    )

    val query =
      """
        |select id from MyTable
        |where time_val >= TIME '11:45:00' and time_val < TIME '12:14:23'
      """.stripMargin
    val tableSource = TestFilterableTableSource(rowTypeInfo, rows, Set("time_val"))
    tableEnv.asInstanceOf[TableEnvironmentInternal]
      .registerTableSourceInternal(tableName, tableSource)
    val results = tableEnv
      .sqlQuery(query)
      .collect()

    val expected = Seq(2, 3).mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testTableSourceWithFilterableTimestamp(): Unit = {
    val tableName = "MyTable"
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = BatchTableEnvironment.create(env, config)

    val rowTypeInfo = new RowTypeInfo(
      Array[TypeInformation[_]](BasicTypeInfo.INT_TYPE_INFO, SqlTimeTypeInfo.TIMESTAMP),
      Array("id", "ts"))

    val rows = Seq(
      makeRow(1, Timestamp.valueOf("2017-07-11 7:23:19")),
      makeRow(2, Timestamp.valueOf("2017-07-12 11:45:00")),
      makeRow(3, Timestamp.valueOf("2017-07-13 11:45:01")),
      makeRow(4, Timestamp.valueOf("2017-07-14 12:14:23")),
      makeRow(5, Timestamp.valueOf("2017-07-13 13:33:12"))
    )

    val query =
      """
        |select id from MyTable
        |where ts >= TIMESTAMP '2017-07-12 11:45:00' and ts < TIMESTAMP '2017-07-14 12:14:23'
      """.stripMargin
    val tableSource = TestFilterableTableSource(rowTypeInfo, rows, Set("ts"))
    tableEnv.asInstanceOf[TableEnvironmentInternal]
      .registerTableSourceInternal(tableName, tableSource)
    val results = tableEnv
      .sqlQuery(query)
      .collect()

    val expected = Seq(2, 3, 5).mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  private def makeRow(fields: Any*): Row = {
    val row = new Row(fields.length)
    val addField = (value: Any, pos: Int) => row.setField(pos, value)
    fields.zipWithIndex.foreach(addField.tupled)
    row
  }
}
