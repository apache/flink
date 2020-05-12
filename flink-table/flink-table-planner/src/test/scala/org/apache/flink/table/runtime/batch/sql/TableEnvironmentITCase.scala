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

package org.apache.flink.table.runtime.batch.sql

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeinfo.Types.{INT, LONG, STRING}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{DataTypes, ResultKind, TableEnvironment, TableEnvironmentITCase, TableResult, TableSchema}
import org.apache.flink.table.runtime.utils.TableProgramsCollectionTestBase
import org.apache.flink.table.runtime.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.table.sinks.CsvTableSink
import org.apache.flink.table.utils.TableTestUtil.{readFromResource, replaceStageId}
import org.apache.flink.table.utils.{MemoryTableSourceSinkUtil, TestingOverwritableTableSink}
import org.apache.flink.test.util.TestBaseUtils
import org.apache.flink.types.Row
import org.apache.flink.util.FileUtils

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists

import org.junit.Assert.{assertEquals, assertFalse, assertTrue, fail}
import org.junit._
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import java.io.File
import java.lang.{Long => JLong}
import java.util

import scala.collection.JavaConverters._
import scala.io.Source

@RunWith(classOf[Parameterized])
class TableEnvironmentITCase(
    configMode: TableConfigMode)
  extends TableProgramsCollectionTestBase(configMode) {

  private val _tempFolder = new TemporaryFolder()

  @Rule
  def tempFolder: TemporaryFolder = _tempFolder

  @Test
  def testSQLTable(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val ds = CollectionDataSets.get3TupleDataSet(env)
    tEnv.createTemporaryView("MyTable", ds, 'a, 'b, 'c)

    val sqlQuery = "SELECT * FROM MyTable WHERE a > 9"

    val result = tEnv.sqlQuery(sqlQuery).select('a.avg, 'b.sum, 'c.count)

    val expected = "15,65,12"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testTableSQLTable(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val t1 = ds.filter('a > 9)

    tEnv.registerTable("MyTable", t1)

    val sqlQuery = "SELECT avg(a) as a1, sum(b) as b1, count(c) as c1 FROM MyTable"

    val result = tEnv.sqlQuery(sqlQuery).select('a1 + 1, 'b1 - 5, 'c1)

    val expected = "16,60,12"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testMultipleSQLQueries(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val t = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("MyTable", t)

    val sqlQuery = "SELECT a as aa FROM MyTable WHERE b = 6"
    val result1 = tEnv.sqlQuery(sqlQuery)
    tEnv.registerTable("ResTable", result1)

    val sqlQuery2 = "SELECT count(aa) FROM ResTable"
    val result2 = tEnv.sqlQuery(sqlQuery2)

    val expected = "6"
    val results = result2.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testSelectWithCompositeType(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val sqlQuery = "SELECT MyTable.a2, MyTable.a1._2 FROM MyTable"

    val ds = env.fromElements(((12, true), "Hello")).toTable(tEnv).as("a1", "a2")
    tEnv.registerTable("MyTable", ds)

    val result = tEnv.sqlQuery(sqlQuery)

    val expected = "Hello,true\n"

    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testInsertIntoMemoryTable(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)
    MemoryTableSourceSinkUtil.clear()

    val t = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv).as("a", "b", "c")
    tEnv.registerTable("sourceTable", t)

    val fieldNames = Array("d", "e", "f")
    val fieldTypes = tEnv.scan("sourceTable").getSchema.getFieldTypes
    val sink = new MemoryTableSourceSinkUtil.UnsafeMemoryAppendTableSink
    tEnv.registerTableSink("targetTable", sink.configure(fieldNames, fieldTypes))

    val sql = "INSERT INTO targetTable SELECT a, b, c FROM sourceTable"
    tEnv.sqlUpdate(sql)
    tEnv.execute("job name")

    val expected = List("1,1,Hi", "2,2,Hello", "3,2,Hello world")
    assertEquals(expected.sorted, MemoryTableSourceSinkUtil.tableDataStrings.sorted)
  }

  @Test
  def testSqlUpdateAndToDataSetWithDataSetSource(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)
    MemoryTableSourceSinkUtil.clear()

    val t = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv).as("a", "b", "c")
    tEnv.registerTable("sourceTable", t)

    val fieldNames = Array("d", "e", "f")
    val fieldTypes = tEnv.scan("sourceTable").getSchema.getFieldTypes
    val sink = new MemoryTableSourceSinkUtil.UnsafeMemoryAppendTableSink
    tEnv.registerTableSink("targetTable", sink.configure(fieldNames, fieldTypes))

    val sql = "INSERT INTO targetTable SELECT a, b, c FROM sourceTable"
    tEnv.sqlUpdate(sql)

    try {
      env.execute("job name")
      fail("Should not happen")
    } catch {
      case e: RuntimeException =>
        assertTrue(e.getMessage.contains("No data sinks have been created yet."))
      case  _ =>
        fail("Should not happen")
    }

    val result = tEnv.sqlQuery("SELECT c, b, a FROM sourceTable").select('a.avg, 'b.sum, 'c.count)

    val resultFile = _tempFolder.newFile().getAbsolutePath
    result.toDataSet[(Integer, Long, Long)]
      .writeAsCsv(resultFile, writeMode=FileSystem.WriteMode.OVERWRITE)

    tEnv.execute("job name")
    val expected1 = List("1,1,Hi", "2,2,Hello", "3,2,Hello world")
    assertEquals(expected1.sorted, MemoryTableSourceSinkUtil.tableDataStrings.sorted)
    // the DataSet has not been executed
    assertEquals("", Source.fromFile(resultFile).mkString)

    env.execute("job")
    val expected2 = "2,5,3\n"
    val actual = Source.fromFile(resultFile).mkString
    assertEquals(expected2, actual)
    // does not trigger the table program execution again
    assertEquals(expected1.sorted, MemoryTableSourceSinkUtil.tableDataStrings.sorted)
  }

  @Test
  def testSqlUpdateAndToDataSetWithTableSource(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)
    MemoryTableSourceSinkUtil.clear()
    tEnv.registerTableSource("sourceTable", TableEnvironmentITCase.getPersonCsvTableSource)

    val fieldNames = Array("d", "e", "f", "g")
    val fieldTypes = tEnv.scan("sourceTable").getSchema.getFieldTypes
    val sink = new MemoryTableSourceSinkUtil.UnsafeMemoryAppendTableSink
    tEnv.registerTableSink("targetTable", sink.configure(fieldNames, fieldTypes))

    val sql = "INSERT INTO targetTable SELECT * FROM sourceTable where id > 7"
    tEnv.sqlUpdate(sql)

    val result = tEnv.sqlQuery("SELECT id as a, score as b FROM sourceTable")
      .select('a.count, 'b.avg)

    val resultFile = _tempFolder.newFile().getAbsolutePath
    result.toDataSet[(Long, Double)]
      .writeAsCsv(resultFile, writeMode=FileSystem.WriteMode.OVERWRITE)

    tEnv.execute("job name")
    val expected1 = List("Kelly,8,2.34,Williams")
    assertEquals(expected1.sorted, MemoryTableSourceSinkUtil.tableDataStrings.sorted)
    // the DataSet has not been executed
    assertEquals("", Source.fromFile(resultFile).mkString)

    env.execute("job")
    val expected2 = "8,24.953750000000003\n"
    val actual = Source.fromFile(resultFile).mkString
    assertEquals(expected2, actual)
    // does not trigger the table program execution again
    assertEquals(expected1.sorted, MemoryTableSourceSinkUtil.tableDataStrings.sorted)
  }

  @Test
  def testToDataSetAndSqlUpdate(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)
    MemoryTableSourceSinkUtil.clear()

    val t = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv).as("a", "b", "c")
    tEnv.registerTable("sourceTable", t)

    val fieldNames = Array("d", "e", "f")
    val fieldTypes = tEnv.scan("sourceTable").getSchema.getFieldTypes
    val sink = new MemoryTableSourceSinkUtil.UnsafeMemoryAppendTableSink
    tEnv.registerTableSink("targetTable", sink.configure(fieldNames, fieldTypes))

    val result = tEnv.sqlQuery("SELECT c, b, a FROM sourceTable").select('a.avg, 'b.sum, 'c.count)
    val resultFile = _tempFolder.newFile().getAbsolutePath
    result.toDataSet[(Integer, Long, Long)]
      .writeAsCsv(resultFile, writeMode=FileSystem.WriteMode.OVERWRITE)

    val sql = "INSERT INTO targetTable SELECT a, b, c FROM sourceTable"
    tEnv.sqlUpdate(sql)

    env.execute("job")
    val expected1 = "2,5,3\n"
    val actual = Source.fromFile(resultFile).mkString
    assertEquals(expected1, actual)

    tEnv.execute("job name")
    val expected2 = List("1,1,Hi", "2,2,Hello", "3,2,Hello world")
    assertEquals(expected2.sorted, MemoryTableSourceSinkUtil.tableDataStrings.sorted)
    // does not trigger the DataSet program execution again
    assertEquals(expected1, Source.fromFile(resultFile).mkString)
  }

  @Test
  def testExecuteSqlWithInsertInto(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)
    MemoryTableSourceSinkUtil.clear()

    val t = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("sourceTable", t)

    val fieldNames = Array("d", "e", "f")
    val fieldTypes = tEnv.scan("sourceTable").getSchema.getFieldTypes
    val sink1 = new MemoryTableSourceSinkUtil.UnsafeMemoryAppendTableSink
    tEnv.registerTableSink("targetTable", sink1.configure(fieldNames, fieldTypes))

    val tableResult = tEnv.executeSql("INSERT INTO targetTable SELECT a, b, c FROM sourceTable")
    checkInsertTableResult(tableResult, "default_catalog.default_database.targetTable")
    // wait job finished
    tableResult.getJobClient.get()
      .getJobExecutionResult(Thread.currentThread().getContextClassLoader)
      .get()
    val expected1 = List("1,1,Hi", "2,2,Hello", "3,2,Hello world")
    assertEquals(expected1.sorted, MemoryTableSourceSinkUtil.tableDataStrings.sorted)
  }

  @Test
  def testExecuteSqlWithInsertOverwrite(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)

    val t = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("sourceTable", t)

    val resultFile = _tempFolder.newFile()
    val sinkPath = resultFile.getAbsolutePath
    val configuredSink = new TestingOverwritableTableSink(sinkPath)
      .configure(Array("d"),  Array(STRING))
    tEnv.registerTableSink("MySink", configuredSink)

    val tableResult1 = tEnv.executeSql("INSERT overwrite MySink SELECT c FROM sourceTable")
    checkInsertTableResult(tableResult1, "default_catalog.default_database.MySink")
    // wait job finished
    tableResult1.getJobClient.get()
      .getJobExecutionResult(Thread.currentThread().getContextClassLoader)
      .get()
    val expected1 = List("Hi", "Hello", "Hello world")
    val actual1 = FileUtils.readFileUtf8(resultFile).split("\n").toList
    assertEquals(expected1.sorted, actual1.sorted)

    val tableResult2 = tEnv.executeSql("INSERT overwrite MySink SELECT c FROM sourceTable")
    checkInsertTableResult(tableResult2, "default_catalog.default_database.MySink")
    // wait job finished
    tableResult2.getJobClient.get()
      .getJobExecutionResult(Thread.currentThread().getContextClassLoader)
      .get()
    val expected2 = List("Hi", "Hello", "Hello world")
    val actual2 = FileUtils.readFileUtf8(resultFile).split("\n").toList
    assertEquals(expected2.sorted, actual2.sorted)
  }

  @Test
  def testExecuteSqlAndSqlUpdate(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)
    MemoryTableSourceSinkUtil.clear()

    val t = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("sourceTable", t)

    val fieldNames = Array("d", "e", "f")
    val fieldTypes = tEnv.scan("sourceTable").getSchema.getFieldTypes
    val sink1 = new MemoryTableSourceSinkUtil.UnsafeMemoryAppendTableSink
    tEnv.registerTableSink("targetTable", sink1.configure(fieldNames, fieldTypes))

    val sink1Path = registerCsvTableSink(tEnv, fieldNames, fieldTypes, "MySink1")
    assertTrue(FileUtils.readFileUtf8(new File(sink1Path)).isEmpty)

    tEnv.sqlUpdate("INSERT INTO MySink1 SELECT * FROM sourceTable where a > 2")

    val tableResult = tEnv.executeSql("INSERT INTO targetTable SELECT a, b, c FROM sourceTable")
    checkInsertTableResult(tableResult, "default_catalog.default_database.targetTable")
    // wait job finished
    tableResult.getJobClient.get()
      .getJobExecutionResult(Thread.currentThread().getContextClassLoader)
      .get()
    val expected1 = List("1,1,Hi", "2,2,Hello", "3,2,Hello world")
    assertEquals(expected1.sorted, MemoryTableSourceSinkUtil.tableDataStrings.sorted)
    assertTrue(FileUtils.readFileUtf8(new File(sink1Path)).isEmpty)

    tEnv.execute("job name")
    val expected2 = List("3,2,Hello world")
    assertEquals(
      expected2.sorted,
      FileUtils.readFileUtf8(new File(sink1Path)).split("\n").toList.sorted)
    assertEquals(expected1.sorted, MemoryTableSourceSinkUtil.tableDataStrings.sorted)
  }

  @Test
  def testExecuteSqlAndToDataSet(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)
    MemoryTableSourceSinkUtil.clear()

    val t = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("sourceTable", t)

    val fieldNames = Array("d", "e", "f")
    val fieldTypes = tEnv.scan("sourceTable").getSchema.getFieldTypes
    val sink = new MemoryTableSourceSinkUtil.UnsafeMemoryAppendTableSink
    tEnv.registerTableSink("targetTable", sink.configure(fieldNames, fieldTypes))

    val result = tEnv.sqlQuery("SELECT c, b, a FROM sourceTable").select('a.avg, 'b.sum, 'c.count)
    val resultFile = _tempFolder.newFile().getAbsolutePath
    result.toDataSet[(Integer, Long, Long)]
      .writeAsCsv(resultFile, writeMode=FileSystem.WriteMode.OVERWRITE)

    val tableResult = tEnv.executeSql("INSERT INTO targetTable SELECT a, b, c FROM sourceTable")
    checkInsertTableResult(tableResult, "default_catalog.default_database.targetTable")
    // wait job finished
    tableResult.getJobClient.get()
      .getJobExecutionResult(Thread.currentThread().getContextClassLoader)
      .get()
    val expected1 = List("1,1,Hi", "2,2,Hello", "3,2,Hello world")
    assertEquals(expected1.sorted, MemoryTableSourceSinkUtil.tableDataStrings.sorted)
    // the DataSet has not been executed
    assertEquals("", Source.fromFile(resultFile).mkString)

    env.execute("job")
    val expected2 = "2,5,3\n"
    val actual = Source.fromFile(resultFile).mkString
    assertEquals(expected2, actual)
    // does not trigger the table program execution again
    assertEquals(expected1.sorted, MemoryTableSourceSinkUtil.tableDataStrings.sorted)
  }

  @Test
  def testExecuteInsert(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)
    MemoryTableSourceSinkUtil.clear()

    val t = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("sourceTable", t)

    val fieldNames = Array("d", "e", "f")
    val fieldTypes = tEnv.scan("sourceTable").getSchema.getFieldTypes
    val sink1 = new MemoryTableSourceSinkUtil.UnsafeMemoryAppendTableSink
    tEnv.registerTableSink("targetTable", sink1.configure(fieldNames, fieldTypes))

    val table = tEnv.sqlQuery("SELECT a, b, c FROM sourceTable")
    val tableResult = table.executeInsert("targetTable")
    checkInsertTableResult(tableResult, "default_catalog.default_database.targetTable")
    // wait job finished
    tableResult.getJobClient.get()
      .getJobExecutionResult(Thread.currentThread().getContextClassLoader)
      .get()
    val expected1 = List("1,1,Hi", "2,2,Hello", "3,2,Hello world")
    assertEquals(expected1.sorted, MemoryTableSourceSinkUtil.tableDataStrings.sorted)
  }

  @Test
  def testExecuteInsertOverwrite(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)

    val t = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("sourceTable", t)

    val resultFile = _tempFolder.newFile()
    val sinkPath = resultFile.getAbsolutePath
    val configuredSink = new TestingOverwritableTableSink(sinkPath)
      .configure(Array("d"),  Array(STRING))
    tEnv.registerTableSink("MySink", configuredSink)

    val tableResult1 = tEnv.sqlQuery("SELECT c FROM sourceTable").executeInsert("MySink", true)
    checkInsertTableResult(tableResult1, "default_catalog.default_database.MySink")
    // wait job finished
    tableResult1.getJobClient.get()
      .getJobExecutionResult(Thread.currentThread().getContextClassLoader)
      .get()
    val expected1 = List("Hi", "Hello", "Hello world")
    val actual1 = FileUtils.readFileUtf8(resultFile).split("\n").toList
    assertEquals(expected1.sorted, actual1.sorted)

    val tableResult2 = tEnv.sqlQuery("SELECT c FROM sourceTable").executeInsert("MySink", true)
    checkInsertTableResult(tableResult2,  "default_catalog.default_database.MySink")
    // wait job finished
    tableResult2.getJobClient.get()
      .getJobExecutionResult(Thread.currentThread().getContextClassLoader)
      .get()
    val expected2 = List("Hi", "Hello", "Hello world")
    val actual2 = FileUtils.readFileUtf8(resultFile).split("\n").toList
    assertEquals(expected2.sorted, actual2.sorted)
  }

  @Test
  def testStatementSet(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)
    MemoryTableSourceSinkUtil.clear()

    val t = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTable", t)

    val sink1Path = _tempFolder.newFile().getAbsolutePath
    val configuredSink1 = new TestingOverwritableTableSink(sink1Path)
      .configure(Array("d", "e", "f"),  Array(INT, LONG, STRING))
    tEnv.registerTableSink("MySink1", configuredSink1)
    assertTrue(FileUtils.readFileUtf8(new File(sink1Path)).isEmpty)

    val sink2Path = _tempFolder.newFile().getAbsolutePath
    val configuredSink2 = new TestingOverwritableTableSink(sink2Path)
      .configure(Array("i", "j", "k"),  Array(INT, LONG, STRING))
    tEnv.registerTableSink("MySink2", configuredSink2)
    assertTrue(FileUtils.readFileUtf8(new File(sink2Path)).isEmpty)

    val stmtSet = tEnv.createStatementSet()
    stmtSet.addInsert("MySink1", tEnv.sqlQuery("select * from MyTable where a > 2"), true)
      .addInsertSql("INSERT OVERWRITE MySink2 SELECT a, b, c FROM MyTable where a <= 2")

    val actual = stmtSet.explain()
    val expected = readFromResource("testStatementSet1.out")
    assertEquals(replaceStageId(expected), replaceTempVariables(replaceStageId(actual)))

    val tableResult = stmtSet.execute()
    // wait job finished
    tableResult.getJobClient.get()
      .getJobExecutionResult(Thread.currentThread().getContextClassLoader)
      .get()
    checkInsertTableResult(
      tableResult,
      "default_catalog.default_database.MySink1",
      "default_catalog.default_database.MySink2")
    val expected1 = List("3,2,Hello world")
    assertEquals(
      expected1.sorted,
      FileUtils.readFileUtf8(new File(sink1Path)).split("\n").toList.sorted)

    val expected2 = List("1,1,Hi", "2,2,Hello")
    assertEquals(
      expected2.sorted,
      FileUtils.readFileUtf8(new File(sink2Path)).split("\n").toList.sorted)
  }

  @Test
  def testExecuteSelect(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)

    val t = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTable", t)

    val tableResult = tEnv.executeSql("select a, c from MyTable where b = 2")
    assertTrue(tableResult.getJobClient.isPresent)
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult.getResultKind)
    assertEquals(
      TableSchema.builder()
        .field("a", DataTypes.INT())
        .field("c", DataTypes.STRING())
        .build(),
      tableResult.getTableSchema)
    val expected = util.Arrays.asList(
      Row.of(Integer.valueOf(2), "Hello"),
      Row.of(Integer.valueOf(3), "Hello world"))
    val actual = Lists.newArrayList(tableResult.collect())
    actual.sort(new util.Comparator[Row]() {
      override def compare(o1: Row, o2: Row): Int = {
        o1.getField(0).asInstanceOf[Int].compareTo(o2.getField(0).asInstanceOf[Int])
      }
    })
    assertEquals(expected, actual)
  }

  private def registerCsvTableSink(
      tEnv: TableEnvironment,
      fieldNames: Array[String],
      fieldTypes: Array[TypeInformation[_]],
      tableName: String): String = {
    val resultFile = _tempFolder.newFile()
    val path = resultFile.getAbsolutePath

    val configuredSink = new CsvTableSink(path, ",", 1, WriteMode.OVERWRITE)
      .configure(fieldNames, fieldTypes)
    tEnv.registerTableSink(tableName, configuredSink)

    path
  }

  private def checkInsertTableResult(tableResult: TableResult, fieldNames: String*): Unit = {
    assertTrue(tableResult.getJobClient.isPresent)
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult.getResultKind)
    assertEquals(
      util.Arrays.asList(fieldNames: _*),
      util.Arrays.asList(tableResult.getTableSchema.getFieldNames: _*))
    val it = tableResult.collect()
    assertTrue(it.hasNext)
    val affectedRowCounts = fieldNames.map(_ => JLong.valueOf(-1L))
    assertEquals(Row.of(affectedRowCounts: _*), it.next())
    assertFalse(it.hasNext)
  }

  private def replaceTempVariables(s: String): String = {
    s.replaceAll("content : TextOutputFormat \\(.*\\)", "content : TextOutputFormat ()")
      .replaceAll("DataSetScan\\(ref=\\[\\d+\\]", "DataSetScan(ref=[]")
  }
}
