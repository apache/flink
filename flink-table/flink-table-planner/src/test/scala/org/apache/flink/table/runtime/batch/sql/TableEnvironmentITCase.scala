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

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.table.api.TableEnvironmentITCase
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.TableProgramsCollectionTestBase
import org.apache.flink.table.runtime.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.table.utils.MemoryTableSourceSinkUtil
import org.apache.flink.test.util.TestBaseUtils
import org.apache.flink.types.Row

import org.junit.Assert.{assertEquals, assertTrue, fail}
import org.junit._
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

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

    val ds = env.fromElements(((12, true), "Hello")).toTable(tEnv).as('a1, 'a2)
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

    val t = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv).as('a, 'b, 'c)
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

    val t = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv).as('a, 'b, 'c)
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

}
