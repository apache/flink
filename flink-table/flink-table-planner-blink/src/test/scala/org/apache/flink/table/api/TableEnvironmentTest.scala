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

package org.apache.flink.table.api

import org.apache.flink.api.common.typeinfo.Types.STRING
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.table.catalog.{GenericInMemoryCatalog, ObjectPath}
import org.apache.flink.table.planner.runtime.stream.sql.FunctionITCase.TestUDF
import org.apache.flink.table.planner.runtime.stream.table.FunctionITCase.SimpleScalarFunction
import org.apache.flink.table.planner.utils.{TableTestUtil, TestTableSourceSinks}
import org.apache.flink.table.sinks.CsvTableSink
import org.apache.flink.types.Row

import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.sql.SqlExplainLevel
import org.hamcrest.Matchers.containsString
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.junit.rules.ExpectedException
import org.junit.{Rule, Test}

import _root_.java.util

import _root_.scala.collection.JavaConverters._

class TableEnvironmentTest {

  // used for accurate exception information checking.
  val expectedException: ExpectedException = ExpectedException.none()

  @Rule
  def thrown: ExpectedException = expectedException

  val env = new StreamExecutionEnvironment(new LocalStreamEnvironment())
  val tableEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)

  @Test
  def testScanNonExistTable(): Unit = {
    thrown.expect(classOf[ValidationException])
    thrown.expectMessage("Table `MyTable` was not found")
    tableEnv.scan("MyTable")
  }

  @Test
  def testRegisterDataStream(): Unit = {
    val table = env.fromElements[(Int, Long, String, Boolean)]().toTable(tableEnv, 'a, 'b, 'c, 'd)
    tableEnv.registerTable("MyTable", table)
    val scanTable = tableEnv.scan("MyTable")
    val relNode = TableTestUtil.toRelNode(scanTable)
    val actual = RelOptUtil.toString(relNode)
    val expected = "LogicalTableScan(table=[[default_catalog, default_database, MyTable]])\n"
    assertEquals(expected, actual)

    // register on a conflict name
    thrown.expect(classOf[ValidationException])
    thrown.expectMessage(
      "Temporary table `default_catalog`.`default_database`.`MyTable` already exists")
    tableEnv.createTemporaryView("MyTable", env.fromElements[(Int, Long)]())
  }

  @Test
  def testSimpleQuery(): Unit = {
    val table = env.fromElements[(Int, Long, String, Boolean)]().toTable(tableEnv, 'a, 'b, 'c, 'd)
    tableEnv.registerTable("MyTable", table)
    val queryTable = tableEnv.sqlQuery("SELECT a, c, d FROM MyTable")
    val relNode = TableTestUtil.toRelNode(queryTable)
    val actual = RelOptUtil.toString(relNode, SqlExplainLevel.NO_ATTRIBUTES)
    val expected = "LogicalProject\n" +
      "  LogicalTableScan\n"
    assertEquals(expected, actual)
  }

  @Test
  def testStreamTableEnvironmentExplain(): Unit = {
    val execEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(execEnv, settings)

    tEnv.registerTableSource("MyTable", TestTableSourceSinks.getPersonCsvTableSource)
    tEnv.registerTableSink("MySink",
      new CsvTableSink("/tmp").configure(Array("first"), Array(STRING)))

    val table1 = tEnv.sqlQuery("select first from MyTable")
    tEnv.insertInto(table1, "MySink")

    val expected = TableTestUtil.readFromResource("/explain/testStreamTableEnvironmentExplain.out")
    val actual = tEnv.explain(false)
    assertEquals(TableTestUtil.replaceStageId(expected), TableTestUtil.replaceStageId(actual))
  }

  @Test
  def testExecuteSqlWithCreateAlterDropTable(): Unit = {
    val createTableStmt =
      """
        |CREATE TABLE tbl1 (
        |  a bigint,
        |  b int,
        |  c varchar
        |) with (
        |  'connector' = 'COLLECTION',
        |  'is-bounded' = 'false'
        |)
      """.stripMargin
    val tableResult1 = tableEnv.executeSql(createTableStmt)
    assertEquals(ResultKind.SUCCESS, tableResult1.getResultKind)
    assertTrue(tableEnv.getCatalog(tableEnv.getCurrentCatalog).get()
      .tableExists(ObjectPath.fromString(s"${tableEnv.getCurrentDatabase}.tbl1")))

    val tableResult2 = tableEnv.executeSql("ALTER TABLE tbl1 SET ('k1' = 'a', 'k2' = 'b')")
    assertEquals(ResultKind.SUCCESS, tableResult2.getResultKind)
    assertEquals(
      Map("connector" -> "COLLECTION", "is-bounded" -> "false", "k1" -> "a", "k2" -> "b").asJava,
      tableEnv.getCatalog(tableEnv.getCurrentCatalog).get()
        .getTable(ObjectPath.fromString(s"${tableEnv.getCurrentDatabase}.tbl1")).getProperties)

    val tableResult3 = tableEnv.executeSql("DROP TABLE tbl1")
    assertEquals(ResultKind.SUCCESS, tableResult3.getResultKind)
    assertFalse(tableEnv.getCatalog(tableEnv.getCurrentCatalog).get()
      .tableExists(ObjectPath.fromString(s"${tableEnv.getCurrentDatabase}.tbl1")))
  }

  @Test
  def testExecuteSqlWithCreateAlterDropDatabase(): Unit = {
    val tableResult1 = tableEnv.executeSql("CREATE DATABASE db1 COMMENT 'db1_comment'")
    assertEquals(ResultKind.SUCCESS, tableResult1.getResultKind)
    assertTrue(tableEnv.getCatalog(tableEnv.getCurrentCatalog).get().databaseExists("db1"))

    val tableResult2 = tableEnv.executeSql("ALTER DATABASE db1 SET ('k1' = 'a', 'k2' = 'b')")
    assertEquals(ResultKind.SUCCESS, tableResult2.getResultKind)
    assertEquals(
      Map("k1" -> "a", "k2" -> "b").asJava,
      tableEnv.getCatalog(tableEnv.getCurrentCatalog).get().getDatabase("db1").getProperties)

    val tableResult3 = tableEnv.executeSql("DROP DATABASE db1")
    assertEquals(ResultKind.SUCCESS, tableResult3.getResultKind)
    assertFalse(tableEnv.getCatalog(tableEnv.getCurrentCatalog).get().databaseExists("db1"))
  }

  @Test
  def testExecuteSqlWithCreateDropFunction(): Unit = {
    val funcName = classOf[TestUDF].getName
    val funcName2 = classOf[SimpleScalarFunction].getName

    val tableResult1 = tableEnv.executeSql(s"CREATE FUNCTION default_database.f1 AS '$funcName'")
    assertEquals(ResultKind.SUCCESS, tableResult1.getResultKind)
    assertTrue(tableEnv.getCatalog(tableEnv.getCurrentCatalog).get()
      .functionExists(ObjectPath.fromString("default_database.f1")))

    val tableResult2 = tableEnv.executeSql(s"ALTER FUNCTION default_database.f1 AS '$funcName2'")
    assertEquals(ResultKind.SUCCESS, tableResult2.getResultKind)
    assertTrue(tableEnv.getCatalog(tableEnv.getCurrentCatalog).get()
      .functionExists(ObjectPath.fromString("default_database.f1")))

    val tableResult3 = tableEnv.executeSql("DROP FUNCTION default_database.f1")
    assertEquals(ResultKind.SUCCESS, tableResult3.getResultKind)
    assertFalse(tableEnv.getCatalog(tableEnv.getCurrentCatalog).get()
      .functionExists(ObjectPath.fromString("default_database.f1")))

    val tableResult4 = tableEnv.executeSql(
      s"CREATE TEMPORARY SYSTEM FUNCTION default_database.f2 AS '$funcName'")
    assertEquals(ResultKind.SUCCESS, tableResult4.getResultKind)
    assertTrue(tableEnv.listUserDefinedFunctions().contains("f2"))

    val tableResult5 = tableEnv.executeSql("DROP TEMPORARY SYSTEM FUNCTION default_database.f2")
    assertEquals(ResultKind.SUCCESS, tableResult5.getResultKind)
    assertFalse(tableEnv.listUserDefinedFunctions().contains("f2"))
  }

  @Test
  def testExecuteSqlWithCreateUseCatalog(): Unit = {
    val tableResult1 = tableEnv.executeSql(
      "CREATE CATALOG my_catalog WITH('type'='generic_in_memory')")
    assertEquals(ResultKind.SUCCESS, tableResult1.getResultKind)
    assertTrue(tableEnv.getCatalog("my_catalog").isPresent)

    assertEquals("default_catalog", tableEnv.getCurrentCatalog)
    val tableResult2 = tableEnv.executeSql("USE CATALOG my_catalog")
    assertEquals(ResultKind.SUCCESS, tableResult2.getResultKind)
    assertEquals("my_catalog", tableEnv.getCurrentCatalog)
  }

  @Test
  def testExecuteSqlWithUseDatabase(): Unit = {
    val tableResult1 = tableEnv.executeSql("CREATE DATABASE db1 COMMENT 'db1_comment'")
    assertEquals(ResultKind.SUCCESS, tableResult1.getResultKind)
    assertTrue(tableEnv.getCatalog(tableEnv.getCurrentCatalog).get().databaseExists("db1"))

    assertEquals("default_database", tableEnv.getCurrentDatabase)
    val tableResult2 = tableEnv.executeSql("USE db1")
    assertEquals(ResultKind.SUCCESS, tableResult2.getResultKind)
    assertEquals("db1", tableEnv.getCurrentDatabase)
  }

  @Test
  def testExecuteSqlWithShowCatalogs(): Unit = {
    tableEnv.registerCatalog("my_catalog", new GenericInMemoryCatalog("my_catalog"))
    val tableResult = tableEnv.executeSql("SHOW CATALOGS")
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult.getResultKind)
    checkData(
      util.Arrays.asList(Row.of("default_catalog"), Row.of("my_catalog")).iterator(),
      tableResult.collect())
  }

  @Test
  def testExecuteSqlWithShowDatabases(): Unit = {
    val tableResult1 = tableEnv.executeSql("CREATE DATABASE db1 COMMENT 'db1_comment'")
    assertEquals(ResultKind.SUCCESS, tableResult1.getResultKind)
    val tableResult2 = tableEnv.executeSql("SHOW DATABASES")
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult2.getResultKind)
    checkData(
      util.Arrays.asList(Row.of("default_database"), Row.of("db1")).iterator(),
      tableResult2.collect())
  }

  @Test
  def testExecuteSqlWithShowTables(): Unit = {
    val createTableStmt =
      """
        |CREATE TABLE tbl1 (
        |  a bigint,
        |  b int,
        |  c varchar
        |) with (
        |  'connector' = 'COLLECTION',
        |  'is-bounded' = 'false'
        |)
      """.stripMargin
    val tableResult1 = tableEnv.executeSql(createTableStmt)
    assertEquals(ResultKind.SUCCESS, tableResult1.getResultKind)

    val tableResult2 = tableEnv.executeSql("SHOW TABLES")
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult2.getResultKind)
    checkData(
      util.Arrays.asList(Row.of("tbl1")).iterator(),
      tableResult2.collect())
  }

  @Test
  def testExecuteSqlWithShowFunctions(): Unit = {
    val tableResult = tableEnv.executeSql("SHOW FUNCTIONS")
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult.getResultKind)
    checkData(
      tableEnv.listFunctions().map(Row.of(_)).toList.asJava.iterator(),
      tableResult.collect())
  }

  @Test
  def testExecuteSqlWithUnsupportedStmt(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage(containsString("Unsupported SQL query!"))
    tableEnv.registerTableSource("MyTable", TestTableSourceSinks.getPersonCsvTableSource)
    // TODO supports select later
    tableEnv.executeSql("select * from MyTable")
  }

  private def checkData(expected: util.Iterator[Row], actual: util.Iterator[Row]): Unit = {
    while (expected.hasNext && actual.hasNext) {
      assertEquals(expected.next(), actual.next())
    }
    assertEquals(expected.hasNext, actual.hasNext)
  }

}
