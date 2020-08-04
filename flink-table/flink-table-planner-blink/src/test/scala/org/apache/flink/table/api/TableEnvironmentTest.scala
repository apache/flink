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
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, _}
import org.apache.flink.table.catalog.{GenericInMemoryCatalog, ObjectPath}
import org.apache.flink.table.planner.operations.SqlConversionException
import org.apache.flink.table.planner.runtime.stream.sql.FunctionITCase.TestUDF
import org.apache.flink.table.planner.runtime.stream.table.FunctionITCase.SimpleScalarFunction
import org.apache.flink.table.planner.utils.TableTestUtil.replaceStageId
import org.apache.flink.table.planner.utils.{TableTestUtil, TestTableSourceSinks}
import org.apache.flink.types.Row

import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.sql.SqlExplainLevel
import org.junit.Assert.{assertEquals, assertFalse, assertTrue, fail}
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
    tableEnv.from("MyTable")
  }

  @Test
  def testRegisterDataStream(): Unit = {
    val table = env.fromElements[(Int, Long, String, Boolean)]().toTable(tableEnv, 'a, 'b, 'c, 'd)
    tableEnv.registerTable("MyTable", table)
    val scanTable = tableEnv.from("MyTable")
    val relNode = TableTestUtil.toRelNode(scanTable)
    val actual = RelOptUtil.toString(relNode)
    val expected = "LogicalTableScan(table=[[default_catalog, default_database, MyTable]])\n"
    assertEquals(expected, actual)

    // register on a conflict name
    thrown.expect(classOf[ValidationException])
    thrown.expectMessage(
      "Temporary table '`default_catalog`.`default_database`.`MyTable`' already exists")
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
    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(execEnv, settings)

    TestTableSourceSinks.createPersonCsvTemporaryTable(tEnv, "MyTable")

    TestTableSourceSinks.createCsvTemporarySinkTable(
      tEnv, new TableSchema(Array("first"), Array(STRING)), "MySink", -1)

    val expected = TableTestUtil.readFromResource("/explain/testStreamTableEnvironmentExplain.out")
    val actual = tEnv.explainSql("insert into MySink select first from MyTable")
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
  def testExecuteSqlWithCreateDropTableIfNotExists(): Unit = {
    val createTableStmt =
      """
        |CREATE TABLE IF NOT EXISTS tbl1 (
        |  a bigint,
        |  b int,
        |  c varchar
        |) with (
        |  'connector' = 'COLLECTION',
        |  'is-bounded' = 'false'
        |)
      """.stripMargin
    // test crate table twice
    val tableResult1 = tableEnv.executeSql(createTableStmt)
    assertEquals(ResultKind.SUCCESS, tableResult1.getResultKind)
    val tableResult2 = tableEnv.executeSql(createTableStmt)
    assertEquals(ResultKind.SUCCESS, tableResult2.getResultKind)
    assertTrue(tableEnv.getCatalog(tableEnv.getCurrentCatalog).get()
      .tableExists(ObjectPath.fromString(s"${tableEnv.getCurrentDatabase}.tbl1")))

    val tableResult3 = tableEnv.executeSql("DROP TABLE IF EXISTS tbl1")
    assertEquals(ResultKind.SUCCESS, tableResult3.getResultKind)
    assertFalse(tableEnv.getCatalog(tableEnv.getCurrentCatalog).get()
      .tableExists(ObjectPath.fromString(s"${tableEnv.getCurrentDatabase}.tbl1")))
  }

  @Test
  def testExecuteSqlWithCreateDropTemporaryTableIfNotExists(): Unit = {
    val createTableStmt =
      """
        |CREATE TEMPORARY TABLE IF NOT EXISTS tbl1 (
        |  a bigint,
        |  b int,
        |  c varchar
        |) with (
        |  'connector' = 'COLLECTION',
        |  'is-bounded' = 'false'
        |)
      """.stripMargin
    // test crate table twice
    val tableResult1 = tableEnv.executeSql(createTableStmt)
    assertEquals(ResultKind.SUCCESS, tableResult1.getResultKind)
    val tableResult2 = tableEnv.executeSql(createTableStmt)
    assertEquals(ResultKind.SUCCESS, tableResult2.getResultKind)
    assertTrue(tableEnv.listTables().contains("tbl1"))

    val tableResult3 = tableEnv.executeSql("DROP TEMPORARY TABLE IF EXISTS tbl1")
    assertEquals(ResultKind.SUCCESS, tableResult3.getResultKind)
    assertFalse(tableEnv.listTables().contains("tbl1"))
  }

  @Test
  def testExecuteSqlWithCreateDropTemporaryTable(): Unit = {
    val createTableStmt =
      """
        |CREATE TEMPORARY TABLE tbl1 (
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
    assert(tableEnv.listTables().sameElements(Array[String]("tbl1")))

    val tableResult2 = tableEnv.executeSql("DROP TEMPORARY TABLE tbl1")
    assertEquals(ResultKind.SUCCESS, tableResult2.getResultKind)
    assert(tableEnv.listTables().sameElements(Array.empty[String]))
  }

  @Test
  def testExecuteSqlWithDropTemporaryTableIfExists(): Unit = {
    val createTableStmt =
      """
        |CREATE TEMPORARY TABLE tbl1 (
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
    assert(tableEnv.listTables().sameElements(Array[String]("tbl1")))

    val tableResult2 = tableEnv.executeSql("DROP TEMPORARY TABLE IF EXISTS tbl1")
    assertEquals(ResultKind.SUCCESS, tableResult2.getResultKind)
    assert(tableEnv.listTables().sameElements(Array.empty[String]))

    val tableResult3 = tableEnv.executeSql("DROP TEMPORARY TABLE IF EXISTS tbl1")
    assertEquals(ResultKind.SUCCESS, tableResult3.getResultKind)
    assert(tableEnv.listTables().sameElements(Array.empty[String]))
  }

  @Test(expected = classOf[ValidationException])
  def testExecuteSqlWithDropTemporaryTableTwice(): Unit = {
    val createTableStmt =
      """
        |CREATE TEMPORARY TABLE tbl1 (
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
    assert(tableEnv.listTables().sameElements(Array[String]("tbl1")))

    val tableResult2 = tableEnv.executeSql("DROP TEMPORARY TABLE tbl1")
    assertEquals(ResultKind.SUCCESS, tableResult2.getResultKind)
    assert(tableEnv.listTables().sameElements(Array.empty[String]))

    // fail the case
    tableEnv.executeSql("DROP TEMPORARY TABLE tbl1")
  }

  @Test
  def testDropTemporaryTableWithFullPath(): Unit = {
    val createTableStmt =
      """
        |CREATE TEMPORARY TABLE tbl1 (
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
    assert(tableEnv.listTables().sameElements(Array[String]("tbl1")))

    val tableResult2 = tableEnv.executeSql(
      "DROP TEMPORARY TABLE default_catalog.default_database.tbl1")
    assertEquals(ResultKind.SUCCESS, tableResult2.getResultKind)
    assert(tableEnv.listTables().sameElements(Array.empty[String]))
  }

  @Test(expected = classOf[ValidationException])
  def testDropTemporaryTableWithInvalidPath(): Unit = {
    val createTableStmt =
      """
        |CREATE TEMPORARY TABLE tbl1 (
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
    assert(tableEnv.listTables().sameElements(Array[String]("tbl1")))

    // fail the case
    tableEnv.executeSql(
      "DROP TEMPORARY TABLE invalid_catalog.invalid_database.tbl1")
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
      s"CREATE TEMPORARY SYSTEM FUNCTION f2 AS '$funcName'")
    assertEquals(ResultKind.SUCCESS, tableResult4.getResultKind)
    assertTrue(tableEnv.listUserDefinedFunctions().contains("f2"))

    val tableResult5 = tableEnv.executeSql("DROP TEMPORARY SYSTEM FUNCTION f2")
    assertEquals(ResultKind.SUCCESS, tableResult5.getResultKind)
    assertFalse(tableEnv.listUserDefinedFunctions().contains("f2"))
  }

  @Test
  def testExecuteSqlWithCreateUseDropCatalog(): Unit = {
    val tableResult1 = tableEnv.executeSql(
      "CREATE CATALOG my_catalog WITH('type'='generic_in_memory')")
    assertEquals(ResultKind.SUCCESS, tableResult1.getResultKind)
    assertTrue(tableEnv.getCatalog("my_catalog").isPresent)

    assertEquals("default_catalog", tableEnv.getCurrentCatalog)
    val tableResult2 = tableEnv.executeSql("USE CATALOG my_catalog")
    assertEquals(ResultKind.SUCCESS, tableResult2.getResultKind)
    assertEquals("my_catalog", tableEnv.getCurrentCatalog)

    val tableResult3 = tableEnv.executeSql("DROP CATALOG my_catalog")
    assertEquals(ResultKind.SUCCESS, tableResult3.getResultKind)
    assertFalse(tableEnv.getCatalog("my_catalog").isPresent)
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
    assertEquals(
      TableSchema.builder().field("catalog name", DataTypes.STRING()).build(),
      tableResult.getTableSchema)
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
    assertEquals(
      TableSchema.builder().field("database name", DataTypes.STRING()).build(),
      tableResult2.getTableSchema)
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
    assertEquals(
      TableSchema.builder().field("table name", DataTypes.STRING()).build(),
      tableResult2.getTableSchema)
    checkData(
      util.Arrays.asList(Row.of("tbl1")).iterator(),
      tableResult2.collect())
  }

  @Test
  def testExecuteSqlWithShowFunctions(): Unit = {
    val tableResult = tableEnv.executeSql("SHOW FUNCTIONS")
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult.getResultKind)
    assertEquals(
      TableSchema.builder().field("function name", DataTypes.STRING()).build(),
      tableResult.getTableSchema)
    checkData(
      tableEnv.listFunctions().map(Row.of(_)).toList.asJava.iterator(),
      tableResult.collect())
  }

  @Test
  def testExecuteSqlWithCreateDropView(): Unit = {
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
    tableEnv.executeSql(createTableStmt)

    val viewResult1 = tableEnv.executeSql("CREATE VIEW IF NOT EXISTS v1 AS SELECT * FROM tbl1")
    assertEquals(ResultKind.SUCCESS, viewResult1.getResultKind)
    assertTrue(tableEnv.getCatalog(tableEnv.getCurrentCatalog).get()
      .tableExists(ObjectPath.fromString(s"${tableEnv.getCurrentDatabase}.v1")))

    val viewResult2 = tableEnv.executeSql("DROP VIEW IF EXISTS v1")
    assertEquals(ResultKind.SUCCESS, viewResult2.getResultKind)
    assertFalse(tableEnv.getCatalog(tableEnv.getCurrentCatalog).get()
      .tableExists(ObjectPath.fromString(s"${tableEnv.getCurrentDatabase}.v1")))
  }

  @Test
  def testExecuteSqlWithCreateDropTemporaryView(): Unit = {
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
    tableEnv.executeSql(createTableStmt)

    val viewResult1 = tableEnv.executeSql(
      "CREATE TEMPORARY VIEW IF NOT EXISTS v1 AS SELECT * FROM tbl1")
    assertEquals(ResultKind.SUCCESS, viewResult1.getResultKind)
    assert(tableEnv.listTables().sameElements(Array[String]("tbl1", "v1")))

    val viewResult2 = tableEnv.executeSql("DROP TEMPORARY VIEW IF EXISTS v1")
    assertEquals(ResultKind.SUCCESS, viewResult2.getResultKind)
    assert(tableEnv.listTables().sameElements(Array[String]("tbl1")))
  }

  @Test
  def testCreateViewWithWrongFieldList(): Unit = {
    thrown.expect(classOf[SqlConversionException])
    thrown.expectMessage("VIEW definition and input fields not match:\n" +
      "\tDef fields: [d].\n" +
      "\tInput fields: [a, b, c].")
    val sourceDDL =
      """
        |CREATE TABLE T1(
        |  a int,
        |  b varchar,
        |  c int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    val sinkDDL =
      """
        |CREATE TABLE T2(
        |  a int,
        |  b varchar,
        |  c int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    val viewDDL =
      """
        |CREATE VIEW IF NOT EXISTS T3(d) AS SELECT * FROM T1
      """.stripMargin

    tableEnv.executeSql(sourceDDL)
    tableEnv.executeSql(sinkDDL)
    tableEnv.executeSql(viewDDL)
  }

  @Test
  def testCreateViewTwice(): Unit = {
    thrown.expect(classOf[ValidationException])
    thrown.expectMessage(
      "Could not execute CreateTable in path `default_catalog`.`default_database`.`T3`")
    val sourceDDL =
      """
        |CREATE TABLE T1(
        |  a int,
        |  b varchar,
        |  c int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    val sinkDDL =
      """
        |CREATE TABLE T2(
        |  a int,
        |  b varchar,
        |  c int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    val viewWith3ColumnDDL =
      """
        |CREATE VIEW T3(d, e, f) AS SELECT a, b, c FROM T1
      """.stripMargin

    val viewWith2ColumnDDL =
      """
        |CREATE VIEW T3(d, e) AS SELECT a, b FROM T1
      """.stripMargin

    tableEnv.executeSql(sourceDDL)
    tableEnv.executeSql(sinkDDL)
    tableEnv.executeSql(viewWith3ColumnDDL)
    tableEnv.executeSql(viewWith2ColumnDDL) // fail the case
  }

  @Test
  def testDropViewWithFullPath(): Unit = {
    val sourceDDL =
      """
        |CREATE TABLE T1(
        |  a int,
        |  b varchar,
        |  c int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    val view1DDL =
      """
        |CREATE VIEW T2(d, e, f) AS SELECT a, b, c FROM T1
      """.stripMargin

    val view2DDL =
      """
        |CREATE VIEW T3(x, y, z) AS SELECT a, b, c FROM T1
      """.stripMargin

    tableEnv.executeSql(sourceDDL)
    tableEnv.executeSql(view1DDL)
    tableEnv.executeSql(view2DDL)

    assert(tableEnv.listTables().sameElements(Array[String]("T1", "T2", "T3")))

    tableEnv.executeSql("DROP VIEW default_catalog.default_database.T2")
    assert(tableEnv.listTables().sameElements(Array[String]("T1", "T3")))

    tableEnv.executeSql("DROP VIEW default_catalog.default_database.T3")
    assert(tableEnv.listTables().sameElements(Array[String]("T1")))
  }

  @Test
  def testDropViewWithPartialPath(): Unit = {
    val sourceDDL =
      """
        |CREATE TABLE T1(
        |  a int,
        |  b varchar,
        |  c int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    val view1DDL =
      """
        |CREATE VIEW T2(d, e, f) AS SELECT a, b, c FROM T1
      """.stripMargin

    val view2DDL =
      """
        |CREATE VIEW T3(x, y, z) AS SELECT a, b, c FROM T1
      """.stripMargin

    tableEnv.executeSql(sourceDDL)
    tableEnv.executeSql(view1DDL)
    tableEnv.executeSql(view2DDL)

    assert(tableEnv.listTables().sameElements(Array[String]("T1", "T2", "T3")))

    tableEnv.executeSql("DROP VIEW T2")
    assert(tableEnv.listTables().sameElements(Array[String]("T1", "T3")))

    tableEnv.executeSql("DROP VIEW default_database.T3")
    assert(tableEnv.listTables().sameElements(Array[String]("T1")))
  }

  @Test
  def testDropViewIfExistsTwice(): Unit = {
    val sourceDDL =
      """
        |CREATE TABLE T1(
        |  a int,
        |  b varchar,
        |  c int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    val viewDDL =
      """
        |CREATE VIEW T2(d, e, f) AS SELECT a, b, c FROM T1
      """.stripMargin


    tableEnv.executeSql(sourceDDL)
    tableEnv.executeSql(viewDDL)

    assert(tableEnv.listTables().sameElements(Array[String]("T1", "T2")))

    tableEnv.executeSql("DROP VIEW IF EXISTS default_catalog.default_database.T2")
    assert(tableEnv.listTables().sameElements(Array[String]("T1")))

    tableEnv.executeSql("DROP VIEW IF EXISTS default_catalog.default_database.T2")
    assert(tableEnv.listTables().sameElements(Array[String]("T1")))
  }

  @Test(expected = classOf[ValidationException])
  def testDropViewTwice(): Unit = {
    val sourceDDL =
      """
        |CREATE TABLE T1(
        |  a int,
        |  b varchar,
        |  c int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    val viewDDL =
      """
        |CREATE VIEW T2(d, e, f) AS SELECT a, b, c FROM T1
      """.stripMargin

    tableEnv.executeSql(sourceDDL)
    tableEnv.executeSql(viewDDL)

    assert(tableEnv.listTables().sameElements(Array[String]("T1", "T2")))

    tableEnv.executeSql("DROP VIEW default_catalog.default_database.T2")
    assert(tableEnv.listTables().sameElements(Array[String]("T1")))

    tableEnv.executeSql("DROP VIEW default_catalog.default_database.T2")
  }

  @Test(expected = classOf[ValidationException])
  def testDropViewWithInvalidPath(): Unit = {
    val sourceDDL =
      """
        |CREATE TABLE T1(
        |  a int,
        |  b varchar,
        |  c int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    val viewDDL =
      """
        |CREATE VIEW T2(d, e, f) AS SELECT a, b, c FROM T1
      """.stripMargin
    tableEnv.executeSql(sourceDDL)
    tableEnv.executeSql(viewDDL)
    assert(tableEnv.listTables().sameElements(Array[String]("T1", "T2")))
    // failed since 'default_catalog1.default_database1.T2' is invalid path
    tableEnv.executeSql("DROP VIEW default_catalog1.default_database1.T2")
  }

  @Test
  def testDropViewWithInvalidPathIfExists(): Unit = {
    val sourceDDL =
      """
        |CREATE TABLE T1(
        |  a int,
        |  b varchar,
        |  c int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    val viewDDL =
      """
        |CREATE VIEW T2(d, e, f) AS SELECT a, b, c FROM T1
      """.stripMargin
    tableEnv.executeSql(sourceDDL)
    tableEnv.executeSql(viewDDL)
    assert(tableEnv.listTables().sameElements(Array[String]("T1", "T2")))
    tableEnv.executeSql("DROP VIEW IF EXISTS default_catalog1.default_database1.T2")
    assert(tableEnv.listTables().sameElements(Array[String]("T1", "T2")))
  }

  @Test
  def testDropTemporaryViewIfExistsTwice(): Unit = {
    val sourceDDL =
      """
        |CREATE TABLE T1(
        |  a int,
        |  b varchar,
        |  c int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    val viewDDL =
      """
        |CREATE TEMPORARY VIEW T2(d, e, f) AS SELECT a, b, c FROM T1
      """.stripMargin

    tableEnv.executeSql(sourceDDL)
    tableEnv.executeSql(viewDDL)

    assert(tableEnv.listTemporaryViews().sameElements(Array[String]("T2")))

    tableEnv.executeSql("DROP TEMPORARY VIEW IF EXISTS default_catalog.default_database.T2")
    assert(tableEnv.listTemporaryViews().sameElements(Array[String]()))

    tableEnv.executeSql("DROP TEMPORARY VIEW IF EXISTS default_catalog.default_database.T2")
    assert(tableEnv.listTemporaryViews().sameElements(Array[String]()))
  }

  @Test(expected = classOf[ValidationException])
  def testDropTemporaryViewTwice(): Unit = {
    val sourceDDL =
      """
        |CREATE TABLE T1(
        |  a int not null,
        |  b varchar,
        |  c int,
        |  ts AS to_timestamp(b),
        |  WATERMARK FOR ts AS ts - INTERVAL '1' SECOND
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    val viewDDL =
      """
        |CREATE TEMPORARY VIEW T2(d, e, f) AS SELECT a, b, c FROM T1
      """.stripMargin

    tableEnv.executeSql(sourceDDL)
    tableEnv.executeSql(viewDDL)

    assert(tableEnv.listTemporaryViews().sameElements(Array[String]("T2")))

    tableEnv.executeSql("DROP TEMPORARY VIEW default_catalog.default_database.T2")
    assert(tableEnv.listTemporaryViews().sameElements(Array[String]()))

    // throws ValidationException since default_catalog.default_database.T2 is not exists
    tableEnv.executeSql("DROP TEMPORARY VIEW default_catalog.default_database.T2")
  }

  @Test
  def testExecuteSqlWithShowViews(): Unit = {
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

    val tableResult2 = tableEnv.executeSql("CREATE VIEW view1 AS SELECT * FROM tbl1")
    assertEquals(ResultKind.SUCCESS, tableResult2.getResultKind)

    val tableResult3 = tableEnv.executeSql("SHOW VIEWS")
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult3.getResultKind)
    assertEquals(
      TableSchema.builder().field("view name", DataTypes.STRING()).build(),
      tableResult3.getTableSchema)
    checkData(
      util.Arrays.asList(Row.of("view1")).iterator(),
      tableResult3.collect())

    val tableResult4 = tableEnv.executeSql("CREATE TEMPORARY VIEW view2 AS SELECT * FROM tbl1")
    assertEquals(ResultKind.SUCCESS, tableResult4.getResultKind)

    // SHOW VIEWS also shows temporary views
    val tableResult5 = tableEnv.executeSql("SHOW VIEWS")
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult5.getResultKind)
    checkData(
      util.Arrays.asList(Row.of("view1"), Row.of("view2")).iterator(),
      tableResult5.collect())
  }

  @Test
  def testExecuteSqlWithExplainSelect(): Unit = {
    val createTableStmt =
      """
        |CREATE TABLE MyTable (
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

    val tableResult2 = tableEnv.executeSql("explain plan for select * from MyTable where a > 10")
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult2.getResultKind)
    val it = tableResult2.collect()
    assertTrue(it.hasNext)
    val row = it.next()
    assertEquals(1, row.getArity)
    val actual = row.getField(0).toString
    val expected = TableTestUtil.readFromResource("/explain/testExecuteSqlWithExplainSelect.out")
    assertEquals(replaceStageId(expected), replaceStageId(actual))
    assertFalse(it.hasNext)
  }

  @Test
  def testExecuteSqlWithExplainInsert(): Unit = {
    val createTableStmt1 =
      """
        |CREATE TABLE MyTable (
        |  a bigint,
        |  b int,
        |  c varchar
        |) with (
        |  'connector' = 'COLLECTION',
        |  'is-bounded' = 'false'
        |)
      """.stripMargin
    val tableResult1 = tableEnv.executeSql(createTableStmt1)
    assertEquals(ResultKind.SUCCESS, tableResult1.getResultKind)

    val createTableStmt2 =
      """
        |CREATE TABLE MySink (
        |  d bigint,
        |  e int
        |) with (
        |  'connector' = 'COLLECTION',
        |  'is-bounded' = 'false'
        |)
      """.stripMargin
    val tableResult2 = tableEnv.executeSql(createTableStmt2)
    assertEquals(ResultKind.SUCCESS, tableResult2.getResultKind)

    val tableResult3 = tableEnv.executeSql(
      "explain plan for insert into MySink select a, b from MyTable where a > 10")
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult3.getResultKind)
    val it = tableResult3.collect()
    assertTrue(it.hasNext)
    val row = it.next()
    assertEquals(1, row.getArity)
    val actual = row.getField(0).toString
    val expected = TableTestUtil.readFromResource("/explain/testExecuteSqlWithExplainInsert.out")
    assertEquals(replaceStageId(expected), replaceStageId(actual))
    assertFalse(it.hasNext)
  }

  @Test
  def testExecuteSqlWithUnsupportedExplain(): Unit = {
    val createTableStmt =
      """
        |CREATE TABLE MyTable (
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

    // TODO we can support them later
    testUnsupportedExplain("explain plan excluding attributes for select * from MyTable")
    testUnsupportedExplain("explain plan including all attributes for select * from MyTable")
    testUnsupportedExplain("explain plan with type for select * from MyTable")
    testUnsupportedExplain("explain plan without implementation for select * from MyTable")
    testUnsupportedExplain("explain plan as xml for select * from MyTable")
    testUnsupportedExplain("explain plan as json for select * from MyTable")
  }

  @Test
  def testExplainSqlWithSelect(): Unit = {
    val createTableStmt =
      """
        |CREATE TABLE MyTable (
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

    val actual = tableEnv.explainSql(
      "select * from MyTable where a > 10", ExplainDetail.CHANGELOG_MODE)
    val expected = TableTestUtil.readFromResource("/explain/testExplainSqlWithSelect.out")
    assertEquals(replaceStageId(expected), replaceStageId(actual))
  }

  @Test
  def testExplainSqlWithInsert(): Unit = {
    val createTableStmt1 =
      """
        |CREATE TABLE MyTable (
        |  a bigint,
        |  b int,
        |  c varchar
        |) with (
        |  'connector' = 'COLLECTION',
        |  'is-bounded' = 'false'
        |)
      """.stripMargin
    val tableResult1 = tableEnv.executeSql(createTableStmt1)
    assertEquals(ResultKind.SUCCESS, tableResult1.getResultKind)

    val createTableStmt2 =
      """
        |CREATE TABLE MySink (
        |  d bigint,
        |  e int
        |) with (
        |  'connector' = 'COLLECTION',
        |  'is-bounded' = 'false'
        |)
      """.stripMargin
    val tableResult2 = tableEnv.executeSql(createTableStmt2)
    assertEquals(ResultKind.SUCCESS, tableResult2.getResultKind)

    val actual = tableEnv.explainSql(
      "insert into MySink select a, b from MyTable where a > 10")
    val expected = TableTestUtil.readFromResource("/explain/testExplainSqlWithInsert.out")
    assertEquals(replaceStageId(expected), replaceStageId(actual))
  }

  @Test
  def testTableExplain(): Unit = {
    val createTableStmt =
      """
        |CREATE TABLE MyTable (
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

    val actual = tableEnv.sqlQuery("select * from MyTable where a > 10")
      .explain(ExplainDetail.CHANGELOG_MODE)
    val expected = TableTestUtil.readFromResource("/explain/testExplainSqlWithSelect.out")
    assertEquals(replaceStageId(expected), replaceStageId(actual))
  }

  private def testUnsupportedExplain(explain: String): Unit = {
    try {
      tableEnv.executeSql(explain)
      fail("This should not happen")
    } catch {
      case e: TableException =>
        assertTrue(e.getMessage.contains("Only default behavior is supported now"))
      case e =>
        fail("This should not happen, " + e.getMessage)
    }
  }

  @Test
  def testDescribeTableOrView(): Unit = {
    val sourceDDL =
      """
        |CREATE TABLE T1(
        |  f0 char(10),
        |  f1 varchar(10),
        |  f2 string,
        |  f3 BOOLEAN,
        |  f4 BINARY(10),
        |  f5 VARBINARY(10),
        |  f6 BYTES,
        |  f7 DECIMAL(10, 3),
        |  f8 TINYINT,
        |  f9 SMALLINT,
        |  f10 INTEGER,
        |  f11 BIGINT,
        |  f12 FLOAT,
        |  f13 DOUBLE,
        |  f14 DATE,
        |  f15 TIME,
        |  f16 TIMESTAMP,
        |  f17 TIMESTAMP(3),
        |  f18 TIMESTAMP WITHOUT TIME ZONE,
        |  f19 TIMESTAMP(3) WITH LOCAL TIME ZONE,
        |  f20 TIMESTAMP WITH LOCAL TIME ZONE,
        |  f21 ARRAY<INT>,
        |  f22 MAP<INT, STRING>,
        |  f23 ROW<f0 INT, f1 STRING>,
        |  f24 int not null,
        |  f25 varchar not null,
        |  f26 row<f0 int not null, f1 int> not null,
        |  ts AS to_timestamp(f25),
        |  PRIMARY KEY(f24, f26) NOT ENFORCED,
        |  WATERMARK FOR ts AS ts - INTERVAL '1' SECOND
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    val viewDDL =
      """
        |CREATE VIEW IF NOT EXISTS T2(d, e, f) AS SELECT f24, f25, f26 FROM T1
      """.stripMargin
    tableEnv.executeSql(sourceDDL)
    tableEnv.executeSql(viewDDL)

    val tableResult1 = tableEnv.executeSql("describe T1")
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult1.getResultKind)
    checkData(
      util.Arrays.asList(
        Row.of("f0", "CHAR(10)", Boolean.box(true), null, null, null),
        Row.of("f1", "VARCHAR(10)", Boolean.box(true), null, null, null),
        Row.of("f2", "STRING", Boolean.box(true), null, null, null),
        Row.of("f3", "BOOLEAN", Boolean.box(true), null, null, null),
        Row.of("f4", "BINARY(10)", Boolean.box(true), null, null, null),
        Row.of("f5", "VARBINARY(10)", Boolean.box(true), null, null, null),
        Row.of("f6", "BYTES", Boolean.box(true), null, null, null),
        Row.of("f7", "DECIMAL(10, 3)", Boolean.box(true), null, null, null),
        Row.of("f8", "TINYINT", Boolean.box(true), null, null, null),
        Row.of("f9", "SMALLINT", Boolean.box(true), null, null, null),
        Row.of("f10", "INT", Boolean.box(true), null, null, null),
        Row.of("f11", "BIGINT", Boolean.box(true), null, null, null),
        Row.of("f12", "FLOAT", Boolean.box(true), null, null, null),
        Row.of("f13", "DOUBLE", Boolean.box(true), null, null, null),
        Row.of("f14", "DATE", Boolean.box(true), null, null, null),
        Row.of("f15", "TIME(0)", Boolean.box(true), null, null, null),
        Row.of("f16", "TIMESTAMP(6)", Boolean.box(true), null, null, null),
        Row.of("f17", "TIMESTAMP(3)", Boolean.box(true), null, null, null),
        Row.of("f18", "TIMESTAMP(6)", Boolean.box(true), null, null, null),
        Row.of("f19", "TIMESTAMP(3) WITH LOCAL TIME ZONE", Boolean.box(true), null, null, null),
        Row.of("f20", "TIMESTAMP(6) WITH LOCAL TIME ZONE", Boolean.box(true), null, null, null),
        Row.of("f21", "ARRAY<INT>", Boolean.box(true), null, null, null),
        Row.of("f22", "MAP<INT, STRING>", Boolean.box(true), null, null, null),
        Row.of("f23", "ROW<`f0` INT, `f1` STRING>", Boolean.box(true), null, null, null),
        Row.of("f24", "INT", Boolean.box(false), "PRI(f24, f26)", null, null),
        Row.of("f25", "STRING", Boolean.box(false), null, null, null),
        Row.of("f26", "ROW<`f0` INT NOT NULL, `f1` INT>", Boolean.box(false),
          "PRI(f24, f26)", null, null),
        Row.of("ts", "TIMESTAMP(3) *ROWTIME*", Boolean.box(true), null, "TO_TIMESTAMP(`f25`)",
          "`ts` - INTERVAL '1' SECOND")
      ).iterator(),
      tableResult1.collect())

    val tableResult2 = tableEnv.executeSql("describe T2")
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult2.getResultKind)
    checkData(
      util.Arrays.asList(
        Row.of("d", "INT", Boolean.box(false), null, null, null),
        Row.of("e", "STRING", Boolean.box(false), null, null, null),
        Row.of("f", "ROW<`f0` INT NOT NULL, `f1` INT>", Boolean.box(false), null, null, null)
      ).iterator(),
      tableResult2.collect())

    // temporary view T2(x, y) masks permanent view T2(d, e, f)
    val temporaryViewDDL =
      """
        |CREATE TEMPORARY VIEW IF NOT EXISTS T2(x, y) AS SELECT f24, f25 FROM T1
      """.stripMargin
    tableEnv.executeSql(temporaryViewDDL)

    val tableResult3 = tableEnv.executeSql("describe T2")
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult3.getResultKind)
    checkData(
      util.Arrays.asList(
        Row.of("x", "INT", Boolean.box(false), null, null, null),
        Row.of("y", "STRING", Boolean.box(false), null, null, null)).iterator(),
      tableResult3.collect())
  }

  private def checkData(expected: util.Iterator[Row], actual: util.Iterator[Row]): Unit = {
    while (expected.hasNext && actual.hasNext) {
      assertEquals(expected.next(), actual.next())
    }
    assertEquals(expected.hasNext, actual.hasNext)
  }

}
