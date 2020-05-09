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

package org.apache.flink.table.api.batch

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{ResultKind, TableException}
import org.apache.flink.table.catalog.{GenericInMemoryCatalog, ObjectPath}
import org.apache.flink.table.runtime.stream.sql.FunctionITCase.{SimpleScalarFunction, TestUDF}
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.table.utils.TableTestUtil.{readFromResource, replaceStageId, _}
import org.apache.flink.types.Row

import org.hamcrest.Matchers.containsString
import org.junit.Assert.{assertEquals, assertFalse, assertTrue, fail}
import org.junit.Test

import java.util

import scala.collection.JavaConverters._

class BatchTableEnvironmentTest extends TableTestBase {

  @Test
  def testSqlWithoutRegistering(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]("tableName", 'a, 'b, 'c)

    val sqlTable = util.tableEnv.sqlQuery(s"SELECT a, b, c FROM $table WHERE b > 12")

    val expected = unaryNode(
      "DataSetCalc",
      batchTableNode(table),
      term("select", "a, b, c"),
      term("where", ">(b, 12)"))

    util.verifyTable(sqlTable, expected)

    val table2 = util.addTable[(Long, Int, String)]('d, 'e, 'f)

    val sqlTable2 = util.tableEnv.sqlQuery(s"SELECT d, e, f FROM $table, $table2 WHERE c = d")

    val join = unaryNode(
      "DataSetJoin",
      binaryNode(
        "DataSetCalc",
        batchTableNode(table),
        batchTableNode(table2),
        term("select", "c")),
      term("where", "=(c, d)"),
      term("join", "c, d, e, f"),
      term("joinType", "InnerJoin"))

    val expected2 = unaryNode(
      "DataSetCalc",
      join,
      term("select", "d, e, f"))

    util.verifyTable(sqlTable2, expected2)
  }

  @Test
  def testExecuteSqlWithCreateDropTable(): Unit = {
    val util = batchTestUtil()

    val createTableStmt =
      """
        |CREATE TABLE tbl1 (
        |  a bigint,
        |  b int,
        |  c varchar
        |) with (
        |  'connector' = 'COLLECTION',
        |  'is-bounded' = 'true'
        |)
      """.stripMargin
    val tableResult1 = util.tableEnv.executeSql(createTableStmt)
    assertEquals(ResultKind.SUCCESS, tableResult1.getResultKind)
    assertTrue(util.tableEnv.getCatalog(util.tableEnv.getCurrentCatalog).get()
      .tableExists(ObjectPath.fromString(s"${util.tableEnv.getCurrentDatabase}.tbl1")))

    val tableResult2 = util.tableEnv.executeSql("ALTER TABLE tbl1 SET ('k1' = 'a', 'k2' = 'b')")
    assertEquals(ResultKind.SUCCESS, tableResult2.getResultKind)
    assertEquals(
      Map("connector" -> "COLLECTION", "is-bounded" -> "true", "k1" -> "a", "k2" -> "b").asJava,
      util.tableEnv.getCatalog(util.tableEnv.getCurrentCatalog).get()
        .getTable(ObjectPath.fromString(s"${util.tableEnv.getCurrentDatabase}.tbl1")).getProperties)

    val tableResult3 = util.tableEnv.executeSql("DROP TABLE tbl1")
    assertEquals(ResultKind.SUCCESS, tableResult3.getResultKind)
    assertFalse(util.tableEnv.getCatalog(util.tableEnv.getCurrentCatalog).get()
      .tableExists(ObjectPath.fromString(s"${util.tableEnv.getCurrentDatabase}.tbl1")))
  }

  @Test
  def testExecuteSqlWithCreateDropTemporaryTable(): Unit = {
    val util = batchTestUtil()

    val createTableStmt =
      """
        |CREATE TEMPORARY TABLE tbl1 (
        |  a bigint,
        |  b int,
        |  c varchar
        |) with (
        |  'connector' = 'COLLECTION',
        |  'is-bounded' = 'true'
        |)
      """.stripMargin
    val tableResult1 = util.tableEnv.executeSql(createTableStmt)
    assertEquals(ResultKind.SUCCESS, tableResult1.getResultKind)
    assert(util.tableEnv.listTables().sameElements(Array[String]("tbl1")))

    val tableResult2 = util.tableEnv.executeSql("DROP TEMPORARY TABLE tbl1")
    assertEquals(ResultKind.SUCCESS, tableResult2.getResultKind)
    assert(util.tableEnv.listTables().sameElements(Array.empty[String]))
  }

  @Test
  def testExecuteSqlWithCreateAlterDropDatabase(): Unit = {
    val util = batchTestUtil()
    val tableResult1 = util.tableEnv.executeSql("CREATE DATABASE db1 COMMENT 'db1_comment'")
    assertEquals(ResultKind.SUCCESS, tableResult1.getResultKind)
    assertTrue(util.tableEnv.getCatalog(util.tableEnv.getCurrentCatalog).get()
      .databaseExists("db1"))

    val tableResult2 = util.tableEnv.executeSql("ALTER DATABASE db1 SET ('k1' = 'a', 'k2' = 'b')")
    assertEquals(ResultKind.SUCCESS, tableResult2.getResultKind)
    assertEquals(
      Map("k1" -> "a", "k2" -> "b").asJava,
      util.tableEnv.getCatalog(util.tableEnv.getCurrentCatalog).get()
        .getDatabase("db1").getProperties)

    val tableResult3 = util.tableEnv.executeSql("DROP DATABASE db1")
    assertEquals(ResultKind.SUCCESS, tableResult3.getResultKind)
    assertFalse(util.tableEnv.getCatalog(util.tableEnv.getCurrentCatalog).get()
      .databaseExists("db1"))
  }

  @Test
  def testExecuteSqlWithCreateDropFunction(): Unit = {
    val util = batchTestUtil()
    val funcName = classOf[TestUDF].getName
    val funcName2 = classOf[SimpleScalarFunction].getName

    val tableResult1 = util.tableEnv.executeSql(
      s"CREATE FUNCTION default_database.f1 AS '$funcName'")
    assertEquals(ResultKind.SUCCESS, tableResult1.getResultKind)
    assertTrue(util.tableEnv.getCatalog(util.tableEnv.getCurrentCatalog).get()
      .functionExists(ObjectPath.fromString("default_database.f1")))

    val tableResult2 = util.tableEnv.executeSql(
      s"ALTER FUNCTION default_database.f1 AS '$funcName2'")
    assertEquals(ResultKind.SUCCESS, tableResult2.getResultKind)
    assertTrue(util.tableEnv.getCatalog(util.tableEnv.getCurrentCatalog).get()
      .functionExists(ObjectPath.fromString("default_database.f1")))

    val tableResult3 = util.tableEnv.executeSql("DROP FUNCTION default_database.f1")
    assertEquals(ResultKind.SUCCESS, tableResult3.getResultKind)
    assertFalse(util.tableEnv.getCatalog(util.tableEnv.getCurrentCatalog).get()
      .functionExists(ObjectPath.fromString("default_database.f1")))

    val tableResult4 = util.tableEnv.executeSql(
      s"CREATE TEMPORARY SYSTEM FUNCTION default_database.f2 AS '$funcName'")
    assertEquals(ResultKind.SUCCESS, tableResult4.getResultKind)
    assertTrue(util.tableEnv.listUserDefinedFunctions().contains("f2"))

    val tableResult5 = util.tableEnv.executeSql(
      "DROP TEMPORARY SYSTEM FUNCTION default_database.f2")
    assertEquals(ResultKind.SUCCESS, tableResult5.getResultKind)
    assertFalse(util.tableEnv.listUserDefinedFunctions().contains("f2"))
  }

  @Test
  def testExecuteSqlWithUseCatalog(): Unit = {
    val util = batchTestUtil()
    util.tableEnv.registerCatalog("my_catalog", new GenericInMemoryCatalog("my_catalog"))
    assertEquals("default_catalog", util.tableEnv.getCurrentCatalog)
    val tableResult2 = util.tableEnv.executeSql("USE CATALOG my_catalog")
    assertEquals(ResultKind.SUCCESS, tableResult2.getResultKind)
    assertEquals("my_catalog", util.tableEnv.getCurrentCatalog)
  }

  @Test
  def testExecuteSqlWithUseDatabase(): Unit = {
    val util = batchTestUtil()
    val tableResult1 = util.tableEnv.executeSql("CREATE DATABASE db1 COMMENT 'db1_comment'")
    assertEquals(ResultKind.SUCCESS, tableResult1.getResultKind)
    assertTrue(util.tableEnv.getCatalog(util.tableEnv.getCurrentCatalog).get()
      .databaseExists("db1"))

    assertEquals("default_database", util.tableEnv.getCurrentDatabase)
    val tableResult2 = util.tableEnv.executeSql("USE db1")
    assertEquals(ResultKind.SUCCESS, tableResult2.getResultKind)
    assertEquals("db1", util.tableEnv.getCurrentDatabase)
  }

  @Test
  def testExecuteSqlWithShowCatalogs(): Unit = {
    val testUtil = batchTestUtil()
    testUtil.tableEnv.registerCatalog("my_catalog", new GenericInMemoryCatalog("my_catalog"))
    val tableResult = testUtil.tableEnv.executeSql("SHOW CATALOGS")
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult.getResultKind)
    checkData(
      util.Arrays.asList(Row.of("default_catalog"), Row.of("my_catalog")).iterator(),
      tableResult.collect())
  }

  @Test
  def testExecuteSqlWithShowDatabases(): Unit = {
    val testUtil = batchTestUtil()
    val tableResult1 = testUtil.tableEnv.executeSql("CREATE DATABASE db1 COMMENT 'db1_comment'")
    assertEquals(ResultKind.SUCCESS, tableResult1.getResultKind)
    testUtil.tableEnv.registerCatalog("my_catalog", new GenericInMemoryCatalog("my_catalog"))
    val tableResult2 = testUtil.tableEnv.executeSql("SHOW DATABASES")
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult2.getResultKind)
    checkData(
      util.Arrays.asList(Row.of("default_database"), Row.of("db1")).iterator(),
      tableResult2.collect())
  }

  @Test
  def testExecuteSqlWithShowTables(): Unit = {
    val testUtil = batchTestUtil()
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
    val tableResult1 = testUtil.tableEnv.executeSql(createTableStmt)
    assertEquals(ResultKind.SUCCESS, tableResult1.getResultKind)

    val tableResult2 = testUtil.tableEnv.executeSql("SHOW TABLES")
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult2.getResultKind)
    checkData(
      util.Arrays.asList(Row.of("tbl1")).iterator(),
      tableResult2.collect())
  }

  @Test
  def testExecuteSqlWithShowFunctions(): Unit = {
    val util = batchTestUtil()
    val tableResult = util.tableEnv.executeSql("SHOW FUNCTIONS")
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult.getResultKind)
    checkData(
      util.tableEnv.listFunctions().map(Row.of(_)).toList.asJava.iterator(),
      tableResult.collect())
  }

  @Test
  def testExecuteSqlWithCreateDropView(): Unit = {
    val util = batchTestUtil()

    val createTableStmt =
      """
        |CREATE TABLE tbl1 (
        |  a bigint,
        |  b int,
        |  c varchar
        |) with (
        |  'connector' = 'COLLECTION',
        |  'is-bounded' = 'true'
        |)
      """.stripMargin
    val tableResult1 = util.tableEnv.executeSql(createTableStmt)
    assertEquals(ResultKind.SUCCESS, tableResult1.getResultKind)
    assertTrue(util.tableEnv.getCatalog(util.tableEnv.getCurrentCatalog).get()
      .tableExists(ObjectPath.fromString(s"${util.tableEnv.getCurrentDatabase}.tbl1")))

    val tableResult2 = util.tableEnv.executeSql("CREATE VIEW view1 AS SELECT * FROM tbl1")
    assertEquals(ResultKind.SUCCESS, tableResult2.getResultKind)
    assertTrue(util.tableEnv.getCatalog(util.tableEnv.getCurrentCatalog).get()
      .tableExists(ObjectPath.fromString(s"${util.tableEnv.getCurrentDatabase}.view1")))

    val tableResult3 = util.tableEnv.executeSql("DROP VIEW view1")
    assertEquals(ResultKind.SUCCESS, tableResult3.getResultKind)
    assertFalse(util.tableEnv.getCatalog(util.tableEnv.getCurrentCatalog).get()
      .tableExists(ObjectPath.fromString(s"${util.tableEnv.getCurrentDatabase}.view1")))
  }

  @Test
  def testExecuteSqlWithShowViews(): Unit = {
    val util = batchTestUtil()
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
    val tableResult1 = util.tableEnv.executeSql(createTableStmt)
    assertEquals(ResultKind.SUCCESS, tableResult1.getResultKind)

    val tableResult2 = util.tableEnv.executeSql("CREATE VIEW view1 AS SELECT * FROM tbl1")
    assertEquals(ResultKind.SUCCESS, tableResult2.getResultKind)

    val tableResult3 = util.tableEnv.executeSql("CREATE TEMPORARY VIEW view2 AS SELECT * FROM tbl1")
    assertEquals(ResultKind.SUCCESS, tableResult3.getResultKind)

    val tableResult4 = util.tableEnv.executeSql("SHOW VIEWS")
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult4.getResultKind)
    checkData(
      util.tableEnv.listViews().map(Row.of(_)).toList.asJava.iterator(),
      tableResult4.collect())
  }

  @Test
  def testExecuteSqlWithExplainSelect(): Unit = {
    val util = batchTestUtil()
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
    val tableResult1 = util.tableEnv.executeSql(createTableStmt)
    assertEquals(ResultKind.SUCCESS, tableResult1.getResultKind)

    val tableResult2 = util.tableEnv.executeSql(
      "explain plan for select * from MyTable where a > 10")
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult2.getResultKind)
    val it = tableResult2.collect()
    assertTrue(it.hasNext)
    val row = it.next()
    assertEquals(1, row.getArity)
    val actual = row.getField(0).toString
    val expected = readFromResource("testExecuteSqlWithExplainSelect1.out")
    assertEquals(replaceStageId(expected), replaceStageId(actual))
    assertFalse(it.hasNext)
  }

  @Test
  def testExecuteSqlWithExplainInsert(): Unit = {
    val util = batchTestUtil()
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
    val tableResult1 = util.tableEnv.executeSql(createTableStmt1)
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
    val tableResult2 = util.tableEnv.executeSql(createTableStmt2)
    assertEquals(ResultKind.SUCCESS, tableResult2.getResultKind)

    val tableResult3 = util.tableEnv.executeSql(
      "explain plan for insert into MySink select a, b from MyTable where a > 10")
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult3.getResultKind)
    val it = tableResult3.collect()
    assertTrue(it.hasNext)
    val row = it.next()
    assertEquals(1, row.getArity)
    val actual = row.getField(0).toString
    val expected = readFromResource("testExecuteSqlWithExplainInsert1.out")
    assertEquals(replaceStageId(expected), replaceStageId(actual))
    assertFalse(it.hasNext)
  }

  @Test
  def testExecuteSqlWithUnsupportedExplain(): Unit = {
    val util = batchTestUtil()
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
    val tableResult1 = util.tableEnv.executeSql(createTableStmt)
    assertEquals(ResultKind.SUCCESS, tableResult1.getResultKind)

    // TODO we can support them later
    testUnsupportedExplain(util.tableEnv,
      "explain plan excluding attributes for select * from MyTable")
    testUnsupportedExplain(util.tableEnv,
      "explain plan including all attributes for select * from MyTable")
    testUnsupportedExplain(util.tableEnv,
      "explain plan with type for select * from MyTable")
    testUnsupportedExplain(util.tableEnv,
      "explain plan without implementation for select * from MyTable")
    testUnsupportedExplain(util.tableEnv,
      "explain plan as xml for select * from MyTable")
    testUnsupportedExplain(util.tableEnv,
      "explain plan as json for select * from MyTable")
  }

  @Test
  def testExplainSqlWithSelect(): Unit = {
    val util = batchTestUtil()
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
    val tableResult1 = util.tableEnv.executeSql(createTableStmt)
    assertEquals(ResultKind.SUCCESS, tableResult1.getResultKind)

    val actual = util.tableEnv.explainSql("select * from MyTable where a > 10")
    val expected = readFromResource("testExplainSqlWithSelect1.out")
    assertEquals(replaceStageId(expected), replaceStageId(actual))
  }

  @Test
  def testExplainSqlWithInsert(): Unit = {
    val util = batchTestUtil()
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
    val tableResult1 = util.tableEnv.executeSql(createTableStmt1)
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
    val tableResult2 = util.tableEnv.executeSql(createTableStmt2)
    assertEquals(ResultKind.SUCCESS, tableResult2.getResultKind)

    val actual = util.tableEnv.explainSql(
      "insert into MySink select a, b from MyTable where a > 10")
    val expected = readFromResource("testExplainSqlWithInsert1.out")
    assertEquals(replaceStageId(expected), replaceStageId(actual))
  }

  @Test
  def testTableExplain(): Unit = {
    val util = batchTestUtil()
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
    val tableResult1 = util.tableEnv.executeSql(createTableStmt)
    assertEquals(ResultKind.SUCCESS, tableResult1.getResultKind)

    val actual = util.tableEnv.sqlQuery("select * from MyTable where a > 10").explain()
    val expected = readFromResource("testExplainSqlWithSelect1.out")
    assertEquals(replaceStageId(expected), replaceStageId(actual))
  }

  private def testUnsupportedExplain(tableEnv: BatchTableEnvironment, explain: String): Unit = {
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

  private def checkData(expected: util.Iterator[Row], actual: util.Iterator[Row]): Unit = {
    while (expected.hasNext && actual.hasNext) {
      assertEquals(expected.next(), actual.next())
    }
    assertEquals(expected.hasNext, actual.hasNext)
  }
}
