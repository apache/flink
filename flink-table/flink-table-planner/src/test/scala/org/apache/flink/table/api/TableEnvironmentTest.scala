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

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.typeinfo.Types.STRING
import org.apache.flink.api.scala._
import org.apache.flink.configuration.{Configuration, ExecutionOptions}
import org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, _}
import org.apache.flink.table.api.internal.TableEnvironmentInternal
import org.apache.flink.table.catalog._
import org.apache.flink.table.factories.{TableFactoryUtil, TableSourceFactoryContextImpl}
import org.apache.flink.table.functions.TestGenericUDF
import org.apache.flink.table.module.ModuleEntry
import org.apache.flink.table.planner.factories.utils.TestCollectionTableFactory._
import org.apache.flink.table.planner.runtime.stream.sql.FunctionITCase.TestUDF
import org.apache.flink.table.planner.runtime.stream.table.FunctionITCase.SimpleScalarFunction
import org.apache.flink.table.planner.utils.TableTestUtil.{replaceNodeIdInOperator, replaceStageId, replaceStreamNodeId}
import org.apache.flink.table.planner.utils.{TableTestUtil, TestTableSourceSinks}
import org.apache.flink.table.types.DataType
import org.apache.flink.types.Row

import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.sql.SqlExplainLevel
import org.assertj.core.api.Assertions.{assertThat, assertThatThrownBy}
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
  def testExplainWithExecuteInsert(): Unit = {
    val execEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(execEnv, settings)

    TestTableSourceSinks.createPersonCsvTemporaryTable(tEnv, "MyTable")

    TestTableSourceSinks.createCsvTemporarySinkTable(
      tEnv, new TableSchema(Array("first"), Array(STRING)), "MySink", -1)

    val expected = TableTestUtil.readFromResource("/explain/testStreamTableEnvironmentExplain.out")
    val actual = tEnv.explainSql("execute insert into MySink select first from MyTable")
    assertEquals(TableTestUtil.replaceStageId(expected), TableTestUtil.replaceStageId(actual))
  }

  @Test
  def testStreamTableEnvironmentExecutionExplainWithEnvParallelism(): Unit = {
    val execEnv = StreamExecutionEnvironment.getExecutionEnvironment
    execEnv.setParallelism(4)
    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(execEnv, settings)

    verifyTableEnvironmentExecutionExplain(tEnv)
  }

  @Test
  def testStreamTableEnvironmentExecutionExplainWithConfParallelism(): Unit = {
    val execEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(execEnv, settings)
    val configuration = new Configuration()
    configuration.setInteger("parallelism.default", 4)
    tEnv.getConfig.addConfiguration(configuration)

    verifyTableEnvironmentExecutionExplain(tEnv)
  }

  private def verifyTableEnvironmentExecutionExplain(tEnv: TableEnvironment): Unit = {
    TestTableSourceSinks.createPersonCsvTemporaryTable(tEnv, "MyTable")

    TestTableSourceSinks.createCsvTemporarySinkTable(
      tEnv, new TableSchema(Array("first"), Array(STRING)), "MySink", -1)

    val expected =
      TableTestUtil.readFromResource("/explain/testStreamTableEnvironmentExecutionExplain.out")
    val actual = tEnv.explainSql("insert into MySink select first from MyTable",
      ExplainDetail.JSON_EXECUTION_PLAN)

    assertEquals(TableTestUtil.replaceNodeIdInOperator(TableTestUtil.replaceStreamNodeId(expected)),
      TableTestUtil.replaceNodeIdInOperator(TableTestUtil.replaceStreamNodeId(actual)))
  }

  @Test
  def testStatementSetExecutionExplain(): Unit = {
    val execEnv = StreamExecutionEnvironment.getExecutionEnvironment
    execEnv.setParallelism(1)
    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(execEnv, settings)

    TestTableSourceSinks.createPersonCsvTemporaryTable(tEnv, "MyTable")

    TestTableSourceSinks.createCsvTemporarySinkTable(
      tEnv, new TableSchema(Array("first"), Array(STRING)), "MySink", -1)

    val expected =
      TableTestUtil.readFromResource("/explain/testStatementSetExecutionExplain.out")
    val statementSet = tEnv.createStatementSet()
    statementSet.addInsertSql("insert into MySink select last from MyTable")
    statementSet.addInsertSql("insert into MySink select first from MyTable")
    val actual = statementSet.explain(ExplainDetail.JSON_EXECUTION_PLAN)

    assertThat(TableTestUtil.replaceNodeIdInOperator(TableTestUtil.replaceStreamNodeId(actual)))
      .isEqualTo(TableTestUtil.replaceNodeIdInOperator(TableTestUtil.replaceStreamNodeId(expected)))
  }

  @Test
  def testExecuteStatementSetExecutionExplain(): Unit = {
    val execEnv = StreamExecutionEnvironment.getExecutionEnvironment
    execEnv.setParallelism(1)
    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(execEnv, settings)

    TestTableSourceSinks.createPersonCsvTemporaryTable(tEnv, "MyTable")

    TestTableSourceSinks.createCsvTemporarySinkTable(
      tEnv, new TableSchema(Array("first"), Array(STRING)), "MySink", -1)

    val expected =
      TableTestUtil.readFromResource("/explain/testStatementSetExecutionExplain.out")

    val actual = tEnv.explainSql(
      "execute statement set begin " +
        "insert into MySink select last from MyTable; " +
        "insert into MySink select first from MyTable; end",
      ExplainDetail.JSON_EXECUTION_PLAN)

    assertEquals(TableTestUtil.replaceNodeIdInOperator(TableTestUtil.replaceStreamNodeId(expected)),
      TableTestUtil.replaceNodeIdInOperator(TableTestUtil.replaceStreamNodeId(actual)))
  }

  @Test
  def testAlterTableResetEmptyOptionKey(): Unit = {
    val statement =
      """
        |CREATE TABLE MyTable (
        |  a bigint,
        |  b int,
        |  c varchar
        |) WITH (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    tableEnv.executeSql(statement)
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage(
      "ALTER TABLE RESET does not support empty key")
    tableEnv.executeSql("ALTER TABLE MyTable RESET ()")
  }

  @Test
  def testAlterTableResetInvalidOptionKey(): Unit = {
    // prepare DDL with invalid table option key
    val statementWithTypo =
      """
        |CREATE TABLE MyTable (
        |  a bigint,
        |  b int,
        |  c varchar
        |) WITH (
        |  'connector' = 'datagen',
        |  'invalid-key' = 'invalid-value'
        |)
      """.stripMargin
    tableEnv.executeSql(statementWithTypo)
    assertThatThrownBy(
        () => tableEnv.executeSql("explain plan for select * from MyTable where a > 10"))
      .hasMessageContaining("Unable to create a source for reading table " +
      "'default_catalog.default_database.MyTable'.\n\n" +
      "Table options are:\n\n'connector'='datagen'\n" +
      "'invalid-key'='invalid-value'" )

    // remove invalid key by RESET
    val alterTableResetStatement = "ALTER TABLE MyTable RESET ('invalid-key')"
    val tableResult = tableEnv.executeSql(alterTableResetStatement)
    assertEquals(ResultKind.SUCCESS, tableResult.getResultKind)
    assertEquals(
      Map("connector" -> "datagen").asJava,
      tableEnv.getCatalog(tableEnv.getCurrentCatalog).get()
        .getTable(ObjectPath.fromString(s"${tableEnv.getCurrentDatabase}.MyTable")).getOptions)
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT,
      tableEnv.executeSql("explain plan for select * from MyTable where a > 10").getResultKind)
  }

  @Test
  def testAlterTableResetOptionalOptionKey(): Unit = {
    val statement =
      """
        |CREATE TABLE MyTable (
        |  a bigint,
        |  b int,
        |  c varchar
        |) WITH (
        |  'connector' = 'COLLECTION',
        |  'is-bounded' = 'false'
        |)
      """.stripMargin
    tableEnv.executeSql(statement)
    checkTableSource("MyTable", false)

    val alterTableResetStatement = "ALTER TABLE MyTable RESET ('is-bounded')"
    val tableResult = tableEnv.executeSql(alterTableResetStatement)
    assertEquals(ResultKind.SUCCESS, tableResult.getResultKind)
    assertEquals(
      Map.apply("connector" -> "COLLECTION").asJava,
      tableEnv.getCatalog(tableEnv.getCurrentCatalog).get()
        .getTable(ObjectPath.fromString(s"${tableEnv.getCurrentDatabase}.MyTable")).getOptions)
    checkTableSource("MyTable", true)
  }

  @Test
  def testAlterTableResetRequiredOptionKey(): Unit = {
    val statement =
      """
        |CREATE TABLE MyTable (
        |  a bigint,
        |  b int,
        |  c varchar
        |) WITH (
        |  'connector' = 'filesystem',
        |  'format' = 'testcsv',
        |  'path' = '_invalid'
        |)
      """.stripMargin
    tableEnv.executeSql(statement)

    val alterTableResetStatement = "ALTER TABLE MyTable RESET ('format')"
    val tableResult = tableEnv.executeSql(alterTableResetStatement)
    assertEquals(ResultKind.SUCCESS, tableResult.getResultKind)
    assertEquals(
      Map("connector" -> "filesystem", "path" -> "_invalid").asJava,
      tableEnv.getCatalog(tableEnv.getCurrentCatalog).get()
        .getTable(ObjectPath.fromString(s"${tableEnv.getCurrentDatabase}.MyTable")).getOptions)
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage(
      "Unable to create a source for reading table 'default_catalog.default_database.MyTable'.")
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT,
      tableEnv.executeSql("explain plan for select * from MyTable where a > 10").getResultKind)
  }

  @Test
  def testAlterTableCompactOnNonManagedTable(): Unit = {
    val statement =
      """
        |CREATE TABLE MyTable (
        |  a bigint,
        |  b int,
        |  c varchar
        |) WITH (
        |  'connector' = 'COLLECTION',
        |  'is-bounded' = 'false'
        |)
      """.stripMargin
    tableEnv.executeSql(statement)

    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("ALTER TABLE COMPACT operation is not supported for " +
          "non-managed table `default_catalog`.`default_database`.`MyTable`")
    tableEnv.executeSql("alter table MyTable compact")
  }

  @Test
  def testAlterTableCompactOnManagedTableUnderStreamingMode(): Unit = {
    val statement =
      """
        |CREATE TABLE MyTable (
        |  a bigint,
        |  b int,
        |  c varchar
        |)
      """.stripMargin
      tableEnv.executeSql(statement)

    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage(
      "Compact managed table only works under batch mode.")
    tableEnv.executeSql("alter table MyTable compact")
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
        .getTable(ObjectPath.fromString(s"${tableEnv.getCurrentDatabase}.tbl1")).getOptions)

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
    // test create table twice
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
      ResolvedSchema.of(Column.physical("catalog name", DataTypes.STRING())),
      tableResult.getResolvedSchema)
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
      ResolvedSchema.of(Column.physical("database name", DataTypes.STRING())),
      tableResult2.getResolvedSchema)
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
      ResolvedSchema.of(Column.physical("table name", DataTypes.STRING())),
      tableResult2.getResolvedSchema)
    checkData(
      util.Arrays.asList(Row.of("tbl1")).iterator(),
      tableResult2.collect())
  }

  @Test
  def testExecuteSqlWithEnhancedShowTables(): Unit = {
    val createCatalogResult = tableEnv.executeSql(
      "CREATE CATALOG catalog1 WITH('type'='generic_in_memory')")
    assertEquals(ResultKind.SUCCESS, createCatalogResult.getResultKind)

    val createDbResult = tableEnv.executeSql(
      "CREATE database catalog1.db1")
    assertEquals(ResultKind.SUCCESS, createDbResult.getResultKind)

    val createTableStmt =
      """
        |CREATE TABLE catalog1.db1.person (
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

    val createTableStmt2 =
      """
        |CREATE TABLE catalog1.db1.dim (
        |  a bigint
        |) with (
        |  'connector' = 'COLLECTION',
        |  'is-bounded' = 'false'
        |)
      """.stripMargin
    val tableResult2 = tableEnv.executeSql(createTableStmt2)
    assertEquals(ResultKind.SUCCESS, tableResult2.getResultKind)

    val tableResult3 = tableEnv.executeSql("SHOW TABLES FROM catalog1.db1 like 'p_r%'")
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult3.getResultKind)
    assertEquals(
      ResolvedSchema.of(Column.physical("table name", DataTypes.STRING())),
      tableResult3.getResolvedSchema)
    checkData(
      util.Arrays.asList(Row.of("person")).iterator(),
      tableResult3.collect())
  }

  @Test
  def testExecuteSqlWithShowFunctions(): Unit = {
    val tableResult = tableEnv.executeSql("SHOW FUNCTIONS")
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult.getResultKind)
    assertEquals(
      ResolvedSchema.of(Column.physical("function name", DataTypes.STRING())),
      tableResult.getResolvedSchema)
    checkData(
      tableEnv.listFunctions().map(Row.of(_)).toList.asJava.iterator(),
      tableResult.collect())

    val funcName = classOf[TestUDF].getName
    val tableResult1 = tableEnv.executeSql(s"CREATE FUNCTION default_database.f1 AS '$funcName'")
    assertEquals(ResultKind.SUCCESS, tableResult1.getResultKind)
    val tableResult2 = tableEnv.executeSql("SHOW USER FUNCTIONS")
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult2.getResultKind)
    assertEquals(
      ResolvedSchema.of(Column.physical("function name", DataTypes.STRING())),
      tableResult2.getResolvedSchema)
    checkData(
      util.Arrays.asList(Row.of("f1")).iterator(),
      tableResult2.collect())
  }

  @Test
  def testExecuteSqlWithLoadModule(): Unit = {
    val result = tableEnv.executeSql("LOAD MODULE dummy")
    assertEquals(ResultKind.SUCCESS, result.getResultKind)
    checkListModules("core", "dummy")
    checkListFullModules(("core", true), ("dummy", true))

    val statement =
      """
        |LOAD MODULE dummy WITH (
        |  'type' = 'dummy'
        |)
      """.stripMargin
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage(
      "Option 'type' = 'dummy' is not supported since module name is used to find module")
    tableEnv.executeSql(statement)
  }

  @Test
  def testExecuteSqlWithLoadParameterizedModule(): Unit = {
    val statement1 =
      """
        |LOAD MODULE dummy WITH (
        |  'dummy-version' = '1'
        |)
      """.stripMargin
    val result = tableEnv.executeSql(statement1)
    assertEquals(ResultKind.SUCCESS, result.getResultKind)
    checkListModules("core", "dummy")
    checkListFullModules(("core", true), ("dummy", true))

    val statement2 =
      """
        |LOAD MODULE dummy WITH (
        |  'dummy-version' = '2'
        |)
      """.stripMargin
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage(
      "Could not execute LOAD MODULE `dummy` WITH ('dummy-version' = '2')." +
        " A module with name 'dummy' already exists")
    tableEnv.executeSql(statement2)
  }

  @Test
  def testExecuteSqlWithLoadCaseSensitiveModuleName(): Unit = {
    val statement1 =
      """
        |LOAD MODULE Dummy WITH (
        |  'dummy-version' = '1'
        |)
      """.stripMargin

    assertThatThrownBy(() => tableEnv.executeSql(statement1))
      .hasMessageContaining(
        "Could not execute LOAD MODULE `Dummy` WITH ('dummy-version' = '1')."
        + " Unable to create module 'Dummy'.")

    val statement2 =
      """
        |LOAD MODULE dummy WITH (
        |  'dummy-version' = '2'
        |)
      """.stripMargin
    val result = tableEnv.executeSql(statement2)
    assertEquals(ResultKind.SUCCESS, result.getResultKind)
    checkListModules("core", "dummy")
    checkListFullModules(("core", true), ("dummy", true))
  }

  @Test
  def testExecuteSqlWithUnloadModuleTwice(): Unit = {
    tableEnv.executeSql("LOAD MODULE dummy")
    checkListModules("core", "dummy")
    checkListFullModules(("core", true), ("dummy", true))

    val result = tableEnv.executeSql("UNLOAD MODULE dummy")
    assertEquals(ResultKind.SUCCESS, result.getResultKind)
    checkListModules("core")
    checkListFullModules(("core", true))

    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage(
      "Could not execute UNLOAD MODULE dummy." +
        " No module with name 'dummy' exists")
    tableEnv.executeSql("UNLOAD MODULE dummy")
  }

  @Test
  def testExecuteSqlWithUseModules(): Unit = {
    tableEnv.executeSql("LOAD MODULE dummy")
    checkListModules("core", "dummy")
    checkListFullModules(("core", true), ("dummy", true))

    val result1 = tableEnv.executeSql("USE MODULES dummy")
    assertEquals(ResultKind.SUCCESS, result1.getResultKind)
    checkListModules("dummy")
    checkListFullModules(("dummy", true), ("core", false))

    val result2 = tableEnv.executeSql("USE MODULES dummy, core")
    assertEquals(ResultKind.SUCCESS, result2.getResultKind)
    checkListModules("dummy", "core")
    checkListFullModules(("dummy", true), ("core", true))

    val result3 = tableEnv.executeSql("USE MODULES core, dummy")
    assertEquals(ResultKind.SUCCESS, result3.getResultKind)
    checkListModules("core", "dummy")
    checkListFullModules(("core", true), ("dummy", true))

    val result4 = tableEnv.executeSql("USE MODULES core")
    assertEquals(ResultKind.SUCCESS, result4.getResultKind)
    checkListModules("core")
    checkListFullModules(("core", true), ("dummy", false))
  }

  @Test
  def testExecuteSqlWithUseUnloadedModules(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage(
      "Could not execute USE MODULES: [core, dummy]. " +
        "No module with name 'dummy' exists")
    tableEnv.executeSql("USE MODULES core, dummy")
  }

  @Test
  def testExecuteSqlWithUseDuplicateModuleNames(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage(
      "Could not execute USE MODULES: [core, core]. " +
        "Module 'core' appears more than once")
    tableEnv.executeSql("USE MODULES core, core")
  }

  @Test
  def testExecuteSqlWithShowModules(): Unit = {
    validateShowModules(("core", true))

    // check result after loading module
    val statement = "LOAD MODULE dummy"
    tableEnv.executeSql(statement)
    validateShowModules(("core", true), ("dummy", true))

    // check result after using modules
    tableEnv.executeSql("USE MODULES dummy")
    validateShowModules(("dummy", true), ("core", false))

    // check result after unloading module
    tableEnv.executeSql("UNLOAD MODULE dummy")
    validateShowModules(("core", false))
  }

  @Test
  def testLegacyModule(): Unit = {
    tableEnv.executeSql("LOAD MODULE LegacyModule")
    validateShowModules(("core", true), ("LegacyModule", true))
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
    thrown.expect(classOf[ValidationException])
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
      ResolvedSchema.of(Column.physical("view name", DataTypes.STRING())),
      tableResult3.getResolvedSchema)
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

    checkExplain("explain plan for select * from MyTable where a > 10",
      "/explain/testExecuteSqlWithExplainSelect.out")
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

    checkExplain("explain plan for insert into MySink select a, b from MyTable where a > 10",
      "/explain/testExecuteSqlWithExplainInsert.out")

    checkExplain("explain plan for insert into MySink(d) select a from MyTable where a > 10",
      "/explain/testExecuteSqlWithExplainInsertPartialColumn.out")
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

  private def testUnsupportedExplain(explain: String): Unit = {
    assertThatThrownBy(() => tableEnv.executeSql(explain))
      .satisfiesAnyOf(
        anyCauseMatches(classOf[TableException], "Only default behavior is supported now"),
        anyCauseMatches(classOf[SqlParserException])
      )
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
  def testExplainSqlWithExecuteSelect(): Unit = {
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
      "execute select * from MyTable where a > 10", ExplainDetail.CHANGELOG_MODE)
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
  def testExecuteSqlWithExplainDetailsSelect(): Unit = {
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

    checkExplain(
      "explain changelog_mode, estimated_cost, json_execution_plan " +
        "select * from MyTable where a > 10",
      "/explain/testExecuteSqlWithExplainDetailsSelect.out");
  }

  @Test
  def testExecuteSqlWithExplainDetailsAndUnion(): Unit = {
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

    val createTableStmt2 =
      """
        |CREATE TABLE MyTable2 (
        |  a bigint,
        |  b int,
        |  c varchar
        |) with (
        |  'connector' = 'COLLECTION',
        |  'is-bounded' = 'false'
        |)
      """.stripMargin
    val tableResult3 = tableEnv.executeSql(createTableStmt2)
    assertEquals(ResultKind.SUCCESS, tableResult3.getResultKind)

    checkExplain(
      "explain changelog_mode, estimated_cost, json_execution_plan " +
        "select * from MyTable union all select * from MyTable2",
      "/explain/testExecuteSqlWithExplainDetailsAndUnion.out")
  }

  @Test
  def testExecuteSqlWithExplainDetailsInsert(): Unit = {
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

    checkExplain(
      "explain changelog_mode, estimated_cost, json_execution_plan " +
        "insert into MySink select a, b from MyTable where a > 10",
      "/explain/testExecuteSqlWithExplainDetailsInsert.out")
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
        |  f27 AS LOCALTIME,
        |  f28 AS CURRENT_TIME,
        |  f29 AS LOCALTIMESTAMP,
        |  f30 AS CURRENT_TIMESTAMP,
        |  f31 AS CURRENT_ROW_TIMESTAMP(),
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

    val expectedResult1 = util.Arrays.asList(
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
        Row.of("f19", "TIMESTAMP_LTZ(3)", Boolean.box(true), null, null, null),
        Row.of("f20", "TIMESTAMP_LTZ(6)", Boolean.box(true), null, null, null),
        Row.of("f21", "ARRAY<INT>", Boolean.box(true), null, null, null),
        Row.of("f22", "MAP<INT, STRING>", Boolean.box(true), null, null, null),
        Row.of("f23", "ROW<`f0` INT, `f1` STRING>", Boolean.box(true), null, null, null),
        Row.of("f24", "INT", Boolean.box(false), "PRI(f24, f26)", null, null),
        Row.of("f25", "STRING", Boolean.box(false), null, null, null),
        Row.of("f26", "ROW<`f0` INT NOT NULL, `f1` INT>", Boolean.box(false),
          "PRI(f24, f26)", null, null),
        Row.of("f27", "TIME(0)", Boolean.box(false), null, "AS LOCALTIME", null),
        Row.of("f28", "TIME(0)", Boolean.box(false), null, "AS CURRENT_TIME", null),
        Row.of("f29", "TIMESTAMP(3)", Boolean.box(false), null, "AS LOCALTIMESTAMP", null),
        Row.of("f30", "TIMESTAMP_LTZ(3)", Boolean.box(false), null,
          "AS CURRENT_TIMESTAMP", null),
        Row.of("f31", "TIMESTAMP_LTZ(3)", Boolean.box(false), null,
          "AS CURRENT_ROW_TIMESTAMP()", null),
        Row.of("ts", "TIMESTAMP(3) *ROWTIME*", Boolean.box(true), null, "AS TO_TIMESTAMP(`f25`)",
          "`ts` - INTERVAL '1' SECOND"))
    val tableResult1 = tableEnv.executeSql("describe T1")
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult1.getResultKind)
    checkData(expectedResult1.iterator(), tableResult1.collect())
    val tableResult2 = tableEnv.executeSql("desc T1")
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult2.getResultKind)
    checkData(expectedResult1.iterator(), tableResult2.collect())

    val expectedResult2 = util.Arrays.asList(
      Row.of("d", "INT", Boolean.box(false), null, null, null),
      Row.of("e", "STRING", Boolean.box(false), null, null, null),
      Row.of("f", "ROW<`f0` INT NOT NULL, `f1` INT>", Boolean.box(false), null, null, null))
    val tableResult3 = tableEnv.executeSql("describe T2")
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult3.getResultKind)
    checkData(expectedResult2.iterator(), tableResult3.collect())
    val tableResult4 = tableEnv.executeSql("desc T2")
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult4.getResultKind)
    checkData(expectedResult2.iterator(), tableResult4.collect())

    // temporary view T2(x, y) masks permanent view T2(d, e, f)
    val temporaryViewDDL =
      """
        |CREATE TEMPORARY VIEW IF NOT EXISTS T2(x, y) AS SELECT f24, f25 FROM T1
      """.stripMargin
    tableEnv.executeSql(temporaryViewDDL)

    val expectedResult3 = util.Arrays.asList(
      Row.of("x", "INT", Boolean.box(false), null, null, null),
      Row.of("y", "STRING", Boolean.box(false), null, null, null));
    val tableResult5 = tableEnv.executeSql("describe T2")
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult5.getResultKind)
    checkData(expectedResult3.iterator(), tableResult5.collect())
    val tableResult6 = tableEnv.executeSql("desc T2")
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult6.getResultKind)
    checkData(
      expectedResult3.iterator(),
      tableResult6.collect())
  }

  @Test
  def testTemporaryOperationListener(): Unit = {
    val listener = new ListenerCatalog("listener_cat")
    val currentCat = tableEnv.getCurrentCatalog
    tableEnv.registerCatalog(listener.getName, listener)
    // test temporary table
    tableEnv.executeSql("create temporary table tbl1 (x int)")
    assertEquals(0, listener.numTempTable)
    tableEnv.executeSql(s"create temporary table ${listener.getName}.`default`.tbl1 (x int)")
    assertEquals(1, listener.numTempTable)
    val tableResult = tableEnv.asInstanceOf[TableEnvironmentInternal].getCatalogManager
      .getTable(ObjectIdentifier.of(listener.getName, "default", "tbl1"))
    assertTrue(tableResult.isPresent)
    assertEquals(listener.tableComment, tableResult.get().getTable[CatalogBaseTable].getComment)
    tableEnv.executeSql("drop temporary table tbl1")
    assertEquals(1, listener.numTempTable)
    tableEnv.executeSql(s"drop temporary table ${listener.getName}.`default`.tbl1")
    assertEquals(0, listener.numTempTable)
    tableEnv.useCatalog(listener.getName)
    tableEnv.executeSql("create temporary table tbl1 (x int)")
    assertEquals(1, listener.numTempTable)
    tableEnv.executeSql("drop temporary table tbl1")
    assertEquals(0, listener.numTempTable)
    tableEnv.useCatalog(currentCat)

    // test temporary view
    tableEnv.executeSql("create temporary view v1 as select 1")
    assertEquals(0, listener.numTempTable)
    tableEnv.executeSql(s"create temporary view ${listener.getName}.`default`.v1 as select 1")
    assertEquals(1, listener.numTempTable)
    val viewResult = tableEnv.asInstanceOf[TableEnvironmentInternal].getCatalogManager
      .getTable(ObjectIdentifier.of(listener.getName, "default", "v1"))
    assertTrue(viewResult.isPresent)
    assertEquals(listener.tableComment, viewResult.get().getTable[CatalogBaseTable].getComment)
    tableEnv.executeSql("drop temporary view v1")
    assertEquals(1, listener.numTempTable)
    tableEnv.executeSql(s"drop temporary view ${listener.getName}.`default`.v1")
    assertEquals(0, listener.numTempTable)
    tableEnv.useCatalog(listener.getName)
    tableEnv.executeSql("create temporary view v1 as select 1")
    assertEquals(1, listener.numTempTable)
    tableEnv.executeSql("drop temporary view  v1")
    assertEquals(0, listener.numTempTable)
    tableEnv.useCatalog(currentCat)

    // test temporary function
    val clzName = "foo.class.name"
    try {
      tableEnv.executeSql(s"create temporary function func1 as '${clzName}'")
      fail("Creating a temporary function with invalid class should fail")
    } catch {
      case _: Exception => //expected
    }
    assertEquals(0, listener.numTempFunc)
    tableEnv.executeSql(
      s"create temporary function ${listener.getName}.`default`.func1 as '${clzName}'")
    assertEquals(1, listener.numTempFunc)
    tableEnv.executeSql("drop temporary function if exists func1")
    assertEquals(1, listener.numTempFunc)
    tableEnv.executeSql(s"drop temporary function ${listener.getName}.`default`.func1")
    assertEquals(0, listener.numTempFunc)
    tableEnv.useCatalog(listener.getName)
    tableEnv.executeSql(s"create temporary function func1 as '${clzName}'")
    assertEquals(1, listener.numTempFunc)
    tableEnv.executeSql("drop temporary function func1")
    assertEquals(0, listener.numTempFunc)
    tableEnv.useCatalog(currentCat)

    listener.close()
  }

  @Test
  def testSetExecutionMode(): Unit = {
    tableEnv.executeSql(
      """
        |CREATE TABLE MyTable (
        |  a bigint
        |) WITH (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    )

    tableEnv.getConfig.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH)

    assertThatThrownBy(() => tableEnv.explainSql("select * from MyTable"))
      .isInstanceOf(classOf[IllegalArgumentException])
      .hasMessageContaining("Mismatch between configured runtime mode and actual runtime mode. " +
        "Currently, the 'execution.runtime-mode' can only be set when instantiating the " +
        "table environment. Subsequent changes are not supported. " +
        "Please instantiate a new TableEnvironment if necessary.")
  }

  private def checkData(expected: util.Iterator[Row], actual: util.Iterator[Row]): Unit = {
    while (expected.hasNext && actual.hasNext) {
      assertEquals(expected.next(), actual.next())
    }
    assertEquals(expected.hasNext, actual.hasNext)
  }

  private def validateShowModules(expectedEntries: (String, java.lang.Boolean)*): Unit = {
    val showModules = tableEnv.executeSql("SHOW MODULES")
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, showModules.getResultKind)
    assertEquals(ResolvedSchema.of(Column.physical("module name", DataTypes.STRING())),
      showModules.getResolvedSchema)

    val showFullModules = tableEnv.executeSql("SHOW FULL MODULES")
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, showFullModules.getResultKind)
    assertEquals(ResolvedSchema.physical(
      Array[String]("module name", "used"),
      Array[DataType](DataTypes.STRING(), DataTypes.BOOLEAN())),
      showFullModules.getResolvedSchema)

    // show modules only list used modules
    checkData(
      expectedEntries.filter(entry => entry._2).map(entry => Row.of(entry._1)).iterator.asJava,
      showModules.collect()
    )

    checkData(
      expectedEntries.map(entry => Row.of(entry._1, entry._2)).iterator.asJava,
      showFullModules.collect())
  }

  private def checkListModules(expected: String*): Unit = {
    val actual = tableEnv.listModules()
    for ((module, i) <- expected.zipWithIndex) {
      assertEquals(module, actual.apply(i))
    }
  }

  private def checkListFullModules(expected: (String, java.lang.Boolean)*): Unit = {
    val actual = tableEnv.listFullModules()
    for ((elem, i) <- expected.zipWithIndex) {
      assertEquals(
        new ModuleEntry(elem._1, elem._2).asInstanceOf[Object],
        actual.apply(i).asInstanceOf[Object])
    }
  }

  private def checkTableSource(tableName: String, expectToBeBounded: java.lang.Boolean): Unit = {
    val resolvedCatalogTable = tableEnv.getCatalog(tableEnv.getCurrentCatalog).get()
      .getTable(ObjectPath.fromString(s"${tableEnv.getCurrentDatabase}.$tableName"))
    val context =
      new TableSourceFactoryContextImpl(
        ObjectIdentifier.of(tableEnv.getCurrentCatalog, tableEnv.getCurrentDatabase, tableName),
      resolvedCatalogTable.asInstanceOf[CatalogTable], new Configuration(), false)
    val source = TableFactoryUtil.findAndCreateTableSource(context)
    assertTrue(source.isInstanceOf[CollectionTableSource])
    assertEquals(expectToBeBounded, source.asInstanceOf[CollectionTableSource].isBounded)
  }

  private def checkExplain(sql: String, resultPath: String): Unit = {
    val tableResult2 = tableEnv.executeSql(sql)
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult2.getResultKind)
    val it = tableResult2.collect()
    assertTrue(it.hasNext)
    val row = it.next()
    assertEquals(1, row.getArity)
    val actual = replaceNodeIdInOperator(replaceStreamNodeId(row.getField(0).toString.trim))
    val expected = replaceNodeIdInOperator(
        replaceStreamNodeId(TableTestUtil.readFromResource(resultPath).trim))
    assertEquals(replaceStageId(expected), replaceStageId(actual))
    assertFalse(it.hasNext)
  }

  class ListenerCatalog(name: String)
    extends GenericInMemoryCatalog(name) with TemporaryOperationListener {

    val tableComment: String = "listener_comment"
    val funcClzName: String = classOf[TestGenericUDF].getName

    var numTempTable = 0
    var numTempFunc = 0

    override def onCreateTemporaryTable(tablePath: ObjectPath, table: CatalogBaseTable)
    : CatalogBaseTable = {
      numTempTable += 1
      if (table.isInstanceOf[CatalogTable]) {
        new CatalogTableImpl(table.getSchema, table.getOptions, tableComment)
      } else {
        val view = table.asInstanceOf[CatalogView]
        new CatalogViewImpl(view.getOriginalQuery, view.getExpandedQuery,
          view.getSchema, view.getOptions, tableComment)
      }
    }

    override def onDropTemporaryTable(tablePath: ObjectPath): Unit = numTempTable -= 1

    override def onCreateTemporaryFunction(functionPath: ObjectPath, function: CatalogFunction)
    : CatalogFunction = {
      numTempFunc += 1
      new CatalogFunctionImpl(funcClzName, function.getFunctionLanguage)
    }

    override def onDropTemporaryFunction(functionPath: ObjectPath): Unit = numTempFunc -= 1
  }

}
