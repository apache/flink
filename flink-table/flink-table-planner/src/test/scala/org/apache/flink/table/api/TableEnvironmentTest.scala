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
import org.apache.flink.api.scala._
import org.apache.flink.configuration.{Configuration, CoreOptions, ExecutionOptions}
import org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches
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
import org.apache.flink.table.planner.utils.{TableTestUtil, TestTableSourceSinks}
import org.apache.flink.table.planner.utils.TableTestUtil.{replaceNodeIdInOperator, replaceStageId, replaceStreamNodeId}
import org.apache.flink.table.test.WithTableEnvironment
import org.apache.flink.table.types.DataType
import org.apache.flink.types.Row
import org.apache.flink.util.CollectionUtil

import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.sql.SqlExplainLevel
import org.assertj.core.api.Assertions.{assertThat, assertThatThrownBy, entry}
import org.junit.jupiter.api.{BeforeEach, Test}
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.parallel.{Execution, ExecutionMode}

import java.nio.file.Path
import java.util

import scala.collection.JavaConverters._

@Execution(ExecutionMode.CONCURRENT)
class TableEnvironmentTest {

  @Test
  @WithTableEnvironment
  def testScanNonExistTable(tableEnv: TableEnvironment): Unit = {
    assertThatThrownBy(() => tableEnv.from("MyTable"))
      .hasMessage("Table `MyTable` was not found.")
  }

  @Test
  @WithTableEnvironment
  def testRegisterDataStream(
      env: StreamExecutionEnvironment,
      tableEnv: StreamTableEnvironment): Unit = {
    val table = env.fromElements[(Int, Long, String, Boolean)]().toTable(tableEnv, 'a, 'b, 'c, 'd)
    tableEnv.createTemporaryView("MyTable", table)
    val scanTable = tableEnv.from("MyTable")
    val relNode = TableTestUtil.toRelNode(scanTable)

    assertThat(RelOptUtil.toString(relNode))
      .isEqualTo("LogicalTableScan(table=[[default_catalog, default_database, MyTable]])\n")

    // register on a conflict name
    assertThatThrownBy(
      () =>
        tableEnv
          .createTemporaryView(
            "MyTable",
            env.fromElements[(Int, Long)]().toTable(tableEnv, 'a, 'b)))
      .hasMessage("Temporary table '`default_catalog`.`default_database`.`MyTable`' already exists")
      .isInstanceOf(classOf[ValidationException])
  }

  @Test
  @WithTableEnvironment
  def testSimpleQuery(env: StreamExecutionEnvironment, tableEnv: StreamTableEnvironment): Unit = {
    val table = env.fromElements[(Int, Long, String, Boolean)]().toTable(tableEnv, 'a, 'b, 'c, 'd)
    tableEnv.createTemporaryView("MyTable", table)
    val queryTable = tableEnv.sqlQuery("SELECT a, c, d FROM MyTable")
    val relNode = TableTestUtil.toRelNode(queryTable)

    assertThat(RelOptUtil.toString(relNode, SqlExplainLevel.NO_ATTRIBUTES))
      .isEqualTo(
        "LogicalProject\n" +
          "  LogicalTableScan\n")
  }

  @Test
  @WithTableEnvironment
  def testStreamTableEnvironmentExplain(
      tEnv: StreamTableEnvironment,
      @TempDir tempDir: Path): Unit = {
    TestTableSourceSinks.createPersonCsvTemporaryTable(tEnv, tempDir, "MyTable")

    TestTableSourceSinks.createCsvTemporarySinkTable(
      tEnv,
      Schema.newBuilder().column("first", DataTypes.STRING()).build(),
      tempDir,
      "MySink")

    val expected = TableTestUtil.readFromResource("/explain/testStreamTableEnvironmentExplain.out")
    val actual = tEnv.explainSql("insert into MySink select first from MyTable")
    assertThat(TableTestUtil.replaceStageId(actual))
      .isEqualTo(TableTestUtil.replaceStageId(expected))
  }

  @Test
  @WithTableEnvironment
  def testExplainWithExecuteInsert(tEnv: StreamTableEnvironment, @TempDir tempDir: Path): Unit = {
    TestTableSourceSinks.createPersonCsvTemporaryTable(tEnv, tempDir, "MyTable")

    TestTableSourceSinks.createCsvTemporarySinkTable(
      tEnv,
      Schema.newBuilder().column("first", DataTypes.STRING()).build(),
      tempDir,
      "MySink")

    val expected = TableTestUtil.readFromResource("/explain/testStreamTableEnvironmentExplain.out")
    val actual = tEnv.explainSql("execute insert into MySink select first from MyTable")
    assertThat(TableTestUtil.replaceStageId(actual))
      .isEqualTo(TableTestUtil.replaceStageId(expected))
  }

  @Test
  def testStreamTableEnvironmentExecutionExplainWithEnvParallelism(@TempDir tempDir: Path): Unit = {
    val execEnv = StreamExecutionEnvironment.getExecutionEnvironment
    execEnv.setParallelism(4)
    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(execEnv, settings)

    verifyTableEnvironmentExecutionExplain(tEnv, tempDir)
  }

  @Test
  def testStreamTableEnvironmentExecutionExplainWithConfParallelism(
      @TempDir tempDir: Path): Unit = {
    val execEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val configuration = new Configuration()
    configuration.set(CoreOptions.DEFAULT_PARALLELISM, Integer.valueOf(4))
    val settings =
      EnvironmentSettings.newInstance().inStreamingMode().withConfiguration(configuration).build()
    val tEnv = StreamTableEnvironment.create(execEnv, settings)

    verifyTableEnvironmentExecutionExplain(tEnv, tempDir)
  }

  private def verifyTableEnvironmentExecutionExplain(
      tEnv: TableEnvironment,
      tempDir: Path): Unit = {
    TestTableSourceSinks.createPersonCsvTemporaryTable(tEnv, tempDir, "MyTable")

    TestTableSourceSinks.createCsvTemporarySinkTable(
      tEnv,
      Schema.newBuilder().column("first", DataTypes.STRING()).build(),
      tempDir,
      "MySink")

    val expected =
      TableTestUtil.readFromResource("/explain/testStreamTableEnvironmentExecutionExplain.out")
    val actual = tEnv.explainSql(
      "insert into MySink select first from MyTable",
      ExplainDetail.JSON_EXECUTION_PLAN)

    assertThat(TableTestUtil.replaceNodeIdInOperator(TableTestUtil.replaceStreamNodeId(actual)))
      .isEqualTo(TableTestUtil.replaceNodeIdInOperator(TableTestUtil.replaceStreamNodeId(expected)))
  }

  @Test
  @WithTableEnvironment
  def testStatementSetExecutionExplain(
      tEnv: StreamTableEnvironment,
      @TempDir tempDir: Path): Unit = {
    tEnv.getConfig.set(CoreOptions.DEFAULT_PARALLELISM, Integer.valueOf(1))
    TestTableSourceSinks.createPersonCsvTemporaryTable(tEnv, tempDir, "MyTable")

    TestTableSourceSinks.createCsvTemporarySinkTable(
      tEnv,
      Schema.newBuilder().column("first", DataTypes.STRING()).build(),
      tempDir,
      "MySink")

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
  @WithTableEnvironment
  def testExecuteStatementSetExecutionExplain(
      tEnv: StreamTableEnvironment,
      @TempDir tempDir: Path): Unit = {
    tEnv.getConfig.set(CoreOptions.DEFAULT_PARALLELISM, Integer.valueOf(1))
    TestTableSourceSinks.createPersonCsvTemporaryTable(tEnv, tempDir, "MyTable")

    TestTableSourceSinks.createCsvTemporarySinkTable(
      tEnv,
      Schema.newBuilder().column("first", DataTypes.STRING()).build(),
      tempDir,
      "MySink")

    val expected =
      TableTestUtil.readFromResource("/explain/testStatementSetExecutionExplain.out")

    val actual = tEnv.explainSql(
      "execute statement set begin " +
        "insert into MySink select last from MyTable; " +
        "insert into MySink select first from MyTable; end",
      ExplainDetail.JSON_EXECUTION_PLAN
    )

    assertThat(TableTestUtil.replaceNodeIdInOperator(TableTestUtil.replaceStreamNodeId(actual)))
      .isEqualTo(TableTestUtil.replaceNodeIdInOperator(TableTestUtil.replaceStreamNodeId(expected)))
  }

  @Test
  @WithTableEnvironment
  def testAlterTableResetEmptyOptionKey(tableEnv: TableEnvironment): Unit = {
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

    assertThatThrownBy(() => tableEnv.executeSql("ALTER TABLE MyTable RESET ()"))
      .hasMessage("ALTER TABLE RESET does not support empty key")
      .isInstanceOf(classOf[ValidationException])
  }

  @Test
  @WithTableEnvironment
  def testAlterTableResetInvalidOptionKey(tableEnv: TableEnvironment): Unit = {
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
      .hasMessageContaining(
        "Unable to create a source for reading table " +
          "'default_catalog.default_database.MyTable'.\n\n" +
          "Table options are:\n\n'connector'='datagen'\n" +
          "'invalid-key'='invalid-value'")

    // remove invalid key by RESET
    val alterTableResetStatement = "ALTER TABLE MyTable RESET ('invalid-key')"
    val tableResult = tableEnv.executeSql(alterTableResetStatement)
    assertThat(tableResult.getResultKind).isEqualTo(ResultKind.SUCCESS)
    assertThat(
      tableEnv
        .getCatalog(tableEnv.getCurrentCatalog)
        .get()
        .getTable(ObjectPath.fromString(s"${tableEnv.getCurrentDatabase}.MyTable"))
        .getOptions)
      .containsOnly(entry("connector", "datagen"))

    assertThat(
      tableEnv
        .executeSql("explain plan for select * from MyTable where a > 10")
        .getResultKind)
      .isEqualTo(ResultKind.SUCCESS_WITH_CONTENT)
  }

  @Test
  @WithTableEnvironment
  def testAlterTableResetOptionalOptionKey(tableEnv: TableEnvironment): Unit = {
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
    checkTableSource(tableEnv, "MyTable", false)

    val alterTableResetStatement = "ALTER TABLE MyTable RESET ('is-bounded')"
    val tableResult = tableEnv.executeSql(alterTableResetStatement)
    assertThat(tableResult.getResultKind).isEqualTo(ResultKind.SUCCESS)
    assertThat(
      tableEnv
        .getCatalog(tableEnv.getCurrentCatalog)
        .get()
        .getTable(ObjectPath.fromString(s"${tableEnv.getCurrentDatabase}.MyTable"))
        .getOptions)
      .containsOnly(entry("connector", "COLLECTION"))
    checkTableSource(tableEnv, "MyTable", true)
  }

  @Test
  @WithTableEnvironment
  def testAlterTableResetRequiredOptionKey(tableEnv: TableEnvironment): Unit = {
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
    assertThat(tableResult.getResultKind).isEqualTo(ResultKind.SUCCESS)
    assertThat(
      tableEnv
        .getCatalog(tableEnv.getCurrentCatalog)
        .get()
        .getTable(ObjectPath.fromString(s"${tableEnv.getCurrentDatabase}.MyTable"))
        .getOptions)
      .containsExactly(entry("connector", "filesystem"), entry("path", "_invalid"))

    assertThatThrownBy(
      () => tableEnv.executeSql("explain plan for select * from MyTable where a > 10"))
      .hasMessageContaining(
        "Unable to create a source for reading table 'default_catalog.default_database.MyTable'.")
      .isInstanceOf(classOf[ValidationException])
  }

  @Test
  @WithTableEnvironment
  def testAlterTableCompactOnNonManagedTable(tableEnv: TableEnvironment): Unit = {
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

    assertThatThrownBy(() => tableEnv.executeSql("alter table MyTable compact"))
      .hasMessage("ALTER TABLE COMPACT operation is not supported for " +
        "non-managed table `default_catalog`.`default_database`.`MyTable`")
      .isInstanceOf(classOf[ValidationException])
  }

  @Test
  @WithTableEnvironment
  def testQueryViewWithHints(tableEnv: TableEnvironment): Unit = {
    val statement =
      """
        |CREATE TABLE MyTable (
        |  a bigint,
        |  b int,
        |  c varchar
        |) WITH (
        |  'connector' = 'datagen'
        |)
      """.stripMargin
    tableEnv.executeSql(statement)
    tableEnv.executeSql("CREATE TEMPORARY VIEW my_view AS SELECT a, c FROM MyTable")

    assertThatThrownBy(
      () => tableEnv.executeSql("SELECT c FROM my_view /*+ OPTIONS('is-bounded' = 'true') */"))
      .hasMessageContaining("View '`default_catalog`.`default_database`.`my_view`' " +
        "cannot be enriched with new options. Hints can only be applied to tables.")
      .isInstanceOf(classOf[ValidationException])

    assertThatThrownBy(
      () =>
        tableEnv.executeSql(
          "CREATE TEMPORARY VIEW your_view AS " +
            "SELECT c FROM my_view /*+ OPTIONS('is-bounded' = 'true') */"))
      .hasMessageContaining("View '`default_catalog`.`default_database`.`my_view`' " +
        "cannot be enriched with new options. Hints can only be applied to tables.")
      .isInstanceOf(classOf[ValidationException])

    tableEnv.executeSql("CREATE TEMPORARY VIEW your_view AS SELECT c FROM my_view ")

    assertThatThrownBy(
      () => tableEnv.executeSql("SELECT * FROM your_view /*+ OPTIONS('is-bounded' = 'true') */"))
      .hasMessageContaining("View '`default_catalog`.`default_database`.`your_view`' " +
        "cannot be enriched with new options. Hints can only be applied to tables.")
      .isInstanceOf(classOf[ValidationException])

  }

  @Test
  @WithTableEnvironment
  def testAlterTableCompactOnManagedTableUnderStreamingMode(tableEnv: TableEnvironment): Unit = {
    val statement =
      """
        |CREATE TABLE MyTable (
        |  a bigint,
        |  b int,
        |  c varchar
        |)
      """.stripMargin
    tableEnv.executeSql(statement)

    assertThatThrownBy(() => tableEnv.executeSql("alter table MyTable compact"))
      .hasMessage("Compact managed table only works under batch mode.")
      .isInstanceOf(classOf[ValidationException])
  }

  @Test
  @WithTableEnvironment
  def testExecuteSqlWithCreateAlterDropTable(tableEnv: TableEnvironment): Unit = {
    val createTableStmt =
      """
        |CREATE TABLE tbl1 (
        |  a bigint,
        |  b int,
        |  c varchar
        |) with (
        |  'connector' = 'datagen'
        |)
      """.stripMargin
    val tableResult1 = tableEnv.executeSql(createTableStmt)
    assertThat(tableResult1.getResultKind).isEqualTo(ResultKind.SUCCESS)
    assertThat(
      tableEnv
        .getCatalog(tableEnv.getCurrentCatalog)
        .get()
        .tableExists(ObjectPath.fromString(s"${tableEnv.getCurrentDatabase}.tbl1"))).isTrue

    val tableResult2 = tableEnv.executeSql("ALTER TABLE tbl1 SET ('k1' = 'a', 'k2' = 'b')")
    assertThat(tableResult2.getResultKind).isEqualTo(ResultKind.SUCCESS)
    assertThat(
      tableEnv
        .getCatalog(tableEnv.getCurrentCatalog)
        .get()
        .getTable(ObjectPath.fromString(s"${tableEnv.getCurrentDatabase}.tbl1"))
        .getOptions)
      .containsOnly(
        entry("connector", "datagen"),
        entry("k1", "a"),
        entry("k2", "b")
      )

    val tableResult3 = tableEnv.executeSql("DROP TABLE tbl1")
    assertThat(tableResult3.getResultKind).isEqualTo(ResultKind.SUCCESS)
    assertThat(
      tableEnv
        .getCatalog(tableEnv.getCurrentCatalog)
        .get()
        .tableExists(ObjectPath.fromString(s"${tableEnv.getCurrentDatabase}.tbl1"))).isFalse
  }

  @Test
  @WithTableEnvironment
  def testExecuteSqlWithCreateDropTableIfNotExists(tableEnv: TableEnvironment): Unit = {
    val createTableStmt =
      """
        |CREATE TABLE IF NOT EXISTS tbl1 (
        |  a bigint,
        |  b int,
        |  c varchar
        |) with (
        |  'connector' = 'datagen'
        |)
      """.stripMargin
    // test create table twice
    val tableResult1 = tableEnv.executeSql(createTableStmt)
    assertThat(tableResult1.getResultKind).isEqualTo(ResultKind.SUCCESS)
    val tableResult2 = tableEnv.executeSql(createTableStmt)
    assertThat(tableResult2.getResultKind).isEqualTo(ResultKind.SUCCESS)
    assertThat(
      tableEnv
        .getCatalog(tableEnv.getCurrentCatalog)
        .get()
        .tableExists(ObjectPath.fromString(s"${tableEnv.getCurrentDatabase}.tbl1"))).isTrue

    val tableResult3 = tableEnv.executeSql("DROP TABLE IF EXISTS tbl1")
    assertThat(tableResult3.getResultKind).isEqualTo(ResultKind.SUCCESS)
    assertThat(
      tableEnv
        .getCatalog(tableEnv.getCurrentCatalog)
        .get()
        .tableExists(ObjectPath.fromString(s"${tableEnv.getCurrentDatabase}.tbl1"))).isFalse
  }

  @Test
  @WithTableEnvironment
  def testExecuteSqlWithCreateDropTemporaryTableIfNotExists(tableEnv: TableEnvironment): Unit = {
    val createTableStmt =
      """
        |CREATE TEMPORARY TABLE IF NOT EXISTS tbl1 (
        |  a bigint,
        |  b int,
        |  c varchar
        |) with (
        |  'connector' = 'datagen'
        |)
      """.stripMargin
    // test crate table twice
    val tableResult1 = tableEnv.executeSql(createTableStmt)
    assertThat(tableResult1.getResultKind).isEqualTo(ResultKind.SUCCESS)
    val tableResult2 = tableEnv.executeSql(createTableStmt)
    assertThat(tableResult2.getResultKind).isEqualTo(ResultKind.SUCCESS)
    assertThat(tableEnv.listTables()).contains("tbl1")

    val tableResult3 = tableEnv.executeSql("DROP TEMPORARY TABLE IF EXISTS tbl1")
    assertThat(tableResult3.getResultKind).isEqualTo(ResultKind.SUCCESS)
    assertThat(tableEnv.listTables()).doesNotContain("tbl1")
  }

  @Test
  @WithTableEnvironment
  def testExecuteSqlWithCreateDropTemporaryTable(tableEnv: TableEnvironment): Unit = {
    val createTableStmt =
      """
        |CREATE TEMPORARY TABLE tbl1 (
        |  a bigint,
        |  b int,
        |  c varchar
        |) with (
        |  'connector' = 'datagen'
        |)
      """.stripMargin
    val tableResult1 = tableEnv.executeSql(createTableStmt)
    assertThat(tableResult1.getResultKind).isEqualTo(ResultKind.SUCCESS)
    assertThat(tableEnv.listTables()).contains("tbl1")

    val tableResult2 = tableEnv.executeSql("DROP TEMPORARY TABLE tbl1")
    assertThat(tableResult2.getResultKind).isEqualTo(ResultKind.SUCCESS)
    assertThat(tableEnv.listTables()).isEmpty()
  }

  @Test
  @WithTableEnvironment
  def testExecuteSqlWithDropTemporaryTableIfExists(tableEnv: TableEnvironment): Unit = {
    val createTableStmt =
      """
        |CREATE TEMPORARY TABLE tbl1 (
        |  a bigint,
        |  b int,
        |  c varchar
        |) with (
        |  'connector' = 'datagen'
        |)
      """.stripMargin
    val tableResult1 = tableEnv.executeSql(createTableStmt)
    assertThat(tableResult1.getResultKind).isEqualTo(ResultKind.SUCCESS)
    assertThat(tableEnv.listTables()).contains("tbl1")

    val tableResult2 = tableEnv.executeSql("DROP TEMPORARY TABLE IF EXISTS tbl1")
    assertThat(tableResult2.getResultKind).isEqualTo(ResultKind.SUCCESS)
    assertThat(tableEnv.listTables()).isEmpty()

    val tableResult3 = tableEnv.executeSql("DROP TEMPORARY TABLE IF EXISTS tbl1")
    assertThat(tableResult3.getResultKind).isEqualTo(ResultKind.SUCCESS)
    assertThat(tableEnv.listTables()).isEmpty()
  }

  @Test
  @WithTableEnvironment
  def testExecuteSqlWithDropTemporaryTableTwice(tableEnv: TableEnvironment): Unit = {
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
    assertThat(tableResult1.getResultKind).isEqualTo(ResultKind.SUCCESS)
    assertThat(tableEnv.listTables()).contains("tbl1")

    val tableResult2 = tableEnv.executeSql("DROP TEMPORARY TABLE tbl1")
    assertThat(tableResult2.getResultKind).isEqualTo(ResultKind.SUCCESS)
    assertThat(tableEnv.listTables()).isEmpty()

    // fail the case
    assertThatThrownBy(() => tableEnv.executeSql("DROP TEMPORARY TABLE tbl1"))
      .isInstanceOf(classOf[ValidationException])
  }

  @Test
  @WithTableEnvironment
  def testDropTemporaryTableWithFullPath(tableEnv: TableEnvironment): Unit = {
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
    assertThat(tableResult1.getResultKind).isEqualTo(ResultKind.SUCCESS)
    assertThat(tableEnv.listTables()).contains("tbl1")

    val tableResult2 =
      tableEnv.executeSql("DROP TEMPORARY TABLE default_catalog.default_database.tbl1")
    assertThat(tableResult2.getResultKind).isEqualTo(ResultKind.SUCCESS)
    assertThat(tableEnv.listTables()).isEmpty()
  }

  @Test
  @WithTableEnvironment
  def testDropTemporaryTableWithInvalidPath(tableEnv: TableEnvironment): Unit = {
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
    assertThat(tableResult1.getResultKind).isEqualTo(ResultKind.SUCCESS)
    assertThat(tableEnv.listTables()).contains("tbl1")

    // fail the case
    assertThatThrownBy(
      () => tableEnv.executeSql("DROP TEMPORARY TABLE invalid_catalog.invalid_database.tbl1"))
      .isInstanceOf(classOf[ValidationException])
  }

  @Test
  @WithTableEnvironment
  def testExecuteSqlWithCreateAlterDropDatabase(tableEnv: TableEnvironment): Unit = {
    val tableResult1 = tableEnv.executeSql("CREATE DATABASE db1 COMMENT 'db1_comment'")
    assertThat(tableResult1.getResultKind).isEqualTo(ResultKind.SUCCESS)
    assertThat(tableEnv.getCatalog(tableEnv.getCurrentCatalog).get().databaseExists("db1")).isTrue

    val tableResult2 = tableEnv.executeSql("ALTER DATABASE db1 SET ('k1' = 'a', 'k2' = 'b')")
    assertThat(tableResult2.getResultKind).isEqualTo(ResultKind.SUCCESS)
    assertThat(
      tableEnv.getCatalog(tableEnv.getCurrentCatalog).get().getDatabase("db1").getProperties)
      .containsExactly(entry("k1", "a"), entry("k2", "b"))

    val tableResult3 = tableEnv.executeSql("DROP DATABASE db1")
    assertThat(tableResult3.getResultKind).isEqualTo(ResultKind.SUCCESS)
    assertThat(tableEnv.getCatalog(tableEnv.getCurrentCatalog).get().databaseExists("db1")).isFalse
  }

  @Test
  @WithTableEnvironment
  def testExecuteSqlWithCreateDropFunction(tableEnv: TableEnvironment): Unit = {
    val funcName = classOf[TestUDF].getName
    val funcName2 = classOf[SimpleScalarFunction].getName

    val tableResult1 = tableEnv.executeSql(s"CREATE FUNCTION default_database.f1 AS '$funcName'")
    assertThat(tableResult1.getResultKind).isEqualTo(ResultKind.SUCCESS)
    assertThat(
      tableEnv
        .getCatalog(tableEnv.getCurrentCatalog)
        .get()
        .functionExists(ObjectPath.fromString("default_database.f1"))).isTrue

    val tableResult2 = tableEnv.executeSql(s"ALTER FUNCTION default_database.f1 AS '$funcName2'")
    assertThat(tableResult2.getResultKind).isEqualTo(ResultKind.SUCCESS)
    assertThat(
      tableEnv
        .getCatalog(tableEnv.getCurrentCatalog)
        .get()
        .functionExists(ObjectPath.fromString("default_database.f1"))).isTrue

    val tableResult3 = tableEnv.executeSql("DROP FUNCTION default_database.f1")
    assertThat(tableResult3.getResultKind).isEqualTo(ResultKind.SUCCESS)
    assertThat(
      tableEnv
        .getCatalog(tableEnv.getCurrentCatalog)
        .get()
        .functionExists(ObjectPath.fromString("default_database.f1"))).isFalse

    val tableResult4 = tableEnv.executeSql(s"CREATE TEMPORARY SYSTEM FUNCTION f2 AS '$funcName'")
    assertThat(tableResult4.getResultKind).isEqualTo(ResultKind.SUCCESS)
    assertThat(tableEnv.listUserDefinedFunctions()).contains("f2")

    val tableResult5 = tableEnv.executeSql("DROP TEMPORARY SYSTEM FUNCTION f2")
    assertThat(tableResult5.getResultKind).isEqualTo(ResultKind.SUCCESS)
    assertThat(tableEnv.listUserDefinedFunctions()).doesNotContain("f2")
  }

  @Test
  @WithTableEnvironment
  def testExecuteSqlWithCreateUseDropCatalog(tableEnv: TableEnvironment): Unit = {
    val tableResult1 =
      tableEnv.executeSql("CREATE CATALOG my_catalog WITH('type'='generic_in_memory')")
    assertThat(tableResult1.getResultKind).isEqualTo(ResultKind.SUCCESS)
    assertThat(tableEnv.getCatalog("my_catalog")).isPresent

    assertThat(tableEnv.getCurrentCatalog).isEqualTo("default_catalog")
    val tableResult2 = tableEnv.executeSql("USE CATALOG my_catalog")
    assertThat(tableResult2.getResultKind).isEqualTo(ResultKind.SUCCESS)
    assertThat(tableEnv.getCurrentCatalog).isEqualTo("my_catalog")

    val tableResult3 = tableEnv.executeSql("DROP CATALOG my_catalog")
    assertThat(tableResult3.getResultKind).isEqualTo(ResultKind.SUCCESS)
    assertThat(tableEnv.getCatalog("my_catalog")).isNotPresent
  }

  @Test
  @WithTableEnvironment
  def testExecuteSqlWithUseDatabase(tableEnv: TableEnvironment): Unit = {
    val tableResult1 = tableEnv.executeSql("CREATE DATABASE db1 COMMENT 'db1_comment'")
    assertThat(tableResult1.getResultKind).isEqualTo(ResultKind.SUCCESS)
    assertThat(tableEnv.getCatalog(tableEnv.getCurrentCatalog).get().databaseExists("db1")).isTrue

    assertThat(tableEnv.getCurrentDatabase).isEqualTo("default_database")
    val tableResult2 = tableEnv.executeSql("USE db1")
    assertThat(tableResult2.getResultKind).isEqualTo(ResultKind.SUCCESS)
    assertThat(tableEnv.getCurrentDatabase).isEqualTo("db1")
  }

  @Test
  @WithTableEnvironment
  def testExecuteSqlWithShowCatalogs(tableEnv: TableEnvironment): Unit = {
    tableEnv.registerCatalog("my_catalog", new GenericInMemoryCatalog("my_catalog"))
    val tableResult = tableEnv.executeSql("SHOW CATALOGS")
    assertThat(tableResult.getResultKind).isEqualTo(ResultKind.SUCCESS_WITH_CONTENT)
    assertThat(tableResult.getResolvedSchema).isEqualTo(
      ResolvedSchema.of(Column.physical("catalog name", DataTypes.STRING())))
    assertThat[Row](CollectionUtil.iteratorToList(tableResult.collect()))
      .containsExactlyInAnyOrder(Row.of("default_catalog"), Row.of("my_catalog"))
  }

  @Test
  @WithTableEnvironment
  def testExecuteSqlWithShowDatabases(tableEnv: TableEnvironment): Unit = {
    val tableResult1 = tableEnv.executeSql("CREATE DATABASE db1 COMMENT 'db1_comment'")
    assertThat(tableResult1.getResultKind).isEqualTo(ResultKind.SUCCESS)
    val tableResult2 = tableEnv.executeSql("SHOW DATABASES")
    assertThat(tableResult2.getResultKind).isEqualTo(ResultKind.SUCCESS_WITH_CONTENT)
    assertThat(tableResult2.getResolvedSchema).isEqualTo(
      ResolvedSchema.of(Column.physical("database name", DataTypes.STRING())))
    assertThat[Row](CollectionUtil.iteratorToList(tableResult2.collect()))
      .containsExactlyInAnyOrder(Row.of("default_database"), Row.of("db1"))
  }

  @Test
  @WithTableEnvironment
  def testExecuteSqlWithShowTables(tableEnv: TableEnvironment): Unit = {
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
    assertThat(tableResult1.getResultKind).isEqualTo(ResultKind.SUCCESS)

    val tableResult2 = tableEnv.executeSql("SHOW TABLES")
    assertThat(tableResult2.getResultKind).isEqualTo(ResultKind.SUCCESS_WITH_CONTENT)
    assertThat(tableResult2.getResolvedSchema).isEqualTo(
      ResolvedSchema.of(Column.physical("table name", DataTypes.STRING())))
    assertThat[Row](CollectionUtil.iteratorToList(tableResult2.collect()))
      .containsOnly(Row.of("tbl1"))
  }

  @Test
  @WithTableEnvironment
  def testExecuteSqlWithEnhancedShowTables(tableEnv: TableEnvironment): Unit = {
    val createCatalogResult =
      tableEnv.executeSql("CREATE CATALOG catalog1 WITH('type'='generic_in_memory')")
    assertThat(createCatalogResult.getResultKind).isEqualTo(ResultKind.SUCCESS)

    val createDbResult = tableEnv.executeSql("CREATE database catalog1.db1")
    assertThat(createDbResult.getResultKind).isEqualTo(ResultKind.SUCCESS)

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
    assertThat(tableResult1.getResultKind).isEqualTo(ResultKind.SUCCESS)

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
    assertThat(tableResult2.getResultKind).isEqualTo(ResultKind.SUCCESS)

    val tableResult3 = tableEnv.executeSql("SHOW TABLES FROM catalog1.db1 like 'p_r%'")
    assertThat(tableResult3.getResultKind).isEqualTo(ResultKind.SUCCESS_WITH_CONTENT)
    assertThat(tableResult3.getResolvedSchema)
      .isEqualTo(ResolvedSchema.of(Column.physical("table name", DataTypes.STRING())))

    assertThat[Row](CollectionUtil.iteratorToList(tableResult3.collect()))
      .containsOnly(Row.of("person"))
  }

  @Test
  @WithTableEnvironment
  def testExecuteSqlWithShowFunctions(tableEnv: TableEnvironment): Unit = {
    val tableResult = tableEnv.executeSql("SHOW FUNCTIONS")
    assertThat(tableResult.getResultKind).isEqualTo(ResultKind.SUCCESS_WITH_CONTENT)
    assertThat(tableResult.getResolvedSchema)
      .isEqualTo(ResolvedSchema.of(Column.physical("function name", DataTypes.STRING())))
    assertThat[Row](CollectionUtil.iteratorToList(tableResult.collect()))
      .isEqualTo(tableEnv.listFunctions().map(Row.of(_)).toList.asJava)

    val funcName = classOf[TestUDF].getName
    val tableResult1 = tableEnv.executeSql(s"CREATE FUNCTION default_database.f1 AS '$funcName'")
    assertThat(tableResult1.getResultKind).isEqualTo(ResultKind.SUCCESS)
    val tableResult2 = tableEnv.executeSql("SHOW USER FUNCTIONS")
    assertThat(tableResult2.getResultKind).isEqualTo(ResultKind.SUCCESS_WITH_CONTENT)
    assertThat(tableResult2.getResolvedSchema)
      .isEqualTo(ResolvedSchema.of(Column.physical("function name", DataTypes.STRING())))
    assertThat[Row](CollectionUtil.iteratorToList(tableResult2.collect()))
      .containsOnly(Row.of("f1"))
  }

  @Test
  @WithTableEnvironment
  def testExecuteSqlWithLoadModule(tableEnv: TableEnvironment): Unit = {
    val result = tableEnv.executeSql("LOAD MODULE dummy")
    assertThat(result.getResultKind).isEqualTo(ResultKind.SUCCESS)
    checkListModules(tableEnv, "core", "dummy")
    checkListFullModules(tableEnv, ("core", true), ("dummy", true))

    val statement =
      """
        |LOAD MODULE dummy WITH (
        |  'type' = 'dummy'
        |)
      """.stripMargin

    assertThatThrownBy(() => tableEnv.executeSql(statement))
      .hasMessageContaining(
        "Option 'type' = 'dummy' is not supported since module name is used to find module")
      .isInstanceOf(classOf[ValidationException])
  }

  @Test
  @WithTableEnvironment
  def testExecuteSqlWithLoadParameterizedModule(tableEnv: TableEnvironment): Unit = {
    val statement1 =
      """
        |LOAD MODULE dummy WITH (
        |  'dummy-version' = '1'
        |)
      """.stripMargin
    val result = tableEnv.executeSql(statement1)
    assertThat(result.getResultKind).isEqualTo(ResultKind.SUCCESS)
    checkListModules(tableEnv, "core", "dummy")
    checkListFullModules(tableEnv, ("core", true), ("dummy", true))

    val statement2 =
      """
        |LOAD MODULE dummy WITH (
        |  'dummy-version' = '2'
        |)
      """.stripMargin

    assertThatThrownBy(() => tableEnv.executeSql(statement2))
      .hasMessage("Could not execute LOAD MODULE `dummy` WITH ('dummy-version' = '2')." +
        " A module with name 'dummy' already exists")
      .isInstanceOf(classOf[ValidationException])
  }

  @Test
  @WithTableEnvironment
  def testExecuteSqlWithLoadCaseSensitiveModuleName(tableEnv: TableEnvironment): Unit = {
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
    assertThat(result.getResultKind).isEqualTo(ResultKind.SUCCESS)
    checkListModules(tableEnv, "core", "dummy")
    checkListFullModules(tableEnv, ("core", true), ("dummy", true))
  }

  @Test
  @WithTableEnvironment
  def testExecuteSqlWithUnloadModuleTwice(tableEnv: TableEnvironment): Unit = {
    tableEnv.executeSql("LOAD MODULE dummy")
    checkListModules(tableEnv, "core", "dummy")
    checkListFullModules(tableEnv, ("core", true), ("dummy", true))

    val result = tableEnv.executeSql("UNLOAD MODULE dummy")
    assertThat(result.getResultKind).isEqualTo(ResultKind.SUCCESS)
    checkListModules(tableEnv, "core")
    checkListFullModules(tableEnv, ("core", true))

    assertThatThrownBy(() => tableEnv.executeSql("UNLOAD MODULE dummy"))
      .hasMessage("Could not execute UNLOAD MODULE dummy." +
        " No module with name 'dummy' exists")
      .isInstanceOf(classOf[ValidationException])
  }

  @Test
  @WithTableEnvironment
  def testExecuteSqlWithUseModules(tableEnv: TableEnvironment): Unit = {
    tableEnv.executeSql("LOAD MODULE dummy")
    checkListModules(tableEnv, "core", "dummy")
    checkListFullModules(tableEnv, ("core", true), ("dummy", true))

    val result1 = tableEnv.executeSql("USE MODULES dummy")
    assertThat(result1.getResultKind).isEqualTo(ResultKind.SUCCESS)
    checkListModules(tableEnv, "dummy")
    checkListFullModules(tableEnv, ("dummy", true), ("core", false))

    val result2 = tableEnv.executeSql("USE MODULES dummy, core")
    assertThat(result2.getResultKind).isEqualTo(ResultKind.SUCCESS)
    checkListModules(tableEnv, "dummy", "core")
    checkListFullModules(tableEnv, ("dummy", true), ("core", true))

    val result3 = tableEnv.executeSql("USE MODULES core, dummy")
    assertThat(result3.getResultKind).isEqualTo(ResultKind.SUCCESS)
    checkListModules(tableEnv, "core", "dummy")
    checkListFullModules(tableEnv, ("core", true), ("dummy", true))

    val result4 = tableEnv.executeSql("USE MODULES core")
    assertThat(result4.getResultKind).isEqualTo(ResultKind.SUCCESS)
    checkListModules(tableEnv, "core")
    checkListFullModules(tableEnv, ("core", true), ("dummy", false))
  }

  @Test
  @WithTableEnvironment
  def testExecuteSqlWithUseUnloadedModules(tableEnv: TableEnvironment): Unit = {
    assertThatThrownBy(() => tableEnv.executeSql("USE MODULES core, dummy"))
      .hasMessage("Could not execute USE MODULES: [core, dummy]. " +
        "No module with name 'dummy' exists")
      .isInstanceOf(classOf[ValidationException])
  }

  @Test
  @WithTableEnvironment
  def testExecuteSqlWithUseDuplicateModuleNames(tableEnv: TableEnvironment): Unit = {
    assertThatThrownBy(() => tableEnv.executeSql("USE MODULES core, core"))
      .hasMessage("Could not execute USE MODULES: [core, core]. " +
        "Module 'core' appears more than once")
      .isInstanceOf(classOf[ValidationException])
  }

  @Test
  @WithTableEnvironment
  def testExecuteSqlWithShowModules(tableEnv: TableEnvironment): Unit = {
    validateShowModules(tableEnv, ("core", true))

    // check result after loading module
    val statement = "LOAD MODULE dummy"
    tableEnv.executeSql(statement)
    validateShowModules(tableEnv, ("core", true), ("dummy", true))

    // check result after using modules
    tableEnv.executeSql("USE MODULES dummy")
    validateShowModules(tableEnv, ("dummy", true), ("core", false))

    // check result after unloading module
    tableEnv.executeSql("UNLOAD MODULE dummy")
    validateShowModules(tableEnv, ("core", false))
  }

  @Test
  @WithTableEnvironment
  def testLegacyModule(tableEnv: TableEnvironment): Unit = {
    tableEnv.executeSql("LOAD MODULE LegacyModule")
    validateShowModules(tableEnv, ("core", true), ("LegacyModule", true))
  }

  @Test
  @WithTableEnvironment
  def testExecuteSqlWithCreateDropView(tableEnv: TableEnvironment): Unit = {
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
    assertThat(viewResult1.getResultKind).isEqualTo(ResultKind.SUCCESS)
    assertThat(
      tableEnv
        .getCatalog(tableEnv.getCurrentCatalog)
        .get()
        .tableExists(ObjectPath.fromString(s"${tableEnv.getCurrentDatabase}.v1"))).isTrue

    val viewResult2 = tableEnv.executeSql("DROP VIEW IF EXISTS v1")
    assertThat(viewResult2.getResultKind).isEqualTo(ResultKind.SUCCESS)
    assertThat(
      tableEnv
        .getCatalog(tableEnv.getCurrentCatalog)
        .get()
        .tableExists(ObjectPath.fromString(s"${tableEnv.getCurrentDatabase}.v1"))).isFalse
  }

  @Test
  @WithTableEnvironment
  def testExecuteSqlWithCreateDropTemporaryView(tableEnv: TableEnvironment): Unit = {
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

    val viewResult1 =
      tableEnv.executeSql("CREATE TEMPORARY VIEW IF NOT EXISTS v1 AS SELECT * FROM tbl1")
    assertThat(viewResult1.getResultKind).isEqualTo(ResultKind.SUCCESS)
    assertThat(tableEnv.listTables()).containsExactlyInAnyOrder("tbl1", "v1")

    val viewResult2 = tableEnv.executeSql("DROP TEMPORARY VIEW IF EXISTS v1")
    assertThat(viewResult2.getResultKind).isEqualTo(ResultKind.SUCCESS)
    assertThat(tableEnv.listTables()).containsExactlyInAnyOrder("tbl1")
  }

  @Test
  @WithTableEnvironment
  def testCreateViewWithWrongFieldList(tableEnv: TableEnvironment): Unit = {
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

    assertThatThrownBy(() => tableEnv.executeSql(viewDDL))
      .hasMessage(
        "VIEW definition and input fields not match:\n" +
          "\tDef fields: [d].\n" +
          "\tInput fields: [a, b, c].")
      .isInstanceOf(classOf[ValidationException])
  }

  @Test
  @WithTableEnvironment
  def testCreateViewTwice(tableEnv: TableEnvironment): Unit = {
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

    assertThatThrownBy(() => tableEnv.executeSql(viewWith2ColumnDDL))
      .hasMessage("Could not execute CreateTable in path `default_catalog`.`default_database`.`T3`")
      .isInstanceOf(classOf[ValidationException])
  }

  @Test
  @WithTableEnvironment
  def testDropViewWithFullPath(tableEnv: TableEnvironment): Unit = {
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

    assertThat(tableEnv.listTables()).containsExactlyInAnyOrder("T1", "T2", "T3")

    tableEnv.executeSql("DROP VIEW default_catalog.default_database.T2")
    assertThat(tableEnv.listTables()).containsExactlyInAnyOrder("T1", "T3")

    tableEnv.executeSql("DROP VIEW default_catalog.default_database.T3")
    assertThat(tableEnv.listTables()).containsExactlyInAnyOrder("T1")
  }

  @Test
  @WithTableEnvironment
  def testDropViewWithPartialPath(tableEnv: TableEnvironment): Unit = {
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

    assertThat(tableEnv.listTables()).containsExactlyInAnyOrder("T1", "T2", "T3")

    tableEnv.executeSql("DROP VIEW T2")
    assertThat(tableEnv.listTables()).containsExactlyInAnyOrder("T1", "T3")

    tableEnv.executeSql("DROP VIEW default_database.T3")
    assertThat(tableEnv.listTables()).containsExactlyInAnyOrder("T1")
  }

  @Test
  @WithTableEnvironment
  def testDropViewIfExistsTwice(tableEnv: TableEnvironment): Unit = {
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

    assertThat(tableEnv.listTables()).containsExactlyInAnyOrder("T1", "T2")

    tableEnv.executeSql("DROP VIEW IF EXISTS default_catalog.default_database.T2")
    assertThat(tableEnv.listTables()).containsExactlyInAnyOrder("T1")

    tableEnv.executeSql("DROP VIEW IF EXISTS default_catalog.default_database.T2")
    assertThat(tableEnv.listTables()).containsExactlyInAnyOrder("T1")
  }

  @Test
  @WithTableEnvironment
  def testDropViewTwice(tableEnv: TableEnvironment): Unit = {
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

    assertThat(tableEnv.listTables()).containsExactlyInAnyOrder("T1", "T2")

    tableEnv.executeSql("DROP VIEW default_catalog.default_database.T2")
    assertThat(tableEnv.listTables()).containsExactlyInAnyOrder("T1")

    assertThatThrownBy(() => tableEnv.executeSql("DROP VIEW default_catalog.default_database.T2"))
      .isInstanceOf(classOf[ValidationException])
  }

  @Test
  @WithTableEnvironment
  def testDropViewWithInvalidPath(tableEnv: TableEnvironment): Unit = {
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
    assertThat(tableEnv.listTables()).containsExactlyInAnyOrder("T1", "T2")

    // failed since 'default_catalog1.default_database1.T2' is invalid path
    assertThatThrownBy(() => tableEnv.executeSql("DROP VIEW default_catalog1.default_database1.T2"))
      .isInstanceOf(classOf[ValidationException])
  }

  @Test
  @WithTableEnvironment
  def testDropViewWithInvalidPathIfExists(tableEnv: TableEnvironment): Unit = {
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
    assertThat(tableEnv.listTables()).containsExactlyInAnyOrder("T1", "T2")

    tableEnv.executeSql("DROP VIEW IF EXISTS default_catalog1.default_database1.T2")
    assertThat(tableEnv.listTables()).containsExactlyInAnyOrder("T1", "T2")
  }

  @Test
  @WithTableEnvironment
  def testDropTemporaryViewIfExistsTwice(tableEnv: TableEnvironment): Unit = {
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

    assertThat(tableEnv.listTemporaryViews()).containsExactlyInAnyOrder("T2")

    tableEnv.executeSql("DROP TEMPORARY VIEW IF EXISTS default_catalog.default_database.T2")
    assertThat(tableEnv.listTemporaryViews()).isEmpty()

    tableEnv.executeSql("DROP TEMPORARY VIEW IF EXISTS default_catalog.default_database.T2")
    assertThat(tableEnv.listTemporaryViews()).isEmpty()
  }

  @Test
  @WithTableEnvironment
  def testDropTemporaryViewTwice(tableEnv: TableEnvironment): Unit = {
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

    assertThat(tableEnv.listTemporaryViews()).containsExactlyInAnyOrder("T2")

    tableEnv.executeSql("DROP TEMPORARY VIEW default_catalog.default_database.T2")
    assertThat(tableEnv.listTemporaryViews()).isEmpty()

    // throws ValidationException since default_catalog.default_database.T2 is not exists
    assertThatThrownBy(
      () => tableEnv.executeSql("DROP TEMPORARY VIEW default_catalog.default_database.T2"))
      .isInstanceOf(classOf[ValidationException])
  }

  @Test
  @WithTableEnvironment
  def testExecuteSqlWithShowViews(tableEnv: TableEnvironment): Unit = {
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
    assertThat(tableResult1.getResultKind).isEqualTo(ResultKind.SUCCESS)

    val tableResult2 = tableEnv.executeSql("CREATE VIEW view1 AS SELECT * FROM tbl1")
    assertThat(tableResult2.getResultKind).isEqualTo(ResultKind.SUCCESS)

    val tableResult3 = tableEnv.executeSql("SHOW VIEWS")
    assertThat(tableResult3.getResultKind).isEqualTo(ResultKind.SUCCESS_WITH_CONTENT)
    assertThat(tableResult3.getResolvedSchema)
      .isEqualTo(ResolvedSchema.of(Column.physical("view name", DataTypes.STRING())))
    assertThat[Row](CollectionUtil.iteratorToList(tableResult3.collect()))
      .containsOnly(Row.of("view1"))

    val tableResult4 = tableEnv.executeSql("CREATE TEMPORARY VIEW view2 AS SELECT * FROM tbl1")
    assertThat(tableResult4.getResultKind).isEqualTo(ResultKind.SUCCESS)

    // SHOW VIEWS also shows temporary views
    val tableResult5 = tableEnv.executeSql("SHOW VIEWS")
    assertThat(tableResult5.getResultKind).isEqualTo(ResultKind.SUCCESS_WITH_CONTENT)
    assertThat[Row](CollectionUtil.iteratorToList(tableResult5.collect()))
      .containsExactlyInAnyOrder(Row.of("view1"), Row.of("view2"))
  }

  @Test
  @WithTableEnvironment
  def testExecuteSqlWithExplainSelect(tableEnv: TableEnvironment): Unit = {
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
    assertThat(tableResult1.getResultKind).isEqualTo(ResultKind.SUCCESS)

    checkExplain(
      tableEnv,
      "explain plan for select * from MyTable where a > 10",
      "/explain/testExecuteSqlWithExplainSelect.out")
  }

  @Test
  @WithTableEnvironment
  def testExecuteSqlWithExplainInsert(tableEnv: TableEnvironment): Unit = {
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
    assertThat(tableResult1.getResultKind).isEqualTo(ResultKind.SUCCESS)

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
    assertThat(tableResult2.getResultKind).isEqualTo(ResultKind.SUCCESS)

    checkExplain(
      tableEnv,
      "explain plan for insert into MySink select a, b from MyTable where a > 10",
      "/explain/testExecuteSqlWithExplainInsert.out")

    checkExplain(
      tableEnv,
      "explain plan for insert into MySink(d) select a from MyTable where a > 10",
      "/explain/testExecuteSqlWithExplainInsertPartialColumn.out")
  }

  @Test
  @WithTableEnvironment
  def testExecuteSqlWithUnsupportedExplain(tableEnv: TableEnvironment): Unit = {
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
    assertThat(tableResult1.getResultKind).isEqualTo(ResultKind.SUCCESS)

    // TODO we can support them later
    testUnsupportedExplain(tableEnv, "explain plan excluding attributes for select * from MyTable")
    testUnsupportedExplain(
      tableEnv,
      "explain plan including all attributes for select * from MyTable")
    testUnsupportedExplain(tableEnv, "explain plan with type for select * from MyTable")
    testUnsupportedExplain(
      tableEnv,
      "explain plan without implementation for select * from MyTable")
    testUnsupportedExplain(tableEnv, "explain plan as xml for select * from MyTable")
    testUnsupportedExplain(tableEnv, "explain plan as json for select * from MyTable")
  }

  private def testUnsupportedExplain(tableEnv: TableEnvironment, explain: String): Unit = {
    assertThatThrownBy(() => tableEnv.executeSql(explain))
      .satisfiesAnyOf(
        anyCauseMatches(classOf[TableException], "Only default behavior is supported now"),
        anyCauseMatches(classOf[SqlParserException])
      )
  }

  @Test
  @WithTableEnvironment
  def testExplainSqlWithSelect(tableEnv: TableEnvironment): Unit = {
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
    assertThat(tableResult1.getResultKind).isEqualTo(ResultKind.SUCCESS)

    val actual =
      tableEnv.explainSql("select * from MyTable where a > 10", ExplainDetail.CHANGELOG_MODE)
    val expected = TableTestUtil.readFromResource("/explain/testExplainSqlWithSelect.out")
    assertThat(replaceStageId(actual)).isEqualTo(replaceStageId(expected))
  }

  @Test
  @WithTableEnvironment
  def testExplainSqlWithExecuteSelect(tableEnv: TableEnvironment): Unit = {
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
    assertThat(tableResult1.getResultKind).isEqualTo(ResultKind.SUCCESS)

    val actual = tableEnv.explainSql(
      "execute select * from MyTable where a > 10",
      ExplainDetail.CHANGELOG_MODE)
    val expected = TableTestUtil.readFromResource("/explain/testExplainSqlWithSelect.out")
    assertThat(replaceStageId(actual)).isEqualTo(replaceStageId(expected))
  }

  @Test
  @WithTableEnvironment
  def testExplainSqlWithInsert(tableEnv: TableEnvironment): Unit = {
    tableEnv.getConfig.set(CoreOptions.DEFAULT_PARALLELISM, Integer.valueOf(1))
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
    assertThat(tableResult1.getResultKind).isEqualTo(ResultKind.SUCCESS)

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
    assertThat(tableResult2.getResultKind).isEqualTo(ResultKind.SUCCESS)

    val actual = tableEnv.explainSql("insert into MySink select a, b from MyTable where a > 10")
    val expected = TableTestUtil.readFromResource("/explain/testExplainSqlWithInsert.out")
    assertThat(replaceStageId(actual)).isEqualTo(replaceStageId(expected))
  }

  @Test
  @WithTableEnvironment
  def testExecuteSqlWithExplainDetailsSelect(tableEnv: TableEnvironment): Unit = {
    tableEnv.getConfig.set(CoreOptions.DEFAULT_PARALLELISM, Integer.valueOf(1))
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
    assertThat(tableResult1.getResultKind).isEqualTo(ResultKind.SUCCESS)

    checkExplain(
      tableEnv,
      "explain changelog_mode, estimated_cost, json_execution_plan " +
        "select * from MyTable where a > 10",
      "/explain/testExecuteSqlWithExplainDetailsSelect.out"
    );
  }

  @Test
  @WithTableEnvironment
  def testExecuteSqlWithExplainDetailsAndUnion(tableEnv: TableEnvironment): Unit = {
    tableEnv.getConfig.set(CoreOptions.DEFAULT_PARALLELISM, Integer.valueOf(1))
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
    assertThat(tableResult1.getResultKind).isEqualTo(ResultKind.SUCCESS)

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
    assertThat(tableResult3.getResultKind).isEqualTo(ResultKind.SUCCESS)

    checkExplain(
      tableEnv,
      "explain changelog_mode, estimated_cost, json_execution_plan " +
        "select * from MyTable union all select * from MyTable2",
      "/explain/testExecuteSqlWithExplainDetailsAndUnion.out"
    )
  }

  @Test
  @WithTableEnvironment
  def testExecuteSqlWithExplainDetailsInsert(tableEnv: TableEnvironment): Unit = {
    tableEnv.getConfig.set(CoreOptions.DEFAULT_PARALLELISM, Integer.valueOf(1))
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
    assertThat(tableResult1.getResultKind).isEqualTo(ResultKind.SUCCESS)

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
    assertThat(tableResult2.getResultKind).isEqualTo(ResultKind.SUCCESS)

    checkExplain(
      tableEnv,
      "explain changelog_mode, estimated_cost, json_execution_plan " +
        "insert into MySink select a, b from MyTable where a > 10",
      "/explain/testExecuteSqlWithExplainDetailsInsert.out"
    )
  }

  @Test
  @WithTableEnvironment
  def testDescribeTableOrView(tableEnv: TableEnvironment): Unit = {
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
      Row.of(
        "f26",
        "ROW<`f0` INT NOT NULL, `f1` INT>",
        Boolean.box(false),
        "PRI(f24, f26)",
        null,
        null),
      Row.of("f27", "TIME(0)", Boolean.box(false), null, "AS LOCALTIME", null),
      Row.of("f28", "TIME(0)", Boolean.box(false), null, "AS CURRENT_TIME", null),
      Row.of("f29", "TIMESTAMP(3)", Boolean.box(false), null, "AS LOCALTIMESTAMP", null),
      Row.of("f30", "TIMESTAMP_LTZ(3)", Boolean.box(false), null, "AS CURRENT_TIMESTAMP", null),
      Row.of(
        "f31",
        "TIMESTAMP_LTZ(3)",
        Boolean.box(false),
        null,
        "AS CURRENT_ROW_TIMESTAMP()",
        null),
      Row.of(
        "ts",
        "TIMESTAMP(3) *ROWTIME*",
        Boolean.box(true),
        null,
        "AS TO_TIMESTAMP(`f25`)",
        "`ts` - INTERVAL '1' SECOND")
    )
    val tableResult1 = tableEnv.executeSql("describe T1")
    assertThat(tableResult1.getResultKind).isEqualTo(ResultKind.SUCCESS_WITH_CONTENT)
    assertThat[Row](CollectionUtil.iteratorToList(tableResult1.collect()))
      .containsExactlyInAnyOrderElementsOf(expectedResult1)

    val tableResult2 = tableEnv.executeSql("desc T1")
    assertThat(tableResult2.getResultKind).isEqualTo(ResultKind.SUCCESS_WITH_CONTENT)
    assertThat[Row](CollectionUtil.iteratorToList(tableResult2.collect()))
      .containsExactlyInAnyOrderElementsOf(expectedResult1)

    val expectedResult2 = util.Arrays.asList(
      Row.of("d", "INT", Boolean.box(false), null, null, null),
      Row.of("e", "STRING", Boolean.box(false), null, null, null),
      Row.of("f", "ROW<`f0` INT NOT NULL, `f1` INT>", Boolean.box(false), null, null, null)
    )
    val tableResult3 = tableEnv.executeSql("describe T2")
    assertThat(tableResult3.getResultKind).isEqualTo(ResultKind.SUCCESS_WITH_CONTENT)
    assertThat[Row](CollectionUtil.iteratorToList(tableResult3.collect()))
      .containsExactlyInAnyOrderElementsOf(expectedResult2)

    val tableResult4 = tableEnv.executeSql("desc T2")
    assertThat(tableResult4.getResultKind).isEqualTo(ResultKind.SUCCESS_WITH_CONTENT)
    assertThat[Row](CollectionUtil.iteratorToList(tableResult4.collect()))
      .containsExactlyInAnyOrderElementsOf(expectedResult2)

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
    assertThat(tableResult5.getResultKind).isEqualTo(ResultKind.SUCCESS_WITH_CONTENT)
    assertThat[Row](CollectionUtil.iteratorToList(tableResult5.collect()))
      .containsExactlyInAnyOrderElementsOf(expectedResult3)

    val tableResult6 = tableEnv.executeSql("desc T2")
    assertThat(tableResult6.getResultKind).isEqualTo(ResultKind.SUCCESS_WITH_CONTENT)
    assertThat[Row](CollectionUtil.iteratorToList(tableResult6.collect()))
      .containsExactlyInAnyOrderElementsOf(expectedResult3)
  }

  @Test
  @WithTableEnvironment
  def testTemporaryOperationListener(tableEnv: TableEnvironment): Unit = {
    val listener = new ListenerCatalog("listener_cat")
    val currentCat = tableEnv.getCurrentCatalog
    tableEnv.registerCatalog(listener.getName, listener)
    // test temporary table
    tableEnv.executeSql("create temporary table tbl1 (x int)")
    assertThat(listener.numTempTable).isEqualTo(0)
    tableEnv.executeSql(s"create temporary table ${listener.getName}.`default`.tbl1 (x int)")
    assertThat(listener.numTempTable).isEqualTo(1)
    val tableResult = tableEnv
      .asInstanceOf[TableEnvironmentInternal]
      .getCatalogManager
      .getTable(ObjectIdentifier.of(listener.getName, "default", "tbl1"))
    assertThat(tableResult).isPresent
    assertThat(tableResult.get().getTable[CatalogBaseTable].getComment)
      .isEqualTo(listener.tableComment)
    tableEnv.executeSql("drop temporary table tbl1")
    assertThat(listener.numTempTable).isEqualTo(1)
    tableEnv.executeSql(s"drop temporary table ${listener.getName}.`default`.tbl1")
    assertThat(listener.numTempTable).isEqualTo(0)
    tableEnv.useCatalog(listener.getName)
    tableEnv.executeSql("create temporary table tbl1 (x int)")
    assertThat(listener.numTempTable).isEqualTo(1)
    tableEnv.executeSql("drop temporary table tbl1")
    assertThat(listener.numTempTable).isEqualTo(0)
    tableEnv.useCatalog(currentCat)

    // test temporary view
    tableEnv.executeSql("create temporary view v1 as select 1")
    assertThat(listener.numTempTable).isEqualTo(0)
    tableEnv.executeSql(s"create temporary view ${listener.getName}.`default`.v1 as select 1")
    assertThat(listener.numTempTable).isEqualTo(1)
    val viewResult = tableEnv
      .asInstanceOf[TableEnvironmentInternal]
      .getCatalogManager
      .getTable(ObjectIdentifier.of(listener.getName, "default", "v1"))
    assertThat(viewResult).isPresent
    assertThat(viewResult.get().getTable[CatalogBaseTable].getComment)
      .isEqualTo(listener.tableComment)
    tableEnv.executeSql("drop temporary view v1")
    assertThat(listener.numTempTable).isEqualTo(1)
    tableEnv.executeSql(s"drop temporary view ${listener.getName}.`default`.v1")
    assertThat(listener.numTempTable).isEqualTo(0)
    tableEnv.useCatalog(listener.getName)
    tableEnv.executeSql("create temporary view v1 as select 1")
    assertThat(listener.numTempTable).isEqualTo(1)
    tableEnv.executeSql("drop temporary view  v1")
    assertThat(listener.numTempTable).isEqualTo(0)
    tableEnv.useCatalog(currentCat)

    // test temporary function
    val clzName = "foo.class.name"
    assertThatThrownBy(
      () => tableEnv.executeSql(s"create temporary function func1 as '$clzName'")).isNotNull
    assertThat(listener.numTempFunc).isEqualTo(0)
    tableEnv.executeSql(
      s"create temporary function ${listener.getName}.`default`.func1 as '$clzName'")
    assertThat(listener.numTempFunc).isEqualTo(1)
    tableEnv.executeSql("drop temporary function if exists func1")
    assertThat(listener.numTempFunc).isEqualTo(1)
    tableEnv.executeSql(s"drop temporary function ${listener.getName}.`default`.func1")
    assertThat(listener.numTempFunc).isEqualTo(0)
    tableEnv.useCatalog(listener.getName)
    tableEnv.executeSql(s"create temporary function func1 as '$clzName'")
    assertThat(listener.numTempFunc).isEqualTo(1)
    tableEnv.executeSql("drop temporary function func1")
    assertThat(listener.numTempFunc).isEqualTo(0)
    tableEnv.useCatalog(currentCat)

    listener.close()
  }

  @Test
  @WithTableEnvironment
  def testSetExecutionMode(tableEnv: TableEnvironment): Unit = {
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
      .hasMessageContaining(
        "Mismatch between configured runtime mode and actual runtime mode. " +
          "Currently, the 'execution.runtime-mode' can only be set when instantiating the " +
          "table environment. Subsequent changes are not supported. " +
          "Please instantiate a new TableEnvironment if necessary.")
  }

  private def validateShowModules(
      tableEnv: TableEnvironment,
      expectedEntries: (String, java.lang.Boolean)*): Unit = {
    val showModules = tableEnv.executeSql("SHOW MODULES")
    assertThat(showModules.getResultKind).isEqualTo(ResultKind.SUCCESS_WITH_CONTENT)
    assertThat(showModules.getResolvedSchema)
      .isEqualTo(ResolvedSchema.of(Column.physical("module name", DataTypes.STRING())))

    val showFullModules = tableEnv.executeSql("SHOW FULL MODULES")
    assertThat(showFullModules.getResultKind).isEqualTo(ResultKind.SUCCESS_WITH_CONTENT)
    assertThat(showFullModules.getResolvedSchema).isEqualTo(
      ResolvedSchema.physical(
        Array[String]("module name", "used"),
        Array[DataType](DataTypes.STRING(), DataTypes.BOOLEAN())))

    // show modules only list used modules
    assertThat[Row](CollectionUtil.iteratorToList(showModules.collect()))
      .containsExactlyInAnyOrderElementsOf(
        expectedEntries.filter(entry => entry._2).map(entry => Row.of(entry._1)).asJava
      )

    assertThat[Row](CollectionUtil.iteratorToList(showFullModules.collect()))
      .containsExactlyInAnyOrderElementsOf(
        expectedEntries.map(entry => Row.of(entry._1, entry._2)).asJava
      )
  }

  private def checkListModules(tableEnv: TableEnvironment, expected: String*): Unit = {
    val actual = tableEnv.listModules()
    for ((module, i) <- expected.zipWithIndex) {
      assertThat(actual.apply(i)).isEqualTo(module)
    }
  }

  private def checkListFullModules(
      tableEnv: TableEnvironment,
      expected: (String, java.lang.Boolean)*): Unit = {
    val actual = tableEnv.listFullModules()
    for ((elem, i) <- expected.zipWithIndex) {
      assertThat(actual.apply(i)).isEqualTo(new ModuleEntry(elem._1, elem._2))
    }
  }

  private def checkTableSource(
      tableEnv: TableEnvironment,
      tableName: String,
      expectToBeBounded: java.lang.Boolean): Unit = {
    val resolvedCatalogTable = tableEnv
      .getCatalog(tableEnv.getCurrentCatalog)
      .get()
      .getTable(ObjectPath.fromString(s"${tableEnv.getCurrentDatabase}.$tableName"))
    val context =
      new TableSourceFactoryContextImpl(
        ObjectIdentifier.of(tableEnv.getCurrentCatalog, tableEnv.getCurrentDatabase, tableName),
        resolvedCatalogTable.asInstanceOf[CatalogTable],
        new Configuration(),
        false)
    val source = TableFactoryUtil.findAndCreateTableSource(context)
    assertThat(source).isInstanceOf(classOf[CollectionTableSource])
    assertThat(source.asInstanceOf[CollectionTableSource].isBounded).isEqualTo(expectToBeBounded)
  }

  private def checkExplain(tableEnv: TableEnvironment, sql: String, resultPath: String): Unit = {
    val tableResult2 = tableEnv.executeSql(sql)
    assertThat(tableResult2.getResultKind).isEqualTo(ResultKind.SUCCESS_WITH_CONTENT)
    val it = tableResult2.collect()
    assertThat(it).hasNext
    val row = it.next()
    assertThat(row.getArity).isEqualTo(1)
    val actual = replaceNodeIdInOperator(replaceStreamNodeId(row.getField(0).toString.trim))
    val expected = replaceNodeIdInOperator(
      replaceStreamNodeId(TableTestUtil.readFromResource(resultPath).trim))

    assertThat(replaceStageId(actual)).isEqualTo(replaceStageId(expected))
    assertThat(it.hasNext).isFalse
  }

  class ListenerCatalog(name: String)
    extends GenericInMemoryCatalog(name)
    with TemporaryOperationListener {

    val tableComment: String = "listener_comment"
    val funcClzName: String = classOf[TestGenericUDF].getName

    var numTempTable = 0
    var numTempFunc = 0

    override def onCreateTemporaryTable(
        tablePath: ObjectPath,
        table: CatalogBaseTable): CatalogBaseTable = {
      numTempTable += 1
      if (table.isInstanceOf[CatalogTable]) {
        new CatalogTableImpl(table.getSchema, table.getOptions, tableComment)
      } else {
        val view = table.asInstanceOf[CatalogView]
        new CatalogViewImpl(
          view.getOriginalQuery,
          view.getExpandedQuery,
          view.getSchema,
          view.getOptions,
          tableComment)
      }
    }

    override def onDropTemporaryTable(tablePath: ObjectPath): Unit = numTempTable -= 1

    override def onCreateTemporaryFunction(
        functionPath: ObjectPath,
        function: CatalogFunction): CatalogFunction = {
      numTempFunc += 1
      new CatalogFunctionImpl(funcClzName, function.getFunctionLanguage)
    }

    override def onDropTemporaryFunction(functionPath: ObjectPath): Unit = numTempFunc -= 1
  }

}
