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
import org.apache.flink.configuration.{Configuration, CoreOptions, ExecutionOptions}
import org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches
import org.apache.flink.streaming.api.environment.{LocalStreamEnvironment, StreamExecutionEnvironment}
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.internal.TableEnvironmentInternal
import org.apache.flink.table.catalog._
import org.apache.flink.table.factories.{TableFactoryUtil, TableSourceFactoryContextImpl}
import org.apache.flink.table.functions.TestGenericUDF
import org.apache.flink.table.legacy.api.TableSchema
import org.apache.flink.table.module.ModuleEntry
import org.apache.flink.table.planner.factories.utils.TestCollectionTableFactory._
import org.apache.flink.table.planner.runtime.stream.sql.FunctionITCase.TestUDF
import org.apache.flink.table.planner.runtime.stream.table.FunctionITCase.SimpleScalarFunction
import org.apache.flink.table.planner.runtime.utils.StreamingEnvUtil
import org.apache.flink.table.planner.utils.{TableTestUtil, TestTableSourceSinks}
import org.apache.flink.table.planner.utils.TableTestUtil.{replaceNodeIdInOperator, replaceStageId, replaceStreamNodeId}
import org.apache.flink.table.types.DataType
import org.apache.flink.table.utils.UserDefinedFunctions.{GENERATED_LOWER_UDF_CLASS, GENERATED_LOWER_UDF_CODE}
import org.apache.flink.testutils.junit.utils.TempDirUtils
import org.apache.flink.types.Row
import org.apache.flink.util.UserClassLoaderJarTestUtils

import _root_.java.util
import _root_.scala.collection.JavaConverters._
import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.sql.SqlExplainLevel
import org.assertj.core.api.Assertions.{assertThat, assertThatThrownBy}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue, fail}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

import java.io.File
import java.nio.file.Path
import java.util.{Collections, UUID}

class TableEnvironmentTest {

  @TempDir
  var tempFolder: Path = _

  val env = new LocalStreamEnvironment()
  val tableEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)
  val batchTableEnv = StreamTableEnvironment.create(env, TableTestUtil.BATCH_SETTING)

  @Test
  def testScanNonExistTable(): Unit = {
    assertThatThrownBy(() => tableEnv.from("MyTable"))
      .hasMessageContaining("Table `MyTable` was not found")
      .isInstanceOf[ValidationException]
  }

  @Test
  def testRegisterDataStream(): Unit = {
    val table = StreamingEnvUtil
      .fromElements[(Int, Long, String, Boolean)](env)
      .toTable(tableEnv, 'a, 'b, 'c, 'd)
    tableEnv.createTemporaryView("MyTable", table)
    val scanTable = tableEnv.from("MyTable")
    val relNode = TableTestUtil.toRelNode(scanTable)
    val actual = RelOptUtil.toString(relNode)
    val expected = "LogicalTableScan(table=[[default_catalog, default_database, MyTable]])\n"
    assertEquals(expected, actual)

    assertThatThrownBy(
      () =>
        tableEnv.createTemporaryView(
          "MyTable",
          StreamingEnvUtil
            .fromElements[(Int, Long)](env)))
      .hasMessageContaining(
        "Temporary table '`default_catalog`.`default_database`.`MyTable`' already exists")
      .isInstanceOf[ValidationException]
  }

  @Test
  def testSimpleQuery(): Unit = {
    val table = StreamingEnvUtil
      .fromElements[(Int, Long, String, Boolean)](env)
      .toTable(tableEnv, 'a, 'b, 'c, 'd)
    tableEnv.createTemporaryView("MyTable", table)
    val queryTable = tableEnv.sqlQuery("SELECT a, c, d FROM MyTable")
    val relNode = TableTestUtil.toRelNode(queryTable)
    val actual = RelOptUtil.toString(relNode, SqlExplainLevel.NO_ATTRIBUTES)
    val expected = "LogicalProject\n" +
      "  LogicalTableScan\n"
    assertEquals(expected, actual)
  }

  @Test
  def testCreateTableWithEnforcedMode(): Unit = {
    // check column constraint
    assertThatThrownBy(() => tableEnv.executeSql("""
                                                   |CREATE TABLE MyTable (
                                                   |  a bigint primary key,
                                                   |  b int,
                                                   |  c varchar
                                                   |) with (
                                                   |  'connector' = 'COLLECTION',
                                                   |  'is-bounded' = 'false'
                                                   |)
    """.stripMargin))
      .hasMessageContaining("Flink doesn't support ENFORCED mode for PRIMARY KEY constraint.")
      .isInstanceOf[ValidationException]

    // check table constraint
    assertThatThrownBy(() => tableEnv.executeSql("""
                                                   |CREATE TABLE MyTable (
                                                   |  a bigint,
                                                   |  b int,
                                                   |  c varchar,
                                                   |  primary key(a)
                                                   |) with (
                                                   |  'connector' = 'COLLECTION',
                                                   |  'is-bounded' = 'false'
                                                   |)
    """.stripMargin))
      .hasMessageContaining("Flink doesn't support ENFORCED mode for PRIMARY KEY constraint.")
      .isInstanceOf[ValidationException]
  }

  @Test
  def testStreamTableEnvironmentExplain(): Unit = {
    val execEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(execEnv, settings)

    TestTableSourceSinks.createPersonCsvTemporaryTable(tEnv, "MyTable")

    TestTableSourceSinks.createCsvTemporarySinkTable(
      tEnv,
      new TableSchema(Array("first"), Array(STRING)),
      "MySink",
      -1)

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
      tEnv,
      new TableSchema(Array("first"), Array(STRING)),
      "MySink",
      -1)

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
    val configuration = new Configuration()
    configuration.set(CoreOptions.DEFAULT_PARALLELISM, Integer.valueOf(4))
    val settings =
      EnvironmentSettings.newInstance().inStreamingMode().withConfiguration(configuration).build()
    val tEnv = StreamTableEnvironment.create(execEnv, settings)

    verifyTableEnvironmentExecutionExplain(tEnv)
  }

  @Test
  def testAddJarWithFullPath(): Unit = {
    validateAddJar(true)
  }

  @Test
  def testAddJarWithRelativePath(): Unit = {
    validateAddJar(false)
  }

  @Test
  def testAddIllegalJar(): Unit = {
    try {
      tableEnv.executeSql(String.format("ADD JAR '%s'", "/path/to/illegal.jar"))
      fail("Should fail.")
    } catch {
      case _: TableException => // expected
    }
  }

  private def validateAddJar(useFullPath: Boolean): Unit = {
    val udfJar = UserClassLoaderJarTestUtils
      .createJarFile(
        TempDirUtils.newFolder(tempFolder, String.format("test-jar-%s", UUID.randomUUID)),
        "test-classloader-udf.jar",
        GENERATED_LOWER_UDF_CLASS,
        String.format(GENERATED_LOWER_UDF_CODE, GENERATED_LOWER_UDF_CLASS)
      )

    val jarPath = if (useFullPath) {
      udfJar.getPath
    } else {
      new File(".").getCanonicalFile.toPath.relativize(udfJar.toPath).toString
    }

    tableEnv.executeSql(String.format("ADD JAR '%s'", jarPath))
    val tableResult = tableEnv.executeSql("SHOW JARS")

    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult.getResultKind)
    checkData(util.Arrays.asList(Row.of(udfJar.getPath)).iterator(), tableResult.collect())
  }

  private def verifyTableEnvironmentExecutionExplain(tEnv: TableEnvironment): Unit = {
    TestTableSourceSinks.createPersonCsvTemporaryTable(tEnv, "MyTable")

    TestTableSourceSinks.createCsvTemporarySinkTable(
      tEnv,
      new TableSchema(Array("first"), Array(STRING)),
      "MySink",
      -1)

    val expected =
      TableTestUtil.readFromResource("/explain/testStreamTableEnvironmentExecutionExplain.out")
    val actual = tEnv.explainSql(
      "insert into MySink select first from MyTable",
      ExplainDetail.JSON_EXECUTION_PLAN)

    assertEquals(
      TableTestUtil.replaceNodeIdInOperator(TableTestUtil.replaceStreamNodeId(expected)),
      TableTestUtil.replaceNodeIdInOperator(TableTestUtil.replaceStreamNodeId(actual))
    )
  }

  @Test
  def testStatementSetExecutionExplain(): Unit = {
    val execEnv = StreamExecutionEnvironment.getExecutionEnvironment
    execEnv.setParallelism(1)
    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(execEnv, settings)

    TestTableSourceSinks.createPersonCsvTemporaryTable(tEnv, "MyTable")

    TestTableSourceSinks.createCsvTemporarySinkTable(
      tEnv,
      new TableSchema(Array("first"), Array(STRING)),
      "MySink",
      -1)

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
      tEnv,
      new TableSchema(Array("first"), Array(STRING)),
      "MySink",
      -1)

    val expected =
      TableTestUtil.readFromResource("/explain/testStatementSetExecutionExplain.out")

    val actual = tEnv.explainSql(
      "execute statement set begin " +
        "insert into MySink select last from MyTable; " +
        "insert into MySink select first from MyTable; end",
      ExplainDetail.JSON_EXECUTION_PLAN
    )

    assertEquals(
      TableTestUtil.replaceNodeIdInOperator(TableTestUtil.replaceStreamNodeId(expected)),
      TableTestUtil.replaceNodeIdInOperator(TableTestUtil.replaceStreamNodeId(actual))
    )
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
    assertThatThrownBy(() => tableEnv.executeSql("ALTER TABLE MyTable RESET ()"))
      .hasMessageContaining("ALTER TABLE RESET does not support empty key")
      .isInstanceOf[ValidationException]
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
      .hasMessageContaining(
        "Unable to create a source for reading table " +
          "'default_catalog.default_database.MyTable'.\n\n" +
          "Table options are:\n\n'connector'='datagen'\n" +
          "'invalid-key'='invalid-value'")

    // remove invalid key by RESET
    val alterTableResetStatement = "ALTER TABLE MyTable RESET ('invalid-key')"
    val tableResult = tableEnv.executeSql(alterTableResetStatement)
    assertEquals(ResultKind.SUCCESS, tableResult.getResultKind)
    assertEquals(
      Map("connector" -> "datagen").asJava,
      tableEnv
        .getCatalog(tableEnv.getCurrentCatalog)
        .get()
        .getTable(ObjectPath.fromString(s"${tableEnv.getCurrentDatabase}.MyTable"))
        .getOptions
    )
    assertEquals(
      ResultKind.SUCCESS_WITH_CONTENT,
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
      tableEnv
        .getCatalog(tableEnv.getCurrentCatalog)
        .get()
        .getTable(ObjectPath.fromString(s"${tableEnv.getCurrentDatabase}.MyTable"))
        .getOptions
    )
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
      tableEnv
        .getCatalog(tableEnv.getCurrentCatalog)
        .get()
        .getTable(ObjectPath.fromString(s"${tableEnv.getCurrentDatabase}.MyTable"))
        .getOptions
    )

    assertThatThrownBy(
      () =>
        tableEnv.executeSql("explain plan for select * from MyTable where a > 10").getResultKind)
      .hasMessageContaining(
        "Unable to create a source for reading table 'default_catalog.default_database.MyTable'.")
      .isInstanceOf[ValidationException]
  }

  @Test
  def testAlterTableAddColumn(): Unit = {
    val statement =
      """
        |CREATE TABLE MyTable (
        |  a BIGINT,
        |  b INT PRIMARY KEY NOT ENFORCED,
        |  c STRING METADATA VIRTUAL,
        |  d TIMESTAMP(3)
        |) WITH (
        |  'connector' = 'COLLECTION',
        |  'is-bounded' = 'false'
        |)
      """.stripMargin
    tableEnv.executeSql(statement)

    assertThatThrownBy(() => tableEnv.executeSql("""
                                                   |ALTER TABLE MyTable ADD (
                                                   |  PRIMARY KEY (a) NOT ENFORCED
                                                   |)
                                                   |""".stripMargin))
      .hasMessageContaining(
        """Failed to execute ALTER TABLE statement.
          |The base table has already defined the primary key constraint [`b`]. You might want to drop it before adding a new one.""".stripMargin)
      .isInstanceOf[ValidationException]

    assertThatThrownBy(() => tableEnv.executeSql("""
                                                   |ALTER TABLE MyTable ADD (
                                                   |  a STRING
                                                   |)
                                                   |""".stripMargin))
      .hasMessageContaining(
        """Failed to execute ALTER TABLE statement.
          |Try to add a column `a` which already exists in the table.""".stripMargin)
      .isInstanceOf[ValidationException]

    assertThatThrownBy(() => tableEnv.executeSql("""
                                                   |ALTER TABLE MyTable ADD (
                                                   |  e STRING AFTER h
                                                   |)
                                                   |""".stripMargin))
      .hasMessageContaining(
        """Failed to execute ALTER TABLE statement.
          |Referenced column `h` by 'AFTER' does not exist in the table.""".stripMargin
      )
      .isInstanceOf[ValidationException]

    tableEnv.executeSql(
      """
        |ALTER TABLE MyTable ADD (
        |  e AS UPPER(c) FIRST,
        |  WATERMARK FOR d AS d - INTERVAL '2' SECOND,
        |  f DOUBLE NOT NULL COMMENT 'f is double' AFTER e,
        |  g ARRAY<INT NOT NULL> NOT NULL,
        |  h MAP<STRING NOT NULL, INT NOT NULL> NOT NULL COMMENT 'a map' after c,
        |  j ROW<j1 ROW<j11 STRING, j12 INT>, j2 MULTISET<DOUBLE NOT NULL>> NOT NULL FIRST
        |)
        |""".stripMargin)

    val expectedResult = util.Arrays.asList(
      Row.of(
        "j",
        "ROW<`j1` ROW<`j11` STRING, `j12` INT>, `j2` MULTISET<DOUBLE NOT NULL>>",
        Boolean.box(false),
        null,
        null,
        null,
        null),
      Row.of("e", "STRING", Boolean.box(true), null, "AS UPPER(`c`)", null, null),
      Row.of("f", "DOUBLE", Boolean.box(false), null, null, null, "f is double"),
      Row.of("a", "BIGINT", Boolean.box(true), null, null, null, null),
      Row.of("b", "INT", Boolean.box(false), "PRI(b)", null, null, null),
      Row.of("c", "STRING", Boolean.box(true), null, "METADATA VIRTUAL", null, null),
      Row.of(
        "h",
        "MAP<STRING NOT NULL, INT NOT NULL>",
        Boolean.box(false),
        null,
        null,
        null,
        "a map"),
      Row.of(
        "d",
        "TIMESTAMP(3) *ROWTIME*",
        Boolean.box(true),
        null,
        null,
        "`d` - INTERVAL '2' SECOND",
        null),
      Row.of("g", "ARRAY<INT NOT NULL>", Boolean.box(false), null, null, null, null)
    )
    val tableResult = tableEnv.executeSql("DESCRIBE MyTable")
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult.getResultKind)
    checkData(expectedResult.iterator(), tableResult.collect())

    assertThatThrownBy(() => tableEnv.executeSql("ALTER TABLE MyTable ADD WATERMARK FOR ts AS ts"))
      .hasMessageContaining(
        """Failed to execute ALTER TABLE statement.
          |The base table has already defined the watermark strategy `d` AS `d` - INTERVAL '2' SECOND. You might want to drop it before adding a new one.""".stripMargin)
      .isInstanceOf[ValidationException]
  }

  @Test
  def testAlterTableAddConstraint(): Unit = {
    val statement =
      """
        |CREATE TABLE MyTable (
        |  a BIGINT,
        |  b INT,
        |  c STRING METADATA VIRTUAL,
        |  d TIMESTAMP(3)
        |) WITH (
        |  'connector' = 'COLLECTION',
        |  'is-bounded' = 'false'
        |)
      """.stripMargin
    tableEnv.executeSql(statement)

    assertThatThrownBy(() => tableEnv.executeSql("""
                                                   |ALTER TABLE MyTable ADD (
                                                   |  f STRING PRIMARY KEY NOT ENFORCED,
                                                   |  PRIMARY KEY (a) NOT ENFORCED
                                                   |)
                                                   |""".stripMargin))
      .hasMessageContaining("Duplicate primary key definition")
      .isInstanceOf[org.apache.flink.sql.parser.error.SqlValidateException]

    assertThatThrownBy(() => tableEnv.executeSql("""
                                                   |ALTER TABLE MyTable ADD (
                                                   |  PRIMARY KEY (c) NOT ENFORCED
                                                   |)
                                                   |""".stripMargin))
      .hasMessageContaining(
        """Failed to execute ALTER TABLE statement.
          |Invalid primary key 'PK_c'. Column 'c' is not a physical column.""".stripMargin)
      .isInstanceOf[ValidationException]

    tableEnv.executeSql("""
                          |ALTER TABLE MyTable ADD (
                          |  CONSTRAINT my_ct PRIMARY KEY(b) NOT ENFORCED
                          |)
                          |""".stripMargin)

    val expectedResult = util.Arrays.asList(
      Row.of("a", "BIGINT", Boolean.box(true), null, null, null),
      Row.of("b", "INT", Boolean.box(false), "PRI(b)", null, null),
      Row.of("c", "STRING", Boolean.box(true), null, "METADATA VIRTUAL", null),
      Row.of("d", "TIMESTAMP(3)", Boolean.box(true), null, null, null)
    )
    val tableResult = tableEnv.executeSql("DESCRIBE MyTable")
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult.getResultKind)
    checkData(expectedResult.iterator(), tableResult.collect())
  }

  @Test
  def testAlterTableAddWatermark(): Unit = {
    val statement =
      """
        |CREATE TABLE MyTable (
        |  a BIGINT,
        |  b INT,
        |  c STRING METADATA VIRTUAL,
        |  d TIMESTAMP(3),
        |  e ROW<e0 STRING, e1 TIMESTAMP(3)>
        |) WITH (
        |  'connector' = 'COLLECTION',
        |  'is-bounded' = 'false'
        |)
      """.stripMargin
    tableEnv.executeSql(statement)

    assertThatThrownBy(() => tableEnv.executeSql("""
                                                   |ALTER TABLE MyTable ADD (
                                                   |  WATERMARK FOR e.e1 AS e.e1
                                                   |)
                                                   |""".stripMargin))
      .hasMessageContaining(
        """Failed to execute ALTER TABLE statement.
          |Watermark strategy on nested column is not supported yet.""".stripMargin)
      .isInstanceOf[ValidationException]

    tableEnv.executeSql("""
                          |ALTER TABLE MyTable ADD (
                          |  WATERMARK FOR d AS d
                          |)
                          |""".stripMargin)

    val expectedResult = util.Arrays.asList(
      Row.of("a", "BIGINT", Boolean.box(true), null, null, null),
      Row.of("b", "INT", Boolean.box(true), null, null, null),
      Row.of("c", "STRING", Boolean.box(true), null, "METADATA VIRTUAL", null),
      Row.of("d", "TIMESTAMP(3) *ROWTIME*", Boolean.box(true), null, null, "`d`"),
      Row.of("e", "ROW<`e0` STRING, `e1` TIMESTAMP(3)>", Boolean.box(true), null, null, null)
    )
    val tableResult = tableEnv.executeSql("DESCRIBE MyTable")
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult.getResultKind)
    checkData(expectedResult.iterator(), tableResult.collect())
  }

  @Test
  def testAlterTableModifyColumn(): Unit = {
    val statement =
      """
        |CREATE TABLE MyTable (
        |  a BIGINT,
        |  b INT,
        |  c STRING METADATA VIRTUAL,
        |  d STRING,
        |  e AS a * 2 + b,
        |  ts1 TIMESTAMP(3),
        |  ts2 TIMESTAMP(3),
        |  PRIMARY KEY(d, b) NOT ENFORCED,
        |  WATERMARK FOR ts2 AS ts2 - INTERVAL '1' SECOND
        |) WITH (
        |  'connector' = 'COLLECTION',
        |  'is-bounded' = 'false'
        |)
      """.stripMargin
    tableEnv.executeSql(statement)

    assertThatThrownBy(() => tableEnv.executeSql("""
                                                   |ALTER TABLE MyTable MODIFY (
                                                   |  x STRING FIRST
                                                   |)
                                                   |""".stripMargin))
      .hasMessageContaining(
        """Failed to execute ALTER TABLE statement.
          |Try to modify a column `x` which does not exist in the table.""".stripMargin)
      .isInstanceOf[ValidationException]

    assertThatThrownBy(() => tableEnv.executeSql("""
                                                   |ALTER TABLE MyTable MODIFY (
                                                   |  b INT FIRST,
                                                   |  a BIGINT AFTER x
                                                   |)
                                                   |""".stripMargin))
      .hasMessageContaining(
        """Failed to execute ALTER TABLE statement.
          |Referenced column `x` by 'AFTER' does not exist in the table.""".stripMargin)
      .isInstanceOf[ValidationException]

    assertThatThrownBy(() => tableEnv.executeSql("""
                                                   |ALTER TABLE MyTable MODIFY (
                                                   |  b BOOLEAN first
                                                   |)
                                                   |""".stripMargin))
      .hasMessageContaining("""Failed to execute ALTER TABLE statement.
                              |Invalid expression for computed column 'e'.""".stripMargin)
      .isInstanceOf[ValidationException]

    tableEnv.executeSql("""
                          |ALTER TABLE MyTable MODIFY (
                          |  b BOOLEAN first,
                          |  e AS a * 2
                          |)
                          |""".stripMargin)
    val expectedResult1 = util.Arrays.asList(
      Row.of("b", "BOOLEAN", Boolean.box(false), "PRI(d, b)", null, null),
      Row.of("a", "BIGINT", Boolean.box(true), null, null, null),
      Row.of("c", "STRING", Boolean.box(true), null, "METADATA VIRTUAL", null),
      Row.of("d", "STRING", Boolean.box(false), "PRI(d, b)", null, null),
      Row.of("e", "BIGINT", Boolean.box(true), null, "AS `a` * 2", null),
      Row.of("ts1", "TIMESTAMP(3)", Boolean.box(true), null, null, null),
      Row.of(
        "ts2",
        "TIMESTAMP(3) *ROWTIME*",
        Boolean.box(true),
        null,
        null,
        "`ts2` - INTERVAL '1' SECOND")
    )
    val tableResult1 = tableEnv.executeSql("DESCRIBE MyTable")
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult1.getResultKind)
    checkData(expectedResult1.iterator(), tableResult1.collect())

    tableEnv.executeSql("""
                          |ALTER TABLE MyTable MODIFY (
                          |  e AS UPPER(c) FIRST,
                          |  WATERMARK FOR ts1 AS ts1,
                          |  a DOUBLE NOT NULL AFTER ts2,
                          |  PRIMARY KEY(d) NOT ENFORCED
                          |)
                          |""".stripMargin)

    val expectedResult2 = util.Arrays.asList(
      Row.of("e", "STRING", Boolean.box(true), null, "AS UPPER(`c`)", null),
      Row.of("b", "BOOLEAN", Boolean.box(false), null, null, null),
      Row.of("c", "STRING", Boolean.box(true), null, "METADATA VIRTUAL", null),
      Row.of("d", "STRING", Boolean.box(false), "PRI(d)", null, null),
      Row.of("ts1", "TIMESTAMP(3) *ROWTIME*", Boolean.box(true), null, null, "`ts1`"),
      Row.of("ts2", "TIMESTAMP(3)", Boolean.box(true), null, null, null),
      Row.of("a", "DOUBLE", Boolean.box(false), null, null, null)
    )
    val tableResult = tableEnv.executeSql("DESCRIBE MyTable")
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult.getResultKind)
    checkData(expectedResult2.iterator(), tableResult.collect())
  }

  @Test
  def testAlterTableModifyConstraint(): Unit = {
    val statement =
      """
        |CREATE TABLE MyTable (
        |  a BIGINT,
        |  b INT,
        |  c STRING METADATA VIRTUAL
        |) WITH (
        |  'connector' = 'COLLECTION',
        |  'is-bounded' = 'false'
        |)
      """.stripMargin
    tableEnv.executeSql(statement)

    assertThatThrownBy(() => tableEnv.executeSql("""
                                                   |ALTER TABLE MyTable MODIFY (
                                                   |  PRIMARY KEY (x) NOT ENFORCED
                                                   |)
                                                   |""".stripMargin))
      .hasMessageContaining(
        """Failed to execute ALTER TABLE statement.
          |The base table does not define any primary key constraint. You might want to add a new one.""".stripMargin)
      .isInstanceOf[ValidationException]

    tableEnv.executeSql("""
                          |ALTER TABLE MyTable ADD (
                          |  PRIMARY KEY(a) NOT ENFORCED
                          |)
                          |""".stripMargin)

    tableEnv.executeSql("""
                          |ALTER TABLE MyTable MODIFY (
                          |  PRIMARY KEY(b) NOT ENFORCED
                          |)
                          |""".stripMargin)
    val expectedResult = util.Arrays.asList(
      Row.of("a", "BIGINT", Boolean.box(false), null, null, null),
      Row.of("b", "INT", Boolean.box(false), "PRI(b)", null, null),
      Row.of("c", "STRING", Boolean.box(true), null, "METADATA VIRTUAL", null)
    )
    val tableResult = tableEnv.executeSql("DESCRIBE MyTable")
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult.getResultKind)
    checkData(expectedResult.iterator(), tableResult.collect())
  }

  @Test
  def testAlterTableModifyWatermark(): Unit = {
    val statement =
      """
        |CREATE TABLE MyTable (
        |  a BIGINT,
        |  b INT,
        |  d TIMESTAMP(3),
        |  e ROW<e0 STRING, e1 TIMESTAMP(3)>,
        |  WATERMARK FOR d AS d - INTERVAL '1' MINUTE
        |) WITH (
        |  'connector' = 'COLLECTION',
        |  'is-bounded' = 'false'
        |)
      """.stripMargin
    tableEnv.executeSql(statement)

    assertThatThrownBy(() => tableEnv.executeSql("""
                                                   |ALTER TABLE MyTable MODIFY (
                                                   |  WATERMARK FOR e.e1 AS e.e1
                                                   |)
                                                   |""".stripMargin))
      .hasMessageContaining(
        """Failed to execute ALTER TABLE statement.
          |Watermark strategy on nested column is not supported yet.""".stripMargin)
      .isInstanceOf[ValidationException]

    tableEnv.executeSql("""
                          |ALTER TABLE MyTable MODIFY (
                          |  WATERMARK FOR d AS d
                          |)
                          |""".stripMargin)

    val expectedResult = util.Arrays.asList(
      Row.of("a", "BIGINT", Boolean.box(true), null, null, null),
      Row.of("b", "INT", Boolean.box(true), null, null, null),
      Row.of("d", "TIMESTAMP(3) *ROWTIME*", Boolean.box(true), null, null, "`d`"),
      Row.of("e", "ROW<`e0` STRING, `e1` TIMESTAMP(3)>", Boolean.box(true), null, null, null)
    )
    val tableResult = tableEnv.executeSql("DESCRIBE MyTable")
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult.getResultKind)
    checkData(expectedResult.iterator(), tableResult.collect())
  }

  @Test
  def testAlterRenameColumn(): Unit = {
    tableEnv.executeSql("""
                          |CREATE TABLE MyTable (
                          | a bigint
                          |) WITH (
                          |  'connector' = 'COLLECTION',
                          |  'is-bounded' = 'false'
                          |)
                          |""".stripMargin)
    tableEnv.executeSql("""
                          |ALTER TABLE MyTable RENAME a TO b
                          |""".stripMargin)
    val tableResult = tableEnv.executeSql("DESCRIBE MyTable")
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult.getResultKind)
    checkData(
      Collections
        .singletonList(Row.of("b", "BIGINT", Boolean.box(true), null, null, null))
        .iterator(),
      tableResult.collect())
  }

  @Test
  def testAlterTableDropColumn(): Unit = {
    val statement =
      """
        |CREATE TABLE MyTable (
        |  a BIGINT,
        |  b INT,
        |  d TIMESTAMP(3),
        |  e ROW<e0 STRING, e1 TIMESTAMP(3)>,
        |  WATERMARK FOR d AS d - INTERVAL '1' MINUTE
        |) WITH (
        |  'connector' = 'COLLECTION',
        |  'is-bounded' = 'false'
        |)
      """.stripMargin
    tableEnv.executeSql(statement)

    tableEnv.executeSql("ALTER TABLE MyTable DROP (e, a)")

    val expectedResult = util.Arrays.asList(
      Row.of("b", "INT", Boolean.box(true), null, null, null),
      Row.of(
        "d",
        "TIMESTAMP(3) *ROWTIME*",
        Boolean.box(true),
        null,
        null,
        "`d` - INTERVAL '1' MINUTE")
    )
    val tableResult = tableEnv.executeSql("DESCRIBE MyTable")
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult.getResultKind)
    checkData(expectedResult.iterator(), tableResult.collect())
  }

  @Test
  def testAlterTableDropConstraint(): Unit = {
    val statement =
      """
        |CREATE TABLE MyTable (
        |  a BIGINT,
        |  b INT,
        |  d TIMESTAMP(3),
        |  e ROW<e0 STRING, e1 TIMESTAMP(3)>,
        |  CONSTRAINT ct PRIMARY KEY(a, b) NOT ENFORCED
        |) WITH (
        |  'connector' = 'COLLECTION',
        |  'is-bounded' = 'false'
        |)
      """.stripMargin
    tableEnv.executeSql(statement)

    tableEnv.executeSql("ALTER TABLE MyTable DROP CONSTRAINT ct")

    val expectedResult = util.Arrays.asList(
      Row.of("a", "BIGINT", Boolean.box(false), null, null, null),
      Row.of("b", "INT", Boolean.box(false), null, null, null),
      Row.of("d", "TIMESTAMP(3)", Boolean.box(true), null, null, null),
      Row.of("e", "ROW<`e0` STRING, `e1` TIMESTAMP(3)>", Boolean.box(true), null, null, null)
    )
    val tableResult1 = tableEnv.executeSql("DESCRIBE MyTable")
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult1.getResultKind)
    checkData(expectedResult.iterator(), tableResult1.collect())

    tableEnv.executeSql("ALTER TABLE MyTable ADD CONSTRAINT ct PRIMARY KEY(a) NOT ENFORCED")
    tableEnv.executeSql("ALTER TABLE MyTable DROP PRIMARY KEY")
    val tableResult2 = tableEnv.executeSql("DESCRIBE MyTable")
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult2.getResultKind)
    checkData(expectedResult.iterator(), tableResult2.collect())
  }

  @Test
  def testAlterTableDropWatermark(): Unit = {
    val statement =
      """
        |CREATE TABLE MyTable (
        |  a BIGINT,
        |  b INT,
        |  d TIMESTAMP(3),
        |  e ROW<e0 STRING, e1 TIMESTAMP(3)>,
        |  WATERMARK FOR d AS d - INTERVAL '1' MINUTE
        |) WITH (
        |  'connector' = 'COLLECTION',
        |  'is-bounded' = 'false'
        |)
      """.stripMargin
    tableEnv.executeSql(statement)

    tableEnv.executeSql("ALTER TABLE MyTable DROP WATERMARK")

    val expectedResult = util.Arrays.asList(
      Row.of("a", "BIGINT", Boolean.box(true), null, null, null),
      Row.of("b", "INT", Boolean.box(true), null, null, null),
      Row.of("d", "TIMESTAMP(3)", Boolean.box(true), null, null, null),
      Row.of("e", "ROW<`e0` STRING, `e1` TIMESTAMP(3)>", Boolean.box(true), null, null, null)
    )
    val tableResult = tableEnv.executeSql("DESCRIBE MyTable")
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult.getResultKind)
    checkData(expectedResult.iterator(), tableResult.collect())
  }

  @Test
  def testAlterTableDropPartitions(): Unit = {
    val createTableStmt =
      """
        |CREATE TABLE tbl (
        |  a INT,
        |  b BIGINT,
        |  c DATE
        |) PARTITIONED BY (b, c)
        |WITH (
        |  'connector' = 'COLLECTION'
        |)
          """.stripMargin
    tableEnv.executeSql(createTableStmt)

    val catalog = tableEnv.getCatalog(tableEnv.getCurrentCatalog).get()
    val spec1 = new CatalogPartitionSpec(Map("b" -> "1000", "c" -> "2020-05-01").asJava)
    val part1 = new CatalogPartitionImpl(Map("k1" -> "v1").asJava, "")
    val spec2 = new CatalogPartitionSpec(Map("b" -> "2000", "c" -> "2020-01-01").asJava)
    val part2 = new CatalogPartitionImpl(Map("k1" -> "v1").asJava, "")
    val tablePath = new ObjectPath("default_database", "tbl")
    // create partition
    catalog.createPartition(tablePath, spec1, part1, false)
    catalog.createPartition(tablePath, spec2, part2, false)

    // check the partitions
    assertThat(catalog.listPartitions(tablePath).toString)
      .isEqualTo(
        "[CatalogPartitionSpec{{b=1000, c=2020-05-01}}," +
          " CatalogPartitionSpec{{b=2000, c=2020-01-01}}]")

    // drop the partitions
    var tableResult =
      tableEnv.executeSql("alter table tbl drop partition(b='1000', c ='2020-05-01')")
    assertEquals(ResultKind.SUCCESS, tableResult.getResultKind)
    assertThat(catalog.listPartitions(tablePath).toString)
      .isEqualTo("[CatalogPartitionSpec{{b=2000, c=2020-01-01}}]")

    // drop the partition again with if exists
    tableResult =
      tableEnv.executeSql("alter table tbl drop if exists partition(b='1000', c='2020-05-01')")
    assertEquals(ResultKind.SUCCESS, tableResult.getResultKind)
    assertThat(catalog.listPartitions(tablePath).toString)
      .isEqualTo("[CatalogPartitionSpec{{b=2000, c=2020-01-01}}]")

    // drop the partition again without if exists,
    // should throw exception then
    assertThatThrownBy(
      () => tableEnv.executeSql("alter table tbl drop partition (b=1000,c='2020-05-01')"))
      .isInstanceOf(classOf[TableException])
      .hasMessageContaining("Could not execute ALTER TABLE default_catalog.default_database.tbl" +
        " DROP PARTITION (b=1000, c=2020-05-01)")
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

    assertThatThrownBy(() => tableEnv.executeSql("alter table MyTable compact"))
      .hasMessageContaining("ALTER TABLE COMPACT operation is not supported for " +
        "non-managed table `default_catalog`.`default_database`.`MyTable`")
      .isInstanceOf[ValidationException]
  }

  @Test
  def testQueryViewWithHints(): Unit = {
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

    assertThatThrownBy(() => tableEnv.executeSql("alter table MyTable compact"))
      .hasMessageContaining("Compact managed table only works under batch mode.")
      .isInstanceOf[ValidationException]
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
    assertTrue(
      tableEnv
        .getCatalog(tableEnv.getCurrentCatalog)
        .get()
        .tableExists(ObjectPath.fromString(s"${tableEnv.getCurrentDatabase}.tbl1")))

    val tableResult2 = tableEnv.executeSql("ALTER TABLE tbl1 SET ('k1' = 'a', 'k2' = 'b')")
    assertEquals(ResultKind.SUCCESS, tableResult2.getResultKind)
    assertEquals(
      Map("connector" -> "COLLECTION", "is-bounded" -> "false", "k1" -> "a", "k2" -> "b").asJava,
      tableEnv
        .getCatalog(tableEnv.getCurrentCatalog)
        .get()
        .getTable(ObjectPath.fromString(s"${tableEnv.getCurrentDatabase}.tbl1"))
        .getOptions
    )

    val tableResult3 = tableEnv.executeSql("DROP TABLE tbl1")
    assertEquals(ResultKind.SUCCESS, tableResult3.getResultKind)
    assertFalse(
      tableEnv
        .getCatalog(tableEnv.getCurrentCatalog)
        .get()
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
    assertTrue(
      tableEnv
        .getCatalog(tableEnv.getCurrentCatalog)
        .get()
        .tableExists(ObjectPath.fromString(s"${tableEnv.getCurrentDatabase}.tbl1")))

    val tableResult3 = tableEnv.executeSql("DROP TABLE IF EXISTS tbl1")
    assertEquals(ResultKind.SUCCESS, tableResult3.getResultKind)
    assertFalse(
      tableEnv
        .getCatalog(tableEnv.getCurrentCatalog)
        .get()
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

  @Test
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
    assertThatThrownBy(() => tableEnv.executeSql("DROP TEMPORARY TABLE tbl1"))
      .isInstanceOf[ValidationException]
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

    val tableResult2 =
      tableEnv.executeSql("DROP TEMPORARY TABLE default_catalog.default_database.tbl1")
    assertEquals(ResultKind.SUCCESS, tableResult2.getResultKind)
    assert(tableEnv.listTables().sameElements(Array.empty[String]))
  }

  @Test
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
    assertThatThrownBy(
      () => tableEnv.executeSql("DROP TEMPORARY TABLE invalid_catalog.invalid_database.tbl1"))
      .isInstanceOf[ValidationException]
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
    assertTrue(
      tableEnv
        .getCatalog(tableEnv.getCurrentCatalog)
        .get()
        .functionExists(ObjectPath.fromString("default_database.f1")))

    val tableResult2 = tableEnv.executeSql(s"ALTER FUNCTION default_database.f1 AS '$funcName2'")
    assertEquals(ResultKind.SUCCESS, tableResult2.getResultKind)
    assertTrue(
      tableEnv
        .getCatalog(tableEnv.getCurrentCatalog)
        .get()
        .functionExists(ObjectPath.fromString("default_database.f1")))

    val tableResult3 = tableEnv.executeSql("DROP FUNCTION default_database.f1")
    assertEquals(ResultKind.SUCCESS, tableResult3.getResultKind)
    assertFalse(
      tableEnv
        .getCatalog(tableEnv.getCurrentCatalog)
        .get()
        .functionExists(ObjectPath.fromString("default_database.f1")))

    val tableResult4 = tableEnv.executeSql(s"CREATE TEMPORARY SYSTEM FUNCTION f2 AS '$funcName'")
    assertEquals(ResultKind.SUCCESS, tableResult4.getResultKind)
    assertTrue(tableEnv.listUserDefinedFunctions().contains("f2"))

    val tableResult5 = tableEnv.executeSql("DROP TEMPORARY SYSTEM FUNCTION f2")
    assertEquals(ResultKind.SUCCESS, tableResult5.getResultKind)
    assertFalse(tableEnv.listUserDefinedFunctions().contains("f2"))
  }

  @Test
  def testExecuteSqlWithCreateUseDropCatalog(): Unit = {
    val tableResult1 =
      tableEnv.executeSql("CREATE CATALOG my_catalog WITH('type'='generic_in_memory')")
    assertEquals(ResultKind.SUCCESS, tableResult1.getResultKind)
    assertTrue(tableEnv.getCatalog("my_catalog").isPresent)

    assertEquals("default_catalog", tableEnv.getCurrentCatalog)
    val tableResult2 = tableEnv.executeSql("USE CATALOG my_catalog")
    assertEquals(ResultKind.SUCCESS, tableResult2.getResultKind)
    assertEquals("my_catalog", tableEnv.getCurrentCatalog)

    assertThatThrownBy(() => tableEnv.executeSql("DROP CATALOG my_catalog"))
      .isInstanceOf(classOf[ValidationException])
      .hasRootCauseMessage("Cannot drop a catalog which is currently in use.")

    tableEnv.executeSql("USE CATALOG default_catalog")

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
      util.Arrays.asList(Row.of("db1"), Row.of("default_database")).iterator(),
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
    checkData(util.Arrays.asList(Row.of("tbl1")).iterator(), tableResult2.collect())
  }

  @Test
  def testExecuteSqlWithEnhancedShowTables(): Unit = {
    val createCatalogResult =
      tableEnv.executeSql("CREATE CATALOG catalog1 WITH('type'='generic_in_memory')")
    assertEquals(ResultKind.SUCCESS, createCatalogResult.getResultKind)

    val createDbResult = tableEnv.executeSql("CREATE database catalog1.db1")
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
    checkData(util.Arrays.asList(Row.of("person")).iterator(), tableResult3.collect())
  }

  @Test
  def testExecuteSqlWithEnhancedShowViews(): Unit = {
    val createCatalogResult =
      tableEnv.executeSql("CREATE CATALOG catalog1 WITH('type'='generic_in_memory')")
    assertEquals(ResultKind.SUCCESS, createCatalogResult.getResultKind)

    val createDbResult = tableEnv.executeSql("CREATE database catalog1.db1")
    assertEquals(ResultKind.SUCCESS, createDbResult.getResultKind)

    val createTableStmt =
      """
        |CREATE VIEW catalog1.db1.view1 AS SELECT 1, 'abc'
      """.stripMargin
    val tableResult1 = tableEnv.executeSql(createTableStmt)
    assertEquals(ResultKind.SUCCESS, tableResult1.getResultKind)

    val createTableStmt2 =
      """
        |CREATE VIEW catalog1.db1.view2 AS SELECT 123
      """.stripMargin
    val tableResult2 = tableEnv.executeSql(createTableStmt2)
    assertEquals(ResultKind.SUCCESS, tableResult2.getResultKind)

    val tableResult3 = tableEnv.executeSql("SHOW VIEWS FROM catalog1.db1 like '%w1'")
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult3.getResultKind)
    assertEquals(
      ResolvedSchema.of(Column.physical("view name", DataTypes.STRING())),
      tableResult3.getResolvedSchema)
    checkData(util.Arrays.asList(Row.of("view1")).iterator(), tableResult3.collect())
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
    checkData(util.Arrays.asList(Row.of("f1")).iterator(), tableResult2.collect())
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

    assertThatThrownBy(() => tableEnv.executeSql(statement))
      .hasMessageContaining(
        "Option 'type' = 'dummy' is not supported since module name is used to find module")
      .isInstanceOf[ValidationException]
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
    assertThatThrownBy(() => tableEnv.executeSql(statement2))
      .hasMessageContaining("Could not execute LOAD MODULE `dummy` WITH ('dummy-version' = '2')." +
        " A module with name 'dummy' already exists")
      .isInstanceOf[ValidationException]
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

    assertThatThrownBy(() => tableEnv.executeSql("UNLOAD MODULE dummy"))
      .hasMessageContaining("Could not execute UNLOAD MODULE dummy." +
        " No module with name 'dummy' exists")
      .isInstanceOf[ValidationException]
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
    assertThatThrownBy(() => tableEnv.executeSql("USE MODULES core, dummy"))
      .hasMessageContaining("Could not execute USE MODULES: [core, dummy]. " +
        "No module with name 'dummy' exists")
      .isInstanceOf[ValidationException]
  }

  @Test
  def testExecuteSqlWithUseDuplicateModuleNames(): Unit = {
    assertThatThrownBy(() => tableEnv.executeSql("USE MODULES core, core"))
      .hasMessageContaining("Could not execute USE MODULES: [core, core]. " +
        "Module 'core' appears more than once")
      .isInstanceOf[ValidationException]
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
    assertTrue(
      tableEnv
        .getCatalog(tableEnv.getCurrentCatalog)
        .get()
        .tableExists(ObjectPath.fromString(s"${tableEnv.getCurrentDatabase}.v1")))

    val viewResult2 = tableEnv.executeSql("DROP VIEW IF EXISTS v1")
    assertEquals(ResultKind.SUCCESS, viewResult2.getResultKind)
    assertFalse(
      tableEnv
        .getCatalog(tableEnv.getCurrentCatalog)
        .get()
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

    val viewResult1 =
      tableEnv.executeSql("CREATE TEMPORARY VIEW IF NOT EXISTS v1 AS SELECT * FROM tbl1")
    assertEquals(ResultKind.SUCCESS, viewResult1.getResultKind)
    assert(tableEnv.listTables().sameElements(Array[String]("tbl1", "v1")))

    val viewResult2 = tableEnv.executeSql("DROP TEMPORARY VIEW IF EXISTS v1")
    assertEquals(ResultKind.SUCCESS, viewResult2.getResultKind)
    assert(tableEnv.listTables().sameElements(Array[String]("tbl1")))
  }

  @Test
  def testCreateViewWithWrongFieldList(): Unit = {
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

    assertThatThrownBy(
      () => {
        tableEnv.executeSql(sourceDDL)
        tableEnv.executeSql(sinkDDL)
        tableEnv.executeSql(viewDDL)
      })
      .hasMessageContaining(
        "VIEW definition and input fields not match:\n" +
          "\tDef fields: [d].\n" +
          "\tInput fields: [a, b, c].")
      .isInstanceOf[ValidationException]
  }

  @Test
  def testCreateViewTwice(): Unit = {
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
      .hasMessageContaining(
        "Could not execute CreateTable in path `default_catalog`.`default_database`.`T3`")
      .isInstanceOf[ValidationException]
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

  @Test
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

    assertThatThrownBy(() => tableEnv.executeSql("DROP VIEW default_catalog.default_database.T2"))
      .isInstanceOf[ValidationException]
  }

  @Test
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
    assertThatThrownBy(() => tableEnv.executeSql("DROP VIEW default_catalog1.default_database1.T2"))
      .isInstanceOf[ValidationException]
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

  @Test
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
    assertThatThrownBy(
      () => tableEnv.executeSql("DROP TEMPORARY VIEW default_catalog.default_database.T2"))
      .isInstanceOf[ValidationException]
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
    checkData(util.Arrays.asList(Row.of("view1")).iterator(), tableResult3.collect())

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

    checkExplain(
      "explain plan for select * from MyTable where a > 10",
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

    checkExplain(
      "explain plan for insert into MySink select a, b from MyTable where a > 10",
      "/explain/testExecuteSqlWithExplainInsert.out")

    checkExplain(
      "explain plan for insert into MySink(d) select a from MyTable where a > 10",
      "/explain/testExecuteSqlWithExplainInsertPartialColumn.out")
  }

  @Test
  def testExecuteSqlWithExplainInsertStaticPartition(): Unit = {
    val createTableStmt1 =
      """
        |CREATE TABLE MyTable (
        |  f0 BIGINT,
        |  f1 INT,
        |  f2 STRING
        |) WITH (
        |  'connector' = 'COLLECTION',
        |  'is-bounded' = 'true'
        |)
      """.stripMargin
    val tableResult1 = batchTableEnv.executeSql(createTableStmt1)
    assertEquals(ResultKind.SUCCESS, tableResult1.getResultKind)

    val createTableStmt2 =
      """
        |CREATE TABLE MySink (
        |  f0 BIGINT,
        |  f1 INT,
        |  f2 STRING
        |) PARTITIONED BY (f2)
        |WITH (
        |  'connector' = 'filesystem',
        |  'path' = '/tmp',
        |  'format' = 'testcsv'
        |)
      """.stripMargin
    val tableResult2 = batchTableEnv.executeSql(createTableStmt2)
    assertEquals(ResultKind.SUCCESS, tableResult2.getResultKind)

    checkExplain(
      "EXPLAIN PLAN FOR INSERT INTO MySink PARTITION (f2 = '123') SELECT f0, f1 FROM MyTable",
      "/explain/testExecuteSqlWithExplainInsertIntoStaticPartition.out",
      streaming = false
    )

    checkExplain(
      "EXPLAIN PLAN FOR INSERT OVERWRITE MySink PARTITION (f2 = '123') SELECT f0, f1 FROM MyTable",
      "/explain/testExecuteSqlWithExplainInsertOverwriteStaticPartition.out",
      streaming = false
    )
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

    val actual =
      tableEnv.explainSql("select * from MyTable where a > 10", ExplainDetail.CHANGELOG_MODE)
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
      "execute select * from MyTable where a > 10",
      ExplainDetail.CHANGELOG_MODE)
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

    val actual = tableEnv.explainSql("insert into MySink select a, b from MyTable where a > 10")
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
      "/explain/testExecuteSqlWithExplainDetailsSelect.out"
    );
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
      "/explain/testExecuteSqlWithExplainDetailsAndUnion.out"
    )
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
      "/explain/testExecuteSqlWithExplainDetailsInsert.out"
    )
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
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult1.getResultKind)
    checkData(expectedResult1.iterator(), tableResult1.collect())
    val tableResult2 = tableEnv.executeSql("desc T1")
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult2.getResultKind)
    checkData(expectedResult1.iterator(), tableResult2.collect())

    val expectedResult2 = util.Arrays.asList(
      Row.of("d", "INT", Boolean.box(false), null, null, null),
      Row.of("e", "STRING", Boolean.box(false), null, null, null),
      Row.of("f", "ROW<`f0` INT NOT NULL, `f1` INT>", Boolean.box(false), null, null, null)
    )
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
    checkData(expectedResult3.iterator(), tableResult6.collect())
  }

  @Test
  def testDescribeTableWithComment(): Unit = {
    val sourceDDL =
      """
        |CREATE TABLE T1(
        |  c0 char(10) comment 'this is the first column',
        |  c1 varchar(10) comment 'this is the second column',
        |  c2 string,
        |  c3 BOOLEAN,
        |  c4 BINARY(10),
        |  c5 VARBINARY(10),
        |  c6 BYTES,
        |  c7 DECIMAL(10, 3),
        |  c8 TINYINT,
        |  c9 SMALLINT,
        |  c10 INTEGER,
        |  c11 BIGINT,
        |  c12 FLOAT,
        |  c13 DOUBLE,
        |  c14 DATE,
        |  c15 TIME,
        |  c16 TIMESTAMP,
        |  c17 TIMESTAMP(3),
        |  c18 TIMESTAMP WITHOUT TIME ZONE,
        |  c19 TIMESTAMP(3) WITH LOCAL TIME ZONE,
        |  c20 TIMESTAMP WITH LOCAL TIME ZONE,
        |  c21 ARRAY<INT>,
        |  c22 MAP<INT, STRING>,
        |  c23 ROW<f0 INT, f1 STRING>,
        |  c24 int not null comment 'this is c24 and part of pk',
        |  c25 varchar not null,
        |  c26 row<f0 int not null, f1 int> not null comment 'this is c26 and part of pk',
        |  c27 AS LOCALTIME,
        |  c28 AS CURRENT_TIME,
        |  c29 AS LOCALTIMESTAMP,
        |  c30 AS CURRENT_TIMESTAMP comment 'notice: computed column',
        |  c31 AS CURRENT_ROW_TIMESTAMP(),
        |  ts AS to_timestamp(c25) comment 'notice: watermark',
        |  PRIMARY KEY(c24, c26) NOT ENFORCED,
        |  WATERMARK FOR ts AS ts - INTERVAL '1' SECOND
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    tableEnv.executeSql(sourceDDL)

    val expectedResult1 = util.Arrays.asList(
      Row.of("c0", "CHAR(10)", Boolean.box(true), null, null, null, "this is the first column"),
      Row.of("c1", "VARCHAR(10)", Boolean.box(true), null, null, null, "this is the second column"),
      Row.of("c2", "STRING", Boolean.box(true), null, null, null, null),
      Row.of("c3", "BOOLEAN", Boolean.box(true), null, null, null, null),
      Row.of("c4", "BINARY(10)", Boolean.box(true), null, null, null, null),
      Row.of("c5", "VARBINARY(10)", Boolean.box(true), null, null, null, null),
      Row.of("c6", "BYTES", Boolean.box(true), null, null, null, null),
      Row.of("c7", "DECIMAL(10, 3)", Boolean.box(true), null, null, null, null),
      Row.of("c8", "TINYINT", Boolean.box(true), null, null, null, null),
      Row.of("c9", "SMALLINT", Boolean.box(true), null, null, null, null),
      Row.of("c10", "INT", Boolean.box(true), null, null, null, null),
      Row.of("c11", "BIGINT", Boolean.box(true), null, null, null, null),
      Row.of("c12", "FLOAT", Boolean.box(true), null, null, null, null),
      Row.of("c13", "DOUBLE", Boolean.box(true), null, null, null, null),
      Row.of("c14", "DATE", Boolean.box(true), null, null, null, null),
      Row.of("c15", "TIME(0)", Boolean.box(true), null, null, null, null),
      Row.of("c16", "TIMESTAMP(6)", Boolean.box(true), null, null, null, null),
      Row.of("c17", "TIMESTAMP(3)", Boolean.box(true), null, null, null, null),
      Row.of("c18", "TIMESTAMP(6)", Boolean.box(true), null, null, null, null),
      Row.of("c19", "TIMESTAMP_LTZ(3)", Boolean.box(true), null, null, null, null),
      Row.of("c20", "TIMESTAMP_LTZ(6)", Boolean.box(true), null, null, null, null),
      Row.of("c21", "ARRAY<INT>", Boolean.box(true), null, null, null, null),
      Row.of("c22", "MAP<INT, STRING>", Boolean.box(true), null, null, null, null),
      Row.of("c23", "ROW<`f0` INT, `f1` STRING>", Boolean.box(true), null, null, null, null),
      Row.of(
        "c24",
        "INT",
        Boolean.box(false),
        "PRI(c24, c26)",
        null,
        null,
        "this is c24 and part of pk"),
      Row.of("c25", "STRING", Boolean.box(false), null, null, null, null),
      Row.of(
        "c26",
        "ROW<`f0` INT NOT NULL, `f1` INT>",
        Boolean.box(false),
        "PRI(c24, c26)",
        null,
        null,
        "this is c26 and part of pk"),
      Row.of("c27", "TIME(0)", Boolean.box(false), null, "AS LOCALTIME", null, null),
      Row.of("c28", "TIME(0)", Boolean.box(false), null, "AS CURRENT_TIME", null, null),
      Row.of("c29", "TIMESTAMP(3)", Boolean.box(false), null, "AS LOCALTIMESTAMP", null, null),
      Row.of(
        "c30",
        "TIMESTAMP_LTZ(3)",
        Boolean.box(false),
        null,
        "AS CURRENT_TIMESTAMP",
        null,
        "notice: computed column"),
      Row.of(
        "c31",
        "TIMESTAMP_LTZ(3)",
        Boolean.box(false),
        null,
        "AS CURRENT_ROW_TIMESTAMP()",
        null,
        null),
      Row.of(
        "ts",
        "TIMESTAMP(3) *ROWTIME*",
        Boolean.box(true),
        null,
        "AS TO_TIMESTAMP(`c25`)",
        "`ts` - INTERVAL '1' SECOND",
        "notice: watermark")
    )
    val tableResult1 = tableEnv.executeSql("describe T1")
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult1.getResultKind)
    checkData(expectedResult1.iterator(), tableResult1.collect())
    val tableResult2 = tableEnv.executeSql("desc T1")
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult2.getResultKind)
    checkData(expectedResult1.iterator(), tableResult2.collect())
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
    val tableResult = tableEnv
      .asInstanceOf[TableEnvironmentInternal]
      .getCatalogManager
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
    val viewResult = tableEnv
      .asInstanceOf[TableEnvironmentInternal]
      .getCatalogManager
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
      tableEnv.executeSql(s"create temporary function func1 as '$clzName'")
      fail("Creating a temporary function with invalid class should fail")
    } catch {
      case _: Exception => // expected
    }
    assertEquals(0, listener.numTempFunc)
    tableEnv.executeSql(
      s"create temporary function ${listener.getName}.`default`.func1 as '$clzName'")
    assertEquals(1, listener.numTempFunc)
    tableEnv.executeSql("drop temporary function if exists func1")
    assertEquals(1, listener.numTempFunc)
    tableEnv.executeSql(s"drop temporary function ${listener.getName}.`default`.func1")
    assertEquals(0, listener.numTempFunc)
    tableEnv.useCatalog(listener.getName)
    tableEnv.executeSql(s"create temporary function func1 as '$clzName'")
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
      .hasMessageContaining(
        "Mismatch between configured runtime mode and actual runtime mode. " +
          "Currently, the 'execution.runtime-mode' can only be set when instantiating the " +
          "table environment. Subsequent changes are not supported. " +
          "Please instantiate a new TableEnvironment if necessary.")
  }

  @Test
  def testAddPartitions(): Unit = {
    val createTableStmt =
      """
        |CREATE TABLE tbl (
        |  a INT,
        |  b BIGINT,
        |  c DATE
        |) PARTITIONED BY (b, c)
        |WITH (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    tableEnv.executeSql(createTableStmt)

    // test add partition
    var tableResult = tableEnv.executeSql(
      "alter table tbl add partition " +
        "(b=1000,c='2020-05-01') partition (b=2000,c='2020-01-01') with ('k'='v')")
    assertEquals(ResultKind.SUCCESS, tableResult.getResultKind)

    val spec1 = new CatalogPartitionSpec(Map("b" -> "1000", "c" -> "2020-05-01").asJava)
    val spec2 = new CatalogPartitionSpec(Map("b" -> "2000", "c" -> "2020-01-01").asJava)

    val catalog = tableEnv.getCatalog(tableEnv.getCurrentCatalog).get()

    val tablePath = new ObjectPath("default_database", "tbl")
    val actual = catalog.listPartitions(tablePath)
    // assert partition spec
    assertEquals(List(spec1, spec2).asJava, actual)

    val part1 = catalog.getPartition(tablePath, spec1)
    val part2 = catalog.getPartition(tablePath, spec2)
    // assert partition properties
    assertEquals(Collections.emptyMap(), part1.getProperties)
    assertEquals(Collections.singletonMap("k", "v"), part2.getProperties)

    // add existed partition with if not exists
    tableResult =
      tableEnv.executeSql("alter table tbl add if not exists partition (b=1000,c='2020-05-01')")
    assertEquals(ResultKind.SUCCESS, tableResult.getResultKind)

    // add existed partition without if not exists
    assertThatThrownBy(
      () => tableEnv.executeSql("alter table tbl add partition (b=1000,c='2020-05-01')"))
      .isInstanceOf(classOf[TableException])
      .hasMessageContaining("Could not execute ALTER TABLE default_catalog.default_database.tbl" +
        " ADD PARTITION (b=1000, c=2020-05-01)")

  }

  @Test
  def testShowPartitions(): Unit = {
    val createTableStmt =
      """
        |CREATE TABLE tbl (
        |  a INT,
        |  b BIGINT,
        |  c DATE
        |) PARTITIONED BY (b, c)
        |WITH (
        |  'connector' = 'COLLECTION'
        |)
          """.stripMargin
    tableEnv.executeSql(createTableStmt)

    // partitions
    val catalog = tableEnv.getCatalog(tableEnv.getCurrentCatalog).get()
    val spec1 = new CatalogPartitionSpec(Map("b" -> "1000", "c" -> "2020-05-01").asJava)
    val part1 = new CatalogPartitionImpl(Map("k1" -> "v1").asJava, "")
    val spec2 = new CatalogPartitionSpec(Map("b" -> "2000", "c" -> "2020-01-01").asJava)
    val part2 = new CatalogPartitionImpl(Map("k1" -> "v1").asJava, "")
    val spec3 = new CatalogPartitionSpec(Map("b" -> "2000", "c" -> "2020-05-01").asJava)
    val part3 = new CatalogPartitionImpl(Map("k1" -> "v1").asJava, "")

    val tablePath = new ObjectPath("default_database", "tbl")
    // create partition
    catalog.createPartition(tablePath, spec1, part1, false)
    catalog.createPartition(tablePath, spec2, part2, false)
    catalog.createPartition(tablePath, spec3, part3, false)

    // test show all partitions
    var tableResult =
      tableEnv.executeSql("show partitions tbl")

    var expectedResult = util.Arrays.asList(
      Row.of("b=1000/c=2020-05-01"),
      Row.of("b=2000/c=2020-01-01"),
      Row.of("b=2000/c=2020-05-01")
    )

    checkData(expectedResult.iterator(), tableResult.collect())

    // test show partitions with partition spec
    tableResult = tableEnv.executeSql("show partitions tbl partition (b=2000)")
    expectedResult = util.Arrays.asList(
      Row.of("b=2000/c=2020-01-01"),
      Row.of("b=2000/c=2020-05-01")
    )
    checkData(expectedResult.iterator(), tableResult.collect())
  }

  @Test
  def testShowColumnsWithLike(): Unit = {
    val createTableStmt =
      """
        |CREATE TABLE TestTb1 (
        |  abc bigint,
        |  b int,
        |  c varchar
        |) with (
        |  'connector' = 'COLLECTION',
        |  'is-bounded' = 'false'
        |)
    """.stripMargin
    tableEnv.executeSql(createTableStmt)

    var sql = "SHOW COLUMNS from TestTb1 LIKE 'abc'"
    var tableResult = tableEnv.executeSql(sql)

    val expectedResult = util.Arrays.asList(
      Row.of("abc", "BIGINT", Boolean.box(true), null, null, null)
    )

    checkData(expectedResult.iterator(), tableResult.collect())

    sql = "SHOW COLUMNS from TestTb1 LIKE 'a.c'"
    tableResult = tableEnv.executeSql(sql)

    checkData(Collections.emptyList().iterator(), tableResult.collect());
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
    assertEquals(
      ResolvedSchema.of(Column.physical("module name", DataTypes.STRING())),
      showModules.getResolvedSchema)

    val showFullModules = tableEnv.executeSql("SHOW FULL MODULES")
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, showFullModules.getResultKind)
    assertEquals(
      ResolvedSchema.physical(
        Array[String]("module name", "used"),
        Array[DataType](DataTypes.STRING(), DataTypes.BOOLEAN())),
      showFullModules.getResolvedSchema
    )

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
    assertTrue(source.isInstanceOf[CollectionTableSource])
    assertEquals(expectToBeBounded, source.asInstanceOf[CollectionTableSource].isBounded)
  }

  private def checkExplain(sql: String, resultPath: String, streaming: Boolean = true): Unit = {
    val tableResult2 = if (streaming) {
      tableEnv.executeSql(sql)
    } else {
      batchTableEnv.executeSql(sql)
    }
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
    extends GenericInMemoryCatalog(name)
    with TemporaryOperationListener {

    val tableComment: String = "listener_comment"
    val modelComment: String = "listener_comment"
    val funcClzName: String = classOf[TestGenericUDF].getName

    var numTempTable = 0
    var numTempModel = 0
    var numTempFunc = 0

    override def onCreateTemporaryTable(
        tablePath: ObjectPath,
        table: CatalogBaseTable): CatalogBaseTable = {
      numTempTable += 1
      if (table.isInstanceOf[CatalogTable]) {
        CatalogTable.of(
          table.getUnresolvedSchema,
          tableComment,
          Collections.emptyList(),
          table.getOptions)
      } else {
        val view = table.asInstanceOf[CatalogView]
        CatalogView.of(
          view.getUnresolvedSchema,
          tableComment,
          view.getOriginalQuery,
          view.getExpandedQuery,
          view.getOptions)
      }
    }

    override def onCreateTemporaryModel(
        modelPath: ObjectPath,
        model: CatalogModel): CatalogModel = {
      numTempModel += 1
      CatalogModel.of(model.getInputSchema, model.getOutputSchema, model.getOptions, modelComment)
    }

    override def onDropTemporaryTable(tablePath: ObjectPath): Unit = numTempTable -= 1

    override def onDropTemporaryModel(modelPath: ObjectPath): Unit = numTempModel -= 1

    override def onCreateTemporaryFunction(
        functionPath: ObjectPath,
        function: CatalogFunction): CatalogFunction = {
      numTempFunc += 1
      new CatalogFunctionImpl(funcClzName, function.getFunctionLanguage)
    }

    override def onDropTemporaryFunction(functionPath: ObjectPath): Unit = numTempFunc -= 1
  }

}
