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
import org.apache.flink.sql.parser.error.SqlValidateException
import org.apache.flink.streaming.api.environment.{LocalStreamEnvironment, StreamExecutionEnvironment}
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.internal.{ModelImpl, TableEnvironmentInternal}
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
import org.assertj.core.api.Assertions.{assertThat, assertThatExceptionOfType, assertThatList, assertThatObject}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

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
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => tableEnv.from("MyTable"))
      .withMessageContaining("Table `MyTable` was not found")
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
    assertThat(actual).isEqualTo(expected)

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () =>
          tableEnv.createTemporaryView(
            "MyTable",
            StreamingEnvUtil
              .fromElements[(Int, Long)](env)))
      .withMessageContaining(
        "Temporary table '`default_catalog`.`default_database`.`MyTable`' already exists")
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
    assertThat(actual).isEqualTo(expected)
  }

  @Test
  def testCreateTableWithEnforcedMode(): Unit = {
    // check column constraint
    assertThatExceptionOfType(classOf[SqlValidateException])
      .isThrownBy(() => tableEnv.executeSql("""
                                              |CREATE TABLE MyTable (
                                              |  a bigint primary key,
                                              |  b int,
                                              |  c varchar
                                              |) with (
                                              |  'connector' = 'COLLECTION',
                                              |  'is-bounded' = 'false'
                                              |)
    """.stripMargin))
      .withMessageContaining("Flink doesn't support ENFORCED mode for PRIMARY KEY constraint.")

    // check table constraint
    assertThatExceptionOfType(classOf[SqlValidateException])
      .isThrownBy(() => tableEnv.executeSql("""
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
      .withMessageContaining("Flink doesn't support ENFORCED mode for PRIMARY KEY constraint.")
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
    assertThat(TableTestUtil.replaceStageId(actual))
      .isEqualTo(TableTestUtil.replaceStageId(expected))
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
    assertThat(TableTestUtil.replaceStageId(actual))
      .isEqualTo(TableTestUtil.replaceStageId(expected))
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
    assertThatExceptionOfType(classOf[TableException])
      .isThrownBy(() => tableEnv.executeSql(String.format("ADD JAR '%s'", "/path/to/illegal.jar")))
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

    assertThatObject(tableResult.getResultKind).isSameAs(ResultKind.SUCCESS_WITH_CONTENT)
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

    assertThat(TableTestUtil.replaceNodeIdInOperator(TableTestUtil.replaceStreamNodeId(actual)))
      .isEqualTo(
        TableTestUtil.replaceNodeIdInOperator(TableTestUtil.replaceStreamNodeId(expected))
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

    assertThat(TableTestUtil.replaceNodeIdInOperator(TableTestUtil.replaceStreamNodeId(actual)))
      .isEqualTo(
        TableTestUtil.replaceNodeIdInOperator(TableTestUtil.replaceStreamNodeId(expected))
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
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => tableEnv.executeSql("ALTER TABLE MyTable RESET ()"))
      .withMessageContaining("ALTER TABLE RESET does not support empty key")
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
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => tableEnv.executeSql("explain plan for select * from MyTable where a > 10"))
      .withMessageContaining(
        "Unable to create a source for reading table " +
          "'default_catalog.default_database.MyTable'.\n\n" +
          "Table options are:\n\n'connector'='datagen'\n" +
          "'invalid-key'='invalid-value'")

    // remove invalid key by RESET
    val alterTableResetStatement = "ALTER TABLE MyTable RESET ('invalid-key')"
    val tableResult = tableEnv.executeSql(alterTableResetStatement)
    assertThatObject(tableResult.getResultKind).isSameAs(ResultKind.SUCCESS)
    assertThat(
      tableEnv
        .getCatalog(tableEnv.getCurrentCatalog)
        .get()
        .getTable(ObjectPath.fromString(s"${tableEnv.getCurrentDatabase}.MyTable"))
        .getOptions
    ).containsAllEntriesOf(util.Map.of("connector", "datagen"))
    assertThatObject(
      tableEnv.executeSql("explain plan for select * from MyTable where a > 10").getResultKind)
      .isSameAs(ResultKind.SUCCESS_WITH_CONTENT)
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
    assertThatObject(tableResult.getResultKind).isSameAs(ResultKind.SUCCESS)
    assertThat(
      tableEnv
        .getCatalog(tableEnv.getCurrentCatalog)
        .get()
        .getTable(ObjectPath.fromString(s"${tableEnv.getCurrentDatabase}.MyTable"))
        .getOptions
    ).containsAllEntriesOf(util.Map.of("connector", "COLLECTION"))
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
    assertThatObject(tableResult.getResultKind).isSameAs(ResultKind.SUCCESS)
    assertThat(
      tableEnv
        .getCatalog(tableEnv.getCurrentCatalog)
        .get()
        .getTable(ObjectPath.fromString(s"${tableEnv.getCurrentDatabase}.MyTable"))
        .getOptions
    ).containsAllEntriesOf(util.Map.of("connector", "filesystem", "path", "_invalid"))

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => tableEnv.executeSql("explain plan for select * from MyTable where a > 10"))
      .withMessageContaining(
        "Unable to create a source for reading table 'default_catalog.default_database.MyTable'.")
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

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => tableEnv.executeSql("""
                                              |ALTER TABLE MyTable ADD (
                                              |  PRIMARY KEY (a) NOT ENFORCED
                                              |)
                                              |""".stripMargin))
      .withMessageContaining(
        """Failed to execute ALTER TABLE statement.
          |The current table has already defined the primary key constraint [`b`]. You might want to drop it before adding a new one.""".stripMargin)

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => tableEnv.executeSql("""
                                              |ALTER TABLE MyTable ADD (
                                              |  a STRING
                                              |)
                                              |""".stripMargin))
      .withMessageContaining("""Failed to execute ALTER TABLE statement.
                               |Column `a` already exists in the table.""".stripMargin)

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => tableEnv.executeSql("""
                                              |ALTER TABLE MyTable ADD (
                                              |  e STRING AFTER h
                                              |)
                                              |""".stripMargin))
      .withMessageContaining(
        """Failed to execute ALTER TABLE statement.
          |Referenced column `h` by 'AFTER' does not exist in the table.""".stripMargin
      )

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
    assertThatObject(tableResult.getResultKind).isSameAs(ResultKind.SUCCESS_WITH_CONTENT)
    checkData(expectedResult.iterator(), tableResult.collect())

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => tableEnv.executeSql("ALTER TABLE MyTable ADD WATERMARK FOR ts AS ts"))
      .withMessageContaining(
        """Failed to execute ALTER TABLE statement.
          |The current table has already defined the watermark strategy `d` AS `d` - INTERVAL '2' SECOND. You might want to drop it before adding a new one.""".stripMargin)
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

    assertThatExceptionOfType(classOf[SqlValidateException])
      .isThrownBy(() => tableEnv.executeSql("""
                                              |ALTER TABLE MyTable ADD (
                                              |  f STRING PRIMARY KEY NOT ENFORCED,
                                              |  PRIMARY KEY (a) NOT ENFORCED
                                              |)
                                              |""".stripMargin))
      .withMessageContaining("Duplicate primary key definition")

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => tableEnv.executeSql("""
                                              |ALTER TABLE MyTable ADD (
                                              |  PRIMARY KEY (c) NOT ENFORCED
                                              |)
                                              |""".stripMargin))
      .withMessageContaining(
        """Failed to execute ALTER TABLE statement.
          |Invalid primary key 'PK_c'. Column 'c' is not a physical column.""".stripMargin)

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
    assertThatObject(tableResult.getResultKind).isSameAs(ResultKind.SUCCESS_WITH_CONTENT)
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

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => tableEnv.executeSql("""
                                              |ALTER TABLE MyTable ADD (
                                              |  WATERMARK FOR e.e1 AS e.e1
                                              |)
                                              |""".stripMargin))
      .withMessageContaining(
        """Failed to execute ALTER TABLE statement.
          |Watermark strategy on nested column is not supported yet.""".stripMargin)

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
    assertThatObject(tableResult.getResultKind).isSameAs(ResultKind.SUCCESS_WITH_CONTENT)
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

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => tableEnv.executeSql("""
                                              |ALTER TABLE MyTable MODIFY (
                                              |  x STRING FIRST
                                              |)
                                              |""".stripMargin))
      .withMessageContaining("""Failed to execute ALTER TABLE statement.
                               |Column `x` does not exist in the table.""".stripMargin)

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => tableEnv.executeSql("""
                                              |ALTER TABLE MyTable MODIFY (
                                              |  b INT FIRST,
                                              |  a BIGINT AFTER x
                                              |)
                                              |""".stripMargin))
      .withMessageContaining(
        """Failed to execute ALTER TABLE statement.
          |Referenced column `x` by 'AFTER' does not exist in the table.""".stripMargin)

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => tableEnv.executeSql("""
                                              |ALTER TABLE MyTable MODIFY (
                                              |  b BOOLEAN first
                                              |)
                                              |""".stripMargin))
      .withMessageContaining("""Failed to execute ALTER TABLE statement.
                               |Invalid expression for computed column 'e'.""".stripMargin)

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
    assertThatObject(tableResult1.getResultKind).isSameAs(ResultKind.SUCCESS_WITH_CONTENT)
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
    assertThatObject(tableResult.getResultKind).isSameAs(ResultKind.SUCCESS_WITH_CONTENT)
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

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => tableEnv.executeSql("""
                                              |ALTER TABLE MyTable MODIFY (
                                              |  PRIMARY KEY (x) NOT ENFORCED
                                              |)
                                              |""".stripMargin))
      .withMessageContaining(
        """Failed to execute ALTER TABLE statement.
          |The current table does not define any primary key constraint. You might want to add a new one.""".stripMargin)

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
    assertThatObject(tableResult.getResultKind).isSameAs(ResultKind.SUCCESS_WITH_CONTENT)
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

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => tableEnv.executeSql("""
                                              |ALTER TABLE MyTable MODIFY (
                                              |  WATERMARK FOR e.e1 AS e.e1
                                              |)
                                              |""".stripMargin))
      .withMessageContaining(
        """Failed to execute ALTER TABLE statement.
          |Watermark strategy on nested column is not supported yet.""".stripMargin)

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
    assertThatObject(tableResult.getResultKind).isSameAs(ResultKind.SUCCESS_WITH_CONTENT)
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
    assertThatObject(tableResult.getResultKind).isSameAs(ResultKind.SUCCESS_WITH_CONTENT)
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
    assertThatObject(tableResult.getResultKind).isSameAs(ResultKind.SUCCESS_WITH_CONTENT)
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
    assertThatObject(tableResult1.getResultKind).isSameAs(ResultKind.SUCCESS_WITH_CONTENT)
    checkData(expectedResult.iterator(), tableResult1.collect())

    tableEnv.executeSql("ALTER TABLE MyTable ADD CONSTRAINT ct PRIMARY KEY(a) NOT ENFORCED")
    tableEnv.executeSql("ALTER TABLE MyTable DROP PRIMARY KEY")
    val tableResult2 = tableEnv.executeSql("DESCRIBE MyTable")
    assertThatObject(tableResult2.getResultKind).isSameAs(ResultKind.SUCCESS_WITH_CONTENT)
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
    assertThatObject(tableResult.getResultKind).isSameAs(ResultKind.SUCCESS_WITH_CONTENT)
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
    assertThatObject(tableResult.getResultKind).isSameAs(ResultKind.SUCCESS)
    assertThat(catalog.listPartitions(tablePath).toString)
      .isEqualTo("[CatalogPartitionSpec{{b=2000, c=2020-01-01}}]")

    // drop the partition again with if exists
    tableResult =
      tableEnv.executeSql("alter table tbl drop if exists partition(b='1000', c='2020-05-01')")
    assertThatObject(tableResult.getResultKind).isSameAs(ResultKind.SUCCESS)
    assertThat(catalog.listPartitions(tablePath).toString)
      .isEqualTo("[CatalogPartitionSpec{{b=2000, c=2020-01-01}}]")

    // drop the partition again without if exists,
    // should throw exception then
    assertThatExceptionOfType(classOf[TableException])
      .isThrownBy(
        () => tableEnv.executeSql("alter table tbl drop partition (b=1000,c='2020-05-01')"))
      .withMessageContaining("Could not execute ALTER TABLE default_catalog.default_database.tbl" +
        " DROP PARTITION (b=1000, c=2020-05-01)")
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

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () => tableEnv.executeSql("SELECT c FROM my_view /*+ OPTIONS('is-bounded' = 'true') */"))
      .withMessageContaining("View '`default_catalog`.`default_database`.`my_view`' " +
        "cannot be enriched with new options. Hints can only be applied to tables.")

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () =>
          tableEnv.executeSql(
            "CREATE TEMPORARY VIEW your_view AS " +
              "SELECT c FROM my_view /*+ OPTIONS('is-bounded' = 'true') */"))
      .withMessageContaining("View '`default_catalog`.`default_database`.`my_view`' " +
        "cannot be enriched with new options. Hints can only be applied to tables.")

    tableEnv.executeSql("CREATE TEMPORARY VIEW your_view AS SELECT c FROM my_view ")

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () => tableEnv.executeSql("SELECT * FROM your_view /*+ OPTIONS('is-bounded' = 'true') */"))
      .withMessageContaining("View '`default_catalog`.`default_database`.`your_view`' " +
        "cannot be enriched with new options. Hints can only be applied to tables.")

  }

  @Test
  def testExecuteSqlWithCreateAlterDropTable(): Unit = {
    createTableForTests()

    assertThat(
      tableEnv
        .getCatalog(tableEnv.getCurrentCatalog)
        .get()
        .tableExists(ObjectPath.fromString(s"${tableEnv.getCurrentDatabase}.T1"))).isTrue

    val tableResult2 = tableEnv.executeSql("ALTER TABLE T1 SET ('k1' = 'a', 'k2' = 'b')")
    assertThatObject(tableResult2.getResultKind).isSameAs(ResultKind.SUCCESS)
    assertThat(
      tableEnv
        .getCatalog(tableEnv.getCurrentCatalog)
        .get()
        .getTable(ObjectPath.fromString(s"${tableEnv.getCurrentDatabase}.T1"))
        .getOptions
    ).containsAllEntriesOf(
      util.Map.of("connector", "COLLECTION", "is-bounded", "false", "k1", "a", "k2", "b"))

    val tableResult3 = tableEnv.executeSql("DROP TABLE T1")
    assertThatObject(tableResult3.getResultKind).isSameAs(ResultKind.SUCCESS)
    assertThat(
      tableEnv
        .getCatalog(tableEnv.getCurrentCatalog)
        .get()
        .tableExists(ObjectPath.fromString(s"${tableEnv.getCurrentDatabase}.T1"))).isFalse
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
    assertThatObject(tableResult1.getResultKind).isSameAs(ResultKind.SUCCESS)
    val tableResult2 = tableEnv.executeSql(createTableStmt)
    assertThatObject(tableResult2.getResultKind).isSameAs(ResultKind.SUCCESS)
    assertThat(
      tableEnv
        .getCatalog(tableEnv.getCurrentCatalog)
        .get()
        .tableExists(ObjectPath.fromString(s"${tableEnv.getCurrentDatabase}.tbl1"))).isTrue

    val tableResult3 = tableEnv.executeSql("DROP TABLE IF EXISTS tbl1")
    assertThatObject(tableResult3.getResultKind).isSameAs(ResultKind.SUCCESS)
    assertThat(
      tableEnv
        .getCatalog(tableEnv.getCurrentCatalog)
        .get()
        .tableExists(ObjectPath.fromString(s"${tableEnv.getCurrentDatabase}.tbl1"))).isFalse
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
    assertThatObject(tableResult1.getResultKind).isSameAs(ResultKind.SUCCESS)
    val tableResult2 = tableEnv.executeSql(createTableStmt)
    assertThatObject(tableResult2.getResultKind).isSameAs(ResultKind.SUCCESS)
    assertThat(tableEnv.listTables()).containsExactly("tbl1")

    val tableResult3 = tableEnv.executeSql("DROP TEMPORARY TABLE IF EXISTS tbl1")
    assertThatObject(tableResult3.getResultKind).isSameAs(ResultKind.SUCCESS)
    assertThat(tableEnv.listTables()).doesNotContain("tbl1")
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
    assertThatObject(tableResult1.getResultKind).isSameAs(ResultKind.SUCCESS)
    assertThat(tableEnv.listTables()).containsExactly("tbl1")

    val tableResult2 = tableEnv.executeSql("DROP TEMPORARY TABLE tbl1")
    assertThatObject(tableResult2.getResultKind).isSameAs(ResultKind.SUCCESS)
    assertThat(tableEnv.listTables()).isEmpty()
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
    assertThatObject(tableResult1.getResultKind).isSameAs(ResultKind.SUCCESS)
    assertThat(tableEnv.listTables()).containsExactly("tbl1")

    val tableResult2 = tableEnv.executeSql("DROP TEMPORARY TABLE IF EXISTS tbl1")
    assertThatObject(tableResult2.getResultKind).isSameAs(ResultKind.SUCCESS)
    assertThat(tableEnv.listTables()).isEmpty()

    val tableResult3 = tableEnv.executeSql("DROP TEMPORARY TABLE IF EXISTS tbl1")
    assertThatObject(tableResult3.getResultKind).isSameAs(ResultKind.SUCCESS)
    assertThat(tableEnv.listTables()).isEmpty()
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
    assertThatObject(tableResult1.getResultKind).isSameAs(ResultKind.SUCCESS)
    assertThat(tableEnv.listTables()).containsExactly("tbl1")

    val tableResult2 = tableEnv.executeSql("DROP TEMPORARY TABLE tbl1")
    assertThatObject(tableResult2.getResultKind).isSameAs(ResultKind.SUCCESS)
    assertThat(tableEnv.listTables()).isEmpty()

    // fail the case
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => tableEnv.executeSql("DROP TEMPORARY TABLE tbl1"))
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
    assertThatObject(tableResult1.getResultKind).isSameAs(ResultKind.SUCCESS)
    assertThat(tableEnv.listTables()).containsExactly("tbl1")

    val tableResult2 =
      tableEnv.executeSql("DROP TEMPORARY TABLE default_catalog.default_database.tbl1")
    assertThatObject(tableResult2.getResultKind).isSameAs(ResultKind.SUCCESS)
    assertThat(tableEnv.listTables()).isEmpty()
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
    assertThatObject(tableResult1.getResultKind).isSameAs(ResultKind.SUCCESS)
    assertThat(tableEnv.listTables()).containsExactly("tbl1")

    // fail the case
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () => tableEnv.executeSql("DROP TEMPORARY TABLE invalid_catalog.invalid_database.tbl1"))
  }

  @Test
  def testExecuteSqlWithCreateAlterDropDatabase(): Unit = {
    val tableResult1 = tableEnv.executeSql("CREATE DATABASE db1 COMMENT 'db1_comment'")
    assertThatObject(tableResult1.getResultKind).isSameAs(ResultKind.SUCCESS)
    assertThat(tableEnv.getCatalog(tableEnv.getCurrentCatalog).get().databaseExists("db1")).isTrue

    val tableResult2 = tableEnv.executeSql("ALTER DATABASE db1 SET ('k1' = 'a', 'k2' = 'b')")
    assertThatObject(tableResult2.getResultKind).isSameAs(ResultKind.SUCCESS)
    assertThat(
      tableEnv.getCatalog(tableEnv.getCurrentCatalog).get().getDatabase("db1").getProperties)
      .containsAllEntriesOf(util.Map.of("k1", "a", "k2", "b"))

    val tableResult3 = tableEnv.executeSql("DROP DATABASE db1")
    assertThatObject(tableResult3.getResultKind).isSameAs(ResultKind.SUCCESS)
    assertThat(tableEnv.getCatalog(tableEnv.getCurrentCatalog).get().databaseExists("db1")).isFalse
  }

  @Test
  def testExecuteSqlWithCreateDropFunction(): Unit = {
    val funcName = classOf[TestUDF].getName
    val funcName2 = classOf[SimpleScalarFunction].getName

    val tableResult1 = tableEnv.executeSql(s"CREATE FUNCTION default_database.f1 AS '$funcName'")
    assertThatObject(tableResult1.getResultKind).isSameAs(ResultKind.SUCCESS)
    assertThat(
      tableEnv
        .getCatalog(tableEnv.getCurrentCatalog)
        .get()
        .functionExists(ObjectPath.fromString("default_database.f1"))).isTrue

    val tableResult2 = tableEnv.executeSql(s"ALTER FUNCTION default_database.f1 AS '$funcName2'")
    assertThatObject(tableResult2.getResultKind).isSameAs(ResultKind.SUCCESS)
    assertThat(
      tableEnv
        .getCatalog(tableEnv.getCurrentCatalog)
        .get()
        .functionExists(ObjectPath.fromString("default_database.f1"))).isTrue

    val tableResult3 = tableEnv.executeSql("DROP FUNCTION default_database.f1")
    assertThatObject(tableResult3.getResultKind).isSameAs(ResultKind.SUCCESS)
    assertThat(
      tableEnv
        .getCatalog(tableEnv.getCurrentCatalog)
        .get()
        .functionExists(ObjectPath.fromString("default_database.f1"))).isFalse

    val tableResult4 = tableEnv.executeSql(s"CREATE TEMPORARY SYSTEM FUNCTION f2 AS '$funcName'")
    assertThatObject(tableResult4.getResultKind).isSameAs(ResultKind.SUCCESS)
    assertThat(tableEnv.listUserDefinedFunctions()).containsExactly("f2")

    val tableResult5 = tableEnv.executeSql("DROP TEMPORARY SYSTEM FUNCTION f2")
    assertThatObject(tableResult5.getResultKind).isSameAs(ResultKind.SUCCESS)
    assertThat(tableEnv.listUserDefinedFunctions()).doesNotContain("f2")
  }

  @Test
  def testExecuteSqlWithCreateUseDropCatalog(): Unit = {
    val tableResult1 =
      tableEnv.executeSql("CREATE CATALOG my_catalog WITH('type'='generic_in_memory')")
    assertThatObject(tableResult1.getResultKind).isSameAs(ResultKind.SUCCESS)
    assertThat(tableEnv.getCatalog("my_catalog")).isPresent

    assertThat(tableEnv.getCurrentCatalog).hasToString("default_catalog")
    val tableResult2 = tableEnv.executeSql("USE CATALOG my_catalog")
    assertThatObject(tableResult2.getResultKind).isSameAs(ResultKind.SUCCESS)
    assertThat(tableEnv.getCurrentCatalog).hasToString("my_catalog")

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => tableEnv.executeSql("DROP CATALOG my_catalog"))
      .havingRootCause()
      .withMessageContaining("Cannot drop a catalog which is currently in use.")

    tableEnv.executeSql("USE CATALOG default_catalog")

    val tableResult3 = tableEnv.executeSql("DROP CATALOG my_catalog")
    assertThatObject(tableResult3.getResultKind).isSameAs(ResultKind.SUCCESS)
    assertThat(tableEnv.getCatalog("my_catalog")).isNotPresent
  }

  @Test
  def testExecuteSqlWithUseDatabase(): Unit = {
    val tableResult1 = tableEnv.executeSql("CREATE DATABASE db1 COMMENT 'db1_comment'")
    assertThatObject(tableResult1.getResultKind).isSameAs(ResultKind.SUCCESS)
    assertThat(tableEnv.getCatalog(tableEnv.getCurrentCatalog).get().databaseExists("db1")).isTrue

    assertThat(tableEnv.getCurrentDatabase).hasToString("default_database")
    val tableResult2 = tableEnv.executeSql("USE db1")
    assertThatObject(tableResult2.getResultKind).isSameAs(ResultKind.SUCCESS)
    assertThat(tableEnv.getCurrentDatabase).hasToString("db1")
  }

  @Test
  def testExecuteSqlWithShowCatalogs(): Unit = {
    tableEnv.registerCatalog("my_catalog", new GenericInMemoryCatalog("my_catalog"))
    val tableResult = tableEnv.executeSql("SHOW CATALOGS")
    assertThatObject(tableResult.getResultKind).isSameAs(ResultKind.SUCCESS_WITH_CONTENT)
    assertThat(ResolvedSchema.of(Column.physical("catalog name", DataTypes.STRING())))
      .isEqualTo(tableResult.getResolvedSchema)
    checkData(
      util.Arrays.asList(Row.of("default_catalog"), Row.of("my_catalog")).iterator(),
      tableResult.collect())
  }

  @Test
  def testExecuteSqlWithShowDatabases(): Unit = {
    val tableResult1 = tableEnv.executeSql("CREATE DATABASE db1 COMMENT 'db1_comment'")
    assertThatObject(tableResult1.getResultKind).isSameAs(ResultKind.SUCCESS)
    val tableResult2 = tableEnv.executeSql("SHOW DATABASES")
    assertThatObject(tableResult2.getResultKind).isSameAs(ResultKind.SUCCESS_WITH_CONTENT)
    assertThat(ResolvedSchema.of(Column.physical("database name", DataTypes.STRING())))
      .isEqualTo(tableResult2.getResolvedSchema)
    checkData(
      util.Arrays.asList(Row.of("db1"), Row.of("default_database")).iterator(),
      tableResult2.collect())
  }

  @Test
  def testExecuteSqlWithShowTables(): Unit = {
    createTableForTests()

    val tableResult2 = tableEnv.executeSql("SHOW TABLES")
    assertThatObject(tableResult2.getResultKind).isSameAs(ResultKind.SUCCESS_WITH_CONTENT)
    assertThat(ResolvedSchema.of(Column.physical("table name", DataTypes.STRING())))
      .isEqualTo(tableResult2.getResolvedSchema)
    checkData(util.Arrays.asList(Row.of("T1")).iterator(), tableResult2.collect())
  }

  @Test
  def testExecuteSqlWithEnhancedShowTables(): Unit = {
    val createCatalogResult =
      tableEnv.executeSql("CREATE CATALOG catalog1 WITH('type'='generic_in_memory')")
    assertThatObject(createCatalogResult.getResultKind).isSameAs(ResultKind.SUCCESS)

    val createDbResult = tableEnv.executeSql("CREATE database catalog1.db1")
    assertThatObject(createDbResult.getResultKind).isSameAs(ResultKind.SUCCESS)

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
    assertThatObject(tableResult1.getResultKind).isSameAs(ResultKind.SUCCESS)

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
    assertThatObject(tableResult2.getResultKind).isSameAs(ResultKind.SUCCESS)

    val tableResult3 = tableEnv.executeSql("SHOW TABLES FROM catalog1.db1 like 'p_r%'")
    assertThatObject(tableResult3.getResultKind).isSameAs(ResultKind.SUCCESS_WITH_CONTENT)
    assertThat(ResolvedSchema.of(Column.physical("table name", DataTypes.STRING())))
      .isEqualTo(tableResult3.getResolvedSchema)
    checkData(util.Arrays.asList(Row.of("person")).iterator(), tableResult3.collect())
  }

  @Test
  def testExecuteSqlWithEnhancedShowViews(): Unit = {
    val createCatalogResult =
      tableEnv.executeSql("CREATE CATALOG catalog1 WITH('type'='generic_in_memory')")
    assertThatObject(createCatalogResult.getResultKind).isSameAs(ResultKind.SUCCESS)

    val createDbResult = tableEnv.executeSql("CREATE database catalog1.db1")
    assertThatObject(createDbResult.getResultKind).isSameAs(ResultKind.SUCCESS)

    val createTableStmt =
      """
        |CREATE VIEW catalog1.db1.view1 AS SELECT 1, 'abc'
      """.stripMargin
    val tableResult1 = tableEnv.executeSql(createTableStmt)
    assertThatObject(tableResult1.getResultKind).isSameAs(ResultKind.SUCCESS)

    val createTableStmt2 =
      """
        |CREATE VIEW catalog1.db1.view2 AS SELECT 123
      """.stripMargin
    val tableResult2 = tableEnv.executeSql(createTableStmt2)
    assertThatObject(tableResult2.getResultKind).isSameAs(ResultKind.SUCCESS)

    val tableResult3 = tableEnv.executeSql("SHOW VIEWS FROM catalog1.db1 like '%w1'")
    assertThatObject(tableResult3.getResultKind).isSameAs(ResultKind.SUCCESS_WITH_CONTENT)
    assertThat(ResolvedSchema.of(Column.physical("view name", DataTypes.STRING())))
      .isEqualTo(tableResult3.getResolvedSchema)
    checkData(util.Arrays.asList(Row.of("view1")).iterator(), tableResult3.collect())
  }

  @Test
  def testExecuteSqlWithShowFunctions(): Unit = {
    val tableResult = tableEnv.executeSql("SHOW FUNCTIONS")
    assertThatObject(tableResult.getResultKind).isSameAs(ResultKind.SUCCESS_WITH_CONTENT)
    assertThat(ResolvedSchema.of(Column.physical("function name", DataTypes.STRING())))
      .isEqualTo(tableResult.getResolvedSchema)
    checkData(
      tableEnv.listFunctions().map(Row.of(_)).toList.asJava.iterator(),
      tableResult.collect())

    val funcName = classOf[TestUDF].getName
    val tableResult1 = tableEnv.executeSql(s"CREATE FUNCTION default_database.f1 AS '$funcName'")
    assertThatObject(tableResult1.getResultKind).isSameAs(ResultKind.SUCCESS)
    val tableResult2 = tableEnv.executeSql("SHOW USER FUNCTIONS")
    assertThatObject(tableResult2.getResultKind).isSameAs(ResultKind.SUCCESS_WITH_CONTENT)
    assertThat(ResolvedSchema.of(Column.physical("function name", DataTypes.STRING())))
      .isEqualTo(tableResult2.getResolvedSchema)
    checkData(util.Arrays.asList(Row.of("f1")).iterator(), tableResult2.collect())
  }

  @Test
  def testExecuteSqlWithLoadModule(): Unit = {
    val result = tableEnv.executeSql("LOAD MODULE dummy")
    assertThatObject(result.getResultKind).isSameAs(ResultKind.SUCCESS)
    checkListModules("core", "dummy")
    checkListFullModules(("core", true), ("dummy", true))

    val statement =
      """
        |LOAD MODULE dummy WITH (
        |  'type' = 'dummy'
        |)
      """.stripMargin

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => tableEnv.executeSql(statement))
      .withMessageContaining(
        "Option 'type' = 'dummy' is not supported since module name is used to find module")
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
    assertThatObject(result.getResultKind).isSameAs(ResultKind.SUCCESS)
    checkListModules("core", "dummy")
    checkListFullModules(("core", true), ("dummy", true))

    val statement2 =
      """
        |LOAD MODULE dummy WITH (
        |  'dummy-version' = '2'
        |)
      """.stripMargin
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => tableEnv.executeSql(statement2))
      .withMessageContaining("Could not execute LOAD MODULE `dummy` WITH ('dummy-version' = '2')." +
        " A module with name 'dummy' already exists")
  }

  @Test
  def testExecuteSqlWithLoadCaseSensitiveModuleName(): Unit = {
    val statement1 =
      """
        |LOAD MODULE Dummy WITH (
        |  'dummy-version' = '1'
        |)
      """.stripMargin

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => tableEnv.executeSql(statement1))
      .withMessageContaining("Could not execute LOAD MODULE `Dummy` WITH ('dummy-version' = '1')."
        + " Unable to create module 'Dummy'.")

    val statement2 =
      """
        |LOAD MODULE dummy WITH (
        |  'dummy-version' = '2'
        |)
      """.stripMargin
    val result = tableEnv.executeSql(statement2)
    assertThatObject(result.getResultKind).isSameAs(ResultKind.SUCCESS)
    checkListModules("core", "dummy")
    checkListFullModules(("core", true), ("dummy", true))
  }

  @Test
  def testExecuteSqlWithUnloadModuleTwice(): Unit = {
    tableEnv.executeSql("LOAD MODULE dummy")
    checkListModules("core", "dummy")
    checkListFullModules(("core", true), ("dummy", true))

    val result = tableEnv.executeSql("UNLOAD MODULE dummy")
    assertThatObject(result.getResultKind).isSameAs(ResultKind.SUCCESS)
    checkListModules("core")
    checkListFullModules(("core", true))

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => tableEnv.executeSql("UNLOAD MODULE dummy"))
      .withMessageContaining("Could not execute UNLOAD MODULE dummy." +
        " No module with name 'dummy' exists")
  }

  @Test
  def testExecuteSqlWithUseModules(): Unit = {
    tableEnv.executeSql("LOAD MODULE dummy")
    checkListModules("core", "dummy")
    checkListFullModules(("core", true), ("dummy", true))

    val result1 = tableEnv.executeSql("USE MODULES dummy")
    assertThatObject(result1.getResultKind).isSameAs(ResultKind.SUCCESS)
    checkListModules("dummy")
    checkListFullModules(("dummy", true), ("core", false))

    val result2 = tableEnv.executeSql("USE MODULES dummy, core")
    assertThatObject(result2.getResultKind).isSameAs(ResultKind.SUCCESS)
    checkListModules("dummy", "core")
    checkListFullModules(("dummy", true), ("core", true))

    val result3 = tableEnv.executeSql("USE MODULES core, dummy")
    assertThatObject(result3.getResultKind).isSameAs(ResultKind.SUCCESS)
    checkListModules("core", "dummy")
    checkListFullModules(("core", true), ("dummy", true))

    val result4 = tableEnv.executeSql("USE MODULES core")
    assertThatObject(result4.getResultKind).isSameAs(ResultKind.SUCCESS)
    checkListModules("core")
    checkListFullModules(("core", true), ("dummy", false))
  }

  @Test
  def testExecuteSqlWithUseUnloadedModules(): Unit = {
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => tableEnv.executeSql("USE MODULES core, dummy"))
      .withMessageContaining("Could not execute USE MODULES: [core, dummy]. " +
        "No module with name 'dummy' exists")
  }

  @Test
  def testExecuteSqlWithUseDuplicateModuleNames(): Unit = {
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => tableEnv.executeSql("USE MODULES core, core"))
      .withMessageContaining("Could not execute USE MODULES: [core, core]. " +
        "Module 'core' appears more than once")
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
  def testExecuteSqlWithCreateDropView(): Unit = {
    createTableForTests()

    val viewResult1 = tableEnv.executeSql("CREATE VIEW IF NOT EXISTS v1 AS SELECT * FROM T1")
    assertThatObject(viewResult1.getResultKind).isSameAs(ResultKind.SUCCESS)
    assertThat(
      tableEnv
        .getCatalog(tableEnv.getCurrentCatalog)
        .get()
        .tableExists(ObjectPath.fromString(s"${tableEnv.getCurrentDatabase}.v1"))).isTrue

    val viewResult2 = tableEnv.executeSql("DROP VIEW IF EXISTS v1")
    assertThatObject(viewResult2.getResultKind).isSameAs(ResultKind.SUCCESS)
    assertThat(
      tableEnv
        .getCatalog(tableEnv.getCurrentCatalog)
        .get()
        .tableExists(ObjectPath.fromString(s"${tableEnv.getCurrentDatabase}.v1"))).isFalse
  }

  @Test
  def testExecuteSqlWithCreateDropTemporaryView(): Unit = {
    createTableForTests()

    val viewResult1 =
      tableEnv.executeSql("CREATE TEMPORARY VIEW IF NOT EXISTS v1 AS SELECT * FROM T1")
    assertThatObject(viewResult1.getResultKind).isSameAs(ResultKind.SUCCESS)
    assertThat(tableEnv.listTables()).containsExactly("T1", "v1")

    val viewResult2 = tableEnv.executeSql("DROP TEMPORARY VIEW IF EXISTS v1")
    assertThatObject(viewResult2.getResultKind).isSameAs(ResultKind.SUCCESS)
    assertThat(tableEnv.listTables()).containsExactly("T1")
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

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () => {
          tableEnv.executeSql(sourceDDL)
          tableEnv.executeSql(sinkDDL)
          tableEnv.executeSql(viewDDL)
        })
      .withMessageContaining(
        "VIEW definition and input fields not match:\n" +
          "\tDef fields: [d].\n" +
          "\tInput fields: [a, b, c].")
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

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => tableEnv.executeSql(viewWith2ColumnDDL))
      .withMessageContaining(
        "Could not execute CreateTable in path `default_catalog`.`default_database`.`T3`")
  }

  @ParameterizedTest
  @ValueSource(booleans = Array[Boolean](true, false))
  def testDropViewWithFullPath(isSql: Boolean): Unit = {
    createViewsForDropTests()

    assertThat(tableEnv.listTables()).containsExactly("T1", "T2", "T3")

    dropView("default_catalog.default_database.T2", isSql)
    assertThat(tableEnv.listTables()).containsExactly("T1", "T3")

    dropView("default_catalog.default_database.T3", isSql)
    assertThat(tableEnv.listTables()).containsExactly("T1")
  }

  @ParameterizedTest
  @ValueSource(booleans = Array[Boolean](true, false))
  def testDropViewWithPartialPath(isSql: Boolean): Unit = {
    createViewsForDropTests()

    assertThat(tableEnv.listTables()).containsExactly("T1", "T2", "T3")

    dropView("T2", isSql)
    assertThat(tableEnv.listTables()).containsExactly("T1", "T3")

    dropView("T3", isSql)
    assertThat(tableEnv.listTables()).containsExactly("T1")
  }

  @Test
  def testDropViewIfExistsTwice(): Unit = {
    createViewsForDropTests()

    assertThat(tableEnv.listTables()).containsExactly("T1", "T2", "T3")

    tableEnv.executeSql("DROP VIEW IF EXISTS default_catalog.default_database.T2")
    assertThat(tableEnv.listTables()).containsExactly("T1", "T3")

    tableEnv.executeSql("DROP VIEW IF EXISTS default_catalog.default_database.T2")
    assertThat(tableEnv.listTables()).containsExactly("T1", "T3")
  }

  @Test
  def testDropViewTwice(): Unit = {
    createViewsForDropTests()

    assertThat(tableEnv.listTables()).containsExactly("T1", "T2", "T3")

    tableEnv.executeSql("DROP VIEW default_catalog.default_database.T2")
    assertThat(tableEnv.listTables()).containsExactly("T1", "T3")

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => tableEnv.executeSql("DROP VIEW default_catalog.default_database.T2"))
  }

  @Test
  def testDropView(): Unit = {
    createViewsForDropTests()

    assertThat(tableEnv.listTables()).containsExactly("T1", "T2", "T3")

    assertThat(tableEnv.dropView("default_catalog.default_database.T2")).isTrue
    assertThat(tableEnv.listTables()).containsExactly("T1", "T3")

    assertThat(tableEnv.dropView("default_catalog.default_database.T2")).isFalse
    assertThat(tableEnv.dropView("invalid.default_database.T2")).isFalse
    assertThat(tableEnv.dropView("default_catalog.invalid.T2")).isFalse
    assertThat(tableEnv.dropView("default_catalog.default_database.invalid")).isFalse
    assertThat(tableEnv.listTables()).containsExactly("T1", "T3")

    tableEnv.createTemporaryView("T3", tableEnv.sqlQuery("SELECT 123"))

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => tableEnv.dropView("T3"))
      .withMessageContaining(
        "Temporary view with identifier '`default_catalog`.`default_database`.`T3`' exists. " +
          "Drop it first before removing the permanent view.")

    tableEnv.dropTemporaryView("T3")

    assertThat(tableEnv.listTables()).containsExactly("T1", "T3")
    // Now can drop permanent view
    tableEnv.dropView("T3")
    assertThat(tableEnv.listTables()).containsExactly("T1")
  }

  @Test
  def testDropTable(): Unit = {
    createTableForTests()

    assertThat(tableEnv.listTables()).containsExactly("T1")

    assertThat(tableEnv.dropTable("default_catalog.default_database.T2")).isFalse
    assertThat(tableEnv.listTables()).containsExactly("T1")

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => tableEnv.dropTable("default_catalog.default_database.T2", false))
      .withMessageContaining(
        "Table with identifier 'default_catalog.default_database.T2' does not exist.")

    assertThat(tableEnv.dropTable("default_catalog.default_database.T2")).isFalse
    assertThat(tableEnv.dropTable("invalid.default_database.T2")).isFalse
    assertThat(tableEnv.dropTable("default_catalog.invalid.T2")).isFalse
    assertThat(tableEnv.dropTable("default_catalog.default_database.invalid")).isFalse
    assertThat(tableEnv.listTables()).containsExactly("T1")

    assertThat(tableEnv.dropTable("default_catalog.default_database.T1")).isTrue
    assertThat(tableEnv.listTables()).isEmpty()
    createTableForTests()
    assertThat(tableEnv.listTables()).containsExactly("T1")

    tableEnv.createTemporaryTable(
      "T1",
      TableDescriptor
        .forConnector("values")
        .schema(Schema.newBuilder().column("col1", DataTypes.INT()).build())
        .build())
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => tableEnv.dropTable("T1"))
      .withMessageContaining(
        "Temporary table with identifier '`default_catalog`.`default_database`.`T1`' exists. " +
          "Drop it first before removing the permanent table.")

    tableEnv.dropTemporaryTable("T1")

    assertThat(tableEnv.listTables()).containsExactly("T1")
    // Now can drop permanent table
    tableEnv.dropTable("T1")
    assertThat(tableEnv.listTables()).isEmpty()
  }

  @Test
  def testDropViewWithInvalidPath(): Unit = {
    createViewsForDropTests()
    assertThat(tableEnv.listTables()).containsExactly("T1", "T2", "T3")
    // failed since 'default_catalog1.default_database1.T2' is invalid path
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => tableEnv.executeSql("DROP VIEW default_catalog1.default_database1.T2"))
  }

  @Test
  def testDropViewWithInvalidPathIfExists(): Unit = {
    createViewsForDropTests()
    assertThat(tableEnv.listTables()).containsExactly("T1", "T2", "T3")
    tableEnv.executeSql("DROP VIEW IF EXISTS default_catalog1.default_database1.T2")
    assertThat(tableEnv.listTables()).containsExactly("T1", "T2", "T3")
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

    assertThat(tableEnv.listTemporaryViews()).containsExactly("T2")

    tableEnv.executeSql("DROP TEMPORARY VIEW IF EXISTS default_catalog.default_database.T2")
    assertThat(tableEnv.listTemporaryViews()).isEmpty()

    tableEnv.executeSql("DROP TEMPORARY VIEW IF EXISTS default_catalog.default_database.T2")
    assertThat(tableEnv.listTemporaryViews()).isEmpty()
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

    assertThat(tableEnv.listTemporaryViews()).containsExactly("T2")

    tableEnv.executeSql("DROP TEMPORARY VIEW default_catalog.default_database.T2")
    assertThat(tableEnv.listTemporaryViews()).isEmpty()

    // throws ValidationException since default_catalog.default_database.T2 is not exists
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () => tableEnv.executeSql("DROP TEMPORARY VIEW default_catalog.default_database.T2"))
  }

  @Test
  def testExecuteSqlWithShowViews(): Unit = {
    createTableForTests()

    val tableResult2 = tableEnv.executeSql("CREATE VIEW view1 AS SELECT * FROM T1")
    assertThatObject(tableResult2.getResultKind).isSameAs(ResultKind.SUCCESS)

    val tableResult3 = tableEnv.executeSql("SHOW VIEWS")
    assertThatObject(tableResult3.getResultKind).isSameAs(ResultKind.SUCCESS_WITH_CONTENT)
    assertThat(ResolvedSchema.of(Column.physical("view name", DataTypes.STRING())))
      .isEqualTo(tableResult3.getResolvedSchema)
    checkData(util.Arrays.asList(Row.of("view1")).iterator(), tableResult3.collect())

    val tableResult4 = tableEnv.executeSql("CREATE TEMPORARY VIEW view2 AS SELECT * FROM T1")
    assertThatObject(tableResult4.getResultKind).isSameAs(ResultKind.SUCCESS)

    // SHOW VIEWS also shows temporary views
    val tableResult5 = tableEnv.executeSql("SHOW VIEWS")
    assertThatObject(tableResult5.getResultKind).isSameAs(ResultKind.SUCCESS_WITH_CONTENT)
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
    assertThatObject(tableResult1.getResultKind).isSameAs(ResultKind.SUCCESS)

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
    assertThatObject(tableResult1.getResultKind).isSameAs(ResultKind.SUCCESS)

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
    assertThatObject(tableResult2.getResultKind).isSameAs(ResultKind.SUCCESS)

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
    assertThatObject(tableResult1.getResultKind).isSameAs(ResultKind.SUCCESS)

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
    assertThatObject(tableResult2.getResultKind).isSameAs(ResultKind.SUCCESS)

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
    assertThatObject(tableResult1.getResultKind).isSameAs(ResultKind.SUCCESS)

    // TODO we can support them later
    testUnsupportedExplain("explain plan excluding attributes for select * from MyTable")
    testUnsupportedExplain("explain plan including all attributes for select * from MyTable")
    testUnsupportedExplain("explain plan with type for select * from MyTable")
    testUnsupportedExplain("explain plan without implementation for select * from MyTable")
    testUnsupportedExplain("explain plan as xml for select * from MyTable")
    testUnsupportedExplain("explain plan as json for select * from MyTable")
  }

  private def testUnsupportedExplain(explain: String): Unit = {
    assertThatExceptionOfType(classOf[SqlParserException])
      .isThrownBy(() => tableEnv.executeSql(explain))
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
    assertThatObject(tableResult1.getResultKind).isSameAs(ResultKind.SUCCESS)

    val actual =
      tableEnv.explainSql("select * from MyTable where a > 10", ExplainDetail.CHANGELOG_MODE)
    val expected = TableTestUtil.readFromResource("/explain/testExplainSqlWithSelect.out")
    assertThat(replaceStageId(actual)).isEqualTo(replaceStageId(expected))
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
    assertThatObject(tableResult1.getResultKind).isSameAs(ResultKind.SUCCESS)

    val actual = tableEnv.explainSql(
      "execute select * from MyTable where a > 10",
      ExplainDetail.CHANGELOG_MODE)
    val expected = TableTestUtil.readFromResource("/explain/testExplainSqlWithSelect.out")
    assertThat(replaceStageId(actual)).isEqualTo(replaceStageId(expected))
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
    assertThatObject(tableResult1.getResultKind).isSameAs(ResultKind.SUCCESS)

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
    assertThatObject(tableResult2.getResultKind).isSameAs(ResultKind.SUCCESS)

    val actual = tableEnv.explainSql("insert into MySink select a, b from MyTable where a > 10")
    val expected = TableTestUtil.readFromResource("/explain/testExplainSqlWithInsert.out")
    assertThat(replaceStageId(actual)).isEqualTo(replaceStageId(expected))
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
    assertThatObject(tableResult1.getResultKind).isSameAs(ResultKind.SUCCESS)

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
    assertThatObject(tableResult1.getResultKind).isSameAs(ResultKind.SUCCESS)

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
    assertThatObject(tableResult3.getResultKind).isSameAs(ResultKind.SUCCESS)

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
    assertThatObject(tableResult1.getResultKind).isSameAs(ResultKind.SUCCESS)

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
    assertThatObject(tableResult2.getResultKind).isSameAs(ResultKind.SUCCESS)

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
    assertThatObject(tableResult1.getResultKind).isSameAs(ResultKind.SUCCESS_WITH_CONTENT)
    checkData(expectedResult1.iterator(), tableResult1.collect())
    val tableResult2 = tableEnv.executeSql("desc T1")
    assertThatObject(tableResult2.getResultKind).isSameAs(ResultKind.SUCCESS_WITH_CONTENT)
    checkData(expectedResult1.iterator(), tableResult2.collect())

    val expectedResult2 = util.Arrays.asList(
      Row.of("d", "INT", Boolean.box(false), null, null, null),
      Row.of("e", "STRING", Boolean.box(false), null, null, null),
      Row.of("f", "ROW<`f0` INT NOT NULL, `f1` INT>", Boolean.box(false), null, null, null)
    )
    val tableResult3 = tableEnv.executeSql("describe T2")
    assertThatObject(tableResult3.getResultKind).isSameAs(ResultKind.SUCCESS_WITH_CONTENT)
    checkData(expectedResult2.iterator(), tableResult3.collect())
    val tableResult4 = tableEnv.executeSql("desc T2")
    assertThatObject(tableResult4.getResultKind).isSameAs(ResultKind.SUCCESS_WITH_CONTENT)
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
    assertThatObject(tableResult5.getResultKind).isSameAs(ResultKind.SUCCESS_WITH_CONTENT)
    checkData(expectedResult3.iterator(), tableResult5.collect())
    val tableResult6 = tableEnv.executeSql("desc T2")
    assertThatObject(tableResult6.getResultKind).isSameAs(ResultKind.SUCCESS_WITH_CONTENT)
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
    assertThatObject(tableResult1.getResultKind).isSameAs(ResultKind.SUCCESS_WITH_CONTENT)
    checkData(expectedResult1.iterator(), tableResult1.collect())
    val tableResult2 = tableEnv.executeSql("desc T1")
    assertThatObject(tableResult2.getResultKind).isSameAs(ResultKind.SUCCESS_WITH_CONTENT)
    checkData(expectedResult1.iterator(), tableResult2.collect())
  }

  @Test
  def testAlterModelOptions(): Unit = {
    val sourceDDL =
      """
        |CREATE MODEL M1
        |  INPUT(f0 char(10), f1 varchar(10))
        |  OUTPUT(f2 string)
        |with (
        |  'task' = 'clustering',
        |  'provider' = 'openai',
        |  'openai.endpoint' = 'some-endpoint'
        |)
      """.stripMargin
    tableEnv.executeSql(sourceDDL)

    val alterDDL =
      """
        |ALTER MODEL M1
        |SET(
        |  'openai.endpoint' = 'openai-endpoint',
        |  'task' = 'embedding'
        |)
        |""".stripMargin
    tableEnv.executeSql(alterDDL)

    assertThat(
      tableEnv
        .getCatalog(tableEnv.getCurrentCatalog)
        .get()
        .getModel(ObjectPath.fromString(s"${tableEnv.getCurrentDatabase}.M1"))
        .getOptions)
      .containsExactlyInAnyOrderEntriesOf(
        util.Map.of("provider", "openai", "task", "embedding", "openai.endpoint", "openai-endpoint")
      )
  }

  @Test
  def testAlterModelEmptyOptions(): Unit = {
    val sourceDDL =
      """
        |CREATE MODEL M1
        |  INPUT(f0 char(10), f1 varchar(10))
        |  OUTPUT(f2 string)
        |with (
        |  'task' = 'clustering',
        |  'provider' = 'openai',
        |  'openai.endpoint' = 'some-endpoint'
        |)
      """.stripMargin
    tableEnv.executeSql(sourceDDL)

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () =>
          tableEnv
            .executeSql("ALTER MODEL M1 SET ()"))
      .withMessageContaining("ALTER MODEL SET does not support empty option.");
  }

  @Test
  def testAlterNonExistModel(): Unit = {
    val alterDDL =
      """
        |ALTER MODEL M1
        |SET(
        |  'provider' = 'azureml',
        |  'azueml.endpoint' = 'azure-endpoint',
        |  'task' = 'clustering'
        |)
        |""".stripMargin
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => tableEnv.executeSql(alterDDL))
      .withMessageContaining("Model `default_catalog`.`default_database`.`M1` doesn't exist.")
  }

  @Test
  def testAlterNonExistModelWithIfExist(): Unit = {
    val alterDDL =
      """
        |ALTER MODEL IF EXISTS M1
        |SET(
        |  'provider' = 'azureml',
        |  'azueml.endpoint' = 'azure-endpoint',
        |  'task' = 'clustering'
        |)
        |""".stripMargin
    tableEnv.executeSql(alterDDL)
  }

  @Test
  def testAlterModelRename(): Unit = {
    val sourceDDL =
      """
        |CREATE MODEL M1
        |  INPUT(f0 char(10), f1 varchar(10))
        |  OUTPUT(f2 string)
        |with (
        |  'task' = 'clustering',
        |  'provider' = 'openai',
        |  'openai.endpoint' = 'some-endpoint'
        |)
      """.stripMargin
    tableEnv.executeSql(sourceDDL)

    val alterDDL =
      """
        |ALTER MODEL M1 RENAME TO M2
        |""".stripMargin
    tableEnv.executeSql(alterDDL)
  }

  @Test
  def testAlterModelRenameNonExist(): Unit = {
    val alterDDL =
      """
        |ALTER MODEL M1 RENAME TO M2
        |""".stripMargin
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => tableEnv.executeSql(alterDDL))
      .withMessageContaining("Model `default_catalog`.`default_database`.`M1` doesn't exist.")
  }

  @Test
  def testAlterModelRenameDifferentCatalog(): Unit = {
    val sourceDDL =
      """
        |CREATE MODEL M1
        |  INPUT(f0 char(10), f1 varchar(10))
        |  OUTPUT(f2 string)
        |with (
        |  'task' = 'clustering',
        |  'provider' = 'openai',
        |  'openai.endpoint' = 'some-endpoint'
        |)
      """.stripMargin
    tableEnv.executeSql(sourceDDL)

    val alterDDL =
      """
        |ALTER MODEL `default_catalog`.`default_database`.`M1` RENAME TO `other_catalog`.`default_database`.`M2`
        |""".stripMargin
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => tableEnv.executeSql(alterDDL))
      .withMessageContaining(
        "The catalog name of the new model name 'other_catalog.default_database.M2' " +
          "must be the same as the old model name 'default_catalog.default_database.M1'.")
  }

  @Test
  def testAlterModelRenameWithIfExists(): Unit = {
    val alterDDL =
      """
        |ALTER MODEL IF EXISTS M1 RENAME TO M2
        |""".stripMargin
    tableEnv.executeSql(alterDDL)
  }

  @Test
  def testAlterModelReset(): Unit = {
    val sourceDDL =
      """
        |CREATE MODEL M1
        |  INPUT(f0 char(10), f1 varchar(10))
        |  OUTPUT(f2 string)
        |with (
        |  'task' = 'clustering',
        |  'provider' = 'openai',
        |  'openai.endpoint' = 'some-endpoint'
        |)
      """.stripMargin
    tableEnv.executeSql(sourceDDL)

    tableEnv.executeSql("ALTER MODEL M1 RESET ('task')");

    assertThat(
      tableEnv
        .getCatalog(tableEnv.getCurrentCatalog)
        .get()
        .getModel(ObjectPath.fromString(s"${tableEnv.getCurrentDatabase}.M1"))
        .getOptions
    ).containsExactlyInAnyOrderEntriesOf(
      util.Map.of("provider", "openai", "openai.endpoint", "some-endpoint"))
  }

  @Test
  def testAlterModelResetNonExist(): Unit = {
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => tableEnv.executeSql("ALTER MODEL M1 RESET ('task')"))
      .withMessageContaining("Model `default_catalog`.`default_database`.`M1` doesn't exist.")
  }

  @Test
  def testAlterModelResetWithIfExists(): Unit = {
    tableEnv.executeSql("ALTER MODEL IF EXISTS M1 RESET ('task')");
  }

  @Test
  def testAlterModelRestEmptyOptionKey(): Unit = {
    val sourceDDL =
      """
        |CREATE MODEL M1
        |  INPUT(f0 char(10), f1 varchar(10))
        |  OUTPUT(f2 string)
        |with (
        |  'task' = 'clustering',
        |  'provider' = 'openai',
        |  'openai.endpoint' = 'some-endpoint'
        |)
      """.stripMargin
    tableEnv.executeSql(sourceDDL)

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () =>
          tableEnv
            .executeSql("ALTER MODEL M1 RESET ()"))
      .withMessageContaining("ALTER MODEL RESET does not support empty key.");
  }

  @Test
  def testCreateModelMissingInput(): Unit = {
    val sourceDDL =
      """
        |CREATE MODEL M1
        |  OUTPUT(f2 string)
        |with (
        |  'task' = 'clustering',
        |  'provider' = 'openai',
        |  'openai.endpoint' = 'some-endpoint'
        |)
      """.stripMargin
    assertThatExceptionOfType(classOf[SqlValidateException])
      .isThrownBy(() => tableEnv.executeSql(sourceDDL))
      .withMessageContaining(
        "Input column list can not be empty with non-empty output column list.")
  }

  @Test
  def testCreateModelDuplicateInputColumn(): Unit = {
    val sourceDDL =
      """
        |CREATE MODEL M1
        |  INPUT(f1 string, f1 string)
        |  OUTPUT(f2 string)
        |with (
        |  'task' = 'clustering',
        |  'provider' = 'openai',
        |  'openai.endpoint' = 'some-endpoint'
        |)
      """.stripMargin
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => tableEnv.executeSql(sourceDDL))
      .withMessageContaining("Duplicate input column name: 'f1'.")
  }

  @Test
  def testCreateModelDuplicateOutputColumn(): Unit = {
    val sourceDDL =
      """
        |CREATE MODEL M1
        |  INPUT(f1 string)
        |  OUTPUT(f2 string, f2 string)
        |with (
        |  'task' = 'clustering',
        |  'provider' = 'openai',
        |  'openai.endpoint' = 'some-endpoint'
        |)
      """.stripMargin
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => tableEnv.executeSql(sourceDDL))
      .withMessageContaining("Duplicate output column name: 'f2'.")
  }

  @Test
  def testCreateModelMissingOutput(): Unit = {
    val sourceDDL =
      """
        |CREATE MODEL M1
        |  INPUT(f1 string)
        |with (
        |  'task' = 'clustering',
        |  'provider' = 'openai',
        |  'openai.endpoint' = 'some-endpoint'
        |)
      """.stripMargin
    assertThatExceptionOfType(classOf[SqlValidateException])
      .isThrownBy(() => tableEnv.executeSql(sourceDDL))
      .withMessageContaining("")
  }

  @Test
  def testCreateModelMissingOption(): Unit = {
    val sourceDDL =
      """
        |CREATE MODEL M1
        |  INPUT(f1 string)
        |  OUTPUT(f2 string)
        |with ()
      """.stripMargin
    assertThatExceptionOfType(classOf[SqlValidateException])
      .isThrownBy(() => tableEnv.executeSql(sourceDDL))
      .withMessageContaining("Model property list can not be empty.")
  }

  @Test
  def testDescribeModelWithComment(): Unit = {
    val sourceDDL =
      """
        |CREATE MODEL M1
        |  INPUT(f0 char(10) COMMENT 'comment', f1 varchar(10))
        |  OUTPUT(f2 string)
        |with (
        |  'task' = 'clustering',
        |  'provider' = 'openai',
        |  'openai.endpoint' = 'some-endpoint'
        |)
      """.stripMargin

    tableEnv.executeSql(sourceDDL)

    val expectedResult1 = util.Arrays.asList(
      Row.of("f0", "CHAR(10)", Boolean.box(true), Boolean.box(true), "comment"),
      Row.of("f1", "VARCHAR(10)", Boolean.box(true), Boolean.box(true), null),
      Row.of("f2", "STRING", Boolean.box(true), Boolean.box(false), null)
    )
    val modelResult1 = tableEnv.executeSql("describe MODEL M1")
    assertThatObject(modelResult1.getResultKind).isSameAs(ResultKind.SUCCESS_WITH_CONTENT)
    checkData(expectedResult1.iterator(), modelResult1.collect())
    val modelResult2 = tableEnv.executeSql("desc model M1")
    assertThatObject(modelResult2.getResultKind).isSameAs(ResultKind.SUCCESS_WITH_CONTENT)
    checkData(expectedResult1.iterator(), modelResult2.collect())
  }

  @Test
  def testDescribeModel(): Unit = {
    val sourceDDL =
      """
        |CREATE MODEL M1
        |  INPUT(f0 char(10), f1 varchar(10))
        |  OUTPUT(f2 string)
        |with (
        |  'task' = 'clustering',
        |  'provider' = 'openai',
        |  'openai.endpoint' = 'some-endpoint'
        |)
      """.stripMargin

    tableEnv.executeSql(sourceDDL)

    val expectedResult1 = util.Arrays.asList(
      Row.of("f0", "CHAR(10)", Boolean.box(true), Boolean.box(true)),
      Row.of("f1", "VARCHAR(10)", Boolean.box(true), Boolean.box(true)),
      Row.of("f2", "STRING", Boolean.box(true), Boolean.box(false))
    )
    val modelResult1 = tableEnv.executeSql("describe MODEL M1")
    assertThatObject(modelResult1.getResultKind).isSameAs(ResultKind.SUCCESS_WITH_CONTENT)
    checkData(expectedResult1.iterator(), modelResult1.collect())
    val modelResult2 = tableEnv.executeSql("desc model M1")
    assertThatObject(modelResult2.getResultKind).isSameAs(ResultKind.SUCCESS_WITH_CONTENT)
    checkData(expectedResult1.iterator(), modelResult2.collect())
  }

  @Test
  def testDescribeModelWithNoInputOutput(): Unit = {
    val sourceDDL =
      """
        |CREATE MODEL M1
        |  COMMENT 'this is a model'
        |with (
        |  'task' = 'clustering',
        |  'provider' = 'openai',
        |  'openai.endpoint' = 'some-endpoint'
        |)
      """.stripMargin

    tableEnv.executeSql(sourceDDL)

    val expectedResult1 = new util.ArrayList[Row]()
    val modelResult1 = tableEnv.executeSql("describe model M1")
    assertThatObject(modelResult1.getResultKind).isSameAs(ResultKind.SUCCESS_WITH_CONTENT)
    checkData(expectedResult1.iterator(), modelResult1.collect())
    val modelResult2 = tableEnv.executeSql("desc model M1")
    assertThatObject(modelResult2.getResultKind).isSameAs(ResultKind.SUCCESS_WITH_CONTENT)
    checkData(expectedResult1.iterator(), modelResult2.collect())
  }

  @Test
  def testShowCreateModel(): Unit = {
    val sourceDDL =
      """
        |CREATE MODEL M1
        |  INPUT(f0 char(10), f1 varchar(10))
        |  OUTPUT(f2 string)
        |COMMENT 'this is a model'
        |with (
        |  'task' = 'clustering',
        |  'provider' = 'openai',
        |  'openai.endpoint' = 'some-endpoint'
        |)
      """.stripMargin

    tableEnv.executeSql(sourceDDL)

    val expectedDDL =
      """|CREATE MODEL `default_catalog`.`default_database`.`M1`
         |INPUT (`f0` CHAR(10), `f1` VARCHAR(10))
         |OUTPUT (`f2` VARCHAR(2147483647))
         |COMMENT 'this is a model'
         |WITH (
         |  'openai.endpoint' = 'some-endpoint',
         |  'provider' = 'openai',
         |  'task' = 'clustering'
         |)
         |""".stripMargin
    val row = tableEnv.executeSql("SHOW CREATE MODEL M1").collect().next()
    assertThat(row.getField(0)).isEqualTo(expectedDDL)
  }

  @Test
  def testShowCreateModelComplexTypes(): Unit = {
    val sourceDDL =
      """
        |CREATE MODEL M1
        |  INPUT(
        |    f0 ARRAY<INT>,
        |    f1 MAP<STRING, INT>,
        |    f2 ROW<name STRING, age INT>,
        |    f3 ROW<name STRING, address ROW<street STRING, city STRING>>,
        |    f4 ARRAY<ROW<id INT, details ROW<color STRING, size INT>>>
        |  )
        |  OUTPUT(
        |    f5 ARRAY<MAP<STRING, INT>>,
        |    f6 ARRAY<ARRAY<STRING>>
        |  )
        |COMMENT 'this is a model'
        |with (
        |  'task' = 'clustering',
        |  'provider' = 'openai',
        |  'openai.endpoint' = 'some-endpoint'
        |)
      """.stripMargin

    tableEnv.executeSql(sourceDDL)

    val expectedDDL =
      """|CREATE MODEL `default_catalog`.`default_database`.`M1`
         |INPUT (`f0` ARRAY<INT>, `f1` MAP<VARCHAR(2147483647), INT>, `f2` ROW<`name` VARCHAR(2147483647), `age` INT>, `f3` ROW<`name` VARCHAR(2147483647), `address` ROW<`street` VARCHAR(2147483647), `city` VARCHAR(2147483647)>>, `f4` ARRAY<ROW<`id` INT, `details` ROW<`color` VARCHAR(2147483647), `size` INT>>>)
         |OUTPUT (`f5` ARRAY<MAP<VARCHAR(2147483647), INT>>, `f6` ARRAY<ARRAY<VARCHAR(2147483647)>>)
         |COMMENT 'this is a model'
         |WITH (
         |  'openai.endpoint' = 'some-endpoint',
         |  'provider' = 'openai',
         |  'task' = 'clustering'
         |)
         |""".stripMargin
    val row = tableEnv.executeSql("SHOW CREATE MODEL M1").collect().next()
    assertThat(row.getField(0)).isEqualTo(expectedDDL)
  }

  @Test
  def testShowCreateTemporaryModel(): Unit = {
    val sourceDDL =
      """
        |CREATE TEMPORARY MODEL M1
        |  INPUT(f0 char(10), f1 varchar(10))
        |  OUTPUT(f2 string)
        |COMMENT 'this is a model'
        |with (
        |  'task' = 'clustering',
        |  'provider' = 'openai',
        |  'openai.endpoint' = 'some-endpoint'
        |)
      """.stripMargin

    tableEnv.executeSql(sourceDDL)

    val expectedDDL =
      """|CREATE TEMPORARY MODEL `default_catalog`.`default_database`.`M1`
         |INPUT (`f0` CHAR(10), `f1` VARCHAR(10))
         |OUTPUT (`f2` VARCHAR(2147483647))
         |COMMENT 'this is a model'
         |WITH (
         |  'openai.endpoint' = 'some-endpoint',
         |  'provider' = 'openai',
         |  'task' = 'clustering'
         |)
         |""".stripMargin
    val row = tableEnv.executeSql("SHOW CREATE MODEL M1").collect().next()
    assertThat(row.getField(0)).isEqualTo(expectedDDL)
  }

  @Test
  def testShowCreateNonExistModel(): Unit = {
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => tableEnv.executeSql("SHOW CREATE MODEL M1"))
      .withMessage(
        "Could not execute SHOW CREATE MODEL. Model with identifier `default_catalog`.`default_database`.`M1` does not exist.")
  }

  @Test
  def testShowCreateModelNoInputOutput(): Unit = {
    val sourceDDL =
      """
        |CREATE MODEL M1
        |  COMMENT 'this is a model'
        |with (
        |  'task' = 'clustering',
        |  'provider' = 'openai',
        |  'openai.endpoint' = 'some-endpoint'
        |)
      """.stripMargin

    tableEnv.executeSql(sourceDDL)

    val expectedDDL =
      """|CREATE MODEL `default_catalog`.`default_database`.`M1`
         |COMMENT 'this is a model'
         |WITH (
         |  'openai.endpoint' = 'some-endpoint',
         |  'provider' = 'openai',
         |  'task' = 'clustering'
         |)
         |""".stripMargin
    val row = tableEnv.executeSql("SHOW CREATE MODEL M1").collect().next()
    assertThat(row.getField(0)).isEqualTo(expectedDDL)
  }

  @Test
  def testShowCreateModelNoComment(): Unit = {
    val sourceDDL =
      """
        |CREATE MODEL M1
        |  INPUT(f0 char(10), f1 varchar(10))
        |  OUTPUT(f2 string)
        |with (
        |  'task' = 'clustering',
        |  'provider' = 'openai',
        |  'openai.endpoint' = 'some-endpoint'
        |)
      """.stripMargin

    tableEnv.executeSql(sourceDDL)

    val expectedDDL =
      """|CREATE MODEL `default_catalog`.`default_database`.`M1`
         |INPUT (`f0` CHAR(10), `f1` VARCHAR(10))
         |OUTPUT (`f2` VARCHAR(2147483647))
         |WITH (
         |  'openai.endpoint' = 'some-endpoint',
         |  'provider' = 'openai',
         |  'task' = 'clustering'
         |)
         |""".stripMargin
    val row = tableEnv.executeSql("SHOW CREATE MODEL M1").collect().next()
    assertThat(row.getField(0)).isEqualTo(expectedDDL)
  }

  @Test
  def testDropModel(): Unit = {
    val sourceDDL =
      """
        |CREATE MODEL M1
        |  INPUT(f0 char(10), f1 varchar(10))
        |  OUTPUT(f2 string)
        |with (
        |  'task' = 'clustering',
        |  'provider' = 'openai',
        |  'openai.endpoint' = 'some-endpoint'
        |)
      """.stripMargin
    tableEnv.executeSql(sourceDDL)

    val dropDDL =
      """
        |DROP MODEL M1
        |""".stripMargin
    tableEnv.executeSql(dropDDL)

    // Alter shouldn't find model now
    val alterDDL =
      """
        |ALTER MODEL M1 RENAME TO M2
        |""".stripMargin
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => tableEnv.executeSql(alterDDL))
      .withMessageContaining("Model `default_catalog`.`default_database`.`M1` doesn't exist.")
  }

  @Test
  def testDropNonExistModel(): Unit = {
    val dropDDL =
      """
        |DROP MODEL M1
        |""".stripMargin
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => tableEnv.executeSql(dropDDL))
      .withMessageContaining(
        "Model with identifier 'default_catalog.default_database.M1' does not exist.")
  }

  @Test
  def testDropNonExistModelWithIfExist(): Unit = {
    val dropDDL =
      """
        |DROP MODEL IF EXISTS M1
        |""".stripMargin
    tableEnv.executeSql(dropDDL)
  }

  @Test
  def testExecuteSqlWithShowModels(): Unit = {
    val createModelStmt = {
      """
        |CREATE MODEL M1
        |  INPUT(f0 char(10), f1 varchar(10))
        |  OUTPUT(f2 string)
        |  with (
        |  'task' = 'clustering',
        |  'provider' = 'openai',
        |  'openai.endpoint' = 'some-endpoint'
        |)
        |""".stripMargin
    }
    val tableResult1 = tableEnv.executeSql(createModelStmt)
    assertThatObject(tableResult1.getResultKind).isSameAs(ResultKind.SUCCESS)

    val tableResult2 = tableEnv.executeSql("SHOW MODELS")
    assertThatObject(tableResult2.getResultKind).isSameAs(ResultKind.SUCCESS_WITH_CONTENT)
    assertThat(ResolvedSchema.of(Column.physical("model name", DataTypes.STRING())))
      .isEqualTo(tableResult2.getResolvedSchema)
    checkData(util.Arrays.asList(Row.of("M1")).iterator(), tableResult2.collect())
  }

  def testExecuteSqlWithEnhancedShowModels(): Unit = {
    val createCatalogResult =
      tableEnv.executeSql("CREATE CATALOG catalog1 WITH('type'='generic_in_memory')")
    assertThatObject(createCatalogResult.getResultKind).isSameAs(ResultKind.SUCCESS)

    val createDbResult = tableEnv.executeSql("CREATE database catalog1.db1")
    assertThatObject(createDbResult.getResultKind).isSameAs(ResultKind.SUCCESS)

    val createModelStmt =
      """
        |CREATE MODEL catalog1.db1.my_model
        |  INPUT(f0 char(10), f1 varchar(10))
        |  OUTPUT(f2 string)
        |  with (
        |  'task' = 'clustering',
        |  'provider' = 'openai',
        |  'openai.endpoint' = 'some-endpoint'
        |)
        |""".stripMargin
    val tableResult1 = tableEnv.executeSql(createModelStmt)
    assertThatObject(tableResult1.getResultKind).isSameAs(ResultKind.SUCCESS)

    val createTableStmt2 =
      """
        |CREATE MODEL catalog1.db1.your_model
        |  INPUT(f0 char(10), f1 varchar(10))
        |  OUTPUT(f2 string)
        |  with (
        |  'task' = 'clustering',
        |  'provider' = 'openai',
        |  'openai.endpoint' = 'some-endpoint'
        |)
        |""".stripMargin

    val tableResult2 = tableEnv.executeSql(createTableStmt2)
    assertThatObject(tableResult2.getResultKind).isSameAs(ResultKind.SUCCESS)

    val tableResult3 = tableEnv.executeSql("SHOW MODELS FROM catalog1.db1 like 'you_mo%'")
    assertThatObject(tableResult3.getResultKind).isSameAs(ResultKind.SUCCESS_WITH_CONTENT)
    assertThat(ResolvedSchema.of(Column.physical("model name", DataTypes.STRING())))
      .isEqualTo(tableResult3.getResolvedSchema)
    checkData(util.Arrays.asList(Row.of("your_model")).iterator(), tableResult3.collect())
  }

  @Test
  def testGetNonExistModel(): Unit = {
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => tableEnv.fromModel("MyModel"))
      .withMessageContaining("Model `MyModel` was not found")
  }

  @Test
  def testGetModel(): Unit = {
    val inputSchema = Schema.newBuilder().column("feature", DataTypes.STRING()).build()

    val outputSchema = Schema.newBuilder().column("response", DataTypes.DOUBLE()).build()
    tableEnv.createModel(
      "MyModel",
      ModelDescriptor
        .forProvider("openai")
        .inputSchema(inputSchema)
        .outputSchema(outputSchema)
        .build())
    assertThat(tableEnv.fromModel("MyModel")).isInstanceOf(classOf[ModelImpl])
  }

  @Test
  def testTemporaryOperationListener(): Unit = {
    val listener = new ListenerCatalog("listener_cat")
    val currentCat = tableEnv.getCurrentCatalog
    tableEnv.registerCatalog(listener.getName, listener)
    // test temporary table
    tableEnv.executeSql("create temporary table tbl1 (x int)")
    assertThat(listener.numTempTable).isZero
    tableEnv.executeSql(s"create temporary table ${listener.getName}.`default`.tbl1 (x int)")
    assertThat(listener.numTempTable).isOne
    val tableResult = tableEnv
      .asInstanceOf[TableEnvironmentInternal]
      .getCatalogManager
      .getTable(ObjectIdentifier.of(listener.getName, "default", "tbl1"))
    assertThat(tableResult).isPresent
    assertThat(tableResult.get().getTable[CatalogBaseTable].getComment)
      .isEqualTo(listener.tableComment)
    tableEnv.executeSql("drop temporary table tbl1")
    assertThat(listener.numTempTable).isOne
    tableEnv.executeSql(s"drop temporary table ${listener.getName}.`default`.tbl1")
    assertThat(listener.numTempTable).isZero
    tableEnv.useCatalog(listener.getName)
    tableEnv.executeSql("create temporary table tbl1 (x int)")
    assertThat(listener.numTempTable).isOne
    tableEnv.executeSql("drop temporary table tbl1")
    assertThat(listener.numTempTable).isZero
    tableEnv.useCatalog(currentCat)

    // test temporary view
    tableEnv.executeSql("create temporary view v1 as select 1")
    assertThat(listener.numTempTable).isZero
    tableEnv.executeSql(s"create temporary view ${listener.getName}.`default`.v1 as select 1")
    assertThat(listener.numTempTable).isOne
    val viewResult = tableEnv
      .asInstanceOf[TableEnvironmentInternal]
      .getCatalogManager
      .getTable(ObjectIdentifier.of(listener.getName, "default", "v1"))
    assertThat(viewResult).isPresent
    assertThat(viewResult.get().getTable[CatalogBaseTable].getComment)
      .isEqualTo(listener.tableComment)
    tableEnv.executeSql("drop temporary view v1")
    assertThat(listener.numTempTable).isOne
    tableEnv.executeSql(s"drop temporary view ${listener.getName}.`default`.v1")
    assertThat(listener.numTempTable).isZero
    tableEnv.useCatalog(listener.getName)
    tableEnv.executeSql("create temporary view v1 as select 1")
    assertThat(listener.numTempTable).isOne
    tableEnv.executeSql("drop temporary view  v1")
    assertThat(listener.numTempTable).isZero
    tableEnv.useCatalog(currentCat)

    // test temporary function
    val clzName = "foo.class.name"
    assertThatExceptionOfType(classOf[Exception])
      .isThrownBy(() => tableEnv.executeSql(s"create temporary function func1 as '$clzName'"))

    assertThat(listener.numTempFunc).isZero
    tableEnv.executeSql(
      s"create temporary function ${listener.getName}.`default`.func1 as '$clzName'")
    assertThat(listener.numTempFunc).isOne
    tableEnv.executeSql("drop temporary function if exists func1")
    assertThat(listener.numTempFunc).isOne
    tableEnv.executeSql(s"drop temporary function ${listener.getName}.`default`.func1")
    assertThat(listener.numTempFunc).isZero
    tableEnv.useCatalog(listener.getName)
    tableEnv.executeSql(s"create temporary function func1 as '$clzName'")
    assertThat(listener.numTempFunc).isOne
    tableEnv.executeSql("drop temporary function func1")
    assertThat(listener.numTempFunc).isZero
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

    assertThatExceptionOfType(classOf[IllegalArgumentException])
      .isThrownBy(() => tableEnv.explainSql("select * from MyTable"))
      .withMessageContaining(
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
    assertThatObject(tableResult.getResultKind).isSameAs(ResultKind.SUCCESS)

    val spec1 = new CatalogPartitionSpec(Map("b" -> "1000", "c" -> "2020-05-01").asJava)
    val spec2 = new CatalogPartitionSpec(Map("b" -> "2000", "c" -> "2020-01-01").asJava)

    val catalog = tableEnv.getCatalog(tableEnv.getCurrentCatalog).get()

    val tablePath = new ObjectPath("default_database", "tbl")
    val actual = catalog.listPartitions(tablePath)
    // assert partition spec
    assertThatList(actual).containsExactly(spec1, spec2)

    val part1 = catalog.getPartition(tablePath, spec1)
    val part2 = catalog.getPartition(tablePath, spec2)
    // assert partition properties
    assertThat(part1.getProperties).isEmpty()
    assertThat(part2.getProperties).isEqualTo(util.Map.of("k", "v"))

    // add existed partition with if not exists
    tableResult =
      tableEnv.executeSql("alter table tbl add if not exists partition (b=1000,c='2020-05-01')")
    assertThatObject(tableResult.getResultKind).isSameAs(ResultKind.SUCCESS)

    // add existed partition without if not exists
    assertThatExceptionOfType(classOf[TableException])
      .isThrownBy(
        () => tableEnv.executeSql("alter table tbl add partition (b=1000,c='2020-05-01')"))
      .withMessageContaining("Could not execute ALTER TABLE default_catalog.default_database.tbl" +
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

  private def dropView(viewName: String, isSql: Boolean): Unit = {
    if (isSql) {
      tableEnv.executeSql("DROP VIEW " + viewName)
    } else {
      tableEnv.dropView(viewName)
    }
  }

  private def createViewsForDropTests(): Unit = {
    createTableForTests()
    val viewDdls = Array[String](
      """
        |CREATE VIEW T2(d, e, f) AS SELECT a, b, c FROM T1
      """.stripMargin,
      """
        |CREATE VIEW T3(x, y, z) AS SELECT a, b, c FROM T1
      """.stripMargin
    )

    for (viewSql <- viewDdls) {
      val view = tableEnv.executeSql(viewSql)
      assertThatObject(view.getResultKind).isSameAs(ResultKind.SUCCESS)
    }
  }

  private def createTableForTests(): Unit = {
    val createTableStmt =
      """
        |CREATE TABLE T1 (
        |  a bigint,
        |  b int,
        |  c varchar
        |) with (
        |  'connector' = 'COLLECTION',
        |  'is-bounded' = 'false'
        |)
      """.stripMargin
    val tableResult = tableEnv.executeSql(createTableStmt)
    assertThatObject(tableResult.getResultKind).isSameAs(ResultKind.SUCCESS)
  }

  private def checkData(expected: util.Iterator[Row], actual: util.Iterator[Row]): Unit = {
    while (expected.hasNext && actual.hasNext) {
      assertThat(actual.next()).isEqualTo(expected.next())
    }
    assertThat(actual.hasNext).isEqualTo(expected.hasNext)
  }

  private def validateShowModules(expectedEntries: (String, java.lang.Boolean)*): Unit = {
    val showModules = tableEnv.executeSql("SHOW MODULES")
    assertThatObject(showModules.getResultKind).isSameAs(ResultKind.SUCCESS_WITH_CONTENT)
    assertThat(ResolvedSchema.of(Column.physical("module name", DataTypes.STRING())))
      .isEqualTo(showModules.getResolvedSchema)

    val showFullModules = tableEnv.executeSql("SHOW FULL MODULES")
    assertThatObject(showFullModules.getResultKind).isSameAs(ResultKind.SUCCESS_WITH_CONTENT)
    assertThat(showFullModules.getResolvedSchema).isEqualTo(
      ResolvedSchema.physical(
        Array[String]("module name", "used"),
        Array[DataType](DataTypes.STRING(), DataTypes.BOOLEAN()))
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
      assertThat(actual.apply(i)).isEqualTo(module)
    }
  }

  private def checkListFullModules(expected: (String, java.lang.Boolean)*): Unit = {
    val actual = tableEnv.listFullModules()
    for ((elem, i) <- expected.zipWithIndex) {
      assertThat(new ModuleEntry(elem._1, elem._2).asInstanceOf[Object])
        .isEqualTo(actual.apply(i).asInstanceOf[Object])
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
    assertThat(source).isInstanceOf(classOf[CollectionTableSource])
    assertThat(source.asInstanceOf[CollectionTableSource].isBounded).isEqualTo(expectToBeBounded)
  }

  private def checkExplain(sql: String, resultPath: String, streaming: Boolean = true): Unit = {
    val tableResult2 = if (streaming) {
      tableEnv.executeSql(sql)
    } else {
      batchTableEnv.executeSql(sql)
    }
    assertThatObject(tableResult2.getResultKind).isSameAs(ResultKind.SUCCESS_WITH_CONTENT)
    val it = tableResult2.collect()
    assertThat(it).hasNext
    val row = it.next()
    assertThat(row.getArity).isOne
    val actual = replaceNodeIdInOperator(replaceStreamNodeId(row.getField(0).toString.trim))
    val expected = replaceNodeIdInOperator(
      replaceStreamNodeId(TableTestUtil.readFromResource(resultPath).trim))
    assertThat(replaceStageId(expected)).isEqualTo(replaceStageId(actual))
    assertThat(it.hasNext).isFalse
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
        CatalogTable
          .newBuilder()
          .schema(table.getUnresolvedSchema)
          .comment(tableComment)
          .options(table.getOptions)
          .build()
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
