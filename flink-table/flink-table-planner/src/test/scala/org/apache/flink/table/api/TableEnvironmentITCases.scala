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
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableEnvironmentITCaseUtils._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.config.TableConfigOptions
import org.apache.flink.table.catalog._
import org.apache.flink.table.planner.factories.utils.TestCollectionTableFactory
import org.apache.flink.table.planner.runtime.utils.TestingAppendSink
import org.apache.flink.table.planner.utils.{TableTestUtil, TestTableSourceSinks}
import org.apache.flink.table.planner.utils.TableTestUtil.{readFromResource, replaceStageId}
import org.apache.flink.table.test.WithTableEnvironment
import org.apache.flink.test.junit5.MiniClusterExtension
import org.apache.flink.types.{Row, RowKind}
import org.apache.flink.util.{CollectionUtil, FileUtils}

import org.assertj.core.api.Assertions.{assertThat, assertThatThrownBy}
import org.junit.jupiter.api.{BeforeEach, Test, TestInstance}
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.parallel.{Execution, ExecutionMode}

import java.io.{File, FileFilter}
import java.lang.{Long => JLong}
import java.nio.file.Path

import scala.collection.JavaConverters._
import scala.collection.mutable

@ExtendWith(Array(classOf[MiniClusterExtension]))
@Execution(ExecutionMode.CONCURRENT)
@TestInstance(Lifecycle.PER_METHOD)
abstract class ParameterizedTableEnvironmentTestBase {

  @BeforeEach
  def setup(tEnv: TableEnvironment, @TempDir tempDir: Path): Unit = {
    TestTableSourceSinks.createPersonCsvTemporaryTable(tEnv, tempDir, "MyTable")
  }

  @Test
  def testExecuteTwiceUsingSameTableEnv(tEnv: TableEnvironment, @TempDir tempDir: Path): Unit = {
    val sink1Path = TestTableSourceSinks.createCsvTemporarySinkTable(
      tEnv,
      Schema.newBuilder().column("first", DataTypes.STRING()).build(),
      tempDir,
      "MySink1")

    val sink2Path = TestTableSourceSinks.createCsvTemporarySinkTable(
      tEnv,
      Schema.newBuilder().column("first", DataTypes.STRING()).build(),
      tempDir,
      "MySink2")

    checkEmptyDir(sink1Path)
    checkEmptyDir(sink2Path)

    val table1 = tEnv.sqlQuery("select first from MyTable")
    table1.executeInsert("MySink1").await()
    assertFirstValues(sink1Path)
    checkEmptyDir(sink2Path)

    // delete first csv file
    FileUtils.deleteDirectory(new File(sink1Path))
    assertThat(new File(sink1Path)).doesNotExist()

    val table2 = tEnv.sqlQuery("select last from MyTable")
    table2.executeInsert("MySink2").await()
    assertThat(new File(sink1Path)).doesNotExist()
    assertLastValues(sink2Path)
  }

  @Test
  def testExplainAndExecuteSingleSink(tEnv: TableEnvironment, @TempDir tempDir: Path): Unit = {
    val sinkPath = TestTableSourceSinks.createCsvTemporarySinkTable(
      tEnv,
      Schema.newBuilder().column("first", DataTypes.STRING()).build(),
      tempDir,
      "MySink1")

    val table1 = tEnv.sqlQuery("select first from MyTable")
    table1.executeInsert("MySink1").await()
    assertFirstValues(sinkPath)
  }

  @Test
  def testExecuteSqlWithInsertInto(tEnv: TableEnvironment, @TempDir tempDir: Path): Unit = {
    val sinkPath = TestTableSourceSinks.createCsvTemporarySinkTable(
      tEnv,
      Schema.newBuilder().column("first", DataTypes.STRING()).build(),
      tempDir,
      "MySink1")
    checkEmptyDir(sinkPath)
    val tableResult = tEnv.executeSql("insert into MySink1 select first from MyTable")
    checkInsertTableResult(tableResult, "default_catalog.default_database.MySink1")
    assertFirstValues(sinkPath)
  }

  @Test
  def testExecuteSqlAndExecuteInsert(tEnv: TableEnvironment, @TempDir tempDir: Path): Unit = {
    val sink1Path = TestTableSourceSinks.createCsvTemporarySinkTable(
      tEnv,
      Schema.newBuilder().column("first", DataTypes.STRING()).build(),
      tempDir,
      "MySink1")
    val sink2Path = TestTableSourceSinks.createCsvTemporarySinkTable(
      tEnv,
      Schema.newBuilder().column("last", DataTypes.STRING()).build(),
      tempDir,
      "MySink2")
    checkEmptyDir(sink1Path)
    checkEmptyDir(sink2Path)

    val tableResult = tEnv.executeSql("insert into MySink1 select first from MyTable")
    checkInsertTableResult(tableResult, "default_catalog.default_database.MySink1")

    assertFirstValues(sink1Path)
    checkEmptyDir(sink2Path)

    // delete first csv directory
    FileUtils.deleteDirectory(new File(sink1Path))

    tEnv.sqlQuery("select last from MyTable").executeInsert("MySink2").await()
    assertThat(new File(sink1Path)).doesNotExist()
    assertLastValues(sink2Path)
  }

  @Test
  def testExecuteInsert(tEnv: TableEnvironment, @TempDir tempDir: Path): Unit = {
    val sinkPath = TestTableSourceSinks.createCsvTemporarySinkTable(
      tEnv,
      Schema.newBuilder().column("first", DataTypes.STRING()).build(),
      tempDir,
      "MySink")
    checkEmptyDir(sinkPath)
    val table = tEnv.sqlQuery("select first from MyTable")
    val tableResult = table.executeInsert("MySink")
    checkInsertTableResult(tableResult, "default_catalog.default_database.MySink")
    assertFirstValues(sinkPath)
  }

  @Test
  def testExecuteInsert2(tEnv: TableEnvironment, @TempDir tempDir: Path): Unit = {
    val sinkPath = TestTableSourceSinks.createCsvTemporarySinkTable(
      tEnv,
      Schema.newBuilder().column("first", DataTypes.STRING()).build(),
      tempDir,
      "MySink")
    checkEmptyDir(sinkPath)
    val tableResult = tEnv.executeSql("execute insert into MySink select first from MyTable")
    checkInsertTableResult(tableResult, "default_catalog.default_database.MySink")
    assertFirstValues(sinkPath)
  }

  @Test
  def testTableDMLSync(tEnv: TableEnvironment, @TempDir tempDir: Path): Unit = {
    tEnv.getConfig.set(TableConfigOptions.TABLE_DML_SYNC, Boolean.box(true))
    val sink1Path = tempDir.resolve("MySink1").toAbsolutePath.toString
    tEnv.executeSql(
      s"""
         |create table MySink1 (
         |  first string,
         |  last string
         |) with (
         |  'connector' = 'filesystem',
         |  'path' = '$sink1Path',
         |  'format' = 'testcsv'
         |)
       """.stripMargin
    )

    val sink2Path = tempDir.resolve("MySink2").toAbsolutePath.toString
    tEnv.executeSql(
      s"""
         |create table MySink2 (
         |  first string
         |) with (
         |  'connector' = 'filesystem',
         |  'path' = '$sink2Path',
         |  'format' = 'testcsv'
         |)
       """.stripMargin
    )

    val sink3Path = tempDir.resolve("MySink3").toAbsolutePath.toString
    tEnv.executeSql(
      s"""
         |create table MySink3 (
         |  last string
         |) with (
         |  'connector' = 'filesystem',
         |  'path' = '$sink3Path',
         |  'format' = 'testcsv'
         |)
       """.stripMargin
    )

    val tableResult1 =
      tEnv.sqlQuery("select first, last from MyTable").executeInsert("MySink1", false)

    val stmtSet = tEnv.createStatementSet()
    stmtSet.addInsertSql("INSERT INTO MySink2 select first from MySink1")
    stmtSet.addInsertSql("INSERT INTO MySink3 select last from MySink1")
    val tableResult2 = stmtSet.execute()

    // checkInsertTableResult will wait the job finished,
    // we should assert file values first to verify job has been finished
    assertFirstValues(sink2Path)
    assertLastValues(sink3Path)

    // check TableResult after verifying file values
    checkInsertTableResult(
      tableResult2,
      "default_catalog.default_database.MySink2",
      "default_catalog.default_database.MySink3")

    // Verify it's no problem to invoke await twice
    tableResult1.await()
    tableResult2.await()
  }

  @Test
  def testStatementSet(tEnv: TableEnvironment, @TempDir tempDir: Path): Unit = {
    val sink1Path = TestTableSourceSinks.createCsvTemporarySinkTable(
      tEnv,
      Schema.newBuilder().column("first", DataTypes.STRING()).build(),
      tempDir,
      "MySink1")

    val sink2Path = TestTableSourceSinks.createCsvTemporarySinkTable(
      tEnv,
      Schema.newBuilder().column("last", DataTypes.STRING()).build(),
      tempDir,
      "MySink2")

    val stmtSet = tEnv.createStatementSet()
    stmtSet
      .addInsert("MySink1", tEnv.sqlQuery("select first from MyTable"))
      .addInsertSql("insert into MySink2 select last from MyTable")

    val actual = stmtSet.explain()
    val expected = TableTestUtil.readFromResource("/explain/testStatementSet.out")
    assertThat(replaceStageId(actual)).isEqualTo(replaceStageId(expected))

    val tableResult = stmtSet.execute()
    checkInsertTableResult(
      tableResult,
      "default_catalog.default_database.MySink1",
      "default_catalog.default_database.MySink2")

    assertFirstValues(sink1Path)
    assertLastValues(sink2Path)
  }

  @Test
  def testExecuteStatementSet(tEnv: TableEnvironment, @TempDir tempDir: Path): Unit = {
    val sink1Path = TestTableSourceSinks.createCsvTemporarySinkTable(
      tEnv,
      Schema.newBuilder().column("first", DataTypes.STRING()).build(),
      tempDir,
      "MySink1")

    val sink2Path = TestTableSourceSinks.createCsvTemporarySinkTable(
      tEnv,
      Schema.newBuilder().column("last", DataTypes.STRING()).build(),
      tempDir,
      "MySink2")

    val tableResult = tEnv.executeSql("""execute statement set begin
                                        |insert into MySink1 select first from MyTable;
                                        |insert into MySink2 select last from MyTable;
                                        |end""".stripMargin)
    checkInsertTableResult(
      tableResult,
      "default_catalog.default_database.MySink1",
      "default_catalog.default_database.MySink2")

    assertFirstValues(sink1Path)
    assertLastValues(sink2Path)
  }

  @Test
  def testExecuteSelect(tEnv: TableEnvironment): Unit = {
    val query = {
      """
        |select id, concat(concat(`first`, ' '), `last`) as `full name`
        |from MyTable where mod(id, 2) = 0
      """.stripMargin
    }
    testExecuteSelectInternal(tEnv, query)
    val query2 = {
      """
        |execute select id, concat(concat(`first`, ' '), `last`) as `full name`
        |from MyTable where mod(id, 2) = 0
      """.stripMargin
    }
    testExecuteSelectInternal(tEnv, query2)
  }

  def testExecuteSelectInternal(tEnv: TableEnvironment, query: String): Unit = {
    val tableResult = tEnv.executeSql(query)
    assertThat(tableResult.getJobClient).isPresent
    assertThat(tableResult.getResultKind).isEqualTo(ResultKind.SUCCESS_WITH_CONTENT)
    assertThat(tableResult.getResolvedSchema).isEqualTo(ResolvedSchema
      .of(Column.physical("id", DataTypes.INT()), Column.physical("full name", DataTypes.STRING())))
    assertThat[Row](CollectionUtil.iteratorToList(tableResult.collect())).containsExactly(
      Row.of(Integer.valueOf(2), "Bob Taylor"),
      Row.of(Integer.valueOf(4), "Peter Smith"),
      Row.of(Integer.valueOf(6), "Sally Miller"),
      Row.of(Integer.valueOf(8), "Kelly Williams")
    )
  }

  @Test
  def testClearOperation(tEnv: TableEnvironment): Unit = {
    TestCollectionTableFactory.reset()
    tEnv.executeSql("create table dest1(x map<int,bigint>) with('connector' = 'COLLECTION')")
    tEnv.executeSql("create table dest2(x int) with('connector' = 'COLLECTION')")
    tEnv.executeSql("create table src(x int) with('connector' = 'COLLECTION')")

    assertThatThrownBy(
      () => tEnv.executeSql("insert into dest1 select count(*) from src")).isNotNull

    tEnv.executeSql("drop table dest1")
    tEnv.executeSql("insert into dest2 select x from src").await()
  }
}

@WithTableEnvironment(executionMode = RuntimeExecutionMode.STREAMING, withDataStream = false)
class TableEnvironmentInStreamingModeITCase extends ParameterizedTableEnvironmentTestBase

@WithTableEnvironment(executionMode = RuntimeExecutionMode.BATCH, withDataStream = false)
class TableEnvironmentInBatchModeITCase extends ParameterizedTableEnvironmentTestBase

@WithTableEnvironment(executionMode = RuntimeExecutionMode.STREAMING, withDataStream = true)
class StreamTableEnvironmentITCase extends ParameterizedTableEnvironmentTestBase

@ExtendWith(Array(classOf[MiniClusterExtension]))
@Execution(ExecutionMode.CONCURRENT)
class MiscTableEnvironmentITCase {

  @Test
  @WithTableEnvironment(executionMode = RuntimeExecutionMode.BATCH)
  def testExecuteInsertOverwrite(tEnv: TableEnvironment, @TempDir tempDir: Path): Unit = {
    TestTableSourceSinks.createPersonCsvTemporaryTable(tEnv, tempDir, "MyTable")
    val sinkPath = tempDir.resolve("MySink").toAbsolutePath.toString
    tEnv.executeSql(
      s"""
         |create table MySink (
         |  first string
         |) with (
         |  'connector' = 'filesystem',
         |  'path' = '$sinkPath',
         |  'format' = 'testcsv'
         |)
       """.stripMargin
    )
    val tableResult1 = tEnv.sqlQuery("select first from MyTable").executeInsert("MySink", true)
    checkInsertTableResult(tableResult1, "default_catalog.default_database.MySink")
    assertFirstValues(sinkPath)

    val tableResult2 = tEnv.sqlQuery("select first from MyTable").executeInsert("MySink", true)
    checkInsertTableResult(tableResult2, "default_catalog.default_database.MySink")
    assertFirstValues(sinkPath)
  }

  @Test
  @WithTableEnvironment(executionMode = RuntimeExecutionMode.BATCH)
  def testExecuteSqlWithInsertOverwrite(tEnv: TableEnvironment, @TempDir tempDir: Path): Unit = {
    TestTableSourceSinks.createPersonCsvTemporaryTable(tEnv, tempDir, "MyTable")
    val sinkPath = tempDir.resolve("MySink").toAbsolutePath.toString
    tEnv.executeSql(
      s"""
         |create table MySink (
         |  first string
         |) with (
         |  'connector' = 'filesystem',
         |  'path' = '$sinkPath',
         |  'format' = 'testcsv'
         |)
       """.stripMargin
    )

    val tableResult1 = tEnv.executeSql("insert overwrite MySink select first from MyTable")
    checkInsertTableResult(tableResult1, "default_catalog.default_database.MySink")
    assertFirstValues(sinkPath)

    val tableResult2 = tEnv.executeSql("insert overwrite MySink select first from MyTable")
    checkInsertTableResult(tableResult2, "default_catalog.default_database.MySink")
    assertFirstValues(sinkPath)
  }

  @Test
  @WithTableEnvironment(executionMode = RuntimeExecutionMode.BATCH)
  def testStatementSetWithSameSinkTableNames(
      tEnv: TableEnvironment,
      @TempDir tempDir: Path): Unit = {
    TestTableSourceSinks.createPersonCsvTemporaryTable(tEnv, tempDir, "MyTable")
    val sinkPath = tempDir.resolve("MySink").toAbsolutePath.toString
    tEnv.executeSql(
      s"""
         |create table MySink (
         |  first string
         |) with (
         |  'connector' = 'filesystem',
         |  'path' = '$sinkPath',
         |  'format' = 'testcsv'
         |)
       """.stripMargin
    )

    val stmtSet = tEnv.createStatementSet()
    stmtSet.addInsert("MySink", tEnv.sqlQuery("select first from MyTable"), true)
    stmtSet.addInsertSql("insert overwrite MySink select last from MyTable")

    val tableResult = stmtSet.execute()
    // only check the schema
    checkInsertTableResult(
      tableResult,
      "default_catalog.default_database.MySink_1",
      "default_catalog.default_database.MySink_2")
  }

  @Test
  @WithTableEnvironment(executionMode = RuntimeExecutionMode.BATCH)
  def testStatementSetWithOverwrite(tEnv: TableEnvironment, @TempDir tempDir: Path): Unit = {
    TestTableSourceSinks.createPersonCsvTemporaryTable(tEnv, tempDir, "MyTable")
    val sink1Path = tempDir.resolve("MySink1").toAbsolutePath.toString
    tEnv.executeSql(
      s"""
         |create table MySink1 (
         |  first string
         |) with (
         |  'connector' = 'filesystem',
         |  'path' = '$sink1Path',
         |  'format' = 'testcsv'
         |)
       """.stripMargin
    )

    val sink2Path = tempDir.resolve("MySink2").toAbsolutePath.toString
    tEnv.executeSql(
      s"""
         |create table MySink2 (
         |  last string
         |) with (
         |  'connector' = 'filesystem',
         |  'path' = '$sink2Path',
         |  'format' = 'testcsv'
         |)
       """.stripMargin
    )

    val stmtSet = tEnv.createStatementSet()
    stmtSet.addInsert("MySink1", tEnv.sqlQuery("select first from MyTable"), true)
    stmtSet.addInsertSql("insert overwrite MySink2 select last from MyTable")

    val tableResult1 = stmtSet.execute()
    checkInsertTableResult(
      tableResult1,
      "default_catalog.default_database.MySink1",
      "default_catalog.default_database.MySink2")

    assertFirstValues(sink1Path)
    assertLastValues(sink2Path)

    // execute again using same StatementSet instance
    stmtSet
      .addInsert("MySink1", tEnv.sqlQuery("select first from MyTable"), true)
      .addInsertSql("insert overwrite MySink2 select last from MyTable")

    val tableResult2 = stmtSet.execute()
    checkInsertTableResult(
      tableResult2,
      "default_catalog.default_database.MySink1",
      "default_catalog.default_database.MySink2")

    assertFirstValues(sink1Path)
    assertLastValues(sink2Path)
  }

  @Test
  @WithTableEnvironment
  def testToDataStreamAndExecuteSql(
      streamEnv: StreamExecutionEnvironment,
      streamTableEnv: StreamTableEnvironment,
      @TempDir tempDir: Path): Unit = {
    TestTableSourceSinks.createPersonCsvTemporaryTable(streamTableEnv, tempDir, "MyTable")
    val sink1Path = TestTableSourceSinks.createCsvTemporarySinkTable(
      streamTableEnv,
      Schema.newBuilder().column("first", DataTypes.STRING()).build(),
      tempDir,
      "MySink1")
    checkEmptyDir(sink1Path)

    val table = streamTableEnv.sqlQuery("select last from MyTable where id > 0")
    val resultSet = streamTableEnv.toAppendStream[Row](table)
    val sink = new TestingAppendSink
    resultSet.addSink(sink)

    val insertStmt = "insert into MySink1 select first from MyTable"

    val explain = streamTableEnv.explainSql(insertStmt)
    assertThat(replaceStageId(explain)).isEqualTo(
      replaceStageId(readFromResource("/explain/testSqlUpdateAndToDataStream.out")))

    streamEnv.execute("test2")
    // the table program is not executed
    checkEmptyDir(sink1Path)
    assertThat[String](sink.getAppendResults.asJava)
      .containsExactlyInAnyOrderElementsOf(getExpectedLastValues)

    streamTableEnv.executeSql(insertStmt).await()
    assertFirstValues(sink1Path)
    // the DataStream program is not executed again because the result in sink is not changed
    assertThat[String](sink.getAppendResults.asJava)
      .containsExactlyInAnyOrderElementsOf(getExpectedLastValues)
  }

  @Test
  @WithTableEnvironment
  def testExecuteSqlAndToDataStream(
      streamEnv: StreamExecutionEnvironment,
      streamTableEnv: StreamTableEnvironment,
      @TempDir tempDir: Path): Unit = {
    TestTableSourceSinks.createPersonCsvTemporaryTable(streamTableEnv, tempDir, "MyTable")
    val sink1Path = TestTableSourceSinks.createCsvTemporarySinkTable(
      streamTableEnv,
      Schema.newBuilder().column("first", DataTypes.STRING()).build(),
      tempDir,
      "MySink1")
    checkEmptyDir(sink1Path)

    val table = streamTableEnv.sqlQuery("select last from MyTable where id > 0")
    val resultSet = streamTableEnv.toAppendStream[Row](table)
    val sink = new TestingAppendSink
    resultSet.addSink(sink)

    val tableResult = streamTableEnv.executeSql("insert into MySink1 select first from MyTable")
    checkInsertTableResult(tableResult, "default_catalog.default_database.MySink1")
    assertFirstValues(sink1Path)

    // the DataStream program is not executed
    assertThat(sink.isInitialized).isFalse

    FileUtils.deleteDirectory(new File(sink1Path))

    streamEnv.execute("test2")
    assertThat[String](sink.getAppendResults.asJava)
      .containsExactlyInAnyOrderElementsOf(getExpectedLastValues)
    // the table program is not executed again
    assertThat(new File(sink1Path)).doesNotExist()
  }

  @Test
  @WithTableEnvironment
  def testFromToDataStreamAndExecuteSql(
      streamEnv: StreamExecutionEnvironment,
      streamTableEnv: StreamTableEnvironment,
      @TempDir tempDir: Path): Unit = {
    val t = streamEnv
      .fromCollection(getPersonData)
      .toTable(streamTableEnv, 'first, 'id, 'score, 'last)
    streamTableEnv.createTemporaryView("MyTable", t)
    val sink1Path = TestTableSourceSinks.createCsvTemporarySinkTable(
      streamTableEnv,
      Schema.newBuilder().column("first", DataTypes.STRING()).build(),
      tempDir,
      "MySink1")
    checkEmptyDir(sink1Path)

    val table = streamTableEnv.sqlQuery("select last from MyTable where id > 0")
    val resultSet = streamTableEnv.toAppendStream[Row](table)
    val sink = new TestingAppendSink
    resultSet.addSink(sink)

    val insertStmt = "insert into MySink1 select first from MyTable"

    val explain = streamTableEnv.explainSql(insertStmt)
    assertThat(replaceStageId(explain)).isEqualTo(
      replaceStageId(readFromResource("/explain/testFromToDataStreamAndSqlUpdate.out")))

    streamEnv.execute("test2")
    // the table program is not executed
    checkEmptyDir(sink1Path)
    assertThat[String](sink.getAppendResults.asJava)
      .containsExactlyInAnyOrderElementsOf(getExpectedLastValues)

    streamTableEnv.executeSql(insertStmt).await()
    assertFirstValues(sink1Path)
    // the DataStream program is not executed again because the result in sink is not changed
    assertThat[String](sink.getAppendResults.asJava)
      .containsExactlyInAnyOrderElementsOf(getExpectedLastValues)
  }

  @Test
  @WithTableEnvironment(executionMode = RuntimeExecutionMode.STREAMING)
  def testExecuteSelectWithUpdateChangesStreamingMode(
      tEnv: TableEnvironment,
      @TempDir tempDir: Path): Unit = {
    TestTableSourceSinks.createPersonCsvTemporaryTable(tEnv, tempDir, "MyTable")
    val tableResult = tEnv.sqlQuery("select count(*) as c from MyTable").execute()
    assertThat(tableResult.getJobClient).isPresent
    assertThat(tableResult.getResultKind).isEqualTo(ResultKind.SUCCESS_WITH_CONTENT)
    assertThat(tableResult.getResolvedSchema).isEqualTo(
      ResolvedSchema.of(Column.physical("c", DataTypes.BIGINT().notNull())))
    assertThat[Row](CollectionUtil.iteratorToList(tableResult.collect()))
      .containsExactly(
        Row.ofKind(RowKind.INSERT, JLong.valueOf(1)),
        Row.ofKind(RowKind.UPDATE_BEFORE, JLong.valueOf(1)),
        Row.ofKind(RowKind.UPDATE_AFTER, JLong.valueOf(2)),
        Row.ofKind(RowKind.UPDATE_BEFORE, JLong.valueOf(2)),
        Row.ofKind(RowKind.UPDATE_AFTER, JLong.valueOf(3)),
        Row.ofKind(RowKind.UPDATE_BEFORE, JLong.valueOf(3)),
        Row.ofKind(RowKind.UPDATE_AFTER, JLong.valueOf(4)),
        Row.ofKind(RowKind.UPDATE_BEFORE, JLong.valueOf(4)),
        Row.ofKind(RowKind.UPDATE_AFTER, JLong.valueOf(5)),
        Row.ofKind(RowKind.UPDATE_BEFORE, JLong.valueOf(5)),
        Row.ofKind(RowKind.UPDATE_AFTER, JLong.valueOf(6)),
        Row.ofKind(RowKind.UPDATE_BEFORE, JLong.valueOf(6)),
        Row.ofKind(RowKind.UPDATE_AFTER, JLong.valueOf(7)),
        Row.ofKind(RowKind.UPDATE_BEFORE, JLong.valueOf(7)),
        Row.ofKind(RowKind.UPDATE_AFTER, JLong.valueOf(8))
      )
  }

  @Test
  @WithTableEnvironment(executionMode = RuntimeExecutionMode.BATCH)
  def testExecuteSelectWithUpdateChangesBatchMode(
      tEnv: TableEnvironment,
      @TempDir tempDir: Path): Unit = {
    TestTableSourceSinks.createPersonCsvTemporaryTable(tEnv, tempDir, "MyTable")
    val tableResult = tEnv.sqlQuery("select count(*) as c from MyTable").execute()
    assertThat(tableResult.getJobClient).isPresent
    assertThat(tableResult.getResultKind).isEqualTo(ResultKind.SUCCESS_WITH_CONTENT)
    assertThat(tableResult.getResolvedSchema).isEqualTo(
      ResolvedSchema.of(Column.physical("c", DataTypes.BIGINT().notNull())))
    assertThat[Row](CollectionUtil.iteratorToList(tableResult.collect()))
      .containsOnly(Row.of(JLong.valueOf(8)))
  }

}

protected object TableEnvironmentITCaseUtils {

  def getPersonData: List[(String, Int, Double, String)] = {
    val data = new mutable.MutableList[(String, Int, Double, String)]
    data.+=(("Mike", 1, 12.3, "Smith"))
    data.+=(("Bob", 2, 45.6, "Taylor"))
    data.+=(("Sam", 3, 7.89, "Miller"))
    data.+=(("Peter", 4, 0.12, "Smith"))
    data.+=(("Liz", 5, 34.5, "Williams"))
    data.+=(("Sally", 6, 6.78, "Miller"))
    data.+=(("Alice", 7, 90.1, "Smith"))
    data.+=(("Kelly", 8, 2.34, "Williams"))
    data.toList
  }

  def assertFirstValues(sinkPath: String): Unit = {
    val expected = List("Mike", "Bob", "Sam", "Peter", "Liz", "Sally", "Alice", "Kelly").asJava
    val actual = readFiles(sinkPath)
    assertThat[String](actual).containsExactlyInAnyOrderElementsOf(expected)
  }

  def assertLastValues(sinkPath: String): Unit = {
    val actual = readFiles(sinkPath)
    assertThat[String](actual).containsExactlyInAnyOrderElementsOf(getExpectedLastValues)
  }

  def getExpectedLastValues: java.util.List[String] = {
    List("Smith", "Taylor", "Miller", "Smith", "Williams", "Miller", "Smith", "Williams").asJava
  }

  def checkEmptyDir(csvFilePath: String): Unit = {
    assertThat(new File(csvFilePath).list()).isEmpty()
  }

  def checkInsertTableResult(tableResult: TableResult, tableNames: String*): Unit = {
    assertThat(tableResult.getJobClient).isPresent
    assertThat(tableResult.getResultKind).isEqualTo(ResultKind.SUCCESS_WITH_CONTENT)

    assertThat[String](tableResult.getResolvedSchema.getColumnNames)
      .containsExactlyElementsOf(tableNames.asJava)

    // return the result until the job is finished
    val it = tableResult.collect()
    assertThat(it).hasNext
    val affectedRowCounts = tableNames.map(_ => JLong.valueOf(-1L))

    assertThat(it.next()).isEqualTo(Row.of(affectedRowCounts: _*))
    assertThat(it.hasNext).isFalse
  }

  def readFiles(csvPath: String): java.util.List[String] = {
    val file = new File(csvPath)
    if (file.isDirectory) {
      file
        .listFiles(new FileFilter() { override def accept(f: File): Boolean = f.isFile })
        .map(FileUtils.readFileUtf8)
        .flatMap(_.split("\n"))
        .toList
        .asJava
    } else {
      FileUtils
        .readFileUtf8(file)
        .split("\n")
        .toList
        .asJava
    }
  }
}
