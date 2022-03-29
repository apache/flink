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

import org.apache.flink.api.common.{JobStatus, RuntimeExecutionMode}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.catalog.{Column, ResolvedSchema}
import org.apache.flink.table.planner.utils.TestTableSourceSinks
import org.apache.flink.table.test.WithTableEnvironment
import org.apache.flink.test.junit5.MiniClusterExtension
import org.apache.flink.types.{Row, RowKind}
import org.apache.flink.util.CollectionUtil

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.{BeforeEach, Test, TestInstance}
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.parallel.{Execution, ExecutionMode}

import java.lang.{Long => JLong}
import java.nio.file.Path
import java.time.Instant

@ExtendWith(Array(classOf[MiniClusterExtension]))
@Execution(ExecutionMode.CONCURRENT)
@TestInstance(Lifecycle.PER_METHOD)
abstract class ParameterizedTableTestBase {

  @BeforeEach
  def setup(tEnv: TableEnvironment, @TempDir tempDir: Path): Unit = {
    TestTableSourceSinks.createPersonCsvTemporaryTable(tEnv, tempDir, "MyTable")
  }

  @Test
  def testExecute(tEnv: TableEnvironment): Unit = {
    val query =
      """
        |select id, concat(concat(`first`, ' '), `last`) as `full name`
        |from MyTable where mod(id, 2) = 0
          """.stripMargin
    val table = tEnv.sqlQuery(query)
    val tableResult = table.execute()
    assertThat(tableResult.getJobClient).isPresent
    assertThat(tableResult.getResultKind).isEqualTo(ResultKind.SUCCESS_WITH_CONTENT)
    assertThat(tableResult.getResolvedSchema).isEqualTo(
      ResolvedSchema.of(
        Column.physical("id", DataTypes.INT()),
        Column.physical("full name", DataTypes.STRING()))
    )
    val it = tableResult.collect()
    assertThat[Row](CollectionUtil.iteratorToList(it))
      .containsExactlyInAnyOrder(
        Row.of(Integer.valueOf(2), "Bob Taylor"),
        Row.of(Integer.valueOf(4), "Peter Smith"),
        Row.of(Integer.valueOf(6), "Sally Miller"),
        Row.of(Integer.valueOf(8), "Kelly Williams")
      )
    it.close()
  }

  @Test
  def testCollectWithClose(tEnv: TableEnvironment): Unit = {
    val query =
      """
        |select id, concat(concat(`first`, ' '), `last`) as `full name`
        |from MyTable where mod(id, 2) = 0
          """.stripMargin
    val table = tEnv.sqlQuery(query)
    val tableResult = table.execute()
    assertThat(tableResult.getJobClient).isPresent
    assertThat(tableResult.getResultKind).isEqualTo(ResultKind.SUCCESS_WITH_CONTENT)
    val it = tableResult.collect()
    it.close()
    val jobStatus =
      try {
        Some(tableResult.getJobClient.get().getJobStatus.get())
      } catch {
        // ignore the exception,
        // because the MiniCluster maybe already been shut down when getting job status
        case _: Throwable => None
      }
    if (jobStatus.isDefined) {
      assertThat(jobStatus.get)
        .isIn(JobStatus.RUNNING, JobStatus.CANCELLING, JobStatus.CANCELED, JobStatus.FINISHED)
    }
  }

  @Test
  def testCollectWithMultiRowtime(tEnv: TableEnvironment): Unit = {
    tEnv.executeSql("""
                      |CREATE TABLE MyTableWithRowtime1 (
                      |  ts AS TO_TIMESTAMP_LTZ(id, 3),
                      |  WATERMARK FOR ts AS ts - INTERVAL '1' MINUTE)
                      |LIKE MyTable""".stripMargin)
    tEnv.executeSql("""
                      |CREATE TABLE MyTableWithRowtime2 (
                      |  ts AS TO_TIMESTAMP_LTZ(id, 3),
                      |  WATERMARK FOR ts AS ts - INTERVAL '1' MINUTE)
                      |LIKE MyTable""".stripMargin)

    val tableResult =
      tEnv.executeSql("""
                        |SELECT MyTableWithRowtime1.ts, MyTableWithRowtime2.ts
                        |FROM MyTableWithRowtime1, MyTableWithRowtime2
                        |WHERE
                        |  MyTableWithRowtime1.first = MyTableWithRowtime2.first AND
                        |  MyTableWithRowtime1.ts = MyTableWithRowtime2.ts""".stripMargin)

    val expected =
      for (i <- 1 to 8)
        yield Row.ofKind(RowKind.INSERT, Instant.ofEpochMilli(i), Instant.ofEpochMilli(i))

    val actual = CollectionUtil.iteratorToList(tableResult.collect())
    assertThat[Row](actual).containsExactlyInAnyOrder(expected: _*)
  }
}

@WithTableEnvironment(executionMode = RuntimeExecutionMode.STREAMING, withDataStream = false)
class TableInStreamingModeITCase extends ParameterizedTableTestBase

@WithTableEnvironment(executionMode = RuntimeExecutionMode.BATCH, withDataStream = false)
class TableInBatchModeITCase extends ParameterizedTableTestBase

@WithTableEnvironment(executionMode = RuntimeExecutionMode.STREAMING, withDataStream = true)
class StreamTableITCase extends ParameterizedTableTestBase

@ExtendWith(Array(classOf[MiniClusterExtension]))
@Execution(ExecutionMode.CONCURRENT)
class MiscTableITCase {

  @Test
  @WithTableEnvironment(executionMode = RuntimeExecutionMode.STREAMING)
  def testExecuteWithUpdateChangesStreamingMode(
      tEnv: TableEnvironment,
      @TempDir tempDir: Path): Unit = {
    TestTableSourceSinks.createPersonCsvTemporaryTable(tEnv, tempDir, "MyTable")
    val tableResult = tEnv.sqlQuery("select count(*) as c from MyTable").execute()
    assertThat(tableResult.getJobClient).isPresent
    assertThat(tableResult.getResultKind).isEqualTo(ResultKind.SUCCESS_WITH_CONTENT)
    assertThat(tableResult.getResolvedSchema).isEqualTo(
      ResolvedSchema.of(Column.physical("c", DataTypes.BIGINT().notNull())))
    assertThat[Row](CollectionUtil.iteratorToList(tableResult.collect()))
      .containsExactlyInAnyOrder(
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
  def testExecuteWithUpdateChangesBatchMode(
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
