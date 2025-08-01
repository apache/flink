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
package org.apache.flink.table.planner.runtime.stream.sql

import org.apache.flink.core.execution.CheckpointingMode
import org.apache.flink.table.api.Schema
import org.apache.flink.table.api.bridge.scala.internal.StreamTableEnvironmentImpl
import org.apache.flink.table.api.config.OptimizerConfigOptions
import org.apache.flink.table.api.config.OptimizerConfigOptions.DeltaJoinStrategy
import org.apache.flink.table.catalog.{CatalogTable, ObjectPath, ResolvedCatalogTable}
import org.apache.flink.table.planner.factories.TestValuesRuntimeFunctions.AsyncTestValueLookupFunction
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.factories.TestValuesTableFactory.changelogRow
import org.apache.flink.table.planner.runtime.utils.{FailingCollectionSource, StreamingTestBase}
import org.apache.flink.types.Row

import org.assertj.core.api.Assertions.{assertThat, assertThatThrownBy}
import org.junit.jupiter.api.{BeforeEach, Test}

import javax.annotation.Nullable

import java.time.LocalDateTime
import java.util.concurrent.TimeUnit

import scala.collection.JavaConversions._

class DeltaJoinITCase extends StreamingTestBase {

  @BeforeEach
  override def before(): Unit = {
    super.before()

    tEnv.getConfig.set(
      OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY,
      DeltaJoinStrategy.FORCE)

    AsyncTestValueLookupFunction.invokeCount.set(0)
  }

  @Test
  def testJoinKeyEqualsIndex(): Unit = {
    val data1 = List(
      changelogRow("+I", Double.box(1.0), Int.box(1), LocalDateTime.of(2021, 1, 1, 1, 1, 1)),
      changelogRow("+I", Double.box(2.0), Int.box(2), LocalDateTime.of(2022, 2, 2, 2, 2, 2)),
      // mismatch
      changelogRow("+I", Double.box(3.0), Int.box(3), LocalDateTime.of(2023, 3, 3, 3, 3, 3))
    )

    val data2 = List(
      changelogRow("+I", Int.box(1), Double.box(1.0), LocalDateTime.of(2021, 1, 1, 1, 1, 1)),
      changelogRow("+I", Int.box(2), Double.box(2.0), LocalDateTime.of(2022, 2, 2, 2, 2, 22)),
      // mismatch
      changelogRow("+I", Int.box(99), Double.box(99.0), LocalDateTime.of(2099, 2, 2, 2, 2, 2))
    )

    // TestValuesRuntimeFunctions#KeyedUpsertingSinkFunction will change the RowKind from
    // "+U" to "+I"
    val expected = List(
      "+I[1.0, 1, 2021-01-01T01:01:01, 1, 1.0, 2021-01-01T01:01:01]",
      "+I[2.0, 2, 2022-02-02T02:02:02, 2, 2.0, 2022-02-02T02:02:22]"
    )
    testUpsertResult(List("a1"), List("b1"), data1, data2, "a1 = b1", expected, 6)
  }

  @Test
  def testJoinKeyContainsIndex(): Unit = {
    val data1 = List(
      changelogRow("+I", Double.box(1.0), Int.box(1), LocalDateTime.of(2021, 1, 1, 1, 1, 1)),
      changelogRow("+I", Double.box(2.0), Int.box(2), LocalDateTime.of(2022, 2, 2, 2, 2, 2)),
      // mismatch
      changelogRow("+I", Double.box(3.0), Int.box(3), LocalDateTime.of(2023, 3, 3, 3, 3, 3))
    )

    val data2 = List(
      changelogRow("+I", Int.box(1), Double.box(1.0), LocalDateTime.of(2021, 1, 1, 1, 1, 1)),
      changelogRow("+I", Int.box(2), Double.box(2.0), LocalDateTime.of(2022, 2, 2, 2, 2, 2)),
      // mismatch
      changelogRow("+I", Int.box(99), Double.box(99.0), LocalDateTime.of(2099, 2, 2, 2, 2, 2))
    )

    // TestValuesRuntimeFunctions#KeyedUpsertingSinkFunction will change the RowKind from
    // "+U" to "+I"
    val expected = List(
      "+I[2.0, 2, 2022-02-02T02:02:02, 2, 2.0, 2022-02-02T02:02:02]",
      "+I[1.0, 1, 2021-01-01T01:01:01, 1, 1.0, 2021-01-01T01:01:01]"
    )
    testUpsertResult(List("a1"), List("b1"), data1, data2, "a1 = b1 and a2 = b2", expected, 6)
  }

  @Test
  def testJoinKeyNotContainsIndex(): Unit = {
    val data1 = List(
      changelogRow("+I", Double.box(1.0), Int.box(1), LocalDateTime.of(2021, 1, 1, 1, 1, 1)),
      changelogRow("+I", Double.box(2.0), Int.box(2), LocalDateTime.of(2022, 2, 2, 2, 2, 2)),
      // mismatch
      changelogRow("+I", Double.box(3.0), Int.box(3), LocalDateTime.of(2023, 3, 3, 3, 3, 3))
    )

    val data2 = List(
      changelogRow("+I", Int.box(1), Double.box(1.0), LocalDateTime.of(2021, 1, 1, 1, 1, 11)),
      changelogRow("+I", Int.box(2), Double.box(2.0), LocalDateTime.of(2022, 2, 2, 2, 2, 22)),
      // mismatch
      changelogRow("+I", Int.box(99), Double.box(99.0), LocalDateTime.of(2099, 2, 2, 2, 2, 2))
    )

    // TestValuesRuntimeFunctions#KeyedUpsertingSinkFunction will change the RowKind from
    // "+U" to "+I"
    val expected = List("+I[1.0, 1, 2022-02-02T02:02:02, 1, 1.0, 2022-02-02T02:02:22]")

    // could not optimize into delta join because join keys do not contain indexes strictly
    assertThatThrownBy(
      () =>
        testUpsertResult(List("a0", "a1"), List("b0", "b1"), data1, data2, "a1 = b1", expected, 6))
      .hasMessageContaining("The current sql doesn't support to do delta join optimization.")
  }

  @Test
  def testWithNonEquiCondition1(): Unit = {
    val data1 = List(
      changelogRow("+I", Double.box(1.0), Int.box(1), LocalDateTime.of(2021, 1, 1, 1, 1, 1)),
      changelogRow("+I", Double.box(2.0), Int.box(2), LocalDateTime.of(2023, 3, 3, 3, 3, 3)),
      // mismatch
      changelogRow("+I", Double.box(3.0), Int.box(3), LocalDateTime.of(2033, 3, 3, 3, 3, 3))
    )

    val data2 = List(
      changelogRow("+I", Int.box(1), Double.box(1.0), LocalDateTime.of(2021, 1, 1, 1, 1, 11)),
      changelogRow("+I", Int.box(2), Double.box(2.0), LocalDateTime.of(2022, 2, 2, 2, 2, 22)),
      // mismatch
      changelogRow("+I", Int.box(99), Double.box(99.0), LocalDateTime.of(2099, 2, 2, 2, 2, 2))
    )

    // TestValuesRuntimeFunctions#KeyedUpsertingSinkFunction will change the RowKind from
    // "+U" to "+I"
    val expected = List("+I[2.0, 2, 2023-03-03T03:03:03, 2, 2.0, 2022-02-02T02:02:22]")

    testUpsertResult(
      List("a0"),
      List("b0"),
      data1,
      data2,
      "a0 = b0 and a1 = b1 and a2 > b2",
      expected,
      6)
  }

  @Test
  def testWithNonEquiCondition2(): Unit = {
    val data1 = List(
      changelogRow("+I", Double.box(1.0), Int.box(1), LocalDateTime.of(2021, 1, 1, 1, 1, 1)),
      changelogRow("+I", Double.box(2.0), Int.box(2), LocalDateTime.of(2023, 3, 3, 3, 3, 3)),
      // mismatch
      changelogRow("+I", Double.box(3.0), Int.box(3), LocalDateTime.of(2033, 3, 3, 3, 3, 3))
    )

    val data2 = List(
      changelogRow("+I", Int.box(1), Double.box(1.0), LocalDateTime.of(2021, 1, 1, 1, 1, 11)),
      changelogRow("+I", Int.box(2), Double.box(2.0), LocalDateTime.of(2022, 2, 2, 2, 2, 22)),
      // mismatch
      changelogRow("+I", Int.box(99), Double.box(99.0), LocalDateTime.of(2099, 2, 2, 2, 2, 2))
    )

    // TestValuesRuntimeFunctions#KeyedUpsertingSinkFunction will change the RowKind from
    // "+U" to "+I"
    val expected = List("+I[2.0, 2, 2023-03-03T03:03:03, 2, 2.0, 2022-02-02T02:02:22]")

    // could not optimize into delta join because there is calc between join and source
    assertThatThrownBy(
      () =>
        testUpsertResult(
          List("a0"),
          List("b0"),
          data1,
          data2,
          "a0 = b0 and a1 = b1 and a2 > TO_TIMESTAMP('2021-01-01 01:01:11')",
          expected,
          6))
      .hasMessageContaining("The current sql doesn't support to do delta join optimization.")

    // could not optimize into delta join because there is calc between join and source
    assertThatThrownBy(
      () =>
        testUpsertResult(
          List("a0"),
          List("b0"),
          data1,
          data2,
          "a0 = b0 and b1 > 1.0",
          expected,
          12))
      .hasMessageContaining("The current sql doesn't support to do delta join optimization.")
  }

  @Test
  def testFilterFieldsBeforeJoin(): Unit = {
    val data1 = List(
      changelogRow("+I", Double.box(1.0), Int.box(1), LocalDateTime.of(2021, 1, 1, 1, 1, 1)),
      changelogRow("+I", Double.box(2.0), Int.box(2), LocalDateTime.of(2022, 2, 2, 2, 2, 2)),
      // mismatch
      changelogRow("+I", Double.box(3.0), Int.box(3), LocalDateTime.of(2033, 3, 3, 3, 3, 3))
    )

    val data2 = List(
      changelogRow("+I", Int.box(1), Double.box(1.0), LocalDateTime.of(2021, 1, 1, 1, 1, 11)),
      changelogRow("+I", Int.box(2), Double.box(2.0), LocalDateTime.of(2022, 2, 2, 2, 2, 22)),
      // mismatch
      changelogRow("+I", Int.box(99), Double.box(99.0), LocalDateTime.of(2099, 2, 2, 2, 2, 2))
    )

    // TestValuesRuntimeFunctions#KeyedUpsertingSinkFunction will change the RowKind from
    // "+U" to "+I"
    val expected = List("+I[1.0, 1, 2021-01-01T01:01:01, 1, 1.0, 2021-01-01T01:01:11]")

    // could not optimize into delta join because there is calc between join and source
    assertThatThrownBy(
      () =>
        testUpsertResult(
          List("a0"),
          List("b0"),
          data1,
          data2,
          "a0 = b0 and a1 = b1 and a2 = TO_TIMESTAMP('2021-01-01 01:01:01')",
          expected,
          6))
      .hasMessageContaining("The current sql doesn't support to do delta join optimization.")

    // could not optimize into delta join because there is calc between join and source
    assertThatThrownBy(
      () => testUpsertResult(List(), List(), data1, data2, "a0 = b0 and b1 = 1.0", expected, 12))
      .hasMessageContaining("The current sql doesn't support to do delta join optimization.")
  }

  @Test
  def testProjectFieldsAfterJoin(): Unit = {
    val data1 = List(
      changelogRow("+I", Double.box(1.0), Int.box(1), LocalDateTime.of(2021, 1, 1, 1, 1, 1)),
      changelogRow("+I", Double.box(2.0), Int.box(2), LocalDateTime.of(2022, 2, 2, 2, 2, 2)),
      // mismatch
      changelogRow("+I", Double.box(3.0), Int.box(3), LocalDateTime.of(2033, 3, 3, 3, 3, 3))
    )

    val data2 = List(
      changelogRow("+I", Int.box(1), Double.box(1.0), LocalDateTime.of(2021, 1, 1, 1, 1, 11)),
      changelogRow("+I", Int.box(2), Double.box(2.0), LocalDateTime.of(2022, 2, 2, 2, 2, 22)),
      // mismatch
      changelogRow("+I", Int.box(99), Double.box(99.0), LocalDateTime.of(2099, 2, 2, 2, 2, 2))
    )

    prepareTable(List("a0"), List("b0"), data1, data2)

    // TestValuesRuntimeFunctions#KeyedUpsertingSinkFunction will change the RowKind from
    // "+U" to "+I"
    val expected = List(
      "+I[1.0, 2, 2021-01-01T01:01:01, 3, 1.0, 2021-01-01T01:01:11]",
      "+I[2.0, 3, 2022-02-02T02:02:02, 4, 2.0, 2022-02-02T02:02:22]"
    )

    tEnv
      .executeSql(
        s"insert into testSnk select a1, a0 + 1, a2, b0 + 2, b1, b2 from testLeft join testRight on a0 = b0")
      .await()
    val result = TestValuesTableFactory.getResultsAsStrings("testSnk")

    assertThat(result.sorted).isEqualTo(expected.sorted)
    assertThat(AsyncTestValueLookupFunction.invokeCount.get()).isEqualTo(6)
  }

  @Test
  def testProjectFieldsBeforeJoin(): Unit = {
    val data1 = List(
      changelogRow("+I", Double.box(1.0), Int.box(1), LocalDateTime.of(2021, 1, 1, 1, 1, 1)),
      changelogRow("+I", Double.box(2.0), Int.box(2), LocalDateTime.of(2022, 2, 2, 2, 2, 2)),
      // mismatch
      changelogRow("+I", Double.box(3.0), Int.box(3), LocalDateTime.of(2033, 3, 3, 3, 3, 3))
    )

    val data2 = List(
      changelogRow("+I", Int.box(1), Double.box(1.0), LocalDateTime.of(2021, 1, 1, 1, 1, 11)),
      changelogRow("+I", Int.box(2), Double.box(2.0), LocalDateTime.of(2022, 2, 2, 2, 2, 22)),
      // mismatch
      changelogRow("+I", Int.box(99), Double.box(99.0), LocalDateTime.of(2099, 2, 2, 2, 2, 2))
    )

    prepareTable(List("a0"), List("b0"), data1, data2)
    tEnv.executeSql(s"""
                       |create table projectedSink(
                       |  l0 int,
                       |  r0 int,
                       |  l1 double,
                       |  l2 timestamp(3),
                       |  primary key(l0, r0) not enforced
                       |) with (
                       |  'connector' = 'values',
                       |  'bounded' = 'false',
                       |  'sink-insert-only' = 'false'
                       |)
                       |""".stripMargin)

    // could not optimize into delta join because there is ProjectPushDownSpec between join and source
    assertThatThrownBy(
      () =>
        tEnv
          .executeSql(
            s"insert into projectedSink select testLeft.a0, testRight.b0, testLeft.a1, testLeft.a2 " +
              s"from testLeft join testRight on a0 = b0")
          .await())
      .hasMessageContaining("The current sql doesn't support to do delta join optimization.")
  }

  @Test
  def testProjectFieldsBeforeJoin2(): Unit = {
    val data1 = List(
      changelogRow("+I", Double.box(1.0), Int.box(1), LocalDateTime.of(2021, 1, 1, 1, 1, 1)),
      changelogRow("+I", Double.box(2.0), Int.box(2), LocalDateTime.of(2022, 2, 2, 2, 2, 2)),
      // mismatch
      changelogRow("+I", Double.box(3.0), Int.box(3), LocalDateTime.of(2033, 3, 3, 3, 3, 3))
    )

    val data2 = List(
      changelogRow("+I", Int.box(1), Double.box(1.0), LocalDateTime.of(2021, 1, 1, 1, 1, 11)),
      changelogRow("+I", Int.box(2), Double.box(2.0), LocalDateTime.of(2022, 2, 2, 2, 2, 22)),
      // mismatch
      changelogRow("+I", Int.box(99), Double.box(99.0), LocalDateTime.of(2099, 2, 2, 2, 2, 2))
    )

    prepareTable(List("a0"), List("b0"), data1, data2)

    // could not optimize into delta join because there is calc between join and source
    assertThatThrownBy(
      () =>
        tEnv
          .executeSql(
            s"insert into testSnk(l0, l1, l2, r0, r1, r2) " +
              "select * from ( " +
              "  select a0, a1, a2 from testLeft" +
              ") join testRight " +
              "on a1 = b1")
          .await())
      .hasMessageContaining("The current sql doesn't support to do delta join optimization.")
  }

  @Test
  def testFailOverAndRestore(): Unit = {
    // enable checkpoint, we are using failing source to force have a complete checkpoint
    // and cover restore path
    env.enableCheckpointing(100, CheckpointingMode.EXACTLY_ONCE)
    FailingCollectionSource.reset()

    val data1 = List(
      changelogRow("+I", Double.box(1.0), Int.box(1), LocalDateTime.of(2021, 1, 1, 1, 1, 1)),
      changelogRow("+I", Double.box(2.0), Int.box(2), LocalDateTime.of(2022, 2, 2, 2, 2, 2)),
      // mismatch
      changelogRow("+I", Double.box(3.0), Int.box(3), LocalDateTime.of(2023, 3, 3, 3, 3, 3))
    )

    val data2 = List(
      changelogRow("+I", Int.box(1), Double.box(2.0), LocalDateTime.of(2021, 1, 1, 1, 1, 1)),
      changelogRow("+I", Int.box(2), Double.box(2.0), LocalDateTime.of(2022, 2, 2, 2, 2, 22)),
      // mismatch
      changelogRow("+I", Int.box(99), Double.box(99.0), LocalDateTime.of(2099, 2, 2, 2, 2, 2))
    )

    // TestValuesRuntimeFunctions#KeyedUpsertingSinkFunction will change the RowKind from
    // "+U" to "+I"
    val expected = List("+I[2.0, 2, 2022-02-02T02:02:02, 2, 2.0, 2022-02-02T02:02:22]")

    testUpsertResult(
      List("a0"),
      List("b0"),
      data1,
      data2,
      "a0 = b0 and a1 = b1",
      expected,
      null,
      testFailingSource = true)
  }

  /** TODO add index in DDL. */
  private def addIndex(tableName: String, indexColumns: List[String]): Unit = {
    if (indexColumns.isEmpty) {
      return
    }

    val catalogName = tEnv.getCurrentCatalog
    val databaseName = tEnv.getCurrentDatabase
    val tablePath = new ObjectPath(databaseName, tableName)
    val catalog = tEnv.getCatalog(catalogName).get()
    val catalogManager = tEnv.asInstanceOf[StreamTableEnvironmentImpl].getCatalogManager
    val schemaResolver = catalogManager.getSchemaResolver

    val resolvedTable = catalog.getTable(tablePath).asInstanceOf[ResolvedCatalogTable]
    val originTable = resolvedTable.getOrigin
    val originSchema = originTable.getUnresolvedSchema

    val newSchema = Schema.newBuilder().fromSchema(originSchema).index(indexColumns).build()

    val newTable = CatalogTable
      .newBuilder()
      .schema(newSchema)
      .comment(originTable.getComment)
      .partitionKeys(originTable.getPartitionKeys)
      .options(originTable.getOptions)
      .build()
    val newResolvedTable = new ResolvedCatalogTable(newTable, schemaResolver.resolve(newSchema))

    catalog.dropTable(tablePath, false)
    catalog.createTable(tablePath, newResolvedTable, false)
  }

  private def testUpsertResult(
      leftIndex: List[String],
      rightIndex: List[String],
      leftData: List[Row],
      rightData: List[Row],
      joinKeyStr: String,
      expected: List[String],
      @Nullable expectedLookupFunctionInvokeCount: Integer,
      testFailingSource: Boolean = false): Unit = {
    prepareTable(leftIndex, rightIndex, leftData, rightData, testFailingSource)

    tEnv
      .executeSql(s"insert into testSnk select * from testLeft join testRight on $joinKeyStr")
      .await(60, TimeUnit.SECONDS)
    val result = TestValuesTableFactory.getResultsAsStrings("testSnk")

    assertThat(result.sorted).isEqualTo(expected.sorted)
    if (expectedLookupFunctionInvokeCount != null) {
      assertThat(AsyncTestValueLookupFunction.invokeCount.get())
        .isEqualTo(expectedLookupFunctionInvokeCount)
    }
  }

  private def prepareTable(
      leftIndex: List[String],
      rightIndex: List[String],
      leftData: List[Row],
      rightData: List[Row],
      testFailingSource: Boolean = false): Unit = {
    tEnv.executeSql("drop table if exists testLeft")
    tEnv.executeSql(s"""
                       |create table testLeft(
                       |  a1 double,
                       |  a0 int,
                       |  a2 timestamp(3)
                       |) with (
                       |  'connector' = 'values',
                       |  'bounded' = 'false',
                       |  'changelog-mode' = 'I',
                       |  'data-id' = '${TestValuesTableFactory.registerData(leftData)}',
                       |  'async' = 'true',
                       |  'failing-source' = '$testFailingSource'
                       |)
                       |""".stripMargin)
    addIndex("testLeft", leftIndex)

    tEnv.executeSql("drop table if exists testRight")
    tEnv.executeSql(s"""
                       |create table testRight(
                       |  b0 int,
                       |  b1 double,
                       |  b2 timestamp(3)
                       |) with (
                       |  'connector' = 'values',
                       |  'bounded' = 'false',
                       |  'changelog-mode' = 'I',
                       |  'data-id' = '${TestValuesTableFactory.registerData(rightData)}',
                       |  'async' = 'true',
                       |  'failing-source' = '$testFailingSource'
                       |)
                       |""".stripMargin)
    addIndex("testRight", rightIndex)

    tEnv.executeSql("drop table if exists testSnk")
    tEnv.executeSql(s"""
                       |create table testSnk(
                       |  l1 double,
                       |  l0 int,
                       |  l2 timestamp(3),
                       |  r0 int,
                       |  r1 double,
                       |  r2 timestamp(3),
                       |  primary key(l0, r0) not enforced
                       |) with (
                       |  'connector' = 'values',
                       |  'bounded' = 'false',
                       |  'sink-insert-only' = 'false'
                       |)
                       |""".stripMargin)
  }

}
