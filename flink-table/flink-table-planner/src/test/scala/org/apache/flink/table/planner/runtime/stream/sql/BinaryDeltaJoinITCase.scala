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
import org.apache.flink.table.planner.{JHashMap, JMap}
import org.apache.flink.table.planner.factories.TestValuesRuntimeFunctions.AsyncTestValueLookupFunction
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.factories.TestValuesTableFactory.changelogRow
import org.apache.flink.table.planner.runtime.utils.FailingCollectionSource
import org.apache.flink.types.Row

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.util.Maps
import org.junit.jupiter.api.TestTemplate

import javax.annotation.Nullable

import java.time.LocalDateTime
import java.util.Collections
import java.util.Objects.requireNonNull
import java.util.concurrent.TimeUnit

import scala.collection.JavaConversions._
import scala.collection.JavaConverters.mapAsScalaMapConverter

/** Tests for binary delta join with two tables. */
class BinaryDeltaJoinITCase(enableCache: Boolean) extends DeltaJoinITCaseBase(enableCache) {

  @TestTemplate
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

    testUpsertResult(
      newTestSpecBuilder()
        .withLeftIndex(List("a1"))
        .withRightIndex(List("b1"))
        .withLeftData(data1)
        .withRightData(data2)
        .withJoinCondition("a1 = b1")
        .withSinkPk(List("l0", "r0"))
        .withExpectedData(expected)
        .withExpectedLookupFunctionInvokeCount(6)
        .build())
  }

  @TestTemplate
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

    testUpsertResult(
      newTestSpecBuilder()
        .withLeftIndex(List("a1"))
        .withRightIndex(List("b1"))
        .withLeftData(data1)
        .withRightData(data2)
        .withJoinCondition("a1 = b1 and a2 = b2")
        .withSinkPk(List("l0", "r0"))
        .withExpectedData(expected)
        .withExpectedLookupFunctionInvokeCount(6)
        .build())
  }

  @TestTemplate
  def testSameJoinKeyColValuesWhileJoinKeyEqualsIndex(): Unit = {
    val data1 = List(
      changelogRow("+I", Double.box(1.0), Int.box(1), LocalDateTime.of(2021, 1, 1, 1, 1, 1)),
      changelogRow("+I", Double.box(1.0), Int.box(1), LocalDateTime.of(2022, 2, 2, 2, 2, 2)),
      // mismatch
      changelogRow("+I", Double.box(3.0), Int.box(3), LocalDateTime.of(2023, 3, 3, 3, 3, 3))
    )

    val data2 = List(
      changelogRow("+I", Int.box(1), Double.box(1.0), LocalDateTime.of(2021, 1, 1, 1, 1, 1)),
      changelogRow("+I", Int.box(1), Double.box(1.0), LocalDateTime.of(2022, 2, 2, 2, 2, 22)),
      // mismatch
      changelogRow("+I", Int.box(99), Double.box(99.0), LocalDateTime.of(2099, 2, 2, 2, 2, 2))
    )

    // TestValuesRuntimeFunctions#KeyedUpsertingSinkFunction will change the RowKind from
    // "+U" to "+I"
    val expected = List(
      "+I[1.0, 1, 2022-02-02T02:02:02, 1, 1.0, 2022-02-02T02:02:22]"
    )

    testUpsertResult(
      newTestSpecBuilder()
        .withLeftIndex(List("a1"))
        .withRightIndex(List("b1"))
        .withLeftData(data1)
        .withRightData(data2)
        .withJoinCondition("a1 = b1")
        .withSinkPk(List("l0", "r0"))
        .withExpectedData(expected)
        .withExpectedLookupFunctionInvokeCount(if (enableCache) 4 else 6)
        .build())
  }

  @TestTemplate
  def testSameJoinKeyColValuesWhileJoinKeyContainsIndex(): Unit = {
    val data1 = List(
      changelogRow("+I", Double.box(1.0), Int.box(1), LocalDateTime.of(2021, 1, 1, 1, 1, 1)),
      changelogRow("+I", Double.box(1.0), Int.box(2), LocalDateTime.of(2021, 1, 1, 1, 1, 1)),
      // mismatch
      changelogRow("+I", Double.box(3.0), Int.box(3), LocalDateTime.of(2023, 3, 3, 3, 3, 3))
    )

    val data2 = List(
      changelogRow("+I", Int.box(1), Double.box(1.0), LocalDateTime.of(2021, 1, 1, 1, 1, 1)),
      changelogRow("+I", Int.box(2), Double.box(1.0), LocalDateTime.of(2021, 1, 1, 1, 1, 1)),
      // mismatch
      changelogRow("+I", Int.box(99), Double.box(99.0), LocalDateTime.of(2099, 2, 2, 2, 2, 2))
    )

    // TestValuesRuntimeFunctions#KeyedUpsertingSinkFunction will change the RowKind from
    // "+U" to "+I"
    val expected = List(
      "+I[1.0, 1, 2021-01-01T01:01:01, 1, 1.0, 2021-01-01T01:01:01]",
      "+I[1.0, 1, 2021-01-01T01:01:01, 2, 1.0, 2021-01-01T01:01:01]",
      "+I[1.0, 2, 2021-01-01T01:01:01, 1, 1.0, 2021-01-01T01:01:01]",
      "+I[1.0, 2, 2021-01-01T01:01:01, 2, 1.0, 2021-01-01T01:01:01]"
    )
    testUpsertResult(
      newTestSpecBuilder()
        .withLeftIndex(List("a1"))
        .withRightIndex(List("b1"))
        .withLeftData(data1)
        .withRightData(data2)
        .withJoinCondition("a1 = b1 and a2 = b2")
        .withSinkPk(List("l0", "r0"))
        .withExpectedData(expected)
        .withExpectedLookupFunctionInvokeCount(if (enableCache) 4 else 6)
        .build()
    )
  }

  @TestTemplate
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
      newTestSpecBuilder()
        .withLeftIndex(List("a0"))
        .withRightIndex(List("b0"))
        .withLeftData(data1)
        .withRightData(data2)
        .withJoinCondition("a0 = b0 and a1 = b1 and a2 > b2")
        .withSinkPk(List("l0", "r0"))
        .withExpectedData(expected)
        .withExpectedLookupFunctionInvokeCount(6)
        .build())
  }

  @TestTemplate
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

    testUpsertResult(
      newTestSpecBuilder()
        .withLeftIndex(List("a0"))
        .withRightIndex(List("b0"))
        .withLeftData(data1)
        .withRightData(data2)
        // the filter "a2 > TO_TIMESTAMP('2021-01-01 01:01:11')" will be pushed down to
        // the right side
        .withJoinCondition("a0 = b0 and a1 = b1 and a2 > TO_TIMESTAMP('2021-01-01 01:01:11')")
        .withSinkPk(List("l0", "r0"))
        .withExpectedData(expected)
        .withExpectedLookupFunctionInvokeCount(5)
        .build())

    after()
    before()

    testUpsertResult(
      newTestSpecBuilder()
        .withLeftIndex(List("a0"))
        .withRightIndex(List("b0"))
        .withLeftData(data1)
        .withRightData(data2)
        // the filter "b1 > 1.0" will be pushed down to the right side
        .withJoinCondition("a0 = b0 and b1 > 1.0")
        .withSinkPk(List("l0", "r0"))
        .withExpectedData(expected)
        .withExpectedLookupFunctionInvokeCount(5)
        .build())
  }

  @TestTemplate
  def testFilterProjectBeforeJoin(): Unit = {
    testFilterProjectBeforeJoinInner(false)
  }

  @TestTemplate
  def testFilterProjectBeforeJoinWithFilterPushDownIntoSource(): Unit = {
    testFilterProjectBeforeJoinInner(true)
  }

  private def testFilterProjectBeforeJoinInner(filterPushDown: Boolean): Unit = {
    val data1 = List(
      changelogRow("+I", Double.box(1.0), Int.box(1), LocalDateTime.of(2021, 1, 1, 1, 1, 1)),
      changelogRow("+I", Double.box(2.0), Int.box(2), LocalDateTime.of(2022, 2, 2, 2, 2, 2)),
      changelogRow("+I", Double.box(3.0), Int.box(3), LocalDateTime.of(2033, 3, 3, 3, 3, 3)),
      // mismatch
      changelogRow("+I", Double.box(4.0), Int.box(4), LocalDateTime.of(2044, 4, 4, 4, 4, 4))
    )

    val data2 = List(
      changelogRow("+I", Int.box(1), Double.box(1.0), LocalDateTime.of(2021, 1, 1, 1, 1, 11)),
      changelogRow("+I", Int.box(2), Double.box(2.0), LocalDateTime.of(2022, 2, 2, 2, 2, 22)),
      changelogRow("+I", Int.box(3), Double.box(3.0), LocalDateTime.of(2023, 3, 3, 3, 3, 33)),
      // mismatch
      changelogRow("+I", Int.box(99), Double.box(99.0), LocalDateTime.of(2099, 2, 2, 2, 2, 2))
    )

    // TestValuesRuntimeFunctions#KeyedUpsertingSinkFunction will change the RowKind from
    // "+U" to "+I"
    val expected1 = List("+I[null, 2, 2022-02-02T02:02:02, 2, null, 2022-02-02T02:02:22]")

    val (leftExtraOptions1, rightExtraOptions1): (JMap[String, String], JMap[String, String]) =
      if (filterPushDown) {
        (Maps.newHashMap("filterable-fields", "a2"), Maps.newHashMap("filterable-fields", "b0"))
      } else {
        (Collections.emptyMap(), Collections.emptyMap())
      }

    testUpsertResult(
      newTestSpecBuilder()
        .withLeftIndex(List("a0"))
        .withRightIndex(List("b0"))
        .withLeftData(data1)
        .withRightData(data2)
        .withLeftExtraOptions(leftExtraOptions1)
        .withRightExtraOptions(rightExtraOptions1)
        .withSinkPk(List("l0", "r0"))
        .withFilterProjectOnLeft(
          "select a0, a2 from testLeft where a2 > TO_TIMESTAMP('2021-01-01 01:01:11')")
        .withFilterProjectOnRight("" +
          "select b0, b2 from testRight where b0 < 3")
        .withJoinCondition("a0 = b0")
        .withPartialInsertCols(List("l0", "l2", "r0", "r2"))
        .withExpectedData(expected1)
        .withExpectedLookupFunctionInvokeCount(5)
        .build())

    after()
    before()

    // TestValuesRuntimeFunctions#KeyedUpsertingSinkFunction will change the RowKind from
    // "+U" to "+I"
    val expected2 = List("+I[null, 3, 2033-03-03T03:03:03, 3, null, 2023-03-03T03:03:33]")

    val (leftExtraOptions2, rightExtraOptions2): (JMap[String, String], JMap[String, String]) =
      if (filterPushDown) {
        (Maps.newHashMap("filterable-fields", "a1"), Maps.newHashMap("filterable-fields", "b0"))
      } else {
        (Collections.emptyMap(), Collections.emptyMap())
      }

    testUpsertResult(
      newTestSpecBuilder()
        .withLeftIndex(List("a0"))
        .withRightIndex(List("b0"))
        .withLeftData(data1)
        .withRightData(data2)
        .withLeftExtraOptions(leftExtraOptions2)
        .withRightExtraOptions(rightExtraOptions2)
        .withSinkPk(List("l0", "r0"))
        .withFilterProjectOnLeft("select a0, a2 from testLeft where a1 > cast(2.0 as double)")
        .withFilterProjectOnRight("" +
          "select b0, b2 from testRight where b0 < 4")
        .withJoinCondition("a0 = b0")
        .withPartialInsertCols(List("l0", "l2", "r0", "r2"))
        .withExpectedData(expected2)
        .withExpectedLookupFunctionInvokeCount(5)
        .build())
  }

  @TestTemplate
  def testPartitionPushDownIntoSource(): Unit = {
    val data1 = List(
      changelogRow("+I", Double.box(50.0), Int.box(1), LocalDateTime.of(2021, 1, 1, 1, 1, 1)),
      changelogRow("+I", Double.box(50.0), Int.box(2), LocalDateTime.of(2022, 2, 2, 2, 2, 2)),
      changelogRow("+I", Double.box(100.0), Int.box(3), LocalDateTime.of(2033, 3, 3, 3, 3, 3)),
      // mismatch
      changelogRow("+I", Double.box(200.0), Int.box(4), LocalDateTime.of(2044, 4, 4, 4, 4, 4))
    )

    val data2 = List(
      changelogRow("+I", Int.box(1), Double.box(100.0), LocalDateTime.of(2021, 1, 1, 1, 1, 11)),
      changelogRow("+I", Int.box(2), Double.box(100.0), LocalDateTime.of(2022, 2, 2, 2, 2, 22)),
      changelogRow("+I", Int.box(3), Double.box(200.0), LocalDateTime.of(2023, 3, 3, 3, 3, 33)),
      // mismatch
      changelogRow("+I", Int.box(99), Double.box(300.0), LocalDateTime.of(2099, 2, 2, 2, 2, 2))
    )

    // TestValuesRuntimeFunctions#KeyedUpsertingSinkFunction will change the RowKind from
    // "+U" to "+I"
    val expected = List("+I[100.0, 3, null, 3, 200.0, null]")

    val (leftExtraOptions1, rightExtraOptions1): (JMap[String, String], JMap[String, String]) =
      (
        java.util.Map.of("partition-list", "a1:50.0;a1:100.0;a1:200.0"),
        java.util.Map.of("partition-list", "b1:100.0;b1:200.0;b1:300.0")
      )

    testUpsertResult(
      newTestSpecBuilder()
        .withLeftIndex(List("a0"))
        .withRightIndex(List("b0"))
        .withLeftPartitionKeys(List("a1"))
        .withRightPartitionKeys(List("b1"))
        .withLeftExtraOptions(leftExtraOptions1)
        .withRightExtraOptions(rightExtraOptions1)
        .withSinkPk(List("l0", "r0"))
        .withLeftData(data1)
        .withRightData(data2)
        .withFilterProjectOnLeft("select a0, a1 from testLeft " +
          "where a1 = cast(100.0 as double)  or a1 = cast(200.0 as double)")
        .withFilterProjectOnRight("select b1, b0 from testRight " +
          "where b1 = cast(200.0 as double)  or b1 = cast(300.0 as double)")
        .withJoinCondition("a0 = b0")
        .withPartialInsertCols(List("l0", "l1", "r1", "r0"))
        .withExpectedData(expected)
        .withExpectedLookupFunctionInvokeCount(4)
        .build())
  }

  @TestTemplate
  def testCdcSourceWithoutDelete(): Unit = {
    val data1 = List(
      // pk1
      changelogRow("+I", Double.box(1.0), Int.box(1), LocalDateTime.of(2021, 1, 1, 1, 1, 1)),
      changelogRow("-U", Double.box(1.0), Int.box(1), LocalDateTime.of(2021, 1, 1, 1, 1, 1)),
      changelogRow("+U", Double.box(11.0), Int.box(1), LocalDateTime.of(2021, 1, 1, 1, 1, 11)),
      // pk2
      changelogRow("+I", Double.box(2.0), Int.box(2), LocalDateTime.of(2022, 2, 2, 2, 2, 2)),
      changelogRow("-U", Double.box(2.0), Int.box(2), LocalDateTime.of(2022, 2, 2, 2, 2, 2)),
      changelogRow("+U", Double.box(22.0), Int.box(2), LocalDateTime.of(2022, 2, 2, 2, 2, 22)),
      // mismatch
      changelogRow("+I", Double.box(3.0), Int.box(3), LocalDateTime.of(2023, 3, 3, 3, 3, 3))
    )

    val data2 = List(
      // pk1
      changelogRow("+I", Int.box(1), Double.box(12.0), LocalDateTime.of(2021, 1, 1, 1, 1, 12)),
      changelogRow("-U", Int.box(1), Double.box(12.0), LocalDateTime.of(2021, 1, 1, 1, 1, 12)),
      changelogRow("+U", Int.box(1), Double.box(13.0), LocalDateTime.of(2021, 1, 1, 1, 1, 13)),
      // pk2
      changelogRow("+I", Int.box(2), Double.box(22.0), LocalDateTime.of(2022, 2, 2, 2, 2, 22)),
      changelogRow("-U", Int.box(2), Double.box(22.0), LocalDateTime.of(2022, 2, 2, 2, 2, 22)),
      changelogRow("+U", Int.box(2), Double.box(23.0), LocalDateTime.of(2022, 2, 2, 2, 2, 23)),
      // mismatch
      changelogRow("+I", Int.box(99), Double.box(99.0), LocalDateTime.of(2099, 2, 2, 2, 2, 2))
    )

    // TestValuesRuntimeFunctions#KeyedUpsertingSinkFunction will change the RowKind from
    // "+U" to "+I"
    val expected = List(
      "+I[11.0, 1, 2021-01-01T01:01:11, 1, 13.0, 2021-01-01T01:01:13]",
      "+I[22.0, 2, 2022-02-02T02:02:22, 2, 23.0, 2022-02-02T02:02:23]"
    )

    testUpsertResult(
      newTestSpecBuilder()
        .withLeftIndex(List("a0"))
        .withRightIndex(List("b0"))
        .withLeftPk(List("a0"))
        .withRightPk(List("b0"))
        .withSinkPk(List("l0", "r0"))
        .withLeftExtraOptions(Maps.newHashMap("changelog-mode", "I,UA,UB"))
        .withRightExtraOptions(Maps.newHashMap("changelog-mode", "I,UA,UB"))
        .withLeftData(data1)
        .withRightData(data2)
        .withJoinCondition("a0 = b0")
        .withExpectedData(expected)
        .withExpectedLookupFunctionInvokeCount(if (enableCache) 6 else 10)
        .build())
  }

  @TestTemplate
  def testFilterFieldsAfterJoin(): Unit = {
    val data1 = List(
      changelogRow("+I", Double.box(1.0), Int.box(1), LocalDateTime.of(2021, 1, 1, 1, 1, 1)),
      changelogRow("+I", Double.box(2.0), Int.box(2), LocalDateTime.of(2022, 2, 2, 2, 2, 2)),
      // mismatch
      changelogRow("+I", Double.box(3.0), Int.box(3), LocalDateTime.of(2033, 3, 3, 3, 3, 3))
    )

    val data2 = List(
      changelogRow("+I", Int.box(1), Double.box(1.0), LocalDateTime.of(2021, 1, 1, 1, 1, 11)),
      changelogRow("+I", Int.box(2), Double.box(3.0), LocalDateTime.of(2022, 2, 2, 2, 2, 33)),
      // mismatch
      changelogRow("+I", Int.box(99), Double.box(99.0), LocalDateTime.of(2099, 2, 2, 2, 2, 2))
    )

    // TestValuesRuntimeFunctions#KeyedUpsertingSinkFunction will change the RowKind from
    // "+U" to "+I"
    val expected = List("+I[2.0, 2, 2022-02-02T02:02:02, 2, 3.0, 2022-02-02T02:02:33]")

    testUpsertResult(
      newTestSpecBuilder()
        .withLeftIndex(List("a0"))
        .withRightIndex(List("b0"))
        .withLeftData(data1)
        .withRightData(data2)
        .withJoinCondition("a0 = b0")
        .withFilterAfterJoin("a1 <> b1")
        .withSinkPk(List("l0", "r0"))
        .withExpectedData(expected)
        .withExpectedLookupFunctionInvokeCount(6)
        .build())
  }

  @TestTemplate
  def testFilterFieldsAfterJoinWithCdcSourceWithoutDelete(): Unit = {
    val data1 = List(
      // pk1
      changelogRow("+I", Double.box(1.0), Int.box(1), LocalDateTime.of(2021, 1, 1, 1, 1, 1)),
      changelogRow("-U", Double.box(1.0), Int.box(1), LocalDateTime.of(2021, 1, 1, 1, 1, 1)),
      changelogRow("+U", Double.box(1.0), Int.box(1), LocalDateTime.of(2021, 1, 1, 1, 1, 2)),
      // pk2
      changelogRow("+I", Double.box(2.0), Int.box(3), LocalDateTime.of(2022, 2, 2, 2, 2, 2)),
      changelogRow("-U", Double.box(2.0), Int.box(3), LocalDateTime.of(2022, 2, 2, 2, 2, 2)),
      changelogRow("+U", Double.box(2.0), Int.box(3), LocalDateTime.of(2022, 2, 2, 2, 2, 3)),
      // mismatch
      changelogRow("+I", Double.box(3.0), Int.box(3), LocalDateTime.of(2033, 3, 3, 3, 3, 3))
    )

    val data2 = List(
      // pk1
      changelogRow("+I", Int.box(1), Double.box(1.0), LocalDateTime.of(2021, 1, 1, 1, 1, 11)),
      changelogRow("+I", Int.box(3), Double.box(2.0), LocalDateTime.of(2022, 2, 2, 2, 2, 22)),
      // mismatch
      changelogRow("+I", Int.box(99), Double.box(99.0), LocalDateTime.of(2099, 2, 2, 2, 2, 2))
    )

    // TestValuesRuntimeFunctions#KeyedUpsertingSinkFunction will change the RowKind from
    // "+U" to "+I"
    val expected = List("+I[2.0, 3, 2022-02-02T02:02:03, 3, 2.0, 2022-02-02T02:02:22]")

    testUpsertResult(
      newTestSpecBuilder()
        .withLeftIndex(List("a0"))
        .withRightIndex(List("b0"))
        .withLeftPk(List("a0", "a1"))
        .withRightPk(List("b0", "b1"))
        .withSinkPk(List("l0", "r0", "l1", "r1"))
        .withLeftExtraOptions(Maps.newHashMap("changelog-mode", "I,UA,UB"))
        .withRightExtraOptions(Maps.newHashMap("changelog-mode", "I,UA,UB"))
        .withLeftData(data1)
        .withRightData(data2)
        .withJoinCondition("a0 = b0")
        .withFilterAfterJoin("a1 < b0")
        .withExpectedData(expected)
        .withExpectedLookupFunctionInvokeCount(if (enableCache) 5 else 8)
        .build())
  }

  @TestTemplate
  def testFilterProjectBeforeJoinWithCdcSourceWithoutDelete(): Unit = {
    testFilterProjectBeforeJoinWithCdcSourceWithoutDeleteInner(false)
  }

  @TestTemplate
  def testFilterProjectBeforeJoinWithCdcSourceWithoutDeleteAndFilterPushDownIntoSource(): Unit = {
    testFilterProjectBeforeJoinWithCdcSourceWithoutDeleteInner(true)
  }

  private def testFilterProjectBeforeJoinWithCdcSourceWithoutDeleteInner(
      filterPushDown: Boolean): Unit = {
    val data1 = List(
      // pk1
      changelogRow("+I", Double.box(1.0), Int.box(1), LocalDateTime.of(2021, 1, 1, 1, 1, 1)),
      changelogRow("-U", Double.box(1.0), Int.box(1), LocalDateTime.of(2021, 1, 1, 1, 1, 1)),
      changelogRow("+U", Double.box(1.0), Int.box(1), LocalDateTime.of(2021, 1, 1, 1, 1, 2)),
      // pk2
      changelogRow("+I", Double.box(2.0), Int.box(2), LocalDateTime.of(2022, 2, 2, 2, 2, 2)),
      changelogRow("-U", Double.box(2.0), Int.box(2), LocalDateTime.of(2022, 2, 2, 2, 2, 2)),
      changelogRow("+U", Double.box(2.0), Int.box(2), LocalDateTime.of(2022, 2, 2, 2, 2, 3)),
      // mismatch
      changelogRow("+I", Double.box(3.0), Int.box(3), LocalDateTime.of(2033, 3, 3, 3, 3, 3))
    )

    val data2 = List(
      // pk1
      changelogRow("+I", Int.box(1), Double.box(1.0), LocalDateTime.of(2021, 1, 1, 1, 1, 11)),
      // pk2
      changelogRow("+I", Int.box(2), Double.box(2.0), LocalDateTime.of(2022, 2, 2, 2, 2, 22)),
      // mismatch
      changelogRow("+I", Int.box(99), Double.box(99.0), LocalDateTime.of(2099, 2, 2, 2, 2, 2))
    )

    // TestValuesRuntimeFunctions#KeyedUpsertingSinkFunction will change the RowKind from
    // "+U" to "+I"
    val expected1 = List("+I[2.0, 2, 2022-02-02T02:02:03, 2, 2.0, null]")

    val (leftExtraOptions1, rightExtraOptions1): (JMap[String, String], JMap[String, String]) =
      if (filterPushDown) {
        (
          java.util.Map.of("filterable-fields", "a2", "changelog-mode", "I,UA,UB"),
          java.util.Map.of("filterable-fields", "b0", "changelog-mode", "I,UA,UB"))
      } else {
        (
          java.util.Map.of("changelog-mode", "I,UA,UB"),
          java.util.Map.of("changelog-mode", "I,UA,UB"))
      }

    testUpsertResult(
      newTestSpecBuilder()
        .withLeftIndex(List("a0"))
        .withRightIndex(List("b0"))
        .withLeftPk(List("a0", "a1"))
        .withRightPk(List("b0", "b1"))
        .withLeftExtraOptions(leftExtraOptions1)
        .withRightExtraOptions(rightExtraOptions1)
        .withSinkPk(List("l0", "r0", "l1", "r1"))
        .withLeftData(data1)
        .withRightData(data2)
        .withFilterProjectOnLeft("select a1, a2, a0 from testLeft where a1 <> cast(1.0 as double)")
        .withFilterProjectOnRight("select b1, b0 from testRight")
        .withJoinCondition("a0 = b0")
        .withPartialInsertCols(List("l1", "l2", "l0", "r1", "r0"))
        .withExpectedData(expected1)
        .withExpectedLookupFunctionInvokeCount(if (enableCache) 5 else 6)
        .build())

    after()
    before()

    // TestValuesRuntimeFunctions#KeyedUpsertingSinkFunction will change the RowKind from
    // "+U" to "+I"
    val expected2 = List("+I[1.0, 1, 2021-01-01T01:01:02, 1, 1.0, null]")

    val (leftExtraOptions2, rightExtraOptions2): (JMap[String, String], JMap[String, String]) =
      if (filterPushDown) {
        (
          java.util.Map.of("filterable-fields", "a1", "changelog-mode", "I,UA,UB"),
          java.util.Map.of("filterable-fields", "b0", "changelog-mode", "I,UA,UB"))
      } else {
        (
          java.util.Map.of("changelog-mode", "I,UA,UB"),
          java.util.Map.of("changelog-mode", "I,UA,UB"))
      }

    testUpsertResult(
      newTestSpecBuilder()
        .withLeftIndex(List("a0"))
        .withRightIndex(List("b0"))
        .withLeftPk(List("a0", "a1"))
        .withRightPk(List("b0", "b1"))
        .withLeftExtraOptions(leftExtraOptions2)
        .withRightExtraOptions(rightExtraOptions2)
        .withSinkPk(List("l0", "r0", "l1", "r1"))
        .withLeftData(data1)
        .withRightData(data2)
        .withFilterProjectOnLeft("select a0, a2, a1 from testLeft")
        .withFilterProjectOnRight("select b1, b0 from testRight where b0 <> 2")
        .withJoinCondition("a0 = b0")
        .withPartialInsertCols(List("l0", "l2", "l1", "r1", "r0"))
        .withExpectedData(expected2)
        .withExpectedLookupFunctionInvokeCount(if (enableCache) 5 else 7)
        .build())
  }

  @TestTemplate
  def testPartitionPushDownIntoCdcSourceWithoutDelete(): Unit = {
    val data1 = List(
      // pk1
      changelogRow("+I", Double.box(50.0), Int.box(1), LocalDateTime.of(2021, 1, 1, 1, 1, 1)),
      changelogRow("-U", Double.box(50.0), Int.box(1), LocalDateTime.of(2021, 1, 1, 1, 1, 1)),
      changelogRow("+U", Double.box(50.0), Int.box(1), LocalDateTime.of(2021, 1, 1, 1, 1, 2)),
      // pk2
      changelogRow("+I", Double.box(50.0), Int.box(2), LocalDateTime.of(2022, 2, 2, 2, 2, 2)),
      changelogRow("-U", Double.box(50.0), Int.box(2), LocalDateTime.of(2022, 2, 2, 2, 2, 2)),
      changelogRow("+U", Double.box(50.0), Int.box(2), LocalDateTime.of(2022, 2, 2, 2, 2, 3)),
      // mismatch
      changelogRow("+I", Double.box(100.0), Int.box(3), LocalDateTime.of(2033, 3, 3, 3, 3, 3))
    )

    val data2 = List(
      // pk1
      changelogRow("+I", Int.box(1), Double.box(50.0), LocalDateTime.of(2021, 1, 1, 1, 1, 11)),
      // pk2
      changelogRow("+I", Int.box(2), Double.box(500.0), LocalDateTime.of(2022, 2, 2, 2, 2, 22)),
      // mismatch
      changelogRow("+I", Int.box(99), Double.box(99.0), LocalDateTime.of(2099, 2, 2, 2, 2, 2))
    )

    // TestValuesRuntimeFunctions#KeyedUpsertingSinkFunction will change the RowKind from
    // "+U" to "+I"
    val expected = List("+I[50.0, 2, 2022-02-02T02:02:03, 2, 500.0, null]")

    val (leftExtraOptions1, rightExtraOptions1): (JMap[String, String], JMap[String, String]) =
      (
        java.util.Map.of("partition-list", "a1:50.0;a1:100.0", "changelog-mode", "I,UA,UB"),
        java.util.Map.of("partition-list", "b1:50.0;b1:500.0;b1:99.0", "changelog-mode", "I,UA,UB")
      )

    testUpsertResult(
      newTestSpecBuilder()
        .withLeftIndex(List("a0"))
        .withRightIndex(List("b0"))
        .withLeftPk(List("a0", "a1"))
        .withRightPk(List("b0", "b1"))
        .withLeftPartitionKeys(List("a1"))
        .withRightPartitionKeys(List("b1"))
        .withLeftExtraOptions(leftExtraOptions1)
        .withRightExtraOptions(rightExtraOptions1)
        .withSinkPk(List("l0", "r0", "l1", "r1"))
        .withLeftData(data1)
        .withRightData(data2)
        .withFilterProjectOnLeft("select a1, a2, a0 from testLeft " +
          "where a1 = cast(50.0 as double)  or a1 = cast(100.0 as double)")
        .withFilterProjectOnRight("select b1, b0 from testRight " +
          "where b1 = cast(500.0 as double)  or b1 = cast(99.0 as double)")
        .withJoinCondition("a0 = b0")
        .withPartialInsertCols(List("l1", "l2", "l0", "r1", "r0"))
        .withExpectedData(expected)
        .withExpectedLookupFunctionInvokeCount(if (enableCache) 5 else 7)
        .build())
  }

  @TestTemplate
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
      .executeSql("""
                    |insert into testSnk
                    | select
                    |   a1,
                    |   a0 + 1,
                    |   a2,
                    |   b0 + 2,
                    |   b1,
                    |   b2
                    |   from testLeft
                    | join testRight
                    |   on a0 = b0
                    | on conflict do deduplicate
                    |""".stripMargin)
      .await()
    val result = TestValuesTableFactory.getResultsAsStrings("testSnk")

    assertThat(result.sorted).isEqualTo(expected.sorted)
    assertThat(AsyncTestValueLookupFunction.invokeCount.get()).isEqualTo(6)
  }

  @TestTemplate
  def testLookupJoinDimTableWithPkAfterJoin(): Unit = {
    testLookupJoinAfterJoinInner(true)
  }

  @TestTemplate
  def testLookupJoinDimTableWithoutPkAfterJoin(): Unit = {
    testLookupJoinAfterJoinInner(false)
  }

  def testLookupJoinAfterJoinInner(dimTableContainsPK: Boolean): Unit = {
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

    val dimData = List(
      changelogRow("+I", Int.box(2), "s2"),
      // mismatch
      changelogRow("+I", Int.box(4), "s4")
    )

    tEnv.executeSql(s"""
                       |create table dim (
                       |  id int ${if (dimTableContainsPK) "primary key not enforced" else ""},
                       |  dim_value string
                       |) with (
                       |  'connector' = 'values',
                       |  'data-id' = '${TestValuesTableFactory.registerData(dimData)}'
                       |)""".stripMargin)

    // TestValuesRuntimeFunctions#KeyedUpsertingSinkFunction will change the RowKind from
    // "+U" to "+I"
    val expected = List(
      "+I[2.0, 2, 2022-02-02T02:02:02, 2, 2.0, 2022-02-02T02:02:22, s2]"
    )

    tEnv.executeSql("alter table testSnk add (dim_value string)")

    tEnv
      .executeSql("""
                    |insert into testSnk
                    | select a1, a0, a2, b0, b1, b2, dim_value
                    | from (
                    |   select
                    |     *, proctime() as pt
                    |   from testLeft
                    |   join testRight
                    |     on a0 = b0
                    | ) tmp
                    | join dim
                    |   for system_time as of pt
                    |     on tmp.a0 = dim.id
                    | on conflict do deduplicate
                    |""".stripMargin)
      .await()
    val result = TestValuesTableFactory.getResultsAsStrings("testSnk")

    assertThat(result.sorted).isEqualTo(expected.sorted)
    assertThat(AsyncTestValueLookupFunction.invokeCount.get()).isEqualTo(6)
  }

  @TestTemplate
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
      newTestSpecBuilder()
        .withLeftIndex(List("a0"))
        .withRightIndex(List("b0"))
        .withLeftData(data1)
        .withRightData(data2)
        .withJoinCondition("a0 = b0 and a1 = b1")
        .withSinkPk(List("l0", "r0"))
        .withExpectedData(expected)
        .withTestFailingSource(true)
        .build())
  }

  @TestTemplate
  def testJoinKeysContainNull(): Unit = {
    val data1 = List(
      // both join keys are null
      changelogRow(
        "+I",
        null.asInstanceOf[java.lang.Double],
        null.asInstanceOf[java.lang.Integer],
        LocalDateTime.of(2021, 1, 1, 1, 1, 1)),
      // one of join keys is null
      changelogRow(
        "+I",
        null.asInstanceOf[java.lang.Double],
        Int.box(21),
        LocalDateTime.of(2022, 2, 2, 2, 2, 21)),
      changelogRow(
        "+I",
        Double.box(22.0),
        null.asInstanceOf[java.lang.Integer],
        LocalDateTime.of(2022, 2, 2, 2, 2, 22)),
      // both join keys are not null
      changelogRow("+I", Double.box(3.0), Int.box(3), LocalDateTime.of(2033, 3, 3, 3, 3, 3))
    )

    val data2 = List(
      // both join keys are null
      changelogRow(
        "+I",
        null.asInstanceOf[java.lang.Integer],
        null.asInstanceOf[java.lang.Double],
        LocalDateTime.of(2021, 1, 1, 1, 1, 1)),
      // one of join keys is null
      changelogRow(
        "+I",
        Int.box(21),
        null.asInstanceOf[java.lang.Double],
        LocalDateTime.of(2022, 2, 2, 2, 2, 21)),
      changelogRow(
        "+I",
        null.asInstanceOf[java.lang.Integer],
        Double.box(22.0),
        LocalDateTime.of(2022, 2, 2, 2, 2, 22)),
      // both join keys are not null
      changelogRow("+I", Int.box(3), Double.box(3.0), LocalDateTime.of(2033, 3, 3, 3, 3, 3))
    )

    val expected = List("+I[3.0, 3, 2033-03-03T03:03:03, 3, 3.0, 2033-03-03T03:03:03]")

    testUpsertResult(
      newTestSpecBuilder()
        .withLeftIndex(List("a0", "a1"))
        .withRightIndex(List("b0", "b1"))
        .withLeftPk(List("a2"))
        .withRightPk(List("b2"))
        .withLeftImmutableCols(List("a0", "a1"))
        .withRightImmutableCols(List("b0", "b1"))
        .withLeftData(data1)
        .withRightData(data2)
        .withJoinCondition("a0 = b0 and a1 = b1")
        .withSinkPk(List("l2", "r2"))
        .withExpectedData(expected)
        .build())
  }

  @TestTemplate
  def testLeftTableEmpty(): Unit = {
    testOneTableEmpty(true)
  }

  @TestTemplate
  def testRightTableEmpty(): Unit = {
    testOneTableEmpty(false)
  }

  def testOneTableEmpty(isLeftTableEmpty: Boolean): Unit = {
    val data1 = if (isLeftTableEmpty) {
      List()
    } else {
      List(
        // both join keys are null
        changelogRow("+I", Double.box(1.0), Int.box(1), LocalDateTime.of(2021, 1, 1, 1, 1, 1))
      )
    }

    val data2 = if (isLeftTableEmpty) {
      List(
        // both join keys are null
        changelogRow("+I", Int.box(1), Double.box(1), LocalDateTime.of(2021, 1, 1, 1, 1, 1))
      )
    } else {
      List()
    }

    val expected = List()

    testUpsertResult(
      newTestSpecBuilder()
        .withLeftIndex(List("a0", "a1"))
        .withRightIndex(List("b0", "b1"))
        .withLeftPk(List("a2"))
        .withRightPk(List("b2"))
        .withLeftImmutableCols(List("a0", "a1"))
        .withRightImmutableCols(List("b0", "b1"))
        .withLeftData(data1)
        .withRightData(data2)
        .withJoinCondition("a0 = b0 and a1 = b1")
        .withSinkPk(List("l2", "r2"))
        .withExpectedData(expected)
        .build())
  }

  private def testUpsertResult(testSpec: TestSpec): Unit = {
    prepareTable(
      testSpec.leftIndex,
      testSpec.rightIndex,
      testSpec.leftImmutableCols.getOrElse(List()),
      testSpec.rightImmutableCols.getOrElse(List()),
      testSpec.leftPk.orNull,
      testSpec.rightPk.orNull,
      testSpec.sinkPk,
      testSpec.leftPartitionKeys,
      testSpec.rightPartitionKeys,
      testSpec.leftData,
      testSpec.rightData,
      testSpec.testFailingSource,
      testSpec.leftExtraOptions,
      testSpec.rightExtraOptions
    )

    val partialInsertStr = if (testSpec.partialInsertCols.isEmpty) {
      ""
    } else {
      s"(${testSpec.partialInsertCols.get.mkString(",")})"
    }

    val queryOnLeft = if (testSpec.filterProjectOnLeft.isEmpty) {
      "testLeft"
    } else {
      s"(${testSpec.filterProjectOnLeft.get})"
    }

    val queryOnRight = if (testSpec.filterProjectOnRight.isEmpty) {
      "testRight"
    } else {
      s"(${testSpec.filterProjectOnRight.get})"
    }

    val filterAfterJoin = if (testSpec.filterAfterJoin.isEmpty) {
      ""
    } else {
      s"where ${testSpec.filterAfterJoin.get}"
    }

    val sql =
      s"""
         | insert into testSnk $partialInsertStr
         | select * from $queryOnLeft join $queryOnRight
         | on ${testSpec.joinCondition}
         | $filterAfterJoin
         | on conflict do deduplicate
         |""".stripMargin
    tEnv
      .executeSql(sql)
      .await(60, TimeUnit.SECONDS)
    val result = TestValuesTableFactory.getResultsAsStrings("testSnk")

    assertThat(result.sorted).isEqualTo(testSpec.expected.sorted)
    if (testSpec.expectedLookupFunctionInvokeCount.isDefined) {
      assertThat(AsyncTestValueLookupFunction.invokeCount.get())
        .isEqualTo(testSpec.expectedLookupFunctionInvokeCount.get)
    }
  }

  private def prepareTable(
      leftIndex: List[String],
      rightIndex: List[String],
      leftData: List[Row],
      rightData: List[Row]): Unit = {
    prepareTable(
      leftIndex,
      rightIndex,
      List(),
      List(),
      null,
      null,
      List("l0", "r0"),
      List(),
      List(),
      leftData,
      rightData,
      testFailingSource = false,
      Collections.emptyMap(),
      Collections.emptyMap()
    )
  }

  private def prepareTable(
      leftIndex: List[String],
      rightIndex: List[String],
      leftImmutableCols: List[String],
      rightImmutableCols: List[String],
      @Nullable leftPk: List[String],
      @Nullable rightPk: List[String],
      sinkPk: List[String],
      leftPartitionKeys: List[String],
      rightPartitionKeys: List[String],
      leftData: List[Row],
      rightData: List[Row],
      testFailingSource: Boolean,
      leftExtraOptions: JMap[String, String],
      rightExtraOptions: JMap[String, String]): Unit = {
    tEnv.executeSql("drop table if exists testLeft")
    val leftExtraOptionsStr =
      if (leftExtraOptions.isEmpty) {
        ""
      } else {
        "," + leftExtraOptions.asScala
          .map { case (key, value) => s"'$key' = '$value'" }
          .mkString(", ")
      }

    val leftPartitionStr =
      if (leftPartitionKeys.isEmpty) {
        ""
      } else {
        s"PARTITIONED BY (${leftPartitionKeys.mkString(",")})"
      }

    tEnv.executeSql(
      s"""
         |create table testLeft(
         |  a1 double,
         |  a0 int,
         |  a2 timestamp(3)
         |  ${if (leftPk == null) "" else s", primary key (${leftPk.mkString(",")}) not enforced"}
         |) $leftPartitionStr
         |with (
         |  'connector' = 'values',
         |  'bounded' = 'false',
         |  'data-id' = '${TestValuesTableFactory.registerData(leftData)}',
         |  'async' = 'true',
         |  'failing-source' = '$testFailingSource'
         |  $leftExtraOptionsStr
         |)
         |""".stripMargin)
    addIndexesAndImmutableCols("testLeft", List(leftIndex), leftImmutableCols)

    tEnv.executeSql("drop table if exists testRight")
    val rightExtraOptionsStr =
      if (rightExtraOptions.isEmpty) {
        ""
      } else {
        "," + rightExtraOptions.asScala
          .map { case (key, value) => s"'$key' = '$value'" }
          .mkString(", ")
      }
    val rightPartitionStr =
      if (rightPartitionKeys.isEmpty) {
        ""
      } else {
        s"PARTITIONED BY (${rightPartitionKeys.mkString(",")})"
      }
    tEnv.executeSql(
      s"""
         |create table testRight(
         |  b0 int,
         |  b1 double,
         |  b2 timestamp(3)
         |  ${if (rightPk == null) "" else s", primary key (${rightPk.mkString(",")}) not enforced"}
         |) $rightPartitionStr
         |with (
         |  'connector' = 'values',
         |  'bounded' = 'false',
         |  'data-id' = '${TestValuesTableFactory.registerData(rightData)}',
         |  'async' = 'true',
         |  'failing-source' = '$testFailingSource'
         |  $rightExtraOptionsStr
         |)
         |""".stripMargin)
    addIndexesAndImmutableCols("testRight", List(rightIndex), rightImmutableCols)

    tEnv.executeSql("drop table if exists testSnk")
    tEnv.executeSql(s"""
                       |create table testSnk(
                       |  l1 double,
                       |  l0 int,
                       |  l2 timestamp(3),
                       |  r0 int,
                       |  r1 double,
                       |  r2 timestamp(3),
                       |  primary key(${sinkPk.mkString(",")}) not enforced
                       |) with (
                       |  'connector' = 'values',
                       |  'bounded' = 'false',
                       |  'sink-insert-only' = 'false'
                       |)
                       |""".stripMargin)
  }

  private def newTestSpecBuilder(): TestSpecBuilder = {
    new TestSpecBuilder
  }

  private case class TestSpec(
      leftIndex: List[String],
      rightIndex: List[String],
      leftImmutableCols: Option[List[String]],
      rightImmutableCols: Option[List[String]],
      leftPk: Option[List[String]],
      rightPk: Option[List[String]],
      partialInsertCols: Option[List[String]],
      sinkPk: List[String],
      leftPartitionKeys: List[String],
      rightPartitionKeys: List[String],
      leftData: List[Row],
      rightData: List[Row],
      filterProjectOnLeft: Option[String] = None,
      filterProjectOnRight: Option[String] = None,
      joinCondition: String,
      filterAfterJoin: Option[String],
      expected: List[String],
      expectedLookupFunctionInvokeCount: Option[Int],
      testFailingSource: Boolean,
      leftExtraOptions: JMap[String, String],
      rightExtraOptions: JMap[String, String]
  )

  private class TestSpecBuilder {
    private var leftIndex: Option[List[String]] = None
    private var rightIndex: Option[List[String]] = None
    private var leftImmutableCols: Option[List[String]] = None
    private var rightImmutableCols: Option[List[String]] = None
    private var leftPk: Option[List[String]] = None
    private var rightPk: Option[List[String]] = None
    private var partialInsertCols: Option[List[String]] = None
    private var sinkPk: Option[List[String]] = None
    private var leftPartitionKeys: Option[List[String]] = None
    private var rightPartitionKeys: Option[List[String]] = None
    private var filterProjectOnLeft: Option[String] = None
    private var filterProjectOnRight: Option[String] = None
    private var joinCondition: Option[String] = None
    private var filterAfterJoin: Option[String] = None
    private var leftData: Option[List[Row]] = None
    private var rightData: Option[List[Row]] = None
    private var expectedData: Option[List[String]] = None
    private var expectedLookupFunctionInvokeCount: Option[Int] = None
    private var testFailingSource: Boolean = false
    private val leftExtraOptions: JMap[String, String] = new JHashMap[String, String]
    private val rightExtraOptions: JMap[String, String] = new JHashMap[String, String]

    def withLeftIndex(index: List[String]): TestSpecBuilder = {
      leftIndex = Some(requireNonNull(index))
      this
    }

    def withRightIndex(index: List[String]): TestSpecBuilder = {
      rightIndex = Some(requireNonNull(index))
      this
    }
    def withLeftImmutableCols(immutableCols: List[String]): TestSpecBuilder = {
      leftImmutableCols = Some(requireNonNull(immutableCols))
      this
    }

    def withRightImmutableCols(immutableCols: List[String]): TestSpecBuilder = {
      rightImmutableCols = Some(requireNonNull(immutableCols))
      this
    }

    def withLeftPk(pk: List[String]): TestSpecBuilder = {
      leftPk = Some(requireNonNull(pk))
      this
    }

    def withRightPk(pk: List[String]): TestSpecBuilder = {
      rightPk = Some(requireNonNull(pk))
      this
    }

    def withPartialInsertCols(cols: List[String]): TestSpecBuilder = {
      partialInsertCols = Some(requireNonNull(cols))
      this
    }

    def withSinkPk(pk: List[String]): TestSpecBuilder = {
      sinkPk = Some(requireNonNull(pk))
      this
    }

    def withLeftPartitionKeys(partitionKeys: List[String]): TestSpecBuilder = {
      leftPartitionKeys = Some(requireNonNull(partitionKeys))
      this
    }

    def withRightPartitionKeys(partitionKeys: List[String]): TestSpecBuilder = {
      rightPartitionKeys = Some(requireNonNull(partitionKeys))
      this
    }

    def withLeftData(data: List[Row]): TestSpecBuilder = {
      leftData = Some(requireNonNull(data))
      this
    }

    def withRightData(data: List[Row]): TestSpecBuilder = {
      rightData = Some(requireNonNull(data))
      this
    }

    def withFilterProjectOnLeft(query: String): TestSpecBuilder = {
      filterProjectOnLeft = Some(requireNonNull(query))
      this
    }

    def withFilterProjectOnRight(query: String): TestSpecBuilder = {
      filterProjectOnRight = Some(requireNonNull(query))
      this
    }

    def withJoinCondition(condition: String): TestSpecBuilder = {
      joinCondition = Some(requireNonNull(condition))
      this
    }

    def withFilterAfterJoin(filter: String): TestSpecBuilder = {
      filterAfterJoin = Some(requireNonNull(filter))
      this
    }

    def withExpectedData(expected: List[String]): TestSpecBuilder = {
      this.expectedData = Some(requireNonNull(expected))
      this
    }

    def withExpectedLookupFunctionInvokeCount(count: Int): TestSpecBuilder = {
      expectedLookupFunctionInvokeCount = Some(requireNonNull(count))
      this
    }

    def withTestFailingSource(flag: Boolean): TestSpecBuilder = {
      testFailingSource = requireNonNull(flag)
      this
    }

    def withLeftExtraOptions(options: JMap[String, String]): TestSpecBuilder = {
      leftExtraOptions.putAll(options)
      this
    }

    def withRightExtraOptions(options: JMap[String, String]): TestSpecBuilder = {
      rightExtraOptions.putAll(options)
      this
    }

    def build(): TestSpec = {
      TestSpec(
        requireNonNull(leftIndex.orNull),
        requireNonNull(rightIndex.orNull),
        leftImmutableCols,
        rightImmutableCols,
        leftPk,
        rightPk,
        partialInsertCols,
        requireNonNull(sinkPk.orNull),
        requireNonNull(leftPartitionKeys.getOrElse(List())),
        requireNonNull(rightPartitionKeys.getOrElse(List())),
        requireNonNull(leftData.orNull),
        requireNonNull(rightData.orNull),
        filterProjectOnLeft,
        filterProjectOnRight,
        requireNonNull(joinCondition.orNull),
        filterAfterJoin,
        requireNonNull(expectedData.orNull),
        expectedLookupFunctionInvokeCount,
        testFailingSource,
        leftExtraOptions,
        rightExtraOptions
      )
    }

  }
}
