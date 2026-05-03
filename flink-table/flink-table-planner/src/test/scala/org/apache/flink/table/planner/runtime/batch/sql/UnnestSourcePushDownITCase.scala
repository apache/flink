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
package org.apache.flink.table.planner.runtime.batch.sql

import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.runtime.utils.BatchTestBase
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.types.Row

import org.junit.jupiter.api.{BeforeEach, Test}

/**
 * End-to-end correctness tests for [[FlinkFilterCorrelateUnnestTransposeRule]] combined with
 * source-level filter and projection pushdown. Uses {@code TestValuesTableFactory} with
 * filterable fields so the planner can drive predicates all the way into the source scan.
 */
class UnnestSourcePushDownITCase extends BatchTestBase {

  // (a, b, c, d) where d = ARRAY<INT>
  private val rows: Seq[Row] = Seq(
    row(1, 10L, "x", Array[Integer](1, 2)),
    row(2, 20L, "y", Array[Integer](3)),
    row(6, 60L, "z", Array[Integer](50, 150)),
    row(7, 70L, "w", Array[Integer](99))
  )

  @BeforeEach
  override def before(): Unit = {
    super.before()
    val dataId = TestValuesTableFactory.registerData(rows)
    tEnv.executeSql(
      s"""
         |CREATE TABLE T (
         |  a INT,
         |  b BIGINT,
         |  c STRING,
         |  d ARRAY<INT>
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$dataId',
         |  'filterable-fields' = 'a;b',
         |  'bounded' = 'true'
         |)
       """.stripMargin)
  }

  @Test
  def testFilterOnLeftPushedIntoSource(): Unit = {
    // a > 5 should be pushed below the Correlate AND into the source scan
    checkResult(
      "SELECT a, s FROM T, UNNEST(d) AS T1(s) WHERE a > 5",
      Seq(
        row(6, 50),
        row(6, 150),
        row(7, 99)
      ))
  }

  @Test
  def testFilterOnRightStaysAtCorrelate(): Unit = {
    // s < 100 stays on right side; source scan sees no predicate
    checkResult(
      "SELECT a, s FROM T, UNNEST(d) AS T1(s) WHERE s < 100",
      Seq(
        row(1, 1),
        row(1, 2),
        row(2, 3),
        row(6, 50),
        row(7, 99)
      ))
  }

  @Test
  def testMixedPredicateBothPushed(): Unit = {
    // a > 5 to left/source, s < 100 to right side
    checkResult(
      "SELECT a, s FROM T, UNNEST(d) AS T1(s) WHERE a > 5 AND s < 100",
      Seq(
        row(6, 50),
        row(7, 99)
      ))
  }

  @Test
  def testLeftJoinFilterOnLeftPushed(): Unit = {
    // LEFT JOIN: left filter still pushes safely. a > 5 selects rows with a in {6, 7}.
    // Both rows have non-empty arrays, so no null-padded rows here.
    checkResult(
      "SELECT a, s FROM T LEFT JOIN UNNEST(d) AS T1(s) ON TRUE WHERE a > 5",
      Seq(
        row(6, 50),
        row(6, 150),
        row(7, 99)
      ))
  }

  @Test
  def testLeftJoinFilterOnRightStaysAbove(): Unit = {
    // LEFT JOIN with right-side filter: must NOT push to right (would change null padding
    // semantics for empty arrays). Verifies LEFT correlate semantics are preserved.
    checkResult(
      "SELECT a, s FROM T LEFT JOIN UNNEST(d) AS T1(s) ON TRUE WHERE s < 100",
      Seq(
        row(1, 1),
        row(1, 2),
        row(2, 3),
        row(6, 50),
        row(7, 99)
      ))
  }

}
