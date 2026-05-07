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
package org.apache.flink.table.planner.plan.rules.logical

import org.apache.flink.table.api._
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.jupiter.api.{BeforeEach, Test}

/**
 * Test for [[FlinkProjectCorrelateUnnestTransposeRule]] and
 * [[FlinkFilterCorrelateUnnestTransposeRule]]. Both rules are wired into
 * [[org.apache.flink.table.planner.plan.rules.FlinkBatchRuleSets.PROJECT_RULES]] /
 * [[org.apache.flink.table.planner.plan.rules.FlinkBatchRuleSets.FILTER_RULES]] (and the stream
 * equivalents), so the standard batch optimization chain exercises them after [[LogicalUnnestRule]]
 * runs.
 */
class FlinkProjectFilterCorrelateUnnestTransposeRuleTest extends TableTestBase {

  private val util = batchTestUtil()

  @BeforeEach
  def setup(): Unit = {
    util.addTableSource[(Int, Int, Long, Array[Int])]("MyTable", 'a, 'b, 'c, 'd)
    util.addTableSource[(Int, Array[(Int, String)])]("MyRowArrayTable", 'a, 'b)
  }

  @Test
  def testInnerUnnestProjectionDropsLeftColumns(): Unit = {
    util.verifyRelPlan("SELECT a, s FROM MyTable, UNNEST(d) AS T(s)")
  }

  @Test
  def testLeftUnnestProjectionDropsLeftColumns(): Unit = {
    util.verifyRelPlan("SELECT a, s FROM MyTable LEFT JOIN UNNEST(d) AS T(s) ON TRUE")
  }

  /**
   * UNNEST of an ARRAY of ROWs. The table has only two columns ({@code a} selected by the project,
   * {@code b} required by the correlation), so there is nothing to prune from the left input — the
   * rule correctly no-ops. The test still locks in the post-rule plan shape for ARRAY&lt;ROW&gt; so
   * a future regression in correlation handling for that shape would surface as a plan diff.
   */
  @Test
  def testInnerUnnestArrayOfRowsAllLeftColumnsUsed(): Unit = {
    util.verifyRelPlan("SELECT a, x FROM MyRowArrayTable, UNNEST(b) AS T(x, y)")
  }

  @Test
  def testInnerUnnestFilterOnLeftOnly(): Unit = {
    util.verifyRelPlan("SELECT a, b, s FROM MyTable, UNNEST(d) AS T(s) WHERE a > 5")
  }

  @Test
  def testInnerUnnestFilterOnRightOnly(): Unit = {
    util.verifyRelPlan("SELECT a, b, s FROM MyTable, UNNEST(d) AS T(s) WHERE s < 100")
  }

  @Test
  def testLeftUnnestFilterOnRightOnly(): Unit = {
    util.verifyRelPlan("SELECT a, s FROM MyTable LEFT JOIN UNNEST(d) AS T(s) ON TRUE WHERE s < 100")
  }

  @Test
  def testInnerUnnestMixedPredicate(): Unit = {
    util.verifyRelPlan(
      "SELECT a, b, s FROM MyTable, UNNEST(d) AS T(s) " +
        "WHERE a > 5 AND s < 100 AND a + s > 10")
  }

  @Test
  def testFilterOnArrayColumn(): Unit = {
    util.verifyRelPlan("SELECT a, s FROM MyTable, UNNEST(d) AS T(s) WHERE d IS NOT NULL")
  }

  @Test
  def testUnnestWithOrdinalityFilterOnPos(): Unit = {
    util.verifyRelPlan(
      "SELECT a, val, pos FROM MyTable " +
        "CROSS JOIN UNNEST(d) WITH ORDINALITY AS T(val, pos) WHERE pos = 1")
  }
}
