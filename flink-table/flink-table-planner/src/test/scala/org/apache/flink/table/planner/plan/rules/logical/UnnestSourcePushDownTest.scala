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

import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.jupiter.api.{BeforeEach, Test}

/**
 * End-to-end plan tests that exercise [[FlinkFilterCorrelateUnnestTransposeRule]] together with
 * the existing source-pushdown rules ([[PushFilterIntoTableSourceScanRule]],
 * [[PushProjectIntoTableSourceScanRule]]). The goal is to confirm that:
 *
 *   - Filters that this rule pushes onto the left input of a Correlate continue down into the
 *     table source when the source supports [[SupportsFilterPushDown]].
 *   - Existing projection pushdown into the table source still works alongside UNNEST (the
 *     planner trims columns from the scan even though we don't yet push Project through Correlate).
 *
 * Stream + batch both go through the same FILTER_RULES set; batch coverage is sufficient to
 * exercise the rule wiring end-to-end.
 */
class UnnestSourcePushDownTest extends TableTestBase {

  private val util = batchTestUtil()

  @BeforeEach
  def setup(): Unit = {
    util.tableEnv.executeSql(
      """
        |CREATE TABLE MyTable (
        |  a INT,
        |  b BIGINT,
        |  c STRING,
        |  d ARRAY<INT>
        |) WITH (
        |  'connector' = 'values',
        |  'filterable-fields' = 'a;b',
        |  'bounded' = 'true'
        |)
      """.stripMargin)
  }

  /**
   * Filter on left-only column should land in the table source scan as a pushed predicate when the
   * source declares it filterable. Without the new rule the filter would sit above the Correlate
   * and the source would see no predicate.
   */
  @Test
  def testFilterOnLeftOnlyPushesIntoSource(): Unit = {
    util.verifyRelPlan("SELECT a, s FROM MyTable, UNNEST(d) AS T(s) WHERE a > 5")
  }

  /** Right-only filter must remain near the UNNEST and never reach the source. */
  @Test
  def testFilterOnRightOnlyDoesNotReachSource(): Unit = {
    util.verifyRelPlan("SELECT a, s FROM MyTable, UNNEST(d) AS T(s) WHERE s < 100")
  }

  /** Mixed predicate splits: left part to source, right part above TFS, mixed stays above. */
  @Test
  def testMixedPredicateSplitsAcrossSourceAndCorrelate(): Unit = {
    util.verifyRelPlan(
      "SELECT a, b, s FROM MyTable, UNNEST(d) AS T(s) " +
        "WHERE a > 5 AND s < 100 AND a + s > 10")
  }

  /**
   * Filter on a left column that is NOT in {@code filterable-fields} stays above the source scan
   * but should still land below the Correlate (i.e. the new rule fires regardless of whether the
   * source can absorb it).
   */
  @Test
  def testFilterOnLeftNonFilterableColumn(): Unit = {
    util.verifyRelPlan("SELECT a, s FROM MyTable, UNNEST(d) AS T(s) WHERE c LIKE 'foo%'")
  }

  /**
   * Existing projection pushdown into source should still trim {@code b, c} from the scan when the
   * top-level query only needs {@code a, s} — we don't push the Project through Correlate but the
   * source-level rule sees the eventual Calc output and prunes the read columns.
   */
  @Test
  def testProjectionPushDownIntoSourceWithUnnest(): Unit = {
    util.verifyRelPlan("SELECT a, s FROM MyTable, UNNEST(d) AS T(s)")
  }

  /**
   * Combination: filter pushed below Correlate AND into source, plus source-level projection
   * pushdown trimming columns the query does not select.
   */
  @Test
  def testFilterAndProjectionBothPushIntoSource(): Unit = {
    util.verifyRelPlan("SELECT a, s FROM MyTable, UNNEST(d) AS T(s) WHERE a > 5")
  }

  /**
   * LEFT correlate variant: filter on the right side must not push to source (would break null
   * padding) but filter on left should still flow into source.
   */
  @Test
  def testLeftCorrelateMixedFilterPushDown(): Unit = {
    util.verifyRelPlan(
      "SELECT a, s FROM MyTable LEFT JOIN UNNEST(d) AS T(s) ON TRUE " +
        "WHERE a > 5 AND s < 100")
  }
}
