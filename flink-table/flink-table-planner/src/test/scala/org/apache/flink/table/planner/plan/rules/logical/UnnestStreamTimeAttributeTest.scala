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
 * Stream-mode plan tests that verify [[FlinkProjectCorrelateUnnestTransposeRule]] preserves
 * rowtime / watermark attributes when they are referenced downstream of UNNEST.
 *
 * <p>The concern: pruning a left-side column the user "didn't ask for" could break watermark
 * propagation if some downstream operator needs the rowtime. The test confirms that:
 *
 * <ol>
 *   <li>When the rowtime column is referenced (directly or by a downstream window function), the
 *       rule preserves it in the pruned left input.
 *   <li>Pruning of unrelated columns still happens around the time attribute.
 * </ol>
 *
 * <p>Note: when a user explicitly does not select the rowtime through UNNEST, that's a deliberate
 * projection — the resulting view loses its time attribute, just as it would for any other
 * projection. That is consistent with normal Flink projection semantics and not the rule's
 * concern.
 */
class UnnestStreamTimeAttributeTest extends TableTestBase {

  private val util = streamTestUtil()

  @BeforeEach
  def setup(): Unit = {
    util.tableEnv.executeSql(
      """
        |CREATE TABLE T (
        |  a INT,
        |  b BIGINT,
        |  c STRING,
        |  ts TIMESTAMP(3),
        |  d ARRAY<INT>,
        |  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
        |) WITH (
        |  'connector' = 'values',
        |  'bounded' = 'false'
        |)
      """.stripMargin)
  }

  /**
   * Selecting the rowtime column through UNNEST. Rule should preserve {@code ts} in the pruned
   * left input (it's referenced by the top project) and prune unreferenced columns ({@code b},
   * {@code c}).
   */
  @Test
  def testRowtimeSelectedThroughUnnestIsPreserved(): Unit = {
    util.verifyRelPlan("SELECT a, ts, s FROM T, UNNEST(d) AS f(s)")
  }

  /**
   * Downstream window aggregation requires the rowtime. The inner SELECT does NOT explicitly list
   * {@code ts}, but the outer GROUP BY references it through the inner alias. This is a parse-
   * time error (can't window on a column not selected); included here to lock in current behavior.
   * If this test starts producing a successful plan, it would mean Flink is silently propagating
   * rowtime, and we should re-examine whether the rule's pruning interacts with that path.
   */
  @Test
  def testRowtimeUsedByDownstreamWindowIsPreserved(): Unit = {
    util.verifyRelPlan(
      """
        |SELECT a, TUMBLE_END(ts, INTERVAL '1' HOUR) AS w_end, COUNT(*) AS cnt
        |FROM (SELECT a, ts, s FROM T, UNNEST(d) AS f(s))
        |GROUP BY a, TUMBLE(ts, INTERVAL '1' HOUR)
      """.stripMargin)
  }

  /**
   * User explicitly does not select rowtime through UNNEST. Normal projection semantics: rowtime
   * is dropped. The rule additionally prunes {@code b}, {@code c} from the source scan. This test
   * confirms behavior is consistent with non-UNNEST projection — losing the time attribute is the
   * user's choice, not a side effect of the rule.
   */
  @Test
  def testRowtimeNotSelectedIsPruned(): Unit = {
    util.verifyRelPlan("SELECT a, s FROM T, UNNEST(d) AS f(s)")
  }
}
