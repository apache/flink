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

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.planner.plan.optimize.program.{BatchOptimizeContext, FlinkChainedProgram, FlinkHepRuleSetProgramBuilder, HEP_RULES_EXECUTION_TYPE}
import org.apache.flink.table.planner.utils.TableTestBase

import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.rel.rules.{FilterJoinRule, FilterMultiJoinMergeRule, JoinToMultiJoinRule, ProjectMultiJoinMergeRule}
import org.apache.calcite.tools.RuleSets
import org.junit.{Before, Test}

class EliminateCrossJoinRuleTest extends TableTestBase {
  private val util = batchTestUtil()

  @Before
  def setup(): Unit = {
    val programs = new FlinkChainedProgram[BatchOptimizeContext]()
    programs.addLast(
      "filter_on_join",
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(RuleSets.ofList(
          FilterJoinRule.FILTER_ON_JOIN))
        .build())
    programs.addLast(
      "prepare_join_reorder",
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(RuleSets.ofList(
          JoinToMultiJoinRule.INSTANCE,
          ProjectMultiJoinMergeRule.INSTANCE,
          FilterMultiJoinMergeRule.INSTANCE))
        .build())
    programs.addLast(
      "eliminate_cross_join",
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(RuleSets.ofList(
          EliminateCrossJoinRule.INSTANCE))
        .build())
    util.replaceBatchProgram(programs)

    util.addTableSource[(Int, Int)]("T1", 'a1, 'b1)
    util.addTableSource[(Int, Int, Int)]("T2", 'a2, 'b2, 'c2)
    util.addTableSource[(Int, Int, Int, Int)]("T3", 'a3, 'b3, 'c3, 'd3)
    util.addTableSource[(Int, Int, Int, Int, Int)]("T4", 'a4, 'b4, 'c4, 'd4, 'e4)
    util.addTableSource[(Int, Int, Int)]("T5", 'a5, 'b5, 'c5)
    util.addTableSource[(Int, Int, Int)]("T6", 'a6, 'b6, 'c6)
    util.addTableSource[(Int, Int, Int)]("T7", 'a7, 'b7, 'c7)
  }

  @Test
  def testInnerJoin(): Unit = {
    util.verifyPlan(
      """
        |SELECT * FROM T1, T2, T3, T4
        |  WHERE a2 = a4
        |  AND b1 = b3
        |  AND c2 = c3
        |""".stripMargin
    )
  }

  @Test
  def testOnlyOneInnerJoin(): Unit = {
    util.verifyPlan(
      """
        |SELECT * FROM T1
        |  INNER JOIN T2 ON a1 = a2
        |""".stripMargin
    )
  }

  @Test
  def testInnerJoinWithCompleteGraph(): Unit = {
    util.verifyPlan(
      """
        |SELECT * FROM T1, T2, T3, T4
        |  WHERE a2 = a4
        |  AND b1 = b3
        |  AND c2 = c3
        |  AND a1 = a4
        |  AND b2 = b1
        |  AND d4 = d3
        |""".stripMargin
    )
  }

  @Test
  def testStarSchema(): Unit = {
    util.verifyPlan(
      """
        |SELECT * FROM T1, T2, T3, T4
        |  WHERE a1 = a3
        |  AND b2 = b3
        |  AND c4 = c3
        |""".stripMargin
    )
  }

  @Test
  def testLeftOuterJoin(): Unit = {
    util.verifyPlan(
      """
        |SELECT * FROM T1
        |  INNER JOIN T2 ON a1 = a2
        |  LEFT JOIN T3 ON b1 = b3
        |  INNER JOIN T4 ON c2 = c4
        |  WHERE b2 IS NOT NULL OR a3 IS NOT NULL
        |""".stripMargin
    )
  }

  @Test
  def testOnlyOneLeftOuterJoin(): Unit = {
    util.verifyPlan(
      """
        |SELECT * FROM T1
        |  LEFT JOIN T2 ON a1 = a2
        |""".stripMargin
    )
  }

  @Test
  def testLeftOuterJoinWithSubQuery(): Unit = {
    util.verifyPlan(
      """
        |SELECT * FROM (
        |  SELECT b2, a3, c3 FROM T1, T2, T3
        |    WHERE a1 = a3
        |    AND b3 = b2
        |) T LEFT JOIN (
        |  SELECT a4, b5 FROM T4, T5, T6
        |    WHERE a4 = a6
        |    AND b6 = b5
        |) S ON a3 = a4 AND b2 = b5
        |INNER JOIN T7 ON c3 = c7
        |""".stripMargin
    )
  }

  @Test
  def testRightOuterJoin(): Unit = {
    util.verifyPlan(
      """
        |SELECT * FROM T1
        |  INNER JOIN T2 ON a1 = a2
        |  RIGHT JOIN T3 ON b1 = b3
        |  INNER JOIN T4 ON c2 = c4
        |  WHERE b2 IS NOT NULL OR a3 IS NOT NULL
        |""".stripMargin
    )
  }

  @Test
  def testOnlyOneRightOuterJoin(): Unit = {
    util.verifyPlan(
      """
        |SELECT * FROM T1
        |  RIGHT JOIN T2 ON a1 = a2
        |""".stripMargin
    )
  }

  @Test
  def testRightOuterJoinWithSubQuery(): Unit = {
    util.verifyPlan(
      """
        |SELECT * FROM (
        |  SELECT b2, a3, c3 FROM T1, T2, T3
        |    WHERE a1 = a3
        |    AND b3 = b2
        |) T RIGHT JOIN (
        |  SELECT a4, b5 FROM T4, T5, T6
        |    WHERE a4 = a6
        |    AND b6 = b5
        |) S ON a3 = a4 AND b2 = b5
        |INNER JOIN T7 ON c3 = c7
        |""".stripMargin
    )
  }

  @Test
  def testFullOuterJoin(): Unit = {
    util.verifyPlan(
      """
        |SELECT * FROM T1
        |  INNER JOIN T2 ON a1 = a2
        |  FULL JOIN T3 ON b1 = b3
        |  INNER JOIN T4 ON c2 = c4
        |  WHERE b2 IS NOT NULL OR a3 IS NOT NULL
        |""".stripMargin
    )
  }

  @Test
  def testOnlyOneFullOuterJoin(): Unit = {
    util.verifyPlan(
      """
        |SELECT * FROM T1
        |  FULL JOIN T2 ON a1 = a2
        |""".stripMargin
    )
  }

  @Test
  def testFullOuterJoinWithSubQuery(): Unit = {
    util.verifyPlan(
      """
        |SELECT * FROM (
        |  SELECT b2, a3, c3 FROM T1, T2, T3
        |    WHERE a1 = a3
        |    AND b3 = b2
        |) T FULL JOIN (
        |  SELECT a4, b5 FROM T4, T5, T6
        |    WHERE a4 = a6
        |    AND b6 = b5
        |) S ON a3 = a4 AND b2 = b5
        |INNER JOIN T7 ON c3 = c7
        |""".stripMargin
    )
  }

  @Test
  def testManyOuterJoins(): Unit = {
    util.verifyPlan(
      """
        |SELECT * FROM (
        |  SELECT a1, b4, c5 FROM T4, T5, T1
        |    INNER JOIN T2 ON a1 = a2
        |    LEFT JOIN T3 ON a1 = a3
        |    WHERE a1 = a4
        |    AND a1 = a5
        |) T
        |  RIGHT JOIN T6 ON a1 = a6
        |  FULL JOIN T7 ON a1 = a7
        |""".stripMargin
    )
  }

  @Test
  def testDisconnectedGraph(): Unit = {
    util.verifyPlan(
      """
        |SELECT * FROM T1, T2, T3, T4, T5, T6
        |  WHERE a1 = a4
        |  AND b2 = b5
        |  AND c3 = c6
        |""".stripMargin
    )
  }

  @Test
  def testNonEquiFilters(): Unit = {
    util.verifyPlan(
      """
        |SELECT * FROM T1, T2, T3, T4
        |  WHERE a1 = a3
        |  AND b2 = b3
        |  AND d4 = d3
        |  AND b1 < b3
        |  AND c2 > c3
        |""".stripMargin
    )
  }

  @Test
  def testFilterWithOuterJoin(): Unit = {
    util.verifyPlan(
      """
        |SELECT * FROM (
        |  SELECT a1, b1, a2 FROM T1
        |    LEFT JOIN T2 ON a1 = a2
        |  ) T
        |  INNER JOIN T3 ON a1 = a3
        |  WHERE a2 IS NOT NULL
        |  AND b1 > b3
        |""".stripMargin
    )
  }

  @Test
  def testLongJoinFilter(): Unit = {
    util.verifyPlan(
      """
        |SELECT * FROM T1, T2, T3, T4, T5, T6
        |  WHERE a1 = a4
        |  AND b5 = b4
        |  AND c6 = c4
        |  AND a3 = a6
        |  AND b2 = b6
        |  AND (b6 < b1 OR a1 > a5 OR b4 > b6)
        |  AND (c3 < c6 OR a1 > a6 OR d4 < d3)
        |""".stripMargin
    )
  }

  @Test
  def testConstantFilter(): Unit = {
    // this join ought to be pruned by other rules,
    // we create this test only for corner case checking
    util.verifyPlan(
      """
        |SELECT * FROM T1
        |  INNER JOIN T2 ON 5 < 3
        |""".stripMargin
    )
  }

  @Test
  def testOneInputFilter(): Unit = {
    util.verifyPlan(
      """
        |SELECT * FROM T1
        |  INNER JOIN T2 ON a1 = 1
        |""".stripMargin
    )
  }

  @Test
  def testTwoInputRefsOnSameSide(): Unit = {
    util.verifyPlan(
      """
        |SELECT * FROM T1, T2, T3
        |  WHERE a1 + a2 = 1
        |  AND b2 = b3
        |""".stripMargin
    )
  }
}
