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
import org.apache.flink.table.planner.plan.optimize.program.{BatchOptimizeContext, FlinkChainedProgram, FlinkHepRuleSetProgramBuilder, HEP_RULES_EXECUTION_TYPE}
import org.apache.flink.table.planner.utils.TableTestBase

import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.rel.rules.{CoreRules, PruneEmptyRules}
import org.apache.calcite.tools.RuleSets
import org.junit.jupiter.api.{BeforeEach, Test}

/**
 * Former test for [[FlinkPruneEmptyRules]] which now replaced by Calcite's
 * [[PruneEmptyRules.JOIN_RIGHT_INSTANCE]].
 */
class FlinkPruneEmptyRulesTest extends TableTestBase {

  private val util = batchTestUtil()

  @BeforeEach
  def setup(): Unit = {
    val programs = new FlinkChainedProgram[BatchOptimizeContext]()
    programs.addLast(
      "rules",
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(RuleSets.ofList(
          FlinkSubQueryRemoveRule.FILTER,
          CoreRules.FILTER_REDUCE_EXPRESSIONS,
          CoreRules.PROJECT_REDUCE_EXPRESSIONS,
          CoreRules.FILTER_SET_OP_TRANSPOSE,
          CoreRules.FILTER_PROJECT_TRANSPOSE,
          CoreRules.PROJECT_MERGE,
          CoreRules.PROJECT_FILTER_VALUES_MERGE,
          FlinkPruneEmptyRules.UNION_INSTANCE,
          PruneEmptyRules.INTERSECT_INSTANCE,
          FlinkPruneEmptyRules.MINUS_INSTANCE,
          PruneEmptyRules.PROJECT_INSTANCE,
          PruneEmptyRules.FILTER_INSTANCE,
          PruneEmptyRules.SORT_INSTANCE,
          PruneEmptyRules.AGGREGATE_INSTANCE,
          PruneEmptyRules.JOIN_LEFT_INSTANCE,
          PruneEmptyRules.JOIN_RIGHT_INSTANCE
        ))
        .build()
    )
    util.replaceBatchProgram(programs)

    util.addTableSource[(Int, Long, String)]("T1", 'a, 'b, 'c)
    util.addTableSource[(Int, Long, String)]("T2", 'd, 'e, 'f)
  }

  @Test
  def testSemiJoinRightIsEmpty(): Unit = {
    util.verifyRelPlan("SELECT * FROM T1 WHERE a IN (SELECT d FROM T2 WHERE 1=0)")
  }

  @Test
  def testAntiJoinRightIsEmpty(): Unit = {
    util.verifyRelPlan("SELECT * FROM T1 WHERE a NOT IN (SELECT d FROM T2 WHERE 1=0)")
  }

  @Test
  def testEmptyFilterProjectUnion(): Unit = {
    val sqlQuery =
      s"""
         |SELECT * FROM (
         |SELECT * FROM (VALUES (10, 1), (30, 3)) AS T (x, y)
         |UNION ALL
         |SELECT * FROM (VALUES (20, 2))
         |)
         |WHERE x + y > 30
       """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testEmptyFilterProjectUnion2(): Unit = {
    val sqlQuery =
      s"""
         |SELECT * FROM (
         |SELECT * FROM (VALUES (10, 1), (30, 3), (30, 3)) AS T (X, Y)
         |UNION
         |SELECT * FROM (VALUES (20, 2))
         |)
         |WHERE X + Y > 30
       """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testEmptyIntersect(): Unit = {
    val sqlQuery =
      s"""
         |SELECT * FROM (VALUES (30, 3))
         |INTERSECT
         |SELECT * FROM (VALUES (10, 1), (30, 3)) AS T (x, y) WHERE x > 50
         |INTERSECT
         |SELECT * FROM (VALUES (30, 3))
       """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testEmptyMinus(): Unit = {
    val sqlQuery =
      s"""
         |SELECT * FROM (VALUES (30, 3)) AS T (x, y)
         |WHERE x > 30
         |EXCEPT
         |SELECT * FROM (VALUES (20, 2))
         |EXCEPT
         |SELECT * FROM (VALUES (40, 4))
       """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testEmptyMinus2(): Unit = {
    val sqlQuery =
      s"""
         |SELECT * FROM (VALUES (30, 3)) AS T (x, y)
         |EXCEPT
         |SELECT * FROM (VALUES (20, 2)) AS T (x, y) WHERE x > 30
         |EXCEPT
         |SELECT * FROM (VALUES (40, 4))
         |EXCEPT
         |SELECT * FROM (VALUES (50, 5)) AS T (x, y) WHERE x > 50
       """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testEmptyMinus3(): Unit = {
    val sqlQuery =
      s"""
         |SELECT * FROM (VALUES (30, 3), (30, 3)) AS T (X, Y)
         |EXCEPT
         |SELECT * FROM (VALUES (20, 2)) AS T (X, Y) WHERE X > 30
       """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }
}
