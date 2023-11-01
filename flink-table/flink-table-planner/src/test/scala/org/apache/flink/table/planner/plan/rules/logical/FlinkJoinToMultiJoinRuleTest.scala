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
import org.apache.flink.table.api._
import org.apache.flink.table.planner.plan.optimize.program.{FlinkBatchProgram, FlinkHepRuleSetProgramBuilder, HEP_RULES_EXECUTION_TYPE}
import org.apache.flink.table.planner.utils.{TableConfigUtils, TableTestBase}

import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.rel.rules.CoreRules
import org.apache.calcite.tools.RuleSets
import org.junit.{Before, Test}

/** Tests for [[org.apache.flink.table.planner.plan.rules.logical.FlinkJoinToMultiJoinRule]]. */
class FlinkJoinToMultiJoinRuleTest extends TableTestBase {
  private val util = batchTestUtil()

  @Before
  def setup(): Unit = {
    util.buildBatchProgram(FlinkBatchProgram.DEFAULT_REWRITE)
    val calciteConfig = TableConfigUtils.getCalciteConfig(util.tableEnv.getConfig)
    calciteConfig.getBatchProgram.get.addLast(
      "rules",
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(RuleSets.ofList(FlinkJoinToMultiJoinRule.INSTANCE, CoreRules.PROJECT_MULTI_JOIN_MERGE))
        .build()
    )

    util.addTableSource[(Int, Long)]("T1", 'a, 'b)
    util.addTableSource[(Int, Long)]("T2", 'c, 'd)
    util.addTableSource[(Int, Long)]("T3", 'e, 'f)
  }

  @Test
  def testInnerJoinInnerJoin(): Unit = {
    // Can translate join to multi join.
    val sqlQuery = "SELECT * FROM T1, T2, T3 WHERE a = c AND a = e"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInnerJoinLeftOuterJoin(): Unit = {
    val sqlQuery = "SELECT * FROM T1 JOIN T2 ON a =c LEFT OUTER JOIN T3 ON a = e"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInnerJoinRightOuterJoin(): Unit = {
    // Cannot translate to one multi join set because right outer join left will generate null.
    val sqlQuery = "SELECT * FROM T1 JOIN T2 ON a =c RIGHT OUTER JOIN T3 ON a = e"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testLeftOuterJoinLeftOuterJoin(): Unit = {
    // Can translate join to multi join.
    val sqlQuery =
      "SELECT * FROM T1 LEFT OUTER JOIN T2 ON a = c LEFT OUTER JOIN T3 ON a = e"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testLeftOuterJoinRightOuterJoin(): Unit = {
    // Cannot translate join to multi join.
    val sqlQuery =
      "SELECT * FROM T1 LEFT OUTER JOIN T2 ON a = c RIGHT OUTER JOIN T3 ON a = e"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testLeftOuterJoinInnerJoin(): Unit = {
    val sqlQuery =
      "SELECT * FROM T1 LEFT OUTER JOIN T2 ON a = c JOIN T3 ON a = e"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testRightOuterJoinRightOuterJoin(): Unit = {
    val sqlQuery =
      "SELECT * FROM T1 RIGHT OUTER JOIN T2 ON a = c RIGHT OUTER JOIN T3 ON a = e"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testSubRightOuterJoinQueryWithKeyInLeft(): Unit = {
    // This case will not be set into one multi join set because T1.a is a
    // null generate column after T1 right outer join T2.
    val sqlQuery =
      "SELECT * FROM T3 RIGHT OUTER JOIN (SELECT * FROM T1 RIGHT OUTER JOIN T2 ON a = c) t ON t.a = T3.e"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testSubRightOuterJoinQueryWithKeyInRight(): Unit = {
    // This case can be set into one multi join set because T2.c is not a
    // null generate column after T1 right outer join T2.
    val sqlQuery =
      "SELECT * FROM T3 RIGHT OUTER JOIN (SELECT * FROM T1 RIGHT OUTER JOIN T2 ON a = c) t ON t.c = T3.e"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testRightOuterJoinLeftOuterJoinWithKeyInLeft(): Unit = {
    val sqlQuery =
      "SELECT * FROM T1 RIGHT OUTER JOIN T2 ON a = c LEFT OUTER JOIN T3 ON a = e"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testRightOuterJoinLeftOuterJoinWithKeyInRight(): Unit = {
    val sqlQuery =
      "SELECT * FROM T1 RIGHT OUTER JOIN T2 ON a = c LEFT OUTER JOIN T3 ON c = e"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testRightOuterJoinInnerJoinWithKeyInLeft(): Unit = {
    val sqlQuery =
      "SELECT * FROM T1 RIGHT OUTER JOIN T2 ON a = c JOIN T3 ON a = e"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testRightOuterJoinInnerJoinWithKeyInRight(): Unit = {
    val sqlQuery =
      "SELECT * FROM T1 RIGHT OUTER JOIN T2 ON a = c JOIN T3 ON c = e"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testFullOuterJoin(): Unit = {
    // Cannot translate join to multi join.
    val sqlQuery = "SELECT * FROM T1 FULL OUTER JOIN T2 ON a = c, T3 WHERE a = e"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testFullOuterJoinInnerJoin(): Unit = {
    val sqlQuery =
      "SELECT * FROM T1 FULL OUTER JOIN T2 ON a = c JOIN T3 ON a = e"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testFullOuterJoinLeftOuterJoin(): Unit = {
    val sqlQuery =
      "SELECT * FROM T1 FULL OUTER JOIN T2 ON a = c LEFT OUTER JOIN T3 ON a = e"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testFullOuterJoinRightOuterJoin(): Unit = {
    val sqlQuery =
      "SELECT * FROM T1 FULL OUTER JOIN T2 ON a = c RIGHT OUTER JOIN T3 ON a = e"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testFullOuterJoinSemiJoin(): Unit = {
    val sqlQuery =
      "SELECT * FROM T1 FULL OUTER JOIN T2 ON a = c WHERE a IN (SELECT e FROM T3)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInnerJoinSemiJoin(): Unit = {
    val sqlQuery =
      "SELECT * FROM (SELECT * FROM T1 JOIN T2 ON a = c) t WHERE a IN (SELECT e FROM T3)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testLeftOuterJoinSemiJoin(): Unit = {
    val sqlQuery =
      "SELECT * FROM T1 LEFT OUTER JOIN T2 ON a = c WHERE a IN (SELECT e FROM T3)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testRightOuterJoinSemiJoin(): Unit = {
    val sqlQuery =
      "SELECT * FROM T1 RIGHT OUTER JOIN T2 ON a = c WHERE a IN (SELECT e FROM T3)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInnerJoinAntiJoin(): Unit = {
    val sqlQuery =
      """
        |SELECT * FROM (SELECT * FROM T1 JOIN T2 ON a = c) t
        |WHERE NOT EXISTS (SELECT e FROM T3  WHERE a = e)
      """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testLeftOuterJoinAntiJoin(): Unit = {
    val sqlQuery =
      """
        |SELECT * FROM (SELECT * FROM T1 LEFT OUTER JOIN T2 ON a = c) t
        |WHERE NOT EXISTS (SELECT e FROM T3  WHERE a = e)
      """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testRightOuterJoinAntiJoin(): Unit = {
    val sqlQuery =
      """
        |SELECT * FROM (SELECT * FROM T1 RIGHT OUTER JOIN T2 ON a = c) t
        |WHERE NOT EXISTS (SELECT e FROM T3  WHERE a = e)
      """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInnerJoinLeftOuterJoinInnerJoinLeftOuterJoin(): Unit = {
    util.addTableSource[(Int, Long)]("T4", 'g, 'h)
    util.addTableSource[(Int, Long)]("T5", 'i, 'j)

    val sqlQuery =
      """
        |SELECT * FROM T1 JOIN T2 ON a = c LEFT OUTER JOIN 
        | T3 ON a = e JOIN
        | T4 ON a = g LEFT OUTER JOIN
        | T5 ON a = i
        """.stripMargin
    util.verifyRelPlan(sqlQuery)

  }

  @Test
  def testLeftOuterJoinInnerJoinLeftOuterJoinInnerJoin(): Unit = {
    util.addTableSource[(Int, Long)]("T4", 'g, 'h)
    util.addTableSource[(Int, Long)]("T5", 'i, 'j)

    val sqlQuery =
      """
        |SELECT * FROM T1 LEFT OUTER JOIN T2 ON a = c JOIN 
        | T3 ON a = e LEFT OUTER JOIN
        | T4 ON a = g JOIN
        | T5 ON a = i
        """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInnerJoinRightOuterJoinInnerJoinRightOuterJoin(): Unit = {
    util.addTableSource[(Int, Long)]("T4", 'g, 'h)
    util.addTableSource[(Int, Long)]("T5", 'i, 'j)

    val sqlQuery =
      """
        |SELECT * FROM T1 JOIN T2 ON a = c RIGHT OUTER JOIN 
        | T3 ON a = e JOIN
        | T4 ON a = g RIGHT OUTER JOIN
        | T5 ON a = i
        """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testRightOuterJoinInnerJoinRightOuterJoinInnerJoin(): Unit = {
    util.addTableSource[(Int, Long)]("T4", 'g, 'h)
    util.addTableSource[(Int, Long)]("T5", 'i, 'j)

    val sqlQuery =
      """
        |SELECT * FROM T1 RIGHT OUTER JOIN T2 ON a = c JOIN
        | T3 ON a = e RIGHT OUTER JOIN
        | T4 ON a = g JOIN
        | T5 ON a = i
        """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testMultiLeftOuterJoinWithAllKeyInLeft: Unit = {
    util.addTableSource[(Int, Long)]("T4", 'g, 'h)
    util.addTableSource[(Int, Long)]("T5", 'i, 'j)

    val sqlQuery =
      """
        |SELECT * FROM T1 LEFT OUTER JOIN 
        |T2 ON a = c LEFT OUTER JOIN 
        |T3 ON a = e LEFT OUTER JOIN
        |T4 ON a = g LEFT OUTER JOIN
        |T5 ON a = i
        """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }
}
