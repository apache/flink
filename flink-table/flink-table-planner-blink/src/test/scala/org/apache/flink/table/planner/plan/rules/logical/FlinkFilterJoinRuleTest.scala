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

/**
  * Tests for [[org.apache.calcite.rel.rules.FilterJoinRule]].
  */
class FlinkFilterJoinRuleTest extends TableTestBase {
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
        .add(RuleSets.ofList(
          CoreRules.FILTER_PROJECT_TRANSPOSE,
          CoreRules.FILTER_INTO_JOIN,
          CoreRules.JOIN_CONDITION_PUSH))
        .build()
    )

    util.addTableSource[(Int, Long)]("leftT", 'a, 'b)
    util.addTableSource[(Int, Long)]("rightT", 'c, 'd)
  }

  @Test
  def testFilterPushDownLeftSemi1(): Unit = {
    val sqlQuery =
      "SELECT * FROM (SELECT * FROM leftT WHERE a IN (SELECT c FROM rightT)) T WHERE T.b > 2"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testFilterPushDownLeftSemi2(): Unit = {
    val sqlQuery =
      "SELECT * FROM (SELECT * FROM leftT WHERE EXISTS (SELECT * FROM rightT)) T WHERE T.b > 2"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testFilterPushDownLeftSemi3(): Unit = {
    val sqlQuery =
      """
        |SELECT * FROM (SELECT * FROM leftT WHERE EXISTS
        |    (SELECT * FROM rightT WHERE a = c)) T WHERE T.b > 2
      """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testJoinConditionPushDownLeftSemi1(): Unit = {
    util.verifyRelPlan("SELECT * FROM leftT WHERE a IN (SELECT c FROM rightT WHERE b > 2)")
  }

  @Test
  def testJoinConditionPushDownLeftSemi2(): Unit = {
    util.verifyRelPlan("SELECT * FROM leftT WHERE EXISTS (SELECT * FROM rightT WHERE b > 2)")
  }

  @Test
  def testJoinConditionPushDownLeftSemi3(): Unit = {
    util.verifyRelPlan(
      "SELECT * FROM leftT WHERE EXISTS (SELECT * FROM rightT WHERE a = c AND b > 2)")
  }

  @Test
  def testFilterPushDownLeftAnti1(): Unit = {
    val sqlQuery =
      """
        |SELECT * FROM (SELECT * FROM leftT WHERE a NOT IN
        |    (SELECT c FROM rightT WHERE c < 3)) T WHERE T.b > 2
      """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testFilterPushDownLeftAnti2(): Unit = {
    val sqlQuery =
      """
        |SELECT * FROM (SELECT * FROM leftT WHERE NOT EXISTS
        |    (SELECT * FROM rightT where c > 10)) T WHERE T.b > 2
      """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testFilterPushDownLeftAnti3(): Unit = {
    val sqlQuery =
      """
        |SELECT * FROM (SELECT * FROM leftT WHERE a NOT IN
        |(SELECT c FROM rightT WHERE b = d AND c < 3)) T WHERE T.b > 2
      """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testFilterPushDownLeftAnti4(): Unit = {
    val sqlQuery =
      """
        |SELECT * FROM (SELECT * FROM leftT WHERE NOT EXISTS
        |    (SELECT * FROM rightT WHERE a = c)) T WHERE T.b > 2
      """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testJoinConditionPushDownLeftAnti1(): Unit = {
    util.verifyRelPlan("SELECT * FROM leftT WHERE a NOT IN (SELECT c FROM rightT WHERE b > 2)")
  }

  @Test
  def testJoinConditionPushDownLeftAnti2(): Unit = {
    util.verifyRelPlan("SELECT * FROM leftT WHERE NOT EXISTS (SELECT * FROM rightT WHERE b > 2)")
  }

  @Test
  def testJoinConditionPushDownLeftAnti3(): Unit = {
    val sqlQuery = "SELECT * FROM leftT WHERE a NOT IN (SELECT c FROM rightT WHERE b = d AND b > 1)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testJoinConditionPushDownLeftAnti4(): Unit = {
    val sqlQuery =
      "SELECT * FROM leftT WHERE NOT EXISTS (SELECT * FROM rightT WHERE a = c AND b > 2)"
    util.verifyRelPlan(sqlQuery)
  }

}
