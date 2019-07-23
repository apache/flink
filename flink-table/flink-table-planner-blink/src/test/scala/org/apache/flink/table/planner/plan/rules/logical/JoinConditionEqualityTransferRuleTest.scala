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
import org.apache.flink.table.planner.plan.optimize.program.{FlinkBatchProgram, FlinkHepRuleSetProgramBuilder, HEP_RULES_EXECUTION_TYPE}
import org.apache.flink.table.planner.utils.{TableConfigUtils, TableTestBase}

import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.tools.RuleSets
import org.junit.{Before, Test}


/**
  * Test for [[JoinConditionEqualityTransferRule]].
  */
class JoinConditionEqualityTransferRuleTest extends TableTestBase {

  private val util = batchTestUtil()

  @Before
  def setup(): Unit = {
    util.buildBatchProgram(FlinkBatchProgram.DEFAULT_REWRITE)
    val calciteConfig = TableConfigUtils.getCalciteConfig(util.tableEnv.getConfig)
    calciteConfig.getBatchProgram.get.addLast(
      "rules",
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(RuleSets.ofList(JoinConditionEqualityTransferRule.INSTANCE))
        .build()
    )

    util.addTableSource[(Int, Int, Int)]("MyTable1", 'a, 'b, 'c)
    util.addTableSource[(Int, Int, Int)]("MyTable2", 'd, 'e, 'f)
  }

  @Test
  def testInnerJoin1(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable1 JOIN MyTable2 ON a = d AND a = e")
  }

  @Test
  def testInnerJoin2(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable1 JOIN MyTable2 ON a = d AND a = e AND b = d")
  }

  @Test
  def testInnerJoin3(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable1 JOIN MyTable2 ON a = d AND a = e AND a = c")
  }

  @Test
  def testInnerJoin4(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable1 JOIN MyTable2 ON a = d AND a = e AND b + 1 = d")
  }

  @Test
  def testInnerJoinWithNonEquiCondition1(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable1 JOIN MyTable2 ON a = d AND a > e")
  }

  @Test
  def testInnerJoinWithNonEquiCondition2(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable1 JOIN MyTable2 ON a = d AND a = e AND b > d")
  }

  @Test
  def testSemiJoin_In1(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable1 WHERE a IN (SELECT d FROM MyTable2 WHERE a = e)")
  }

  @Test
  def testSemiJoin_In2(): Unit = {
    val sqlQuery =
      "SELECT * FROM MyTable1 WHERE a IN (SELECT d FROM MyTable2 WHERE a = e AND b = d)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSemiJoinWithNonEquiCondition_In1(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable1 WHERE a IN (SELECT d FROM MyTable2 WHERE a > e)")
  }

  @Test
  def testSemiJoinWithNonEquiCondition_In2(): Unit = {
    val sqlQuery =
      "SELECT * FROM MyTable1 WHERE a IN (SELECT d FROM MyTable2 WHERE a > e AND b = d)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSemiJoin_Exist1(): Unit = {
    val sqlQuery =
      """
        |SELECT * FROM MyTable1 WHERE EXISTS (SELECT * FROM MyTable2 WHERE a = d AND a = e)
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSemiJoin_Exist2(): Unit = {
    val sqlQuery =
      """
        |SELECT * FROM MyTable1 WHERE EXISTS
        |    (SELECT * FROM MyTable2 WHERE a = d AND a = e AND b = d)
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSemiJoinWithNonEquiCondition_Exist1(): Unit = {
    val sqlQuery =
      """
        |SELECT * FROM MyTable1 WHERE EXISTS (SELECT * FROM MyTable2 WHERE a = d AND a > e)
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSemiJoinWithNonEquiCondition_Exist2(): Unit = {
    val sqlQuery =
      """
        |SELECT * FROM MyTable1 WHERE EXISTS
        |    (SELECT * FROM MyTable2 WHERE a = d AND a = e AND b > d)
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }
}
