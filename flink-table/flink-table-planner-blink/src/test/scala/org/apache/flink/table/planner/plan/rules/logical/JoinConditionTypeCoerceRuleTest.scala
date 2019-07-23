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
  * Test for [[JoinConditionTypeCoerceRule]].
  * Now only semi-join rewrite will lost the type consistency, so we only cover this kind of
  * cases.
  */
class JoinConditionTypeCoerceRuleTest extends TableTestBase {
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
        .add(RuleSets.ofList(JoinConditionTypeCoerceRule.INSTANCE))
        .build()
    )

    util.addTableSource[(Int, Long, Float, Double, java.math.BigDecimal)]("T1", 'a, 'b, 'c, 'd, 'e)
    util.addTableSource[(Int, Long, Float, Double, java.math.BigDecimal)]("T2", 'a, 'b, 'c, 'd, 'e)
  }

  @Test
  def testInToSemiJoinIntEqualsLong(): Unit = {
    util.verifyPlanWithType("SELECT * FROM T1 WHERE T1.a IN (SELECT b FROM T2)")
  }

  @Test
  def testInToSemiJoinIntEqualsFloat(): Unit = {
    util.verifyPlanWithType("SELECT * FROM T1 WHERE T1.a IN (SELECT c FROM T2)")
  }

  @Test
  def testInToSemiJoinIntEqualsDouble(): Unit = {
    util.verifyPlanWithType("SELECT * FROM T1 WHERE T1.a IN (SELECT d FROM T2)")
  }

  @Test
  def testInToSemiJoinIntEqualsDecimal(): Unit = {
    util.verifyPlanWithType("SELECT * FROM T1 WHERE T1.a IN (SELECT e FROM T2)")
  }

  @Test
  def testInToSemiJoinFloatEqualsDouble(): Unit = {
    util.verifyPlanWithType("SELECT * FROM T1 WHERE T1.c IN (SELECT d FROM T2)")
  }

  @Test
  def testInToSemiJoinFloatEqualsDecimal(): Unit = {
    util.verifyPlanWithType("SELECT * FROM T1 WHERE T1.c IN (SELECT e FROM T2)")
  }

  @Test
  def testInToSemiJoinDoubleEqualsDecimal(): Unit = {
    util.verifyPlanWithType("SELECT * FROM T1 WHERE T1.d IN (SELECT e FROM T2)")
  }

  // Test nullability mismatch.
  @Test
  def testJoinConditionEqualsTypesNotEquals01(): Unit = {
    val sqlQuery = "SELECT a FROM T1 LEFT JOIN (SELECT COUNT(*) AS cnt FROM T2) AS x ON a = x.cnt"
    util.verifyPlanWithType(sqlQuery)
  }

  // Join with or predicate is not rewrite by subquery rule but decorrelate.
  @Test
  def testJoinConditionEqualsTypesNotEquals02(): Unit = {
    util.addTableSource[(String, Short, Int)]("T3", 't3a, 't3b, 't3c)
    util.addTableSource[(String, Short, Int)]("T4", 't4a, 't4b, 't4c)

    val sqlQuery =
      """
        |-- TC 01.04
        |SELECT t3a,
        |       t3b
        |FROM   T3
        |WHERE  t3c IN (SELECT t4b
        |               FROM   T4
        |               WHERE  t3a = t4a
        |                       OR t3b > t4b)
      """.stripMargin
    util.verifyPlanWithType(sqlQuery)
  }
}
