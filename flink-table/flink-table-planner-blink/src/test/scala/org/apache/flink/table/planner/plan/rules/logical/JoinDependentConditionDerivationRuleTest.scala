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
import org.apache.calcite.rel.rules.FilterJoinRule
import org.apache.calcite.tools.RuleSets
import org.junit.{Before, Test}


/**
  * Test for [[JoinDependentConditionDerivationRule]].
  */
class JoinDependentConditionDerivationRuleTest extends TableTestBase {

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
        .add(RuleSets.ofList(
          FilterJoinRule.FILTER_ON_JOIN,
          FilterJoinRule.JOIN,
          JoinDependentConditionDerivationRule.INSTANCE))
        .build()
    )

    util.addTableSource[(Int, Long, String)]("MyTable1", 'a, 'b, 'c)
    util.addTableSource[(Int, Long, Int, String, Long)]("MyTable2", 'd, 'e, 'f, 'g, 'h)
  }

  @Test
  def testSimple(): Unit = {
    val sqlQuery =
      "SELECT a, d FROM MyTable1, MyTable2 WHERE (a = 1 AND d = 2) OR (a = 2 AND d = 1)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testAnd(): Unit = {
    val sqlQuery =
      "SELECT a, d FROM MyTable1, MyTable2 WHERE b = e AND ((a = 1 AND d = 2) OR (a = 2 AND d = 1))"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testCanNotMatchThisRule(): Unit = {
    val sqlQuery =
      "SELECT a, d FROM MyTable1, MyTable2 WHERE b = e OR ((a = 1 AND d = 2) OR (a = 2 AND d = 1))"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testThreeOr(): Unit = {
    val sqlQuery = "SELECT a, d FROM MyTable1, MyTable2 WHERE " +
      "(b = e AND a = 0) OR ((a = 1 AND d = 2) OR (a = 2 AND d = 1))"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testAndOr(): Unit = {
    val sqlQuery = "SELECT a, d FROM MyTable1, MyTable2 WHERE " +
      "((a = 1 AND d = 2) OR (a = 2 AND d = 1)) AND ((a = 3 AND d = 4) OR (a = 4 AND d = 3))"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiFields(): Unit = {
    val sqlQuery = "SELECT a, d FROM MyTable1, MyTable2 WHERE " +
      "(a = 1 AND b = 1 AND d = 2 AND e = 2) OR (a = 2 AND b = 2 AND d = 1 AND e = 1)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiSingleSideFields(): Unit = {
    val sqlQuery = "SELECT a, d FROM MyTable1, MyTable2 WHERE " +
      "(a = 1 AND b = 1 AND d = 2 AND e = 2) OR (d = 1 AND e = 1)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiJoins(): Unit = {
    val sqlQuery =
      """
        |SELECT T1.a, T2.d FROM MyTable1 T1,
        |  (SELECT * FROM MyTable1, MyTable2 WHERE a = d) T2 WHERE
        |(T1.a = 1 AND T1.b = 1 AND T2.a = 2 AND T2.e = 2)
        |OR
        |(T1.a = 2 AND T2.b = 2 AND T2.d = 1 AND T2.e = 1)
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

}
