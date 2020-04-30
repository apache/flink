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
  * Tests for [[SimplifyJoinConditionRule]].
  */
class SimplifyJoinConditionRuleTest extends TableTestBase {

  private val util = batchTestUtil()

  @Before
  def setup(): Unit = {
    util.buildBatchProgram(FlinkBatchProgram.DEFAULT_REWRITE)
    val calciteConfig = TableConfigUtils.getCalciteConfig(util.tableEnv.getConfig)
    calciteConfig.getBatchProgram.get.addLast(
      "SimplifyJoinConditionRule",
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(RuleSets.ofList(SimplifyJoinConditionRule.INSTANCE))
        .build()
    )

    util.addTableSource[(Int, Long, String)]("MyTable1", 'a, 'b, 'c)
    util.addTableSource[(Int, Long, String)]("MyTable2", 'd, 'e, 'f)
  }

  @Test
  def testSimplifyJoinCondition(): Unit = {
    val sqlQuery = "SELECT d FROM MyTable1 JOIN MyTable2 ON (d = a AND a > 2) OR (d = a AND b = 1)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSimplifyJoinConditionFromSubQuery(): Unit = {
    val sqlQuery =
      """
        |SELECT a FROM MyTable1 WHERE b = (
        |    SELECT COUNT(*) FROM MyTable2 WHERE (d = a AND d < 2) OR (d = a AND b = 5))
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }
}
