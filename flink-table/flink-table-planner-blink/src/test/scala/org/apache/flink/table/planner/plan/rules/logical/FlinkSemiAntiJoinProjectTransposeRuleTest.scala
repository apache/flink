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
import org.apache.calcite.tools.RuleSets
import org.junit.{Before, Test}

/**
  * Test for [[FlinkSemiAntiJoinProjectTransposeRule]].
  */
class FlinkSemiAntiJoinProjectTransposeRuleTest extends TableTestBase {

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
          FlinkSemiAntiJoinProjectTransposeRule.INSTANCE,
          FlinkSemiAntiJoinFilterTransposeRule.INSTANCE,
          FlinkSemiAntiJoinJoinTransposeRule.INSTANCE))
        .build()
    )

    util.addTableSource[(Int, Long, String)]("x", 'a, 'b, 'c)
    util.addTableSource[(Int, Long, String)]("y", 'd, 'e, 'f)
    util.addTableSource[(Int, Long, String)]("z", 'i, 'j, 'k)
  }

  @Test
  def testTranspose(): Unit = {
    val sqlQuery =
      """
        |SELECT a, f FROM
        |    (SELECT a, b, d, e, f FROM x, y WHERE x.c = y.f) xy
        |WHERE xy.e > 100 AND xy.d IN (SELECT z.i FROM z WHERE z.j < 50)
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testCannotTranspose(): Unit = {
    val sqlQuery =
      """
        |SELECT a, f FROM
        |    (SELECT a * 2 as a, b, d + 1 as d, e, f FROM x, y WHERE x.c = y.f) xy
        |WHERE xy.e > 100 AND xy.d IN (SELECT z.i FROM z WHERE z.j < 50)
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

}

