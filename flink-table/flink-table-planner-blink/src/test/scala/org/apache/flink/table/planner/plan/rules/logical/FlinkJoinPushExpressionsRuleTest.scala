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
import org.apache.flink.table.planner.plan.optimize.program.{BatchOptimizeContext, FlinkChainedProgram, FlinkHepRuleSetProgramBuilder, HEP_RULES_EXECUTION_TYPE}
import org.apache.flink.table.planner.utils.TableTestBase

import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.tools.RuleSets
import org.junit.{Before, Test}

/**
  * Tests for [[FlinkJoinPushExpressionsRule]].
  */
class FlinkJoinPushExpressionsRuleTest extends TableTestBase {

  private val util = batchTestUtil()

  @Before
  def setup(): Unit = {
    val programs = new FlinkChainedProgram[BatchOptimizeContext]()
    programs.addLast(
      "rules",
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(RuleSets.ofList(
          SimplifyFilterConditionRule.EXTENDED,
          FlinkRewriteSubQueryRule.FILTER,
          FlinkSubQueryRemoveRule.FILTER,
          JoinConditionTypeCoerceRule.INSTANCE,
          FlinkJoinPushExpressionsRule.INSTANCE))
        .build()
    )
    util.replaceBatchProgram(programs)

    util.addTableSource[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTableSource[(Int, Long, String)]("r", 'd, 'e, 'f)
  }

  @Test
  def testPushExpressionsOnSemiJoin_In(): Unit = {
    util.verifyRelPlan("SELECT * FROM l WHERE a IN (SELECT d FROM r WHERE b + 1 = e)")
  }

  @Test
  def testPushExpressionsOnSemiJoin_Exists(): Unit = {
    util.verifyRelPlan(
      "SELECT * FROM l WHERE EXISTS (SELECT d FROM r WHERE CAST(b AS INTEGER) = d)")
  }

  @Test
  def testPushExpressionsOnSemiJoin_NotIn(): Unit = {
    util.verifyRelPlan("SELECT * FROM l WHERE a NOT IN (SELECT d FROM r WHERE b + 1 = e)")
  }

  @Test
  def testPushExpressionsOnSemiJoin_NotExists(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE NOT EXISTS (SELECT d FROM r WHERE CAST(b AS INTEGER) = d)"
    util.verifyRelPlan(sqlQuery)
  }

}
