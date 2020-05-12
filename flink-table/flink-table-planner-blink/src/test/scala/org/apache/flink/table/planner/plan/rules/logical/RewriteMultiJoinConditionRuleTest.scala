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
import org.apache.flink.table.planner.plan.optimize.program.{BatchOptimizeContext, FlinkChainedProgram, FlinkGroupProgramBuilder, FlinkHepRuleSetProgramBuilder, HEP_RULES_EXECUTION_TYPE}
import org.apache.flink.table.planner.utils.TableTestBase

import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.rel.rules.{FilterJoinRule, FilterMultiJoinMergeRule, JoinToMultiJoinRule, ProjectMultiJoinMergeRule}
import org.apache.calcite.tools.RuleSets
import org.junit.{Before, Test}

/**
  * Test for [[RewriteMultiJoinConditionRule]].
  */
class RewriteMultiJoinConditionRuleTest extends TableTestBase {
  private val util = batchTestUtil()

  @Before
  def setup(): Unit = {
    val program = new FlinkChainedProgram[BatchOptimizeContext]()
    program.addLast(
      "rules",
      FlinkGroupProgramBuilder.newBuilder[BatchOptimizeContext]
        .addProgram(FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(RuleSets.ofList(
            FilterJoinRule.FILTER_ON_JOIN,
            FilterJoinRule.JOIN))
          .build(), "push filter into join")
        .addProgram(FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(RuleSets.ofList(
            ProjectMultiJoinMergeRule.INSTANCE,
            FilterMultiJoinMergeRule.INSTANCE,
            JoinToMultiJoinRule.INSTANCE))
          .build(), "merge join to MultiJoin")
        .addProgram(FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(RuleSets.ofList(RewriteMultiJoinConditionRule.INSTANCE))
          .build(), "RewriteMultiJoinConditionRule")
        .build())

    util.replaceBatchProgram(program)

    util.addTableSource[(Int, Long)]("A", 'a1, 'a2)
    util.addTableSource[(Int, Long)]("B", 'b1, 'b2)
    util.addTableSource[(Int, Long)]("C", 'c1, 'c2)
    util.addTableSource[(Int, Long)]("D", 'd1, 'd2)
  }

  @Test
  def testMultiJoin_InnerJoin1(): Unit = {
    val sqlQuery = "SELECT * FROM A, B WHERE a1 = b1"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiJoin_InnerJoin2(): Unit = {
    val sqlQuery = "SELECT * FROM A, B, C WHERE a1 = b1 AND a1 = c1"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiJoin_InnerJoin3(): Unit = {
    val sqlQuery = "SELECT * FROM A, B, C, D WHERE a1 = b1 AND b1 = c1 AND c1 = d1"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiJoin_InnerJoin4(): Unit = {
    // non-equi join condition
    val sqlQuery = "SELECT * FROM A, B, C WHERE a1 = b1 AND a1 > c1"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiJoin_InnerJoin5(): Unit = {
    val sqlQuery = "SELECT * FROM A, B, C WHERE a1 + 1 = b1 AND a1 + 1 = c1"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiJoin_LeftJoin1(): Unit = {
    val sqlQuery = "SELECT * FROM A LEFT JOIN B ON a1 = b1"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiJoin_LeftJoin2(): Unit = {
    val sqlQuery = "SELECT * FROM A JOIN B ON a1 = b1 LEFT JOIN C ON b1 = c1"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiJoin_LeftJoin3(): Unit = {
    val sqlQuery = "SELECT * FROM A LEFT JOIN B ON a1 = b1 JOIN C ON a1 = c1"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiJoin_RightJoin1(): Unit = {
    val sqlQuery = "SELECT * FROM A RIGHT JOIN B ON a1 = b1"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiJoin_RightJoin2(): Unit = {
    val sqlQuery = "SELECT * FROM A JOIN B ON a1 = b1 RIGHT JOIN C ON b1 = c1"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiJoin_RightJoin3(): Unit = {
    val sqlQuery = "SELECT * FROM A RIGHT JOIN B ON a1 = b1 JOIN C ON a1 = c1"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiJoin_FullJoin1(): Unit = {
    val sqlQuery = "SELECT * FROM A FULL OUTER JOIN B ON a1 = b1"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiJoin_FullJoin2(): Unit = {
    val sqlQuery = "SELECT * FROM A FULL OUTER JOIN B ON a1 = b1 FULL OUTER JOIN C ON a1 = c1"
    util.verifyPlan(sqlQuery)
  }

}
