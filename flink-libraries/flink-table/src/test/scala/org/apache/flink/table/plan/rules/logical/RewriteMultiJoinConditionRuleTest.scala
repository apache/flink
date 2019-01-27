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
package org.apache.flink.table.plan.rules.logical

import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.api.scala._
import org.apache.flink.table.calcite.CalciteConfig
import org.apache.flink.table.plan.optimize._
import org.apache.flink.table.plan.rules.FlinkBatchExecRuleSets
import org.apache.flink.table.util.TableTestBase

import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.rel.rules.{FilterMultiJoinMergeRule, ProjectMultiJoinMergeRule, _}
import org.apache.calcite.tools.RuleSets
import org.junit.{Before, Test}

class RewriteMultiJoinConditionRuleTest extends TableTestBase {
  private val util = batchTestUtil()

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

  @Before
  def setup(): Unit = {
    def buildPrograms(): FlinkChainedPrograms[BatchOptimizeContext] = {
      val programs = new FlinkChainedPrograms[BatchOptimizeContext]()

      programs.addLast(
        "semi_join",
        FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(FlinkBatchExecRuleSets.SEMI_JOIN_RULES)
          .build()
      )

      // convert sub-queries before query decorrelation
      programs.addLast(
        "subquery",
        FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(FlinkBatchExecRuleSets.TABLE_SUBQUERY_RULES)
          .build())

      // convert table references
      programs.addLast(
        "table_ref",
        FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(FlinkBatchExecRuleSets.TABLE_REF_RULES)
          .build())

      // decorrelate
      programs.addLast("decorrelate", new FlinkDecorrelateProgram)

      // normalize the logical plan
      programs.addLast(
        "normalization",
        FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(FlinkBatchExecRuleSets.BATCH_EXEC_DEFAULT_REWRITE_RULES)
          .build())

      // join reorder
      programs.addLast(
        "join_reorder",
        FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(RuleSets.ofList(
            FilterJoinRule.FILTER_ON_JOIN,
            FilterAggregateTransposeRule.INSTANCE,
            ProjectFilterTransposeRule.INSTANCE,
            FilterProjectTransposeRule.INSTANCE,
            JoinToMultiJoinRule.INSTANCE,
            ProjectMultiJoinMergeRule.INSTANCE,
            FilterMultiJoinMergeRule.INSTANCE,
            RewriteMultiJoinConditionRule.INSTANCE
          ))
          .build())
      programs
    }
    val builder = CalciteConfig.createBuilder(util.tableEnv.getConfig.getCalciteConfig)
      .replaceBatchPrograms(buildPrograms())
    util.tableEnv.config.setCalciteConfig(builder.build())
    util.getTableEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_JOIN_REORDER_ENABLED, true)

    util.addTable[(Int, Long)]("A", 'a1, 'a2)
    util.addTable[(Int, Long)]("B", 'b1, 'b2)
    util.addTable[(Int, Long)]("C", 'c1, 'c2)
    util.addTable[(Int, Long)]("D", 'd1, 'd2)
  }

}
