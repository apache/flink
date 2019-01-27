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
import org.apache.flink.table.api.scala._
import org.apache.flink.table.calcite.CalciteConfig
import org.apache.flink.table.plan.optimize.FlinkBatchPrograms.{DECORRELATE, DEFAULT_REWRITE, SUBQUERY_REWRITE}
import org.apache.flink.table.plan.optimize._
import org.apache.flink.table.plan.rules.FlinkBatchExecRuleSets
import org.apache.flink.table.util.TableTestBase

import org.apache.calcite.plan.hep.HepMatchOrder
import org.junit.{Before, Test}

/**
  * Now only semi-join rewrite will lost the type consistency, so we only cover this kind of
  * cases.
  */
class JoinConditionTypeCoerceRuleTest extends TableTestBase {
  private val util = batchTestUtil()

  @Before
  def setup(): Unit = {
    val programs = new FlinkChainedPrograms[BatchOptimizeContext]()
    // convert queries before query decorrelation
    programs.addLast(
      SUBQUERY_REWRITE,
      FlinkGroupProgramBuilder.newBuilder[BatchOptimizeContext]
        .addProgram(FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(FlinkBatchExecRuleSets.SEMI_JOIN_RULES)
          .build(), "rewrite sub-queries to semi-join")
        .addProgram(FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(FlinkBatchExecRuleSets.TABLE_SUBQUERY_RULES)
          .build(), "sub-queries remove")
        .build()
    )

    // decorrelate
    programs.addLast(
      DECORRELATE,
      FlinkGroupProgramBuilder.newBuilder[BatchOptimizeContext]
        .addProgram(new FlinkDecorrelateProgram, "decorrelate")
        .addProgram(new FlinkCorrelateVariablesValidationProgram, "correlate variables validation")
        .build()
    )

    // default rewrite
    programs.addLast(
      DEFAULT_REWRITE,
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(FlinkBatchExecRuleSets.BATCH_EXEC_DEFAULT_REWRITE_RULES)
        .build())

    val calciteConfig = CalciteConfig.createBuilder(util.tableEnv.getConfig.getCalciteConfig)
      .replaceBatchPrograms(programs).build()
    util.tableEnv.getConfig.setCalciteConfig(calciteConfig)

    util.addTable[(Int, Long, Float, Double, java.math.BigDecimal)]("t1", 'a, 'b, 'c, 'd, 'e)
    util.addTable[(Int, Long, Float, Double, java.math.BigDecimal)]("t2", 'a, 'b, 'c, 'd, 'e)
  }

  @Test
  def testInToSemiJoinIntEqualsLong(): Unit = {
    val sqlQuery = "select * from t1 where t1.a in (select b from t2)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInToSemiJoinIntEqualsFloat(): Unit = {
    val sqlQuery = "select * from t1 where t1.a in (select c from t2)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInToSemiJoinIntEqualsDouble(): Unit = {
    val sqlQuery = "select * from t1 where t1.a in (select d from t2)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInToSemiJoinIntEqualsDecimal(): Unit = {
    val sqlQuery = "select * from t1 where t1.a in (select e from t2)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInToSemiJoinFloatEqualsDouble(): Unit = {
    val sqlQuery = "select * from t1 where t1.c in (select d from t2)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInToSemiJoinFloatEqualsDecimal(): Unit = {
    val sqlQuery = "select * from t1 where t1.c in (select e from t2)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInToSemiJoinDoubleEqualsDecimal(): Unit = {
    val sqlQuery = "select * from t1 where t1.d in (select e from t2)"
    util.verifyPlan(sqlQuery)
  }

  // Test nullability mismatch.
  @Test
  def testJoinConditionEqualsTypesNotEquals01(): Unit = {
    val queryLeftJoin = "SELECT a FROM t1 LEFT JOIN " +
      "(SELECT COUNT(*) AS cnt FROM t2) AS x ON a = x.cnt"
    util.verifyPlan(queryLeftJoin)
  }

  // Join with or predicate is not rewrite by subquery rule but decorrelate.
  @Test
  def testJoinConditionEqualsTypesNotEquals02(): Unit = {
    util.addTable[(String, Short, Int)]("t3", 't3a, 't3b, 't3c)
    util.addTable[(String, Short, Int)]("t4", 't4a, 't4b, 't4c)

    val sqlQuery =
      """
        |-- TC 01.04
        |SELECT t3a,
        |       t3b
        |FROM   t3
        |WHERE  t3c IN (SELECT t4b
        |               FROM   t4
        |               WHERE  t3a = t4a
        |                       OR t3b > t4b)
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }
}
