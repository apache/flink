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
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.{FlinkLogicalCalc, FlinkLogicalLegacyTableSourceScan}
import org.apache.flink.table.planner.plan.optimize.program._
import org.apache.flink.table.planner.plan.rules.FlinkBatchRuleSets
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.NonDeterministicUdf
import org.apache.flink.table.planner.utils.TableTestBase

import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.rel.rules._
import org.apache.calcite.tools.RuleSets
import org.junit.{Before, Test}

/**
  * Test for [[FlinkCalcMergeRule]].
  */
class FlinkCalcMergeRuleTest extends TableTestBase {
  private val util = batchTestUtil()

  @Before
  def setup(): Unit = {
    val programs = new FlinkChainedProgram[BatchOptimizeContext]()
    programs.addLast(
      "table_ref",
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(FlinkBatchRuleSets.TABLE_REF_RULES)
        .build())
    programs.addLast(
      "logical",
      FlinkVolcanoProgramBuilder.newBuilder
        .add(RuleSets.ofList(
          CoreRules.FILTER_TO_CALC,
          CoreRules.PROJECT_TO_CALC,
          FlinkCalcMergeRule.INSTANCE,
          FlinkLogicalCalc.CONVERTER,
          FlinkLogicalLegacyTableSourceScan.CONVERTER
        ))
        .setRequiredOutputTraits(Array(FlinkConventions.LOGICAL))
        .build())
    util.replaceBatchProgram(programs)

    util.addTableSource[(Int, Int, String)]("MyTable", 'a, 'b, 'c)
    util.addFunction("random_udf", new NonDeterministicUdf)
  }

  @Test
  def testCalcMergeWithSameDigest(): Unit = {
    util.verifyRelPlan("SELECT a, b FROM (SELECT * FROM MyTable WHERE a = b) t WHERE b = a")
  }

  @Test
  def testCalcMergeWithNonDeterministicExpr1(): Unit = {
    val sqlQuery = "SELECT a, a1 FROM (SELECT a, random_udf(a) AS a1 FROM MyTable) t WHERE a1 > 10"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testCalcMergeWithNonDeterministicExpr2(): Unit = {
    val sqlQuery = "SELECT a FROM (SELECT a FROM MyTable) t WHERE random_udf(a) > 10"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testCalcMergeWithNestedNonDeterministicExpr(): Unit = {
    val sqlQuery = "SELECT random_udf(a1) as a2 FROM (SELECT random_udf(a) as" +
      " a1, b FROM MyTable) t WHERE b > 10"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testCalcMergeWithTopMultiNonDeterministicExpr(): Unit = {
    val sqlQuery = "SELECT random_udf(a1) as a2, random_udf(a1) as a3 FROM" +
      " (SELECT random_udf(a) as a1, b FROM MyTable) t WHERE b > 10"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testCalcMergeTopFilterHasNonDeterministicExpr(): Unit = {
    val sqlQuery = "SELECT a, c FROM" +
      " (SELECT a, random_udf(b) as b1, c FROM MyTable) t WHERE b1 > 10"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testCalcMergeWithBottomMultiNonDeterministicExpr(): Unit = {
    val sqlQuery = "SELECT a1, b2 FROM" +
      " (SELECT random_udf(a) as a1, random_udf(b) as b2, c FROM MyTable) t WHERE c > 10"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testCalcMergeWithBottomMultiNonDeterministicInConditionExpr(): Unit = {
    val sqlQuery = "SELECT c FROM" +
      " (SELECT random_udf(a) as a1, random_udf(b) as b2, c FROM MyTable) t WHERE a1 > b2"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testCalcMergeWithoutInnerNonDeterministicExpr(): Unit = {
    val sqlQuery = "SELECT a, c FROM (SELECT a, random_udf(a) as a1, c FROM MyTable) t WHERE c > 10"
    util.verifyRelPlan(sqlQuery)
  }
}
