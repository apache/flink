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
import org.apache.flink.table.calcite.CalciteConfig
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.logical.{FlinkLogicalCalc, FlinkLogicalTableSourceScan}
import org.apache.flink.table.plan.optimize.program._
import org.apache.flink.table.plan.rules.FlinkBatchRuleSets
import org.apache.flink.table.runtime.utils.JavaUserDefinedScalarFunctions.NonDeterministicUdf
import org.apache.flink.table.util.TableTestBase

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
          FilterToCalcRule.INSTANCE,
          ProjectToCalcRule.INSTANCE,
          FlinkCalcMergeRule.INSTANCE,
          FlinkLogicalCalc.CONVERTER,
          FlinkLogicalTableSourceScan.CONVERTER
        ))
        .setRequiredOutputTraits(Array(FlinkConventions.LOGICAL))
        .build())
    val calciteConfig = CalciteConfig.createBuilder(util.tableEnv.getConfig.getCalciteConfig)
      .replaceBatchProgram(programs).build()
    util.tableEnv.getConfig.setCalciteConfig(calciteConfig)

    util.addTableSource[(Int, Int, String)]("MyTable", 'a, 'b, 'c)
  }

  @Test
  def testCalcMergeWithSameDigest(): Unit = {
    util.verifyPlan("SELECT a, b FROM (SELECT * FROM MyTable WHERE a = b) t WHERE b = a")
  }

  @Test
  def testCalcMergeWithNonDeterministicExpr1(): Unit = {
    util.addFunction("random_udf", new NonDeterministicUdf)
    val sqlQuery = "SELECT a, a1 FROM (SELECT a, random_udf(a) AS a1 FROM MyTable) t WHERE a1 > 10"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testCalcMergeWithNonDeterministicExpr2(): Unit = {
    util.addFunction("random_udf", new NonDeterministicUdf)
    val sqlQuery = "SELECT a FROM (SELECT a FROM MyTable) t WHERE random_udf(a) > 10"
    util.verifyPlan(sqlQuery)
  }
}
