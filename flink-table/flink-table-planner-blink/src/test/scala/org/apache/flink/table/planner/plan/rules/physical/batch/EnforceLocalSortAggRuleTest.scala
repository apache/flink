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

package org.apache.flink.table.planner.plan.rules.physical.batch

import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.config.{ExecutionConfigOptions, OptimizerConfigOptions}
import org.apache.flink.table.functions.UserDefinedFunction
import org.apache.flink.table.planner.calcite.CalciteConfig
import org.apache.flink.table.planner.plan.optimize.program.FlinkBatchProgram
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions.WeightedAvg
import org.apache.flink.table.planner.utils.TableConfigUtils

import org.apache.calcite.rel.core.Aggregate
import org.apache.calcite.tools.RuleSets
import org.junit.{Before, Test}


/**
  * Test for [[EnforceLocalSortAggRule]].
  */
class EnforceLocalSortAggRuleTest extends EnforceLocalAggRuleTestBase {

  @Before
  override def setup(): Unit = {
    super.setup()
    util.addFunction("weightedAvg", new WeightedAvg)

    val program = FlinkBatchProgram.buildProgram(util.tableEnv.getConfig.getConfiguration)
    // remove the original BatchExecSortAggRule and add BatchExecSortAggRuleForOnePhase
    // to let the physical phase generate one phase aggregate
    program.getFlinkRuleSetProgram(FlinkBatchProgram.PHYSICAL)
      .get.remove(RuleSets.ofList(BatchExecSortAggRule.INSTANCE))
    program.getFlinkRuleSetProgram(FlinkBatchProgram.PHYSICAL)
      .get.add(RuleSets.ofList(BatchExecSortAggRuleForOnePhase.INSTANCE))

    var calciteConfig = TableConfigUtils.getCalciteConfig(util.tableEnv.getConfig)
    calciteConfig = CalciteConfig.createBuilder(calciteConfig)
      .replaceBatchProgram(program).build()
    util.tableEnv.getConfig.setPlannerConfig(calciteConfig)
    // only enabled SortAgg
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashAgg")
    util.tableEnv.getConfig.getConfiguration.setString(
      OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY, "TWO_PHASE")
  }

  @Test
  def testRollupWithUnmergeableAggCall(): Unit = {
    util.verifyRelPlan("SELECT weightedAvg(a, 1) FROM t GROUP BY ROLLUP (b, c)")
  }

  @Test
  def testCubeWithUnmergeableAggCall(): Unit = {
    util.verifyRelPlan("SELECT weightedAvg(d, 1) FROM t GROUP BY CUBE (a, b)")
  }

  @Test
  def testGroupSetsWithUnmergeableAggCall(): Unit = {
    util.verifyRelPlan("select weightedAvg(a, 1) FROM t GROUP BY GROUPING SETS ((b, c), (b, d))")
  }
}

/**
  * Planner rule that ignore the [[OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY]]
  * value, and only enable one phase aggregate.
  * This rule only used for test.
  */
class BatchExecSortAggRuleForOnePhase extends BatchExecSortAggRule {
  override protected def isTwoPhaseAggWorkable(
      aggFunctions: Array[UserDefinedFunction], tableConfig: TableConfig): Boolean = false

  override protected def isOnePhaseAggWorkable(agg: Aggregate,
      aggFunctions: Array[UserDefinedFunction], tableConfig: TableConfig): Boolean = true
}

object BatchExecSortAggRuleForOnePhase {
  val INSTANCE = new BatchExecSortAggRuleForOnePhase
}
