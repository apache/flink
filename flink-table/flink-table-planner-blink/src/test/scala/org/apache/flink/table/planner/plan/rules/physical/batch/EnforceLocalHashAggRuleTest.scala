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

import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.config.{ExecutionConfigOptions, OptimizerConfigOptions}
import org.apache.flink.table.functions.UserDefinedFunction
import org.apache.flink.table.planner.calcite.CalciteConfig
import org.apache.flink.table.planner.plan.optimize.program.FlinkBatchProgram
import org.apache.flink.table.planner.utils.TableConfigUtils

import org.apache.calcite.rel.core.Aggregate
import org.apache.calcite.tools.RuleSets
import org.junit.Before


/**
  * Test for [[EnforceLocalHashAggRule]].
  */
class EnforceLocalHashAggRuleTest extends EnforceLocalAggRuleTestBase {

  @Before
  override def setup(): Unit = {
    super.setup()
    val program = FlinkBatchProgram.buildProgram(util.tableEnv.getConfig.getConfiguration)
    // remove the original BatchExecHashAggRule and add BatchExecHashAggRuleForOnePhase
    // to let the physical phase generate one phase aggregate
    program.getFlinkRuleSetProgram(FlinkBatchProgram.PHYSICAL)
      .get.remove(RuleSets.ofList(BatchExecHashAggRule.INSTANCE))
    program.getFlinkRuleSetProgram(FlinkBatchProgram.PHYSICAL)
      .get.add(RuleSets.ofList(BatchExecHashAggRuleForOnePhase.INSTANCE))

    var calciteConfig = TableConfigUtils.getCalciteConfig(util.tableEnv.getConfig)
    calciteConfig = CalciteConfig.createBuilder(calciteConfig)
      .replaceBatchProgram(program).build()
    util.tableEnv.getConfig.setPlannerConfig(calciteConfig)
    // only enabled HashAgg
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "SortAgg")
    util.tableEnv.getConfig.getConfiguration.setString(
      OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY, "TWO_PHASE")
  }

}

/**
  * Planner rule that ignore the [[OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY]]
  * value, and only enable one phase aggregate.
  * This rule only used for test.
  */
class BatchExecHashAggRuleForOnePhase extends BatchExecHashAggRule {
  override protected def isTwoPhaseAggWorkable(
      aggFunctions: Array[UserDefinedFunction], tableConfig: TableConfig): Boolean = false

  override protected def isOnePhaseAggWorkable(agg: Aggregate,
      aggFunctions: Array[UserDefinedFunction], tableConfig: TableConfig): Boolean = true
}

object BatchExecHashAggRuleForOnePhase {
  val INSTANCE = new BatchExecHashAggRuleForOnePhase
}
