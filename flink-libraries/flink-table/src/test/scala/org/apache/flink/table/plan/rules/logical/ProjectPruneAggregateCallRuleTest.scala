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

import org.apache.flink.table.calcite.CalciteConfig
import org.apache.flink.table.plan.optimize.FlinkBatchPrograms.LOGICAL
import org.apache.flink.table.plan.optimize.{FlinkBatchPrograms, FlinkHepRuleSetProgramBuilder, HEP_RULES_EXECUTION_TYPE}

import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule
import org.apache.calcite.tools.RuleSets

import scala.collection.JavaConversions._

class ProjectPruneAggregateCallRuleTest extends PruneAggregateCallRuleTest {

  override def setup(): Unit = {
    super.setup()
    val batchPrograms = FlinkBatchPrograms.buildPrograms(util.getTableEnv.getConfig.getConf)
    var startRemove = false
    batchPrograms.getProgramNames.foreach { name =>
      if (name.equals(LOGICAL)) {
        startRemove = true
      }
      if (startRemove) {
        batchPrograms.remove(name)
      }
    }
    batchPrograms.addLast(
      LOGICAL,
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(RuleSets.ofList(
          AggregateReduceGroupingRule.INSTANCE,
          ProjectFilterTransposeRule.INSTANCE,
          PruneAggregateCallRule.PROJECT_ON_AGGREGATE)
        ).build())

    val calciteConfig = CalciteConfig.createBuilder(util.tableEnv.getConfig.getCalciteConfig)
      .replaceBatchPrograms(batchPrograms).build()
    util.tableEnv.getConfig.setCalciteConfig(calciteConfig)
  }
}
