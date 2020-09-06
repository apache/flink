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

package org.apache.flink.table.plan

import org.apache.calcite.plan.{Context, RelOptPlanner}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.sql2rel.RelDecorrelator
import org.apache.calcite.tools.RuleSet
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.internal.TableEnvImpl
import org.apache.flink.table.calcite.CalciteConfig
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.rules.FlinkRuleSets
import org.apache.flink.table.planner.PlanningConfigurationBuilder

/**
  * An [[Optimizer]] that can be used for optimizing a batch plan. Should be used to create an
  * optimized tree from a logical input tree.
  *
  * @param calciteConfig                provider for [[CalciteConfig]]. It is a provider because the
  *                                     [[TableConfig]] in a [[TableEnvImpl]] is mutable.
  * @param planningConfigurationBuilder provider for [[RelOptPlanner]] and [[Context]]
  */
class BatchOptimizer(
    calciteConfig: () => CalciteConfig,
    planningConfigurationBuilder: PlanningConfigurationBuilder)
  extends Optimizer(calciteConfig, planningConfigurationBuilder) {

  /**
    * Generates the optimized [[RelNode]] tree from the original relational node tree.
    *
    * @param relNode The original [[RelNode]] tree
    * @return The optimized [[RelNode]] tree
    */
  def optimize(relNode: RelNode): RelNode = {
    val convSubQueryPlan = optimizeConvertSubQueries(relNode)
    val expandedPlan = optimizeExpandPlan(convSubQueryPlan)
    val decorPlan = RelDecorrelator.decorrelateQuery(expandedPlan)
    val normalizedPlan = optimizeNormalizeLogicalPlan(decorPlan)
    val logicalPlan = optimizeLogicalPlan(normalizedPlan)
    val logicalRewritePlan = optimizeLogicalRewritePlan(logicalPlan)
    optimizePhysicalPlan(logicalRewritePlan, FlinkConventions.DATASET)
  }

  /**
    * Returns the built-in normalization rules that are defined by the optimizer.
    */
  protected def getBuiltInNormRuleSet: RuleSet = FlinkRuleSets.DATASET_NORM_RULES

  /**
    * Returns the built-in optimization rules that are defined by the optimizer.
    */
  protected def getBuiltInPhysicalOptRuleSet: RuleSet = FlinkRuleSets.DATASET_OPT_RULES

}
