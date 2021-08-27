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

import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.physical.batch.{BatchPhysicalHashAggregate, BatchPhysicalLocalHashAggregate}

import org.apache.calcite.plan.RelOptRule._
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode

/**
 * There maybe exist a subTree like localHashAggregate -> globalHashAggregate which the middle
 * shuffle is removed. The rule could remove redundant localHashAggregate node.
 */
class RemoveRedundantLocalHashAggRule extends RelOptRule(
  operand(classOf[BatchPhysicalHashAggregate],
    operand(classOf[BatchPhysicalLocalHashAggregate],
      operand(classOf[RelNode], FlinkConventions.BATCH_PHYSICAL, any))),
  "RemoveRedundantLocalHashAggRule") {

  override def onMatch(call: RelOptRuleCall): Unit = {
    val globalAgg: BatchPhysicalHashAggregate = call.rel(0)
    val localAgg: BatchPhysicalLocalHashAggregate = call.rel(1)
    val inputOfLocalAgg = localAgg.getInput
    val newGlobalAgg = new BatchPhysicalHashAggregate(
      globalAgg.getCluster,
      globalAgg.getTraitSet,
      inputOfLocalAgg,
      globalAgg.getRowType,
      inputOfLocalAgg.getRowType,
      inputOfLocalAgg.getRowType,
      localAgg.grouping,
      localAgg.auxGrouping,
      // Use the localAgg agg calls because the global agg call filters was removed,
      // see BatchPhysicalHashAggRule for details.
      localAgg.getAggCallToAggFunction,
      isMerge = false)
    call.transformTo(newGlobalAgg)
  }
}

object RemoveRedundantLocalHashAggRule {
  val INSTANCE = new RemoveRedundantLocalHashAggRule
}
