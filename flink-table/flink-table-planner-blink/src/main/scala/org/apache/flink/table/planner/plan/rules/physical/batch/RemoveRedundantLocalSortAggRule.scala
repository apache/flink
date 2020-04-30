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
import org.apache.flink.table.planner.plan.nodes.physical.batch.{BatchExecLocalSortAggregate, BatchExecSort, BatchExecSortAggregate}

import org.apache.calcite.plan.RelOptRule._
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptRuleOperand}
import org.apache.calcite.rel.RelNode

/**
  * There maybe exist a subTree like localSortAggregate -> globalSortAggregate, or
  * localSortAggregate -> sort -> globalSortAggregate which the middle shuffle is removed.
  * The rule could remove redundant localSortAggregate node.
  */
abstract class RemoveRedundantLocalSortAggRule(
    operand: RelOptRuleOperand,
    ruleName: String) extends RelOptRule(operand, ruleName) {

  override def onMatch(call: RelOptRuleCall): Unit = {
    val globalAgg = getOriginalGlobalAgg(call)
    val localAgg = getOriginalLocalAgg(call)
    val inputOfLocalAgg = getOriginalInputOfLocalAgg(call)
    val newGlobalAgg = new BatchExecSortAggregate(
      globalAgg.getCluster,
      call.builder(),
      globalAgg.getTraitSet,
      inputOfLocalAgg,
      globalAgg.getRowType,
      inputOfLocalAgg.getRowType,
      inputOfLocalAgg.getRowType,
      localAgg.getGrouping,
      localAgg.getAuxGrouping,
      // Use the localAgg agg calls because the global agg call filters was removed,
      // see BatchExecSortAggRule for details.
      localAgg.getAggCallToAggFunction,
      isMerge = false)
    call.transformTo(newGlobalAgg)
  }

  private[table] def getOriginalGlobalAgg(call: RelOptRuleCall): BatchExecSortAggregate

  private[table] def getOriginalLocalAgg(call: RelOptRuleCall): BatchExecLocalSortAggregate

  private[table] def getOriginalInputOfLocalAgg(call: RelOptRuleCall): RelNode

}

class RemoveRedundantLocalSortAggWithoutSortRule extends RemoveRedundantLocalSortAggRule(
  operand(classOf[BatchExecSortAggregate],
    operand(classOf[BatchExecLocalSortAggregate],
      operand(classOf[RelNode], FlinkConventions.BATCH_PHYSICAL, any))),
  "RemoveRedundantLocalSortAggWithoutSortRule") {

  override private[table] def getOriginalGlobalAgg(call: RelOptRuleCall): BatchExecSortAggregate = {
    call.rels(0).asInstanceOf[BatchExecSortAggregate]
  }

  override private[table] def getOriginalLocalAgg(
      call: RelOptRuleCall): BatchExecLocalSortAggregate = {
    call.rels(1).asInstanceOf[BatchExecLocalSortAggregate]
  }

  override private[table] def getOriginalInputOfLocalAgg(call: RelOptRuleCall): RelNode = {
    call.rels(2)
  }

}

class RemoveRedundantLocalSortAggWithSortRule extends RemoveRedundantLocalSortAggRule(
  operand(classOf[BatchExecSortAggregate],
    operand(classOf[BatchExecSort],
      operand(classOf[BatchExecLocalSortAggregate],
        operand(classOf[RelNode], FlinkConventions.BATCH_PHYSICAL, any)))),
  "RemoveRedundantLocalSortAggWithSortRule") {

  override private[table] def getOriginalGlobalAgg(call: RelOptRuleCall): BatchExecSortAggregate = {
    call.rels(0).asInstanceOf[BatchExecSortAggregate]
  }

  override private[table] def getOriginalLocalAgg(
      call: RelOptRuleCall): BatchExecLocalSortAggregate = {
    call.rels(2).asInstanceOf[BatchExecLocalSortAggregate]
  }

  override private[table] def getOriginalInputOfLocalAgg(call: RelOptRuleCall): RelNode = {
    call.rels(3)
  }

}

object RemoveRedundantLocalSortAggRule {
  val WITHOUT_SORT = new RemoveRedundantLocalSortAggWithoutSortRule
  val WITH_SORT = new RemoveRedundantLocalSortAggWithSortRule
}
