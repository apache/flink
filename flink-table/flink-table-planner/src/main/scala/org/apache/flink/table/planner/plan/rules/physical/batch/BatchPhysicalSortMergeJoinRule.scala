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

import org.apache.flink.table.planner.hint.JoinStrategy
import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalJoin
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalSortMergeJoin
import org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.Join
import org.apache.calcite.util.ImmutableIntList

/**
 * Rule that converts [[FlinkLogicalJoin]] to [[BatchPhysicalSortMergeJoin]] if there exists at
 * least one equal-join condition and SortMergeJoin is enabled.
 */
class BatchPhysicalSortMergeJoinRule
  extends RelOptRule(
    operand(classOf[FlinkLogicalJoin], operand(classOf[RelNode], any)),
    "BatchPhysicalSortMergeJoinRule")
  with BatchPhysicalJoinRuleBase {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join: Join = call.rel(0)
    val tableConfig = unwrapTableConfig(join)
    canUseJoinStrategy(join, tableConfig, JoinStrategy.SHUFFLE_MERGE)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val join: Join = call.rel(0)
    val joinInfo = join.analyzeCondition
    val left = join.getLeft
    val right = join.getRight

    def getTraitSetByShuffleKeys(
        shuffleKeys: ImmutableIntList,
        requireStrict: Boolean): RelTraitSet = {
      call.getPlanner
        .emptyTraitSet()
        .replace(FlinkConventions.BATCH_PHYSICAL)
        .replace(FlinkRelDistribution.hash(shuffleKeys, requireStrict))
    }

    val leftRequiredTrait =
      getTraitSetByShuffleKeys(joinInfo.leftKeys, requireStrict = true)
    val rightRequiredTrait =
      getTraitSetByShuffleKeys(joinInfo.rightKeys, requireStrict = true)

    val newLeft = RelOptRule.convert(left, leftRequiredTrait)
    val newRight = RelOptRule.convert(right, rightRequiredTrait)

    val providedTraitSet = call.getPlanner
      .emptyTraitSet()
      .replace(FlinkConventions.BATCH_PHYSICAL)
    // do not try to remove redundant sort for shorter optimization time
    val newJoin = new BatchPhysicalSortMergeJoin(
      join.getCluster,
      providedTraitSet,
      newLeft,
      newRight,
      join.getCondition,
      join.getJoinType,
      false,
      false)
    call.transformTo(newJoin)
  }
}

object BatchPhysicalSortMergeJoinRule {
  val INSTANCE: RelOptRule = new BatchPhysicalSortMergeJoinRule
}
