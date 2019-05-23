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

package org.apache.flink.table.plan.rules.physical.batch

import org.apache.flink.table.api.{OperatorType, PlannerConfigOptions}
import org.apache.flink.table.calcite.FlinkContext
import org.apache.flink.table.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalJoin
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSortMergeJoin
import org.apache.flink.table.plan.util.FlinkRelOptUtil

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.core.Join
import org.apache.calcite.rel.{RelCollations, RelNode}
import org.apache.calcite.util.ImmutableIntList

import scala.collection.JavaConversions._

/**
  * Rule that converts [[FlinkLogicalJoin]] to [[BatchExecSortMergeJoin]]
  * if there exists at least one equal-join condition and SortMergeJoin is enabled.
  */
class BatchExecSortMergeJoinRule
  extends RelOptRule(
    operand(classOf[FlinkLogicalJoin],
      operand(classOf[RelNode], any)),
    "BatchExecSortMergeJoinRule")
  with BatchExecJoinRuleBase {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join: Join = call.rel(0)
    val joinInfo = join.analyzeCondition
    val tableConfig = call.getPlanner.getContext.asInstanceOf[FlinkContext].getTableConfig
    val isSortMergeJoinEnabled = tableConfig.isOperatorEnabled(OperatorType.SortMergeJoin)
    !joinInfo.pairs().isEmpty && isSortMergeJoinEnabled
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val join: Join = call.rel(0)
    val joinInfo = join.analyzeCondition
    val left = join.getLeft
    val right = join.getRight

    def getTraitSetByShuffleKeys(
        shuffleKeys: ImmutableIntList,
        requireStrict: Boolean,
        requireCollation: Boolean): RelTraitSet = {
      var traitSet = call.getPlanner.emptyTraitSet()
        .replace(FlinkConventions.BATCH_PHYSICAL)
        .replace(FlinkRelDistribution.hash(shuffleKeys, requireStrict))
      if (requireCollation) {
        val fieldCollations = shuffleKeys.map(FlinkRelOptUtil.ofRelFieldCollation(_))
        val relCollation = RelCollations.of(fieldCollations)
        traitSet = traitSet.replace(relCollation)
      }
      traitSet
    }

    def transformToEquiv(
        leftRequiredShuffleKeys: ImmutableIntList,
        rightRequiredShuffleKeys: ImmutableIntList,
        requireLeftSorted: Boolean,
        requireRightSorted: Boolean): Unit = {

      val leftRequiredTrait = getTraitSetByShuffleKeys(
        leftRequiredShuffleKeys, requireStrict = true, requireLeftSorted)
      val rightRequiredTrait = getTraitSetByShuffleKeys(
        rightRequiredShuffleKeys, requireStrict = true, requireRightSorted)

      val newLeft = RelOptRule.convert(left, leftRequiredTrait)
      val newRight = RelOptRule.convert(right, rightRequiredTrait)

      val providedTraitSet = call.getPlanner
        .emptyTraitSet()
        .replace(FlinkConventions.BATCH_PHYSICAL)
      val newJoin = new BatchExecSortMergeJoin(
        join.getCluster,
        providedTraitSet,
        newLeft,
        newRight,
        join.getCondition,
        join.getJoinType,
        requireLeftSorted,
        requireRightSorted)
      call.transformTo(newJoin)
    }

    val tableConfig = call.getPlanner.getContext.asInstanceOf[FlinkContext].getTableConfig
    val candidates = if (tableConfig.getConf.getBoolean(
      PlannerConfigOptions.SQL_OPTIMIZER_SMJ_REMOVE_SORT_ENABLED)) {
      // add more possibility to remove redundant sort, and longer optimization time
      Array((false, false), (true, false), (false, true), (true, true))
    } else {
      // will not try to remove redundant sort, and shorter optimization time
      Array((false, false))
    }
    candidates.foreach {
      case (requireLeftSorted, requireRightSorted) =>
        transformToEquiv(
          joinInfo.leftKeys,
          joinInfo.rightKeys,
          requireLeftSorted,
          requireRightSorted)
    }

    // add more possibility to only shuffle by partial joinKeys, now only single one
    val isShuffleByPartialKeyEnabled = tableConfig.getConf.getBoolean(
      PlannerConfigOptions.SQL_OPTIMIZER_SHUFFLE_PARTIAL_KEY_ENABLED)
    if (isShuffleByPartialKeyEnabled && joinInfo.pairs().length > 1) {
      joinInfo.pairs().foreach { pair =>
        // sort require full key not partial key,
        // so requireLeftSorted and requireRightSorted should both be false here
        transformToEquiv(
          ImmutableIntList.of(pair.source),
          ImmutableIntList.of(pair.target),
          requireLeftSorted = false,
          requireRightSorted = false)
      }
    }
  }
}

object BatchExecSortMergeJoinRule {
  val INSTANCE: RelOptRule = new BatchExecSortMergeJoinRule
}
