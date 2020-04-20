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
import org.apache.flink.table.api.config.OptimizerConfigOptions
import org.apache.flink.table.planner.JDouble
import org.apache.flink.table.planner.calcite.FlinkContext
import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalJoin
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecHashJoin
import org.apache.flink.table.planner.plan.utils.OperatorType
import org.apache.flink.table.planner.utils.TableConfigUtils.isOperatorDisabled

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{Join, JoinRelType}
import org.apache.calcite.util.ImmutableIntList

import java.util

import scala.collection.JavaConversions._

/**
  * Rule that converts [[FlinkLogicalJoin]] to [[BatchExecHashJoin]]
  * if there exists at least one equal-join condition and
  * ShuffleHashJoin or BroadcastHashJoin are enabled.
  */
class BatchExecHashJoinRule
  extends RelOptRule(
    operand(classOf[FlinkLogicalJoin],
      operand(classOf[RelNode], any)),
    "BatchExecHashJoinRule")
  with BatchExecJoinRuleBase {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join: Join = call.rel(0)
    val joinInfo = join.analyzeCondition
    // join keys must not be empty
    if (joinInfo.pairs().isEmpty) {
      return false
    }

    val tableConfig = call.getPlanner.getContext.unwrap(classOf[FlinkContext]).getTableConfig
    val isShuffleHashJoinEnabled = !isOperatorDisabled(tableConfig, OperatorType.ShuffleHashJoin)
    val isBroadcastHashJoinEnabled = !isOperatorDisabled(
      tableConfig, OperatorType.BroadcastHashJoin)

    val leftSize = binaryRowRelNodeSize(join.getLeft)
    val rightSize = binaryRowRelNodeSize(join.getRight)
    val (isBroadcast, _) = canBroadcast(join.getJoinType, leftSize, rightSize, tableConfig)

    // TODO use shuffle hash join if isBroadcast is true and isBroadcastHashJoinEnabled is false ?
    if (isBroadcast) isBroadcastHashJoinEnabled else isShuffleHashJoinEnabled
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val tableConfig = call.getPlanner.getContext.unwrap(classOf[FlinkContext]).getTableConfig
    val join: Join = call.rel(0)
    val joinInfo = join.analyzeCondition
    val joinType = join.getJoinType

    val left = join.getLeft
    val (right, tryDistinctBuildRow) = joinType match {
      case JoinRelType.SEMI | JoinRelType.ANTI =>
        // We can do a distinct to buildSide(right) when semi join.
        val distinctKeys = 0 until join.getRight.getRowType.getFieldCount
        val useBuildDistinct = chooseSemiBuildDistinct(join.getRight, distinctKeys)
        if (useBuildDistinct) {
          (addLocalDistinctAgg(join.getRight, distinctKeys, call.builder()), true)
        } else {
          (join.getRight, false)
        }
      case _ => (join.getRight, false)
    }

    val leftSize = binaryRowRelNodeSize(left)
    val rightSize = binaryRowRelNodeSize(right)

    val (isBroadcast, leftIsBroadcast) = canBroadcast(joinType, leftSize, rightSize, tableConfig)

    val leftIsBuild = if (isBroadcast) {
      leftIsBroadcast
    } else if (leftSize == null || rightSize == null || leftSize == rightSize) {
      // use left to build hash table if leftSize or rightSize is unknown or equal size.
      // choose right to build if join is SEMI/ANTI.
      !join.getJoinType.projectsRight
    } else {
      leftSize < rightSize
    }

    def transformToEquiv(leftRequiredTrait: RelTraitSet, rightRequiredTrait: RelTraitSet): Unit = {
      val newLeft = RelOptRule.convert(left, leftRequiredTrait)
      val newRight = RelOptRule.convert(right, rightRequiredTrait)
      val providedTraitSet = join.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL)

      val newJoin = new BatchExecHashJoin(
        join.getCluster,
        providedTraitSet,
        newLeft,
        newRight,
        join.getCondition,
        join.getJoinType,
        leftIsBuild,
        isBroadcast,
        tryDistinctBuildRow)

      call.transformTo(newJoin)
    }

    if (isBroadcast) {
      val probeTrait = join.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL)
      val buildTrait = join.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL)
        .replace(FlinkRelDistribution.BROADCAST_DISTRIBUTED)
      if (leftIsBroadcast) {
        transformToEquiv(buildTrait, probeTrait)
      } else {
        transformToEquiv(probeTrait, buildTrait)
      }
    } else {
      val toHashTraitByColumns = (columns: util.Collection[_ <: Number]) =>
        join.getCluster.getPlanner.emptyTraitSet.
          replace(FlinkConventions.BATCH_PHYSICAL).
          replace(FlinkRelDistribution.hash(columns))
      transformToEquiv(
        toHashTraitByColumns(joinInfo.leftKeys),
        toHashTraitByColumns(joinInfo.rightKeys))

      // add more possibility to only shuffle by partial joinKeys, now only single one
      val isShuffleByPartialKeyEnabled = tableConfig.getConfiguration.getBoolean(
        BatchExecJoinRuleBase.TABLE_OPTIMIZER_SHUFFLE_BY_PARTIAL_KEY_ENABLED)
      if (isShuffleByPartialKeyEnabled && joinInfo.pairs().length > 1) {
        joinInfo.pairs().foreach { pair =>
          transformToEquiv(
            toHashTraitByColumns(ImmutableIntList.of(pair.source)),
            toHashTraitByColumns(ImmutableIntList.of(pair.target)))
        }
      }
    }

  }

  /**
    * Decides whether the join can convert to BroadcastHashJoin.
    *
    * @param joinType  flink join type
    * @param leftSize  size of join left child
    * @param rightSize size of join right child
    * @return an Tuple2 instance. The first element of tuple is true if join can convert to
    *         broadcast hash join, false else. The second element of tuple is true if left side used
    *         as broadcast side, false else.
    */
  private def canBroadcast(
      joinType: JoinRelType,
      leftSize: JDouble,
      rightSize: JDouble,
      tableConfig: TableConfig): (Boolean, Boolean) = {
    // if leftSize or rightSize is unknown, cannot use broadcast
    if (leftSize == null || rightSize == null) {
      return (false, false)
    }
    val threshold = tableConfig.getConfiguration.getLong(
      OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD)
    joinType match {
      case JoinRelType.LEFT => (rightSize <= threshold, false)
      case JoinRelType.RIGHT => (leftSize <= threshold, true)
      case JoinRelType.FULL => (false, false)
      case JoinRelType.INNER =>
        (leftSize <= threshold || rightSize <= threshold, leftSize < rightSize)
      // left side cannot be used as build side in SEMI/ANTI join.
      case JoinRelType.SEMI | JoinRelType.ANTI => (rightSize <= threshold, false)
    }
  }
}

object BatchExecHashJoinRule {
  val INSTANCE = new BatchExecHashJoinRule
}
