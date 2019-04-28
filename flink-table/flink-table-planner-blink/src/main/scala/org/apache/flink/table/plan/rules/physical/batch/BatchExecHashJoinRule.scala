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

import org.apache.flink.table.JDouble
import org.apache.flink.table.api.{OperatorType, PlannerConfigOptions, TableConfig}
import org.apache.flink.table.calcite.FlinkContext
import org.apache.flink.table.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalJoin
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecHashJoin
import org.apache.flink.table.runtime.join.FlinkJoinType

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{Join, SemiJoin}
import org.apache.calcite.util.ImmutableIntList

import java.util

import scala.collection.JavaConversions._

/**
  * Rule that converts [[FlinkLogicalJoin]] to [[BatchExecHashJoin]]
  * if there exists at least one equal-join condition and
  * ShuffleHashJoin or BroadcastHashJoin are enabled.
  */
class BatchExecHashJoinRule(joinClass: Class[_ <: Join])
  extends RelOptRule(
    operand(joinClass,
      operand(classOf[RelNode], any)),
    s"BatchExecHashJoinRule_${joinClass.getSimpleName}")
  with BatchExecJoinRuleBase {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join: Join = call.rel(0)
    val joinInfo = join.analyzeCondition
    // join keys must not be empty
    if (joinInfo.pairs().isEmpty) {
      return false
    }

    val tableConfig = call.getPlanner.getContext.asInstanceOf[FlinkContext].getTableConfig
    val isShuffleHashJoinEnabled = tableConfig.isOperatorEnabled(OperatorType.ShuffleHashJoin)
    val isBroadcastHashJoinEnabled = tableConfig.isOperatorEnabled(OperatorType.BroadcastHashJoin)

    val joinType = getFlinkJoinType(join)
    val leftSize = binaryRowRelNodeSize(join.getLeft)
    val rightSize = binaryRowRelNodeSize(join.getRight)
    val (isBroadcast, _) = canBroadcast(joinType, leftSize, rightSize, tableConfig)

    // TODO use shuffle hash join if isBroadcast is true and isBroadcastHashJoinEnabled is false ?
    if (isBroadcast) isBroadcastHashJoinEnabled else isShuffleHashJoinEnabled
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val tableConfig = call.getPlanner.getContext.asInstanceOf[FlinkContext].getTableConfig
    val join: Join = call.rel(0)
    val joinInfo = join.analyzeCondition
    val joinType = getFlinkJoinType(join)

    val left = join.getLeft
    val right = join.getRight

    val leftSize = binaryRowRelNodeSize(left)
    val rightSize = binaryRowRelNodeSize(right)

    val (isBroadcast, leftIsBroadcast) = canBroadcast(joinType, leftSize, rightSize, tableConfig)

    val leftIsBuild = if (isBroadcast) {
      leftIsBroadcast
    } else if (leftSize == null || rightSize == null || leftSize == rightSize) {
      // use left to build hash table if leftSize or rightSize is unknown or equal size.
      // choose right to build if join is semiJoin.
      !join.isInstanceOf[SemiJoin]
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
        isBroadcast)

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
      val isShuffleByPartialKeyEnabled = tableConfig.getConf.getBoolean(
        PlannerConfigOptions.SQL_OPTIMIZER_SHUFFLE_PARTIAL_KEY_ENABLED)
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
      joinType: FlinkJoinType,
      leftSize: JDouble,
      rightSize: JDouble,
      tableConfig: TableConfig): (Boolean, Boolean) = {
    // if leftSize or rightSize is unknown, cannot use broadcast
    if (leftSize == null || rightSize == null) {
      return (false, false)
    }
    val threshold = tableConfig.getConf.getLong(
      PlannerConfigOptions.SQL_OPTIMIZER_HASH_JOIN_BROADCAST_THRESHOLD)
    joinType match {
      case FlinkJoinType.LEFT => (rightSize <= threshold, false)
      case FlinkJoinType.RIGHT => (leftSize <= threshold, true)
      case FlinkJoinType.FULL => (false, false)
      case FlinkJoinType.INNER =>
        (leftSize <= threshold || rightSize <= threshold, leftSize < rightSize)
      // left side cannot be used as build side in SEMI/ANTI join.
      case FlinkJoinType.SEMI | FlinkJoinType.ANTI => (rightSize <= threshold, false)
    }
  }
}

object BatchExecHashJoinRule {
  val INSTANCE = new BatchExecHashJoinRule(classOf[FlinkLogicalJoin])
}
