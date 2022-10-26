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

import org.apache.flink.table.api.TableException
import org.apache.flink.table.planner.hint.JoinStrategy
import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalJoin
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalHashJoin
import org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{Join, JoinRelType}
import org.apache.calcite.util.ImmutableIntList

import java.util

import scala.collection.JavaConversions._

/**
 * Rule that converts [[FlinkLogicalJoin]] to [[BatchPhysicalHashJoin]] if there exists at least one
 * equal-join condition and ShuffleHashJoin or BroadcastHashJoin are enabled.
 */
class BatchPhysicalHashJoinRule
  extends RelOptRule(
    operand(classOf[FlinkLogicalJoin], operand(classOf[RelNode], any)),
    "BatchPhysicalHashJoinRule")
  with BatchPhysicalJoinRuleBase {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join: Join = call.rel(0)
    val tableConfig = unwrapTableConfig(join)

    canUseJoinStrategy(join, tableConfig, JoinStrategy.BROADCAST) ||
    canUseJoinStrategy(join, tableConfig, JoinStrategy.SHUFFLE_HASH)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val join: Join = call.rel(0)
    val tableConfig = unwrapTableConfig(join)
    val joinInfo = join.analyzeCondition
    val joinType = join.getJoinType

    val left = join.getLeft
    val (right, tryDistinctBuildRow) = joinType match {
      case JoinRelType.SEMI | JoinRelType.ANTI =>
        // We can do a distinct to buildSide(right) when semi join.
        val distinctKeys = 0 until join.getRight.getRowType.getFieldCount
        val useBuildDistinct = chooseSemiBuildDistinct(join.getRight, distinctKeys)
        if (useBuildDistinct) {
          (addLocalDistinctAgg(join.getRight, distinctKeys), true)
        } else {
          (join.getRight, false)
        }
      case _ => (join.getRight, false)
    }

    val firstValidJoinHintOp = getFirstValidJoinHint(join, tableConfig)

    val (isBroadcast: Boolean, isLeftToBroadcastOrBuild: Boolean) = firstValidJoinHintOp match {
      case Some(firstValidJoinHint) =>
        firstValidJoinHint match {
          case JoinStrategy.BROADCAST =>
            val (_, isLeftToBroadcast: Boolean) =
              checkBroadcast(join, tableConfig, withBroadcastHint = true)
            (true, isLeftToBroadcast)
          case JoinStrategy.SHUFFLE_HASH =>
            val (_, isLeftToBuild: Boolean) =
              checkShuffleHash(join, tableConfig, withShuffleHashHint = true)
            (false, isLeftToBuild)
          case _ =>
            // this should not happen
            throw new TableException(
              String.format(
                "The planner is trying to convert the " +
                  "`FlinkLogicalJoin` using BROADCAST or SHUFFLE_HASH," +
                  " but the first valid join hint is not BROADCAST or SHUFFLE_HASH: %s",
                firstValidJoinHint
              ))
        }
      case None =>
        // treat as non-join-hints
        val (canBroadcast, isLeftToBroadcast) =
          checkBroadcast(join, tableConfig, withBroadcastHint = false)

        if (canBroadcast) {
          (true, isLeftToBroadcast)
        } else {
          val (_, isLeftToBuild) = checkShuffleHash(join, tableConfig, withShuffleHashHint = false)
          (false, isLeftToBuild)
        }
    }

    def transformToEquiv(leftRequiredTrait: RelTraitSet, rightRequiredTrait: RelTraitSet): Unit = {
      val newLeft = RelOptRule.convert(left, leftRequiredTrait)
      val newRight = RelOptRule.convert(right, rightRequiredTrait)
      val providedTraitSet = join.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL)

      val newJoin = new BatchPhysicalHashJoin(
        join.getCluster,
        providedTraitSet,
        newLeft,
        newRight,
        join.getCondition,
        join.getJoinType,
        isLeftToBroadcastOrBuild,
        isBroadcast,
        tryDistinctBuildRow)

      call.transformTo(newJoin)
    }

    if (isBroadcast) {
      val probeTrait = join.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL)
      val buildTrait = join.getTraitSet
        .replace(FlinkConventions.BATCH_PHYSICAL)
        .replace(FlinkRelDistribution.BROADCAST_DISTRIBUTED)
      if (isLeftToBroadcastOrBuild) {
        transformToEquiv(buildTrait, probeTrait)
      } else {
        transformToEquiv(probeTrait, buildTrait)
      }
    } else {
      val toHashTraitByColumns = (columns: util.Collection[_ <: Number]) =>
        join.getCluster.getPlanner.emptyTraitSet
          .replace(FlinkConventions.BATCH_PHYSICAL)
          .replace(FlinkRelDistribution.hash(columns))
      transformToEquiv(
        toHashTraitByColumns(joinInfo.leftKeys),
        toHashTraitByColumns(joinInfo.rightKeys))

      // add more possibility to only shuffle by partial joinKeys, now only single one
      val isShuffleByPartialKeyEnabled =
        tableConfig.get(BatchPhysicalJoinRuleBase.TABLE_OPTIMIZER_SHUFFLE_BY_PARTIAL_KEY_ENABLED)
      if (isShuffleByPartialKeyEnabled && joinInfo.pairs().length > 1) {
        joinInfo.pairs().foreach {
          pair =>
            transformToEquiv(
              toHashTraitByColumns(ImmutableIntList.of(pair.source)),
              toHashTraitByColumns(ImmutableIntList.of(pair.target)))
        }
      }
    }

  }

}

object BatchPhysicalHashJoinRule {
  val INSTANCE = new BatchPhysicalHashJoinRule
}
