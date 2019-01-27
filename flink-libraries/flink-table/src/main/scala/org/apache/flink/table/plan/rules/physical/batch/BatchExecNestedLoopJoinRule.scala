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

import org.apache.flink.table.api.OperatorType
import org.apache.flink.table.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.plan.`trait`.FlinkRelDistribution.BROADCAST_DISTRIBUTED
import org.apache.flink.table.plan.nodes.FlinkConventions.BATCH_PHYSICAL
import org.apache.flink.table.plan.nodes.logical.{FlinkLogicalJoin, FlinkLogicalSemiJoin}
import org.apache.flink.table.plan.nodes.physical.batch.{BatchExecNestedLoopJoin, BatchExecNestedLoopSemiJoin}
import org.apache.flink.table.plan.rules.physical.batch.BatchExecNestedLoopJoinRule.transformToNestedLoopJoin
import org.apache.flink.table.plan.util.FlinkRelOptUtil

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{Join, JoinRelType, SemiJoin}

class BatchExecNestedLoopJoinRule(joinClass: Class[_ <: Join])
  extends RelOptRule(
    operand(
      joinClass,
      operand(classOf[RelNode], any)), s"BatchExecNestedLoopJoinRule_${joinClass.getSimpleName}")
  with BatchExecJoinRuleBase {

  override def matches(call: RelOptRuleCall): Boolean =
    FlinkRelOptUtil.getTableConfig(call.rel(0)).enabledGivenOpType(OperatorType.NestedLoopJoin)

  private def isLeftBuild(join: Join, left: RelNode, right: RelNode): Boolean = {
    if (join.isInstanceOf[SemiJoin]) {
      return false
    }
    join.getJoinType match {
      case JoinRelType.LEFT => false
      case JoinRelType.RIGHT => true
      case JoinRelType.INNER | JoinRelType.FULL =>
        val leftSize = binaryRowRelNodeSize(left)
        val rightSize = binaryRowRelNodeSize(right)
        // use left as build size if leftSize or rightSize is unknown.
        if (leftSize == null || rightSize == null) {
          true
        } else {
          leftSize <= rightSize
        }
    }
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val join: Join = call.rel(0)
    val left = join.getLeft
    val right = {
      val right = join.getRight
      join match {
        case _: SemiJoin =>
          // We can do a distinct to buildSide(right) when semi join.
          val distinctKeys = 0 until right.getRowType.getFieldCount
          val useBuildDistinct = chooseSemiBuildDistinct(right, distinctKeys)
          if (useBuildDistinct) {
            addLocalDistinctAgg(right, distinctKeys, call.builder())
          } else {
            right
          }
        case _ => join.getRight
      }
    }

    val leftIsBuild = isLeftBuild(join, left, right)

    call.transformTo(
      transformToNestedLoopJoin(join, leftIsBuild, left, right, description, singleRowJoin = false))
  }
}

object BatchExecNestedLoopJoinRule {
  val INSTANCE: RelOptRule = new BatchExecNestedLoopJoinRule(classOf[FlinkLogicalJoin])
  val SEMI_JOIN: RelOptRule = new BatchExecNestedLoopJoinRule(classOf[FlinkLogicalSemiJoin])

  def transformToNestedLoopJoin(
      join: Join,
      leftIsBuild: Boolean,
      left: RelNode,
      right: RelNode,
      description: String,
      singleRowJoin: Boolean): RelNode = {
    var leftRequiredTrait = join.getTraitSet.replace(BATCH_PHYSICAL)
    var rightRequiredTrait = join.getTraitSet.replace(BATCH_PHYSICAL)

    if (join.getJoinType == JoinRelType.FULL) {
      leftRequiredTrait = leftRequiredTrait.replace(FlinkRelDistribution.SINGLETON)
      rightRequiredTrait = rightRequiredTrait.replace(FlinkRelDistribution.SINGLETON)
    } else {
      if (leftIsBuild) {
        leftRequiredTrait = leftRequiredTrait.replace(BROADCAST_DISTRIBUTED)
      } else {
        rightRequiredTrait = rightRequiredTrait.replace(BROADCAST_DISTRIBUTED)
      }
    }

    val newLeft = RelOptRule.convert(left, leftRequiredTrait)
    val newRight = RelOptRule.convert(right, rightRequiredTrait)
    val providedTraitSet = join.getTraitSet.replace(BATCH_PHYSICAL)

    join match {
      case sj: SemiJoin =>
        new BatchExecNestedLoopSemiJoin(
          sj.getCluster,
          providedTraitSet,
          newLeft,
          newRight,
          sj.getCondition,
          sj.getLeftKeys,
          sj.getRightKeys,
          sj.isAnti,
          singleRowJoin,
          description)
      case _ =>
        new BatchExecNestedLoopJoin(
          join.getCluster,
          providedTraitSet,
          newLeft,
          newRight,
          leftIsBuild,
          join.getCondition,
          join.getJoinType,
          singleRowJoin,
          description)
    }
  }
}
