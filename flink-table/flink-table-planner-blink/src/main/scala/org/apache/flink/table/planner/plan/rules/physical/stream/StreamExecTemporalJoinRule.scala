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

package org.apache.flink.table.planner.plan.rules.physical.stream

import java.util

import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical._
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecTemporalJoin
import org.apache.flink.table.planner.plan.utils.{FlinkRelOptUtil, IntervalJoinUtil, TemporalJoinUtil}
import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.JoinRelType
import org.apache.flink.util.Preconditions.checkState

/**
 * Rule that matches a temporal join node and converts it to [[StreamExecTemporalJoin]],
 * the temporal join node is a [[FlinkLogicalJoin]] which contains [[TemporalJoinCondition]].
 */
class StreamExecTemporalJoinRule
  extends RelOptRule(
    operand(classOf[FlinkLogicalJoin],
      operand(classOf[FlinkLogicalRel], any()),
      operand(classOf[FlinkLogicalRel], any())),
    "StreamExecTemporalJoinRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join = call.rel[FlinkLogicalJoin](0)
    if (!TemporalJoinUtil.containsTemporalJoinCondition(join.getCondition)) {
      return false
    }

    //INITIAL_TEMPORAL_JOIN_CONDITION should not appear in physical phase.
    checkState(!TemporalJoinUtil.containsInitialTemporalJoinCondition(join.getCondition))

    matchesTemporalTableJoin(join) || matchesTemporalTableFunctionJoin(join)
  }

  private def matchesTemporalTableJoin(join: FlinkLogicalJoin): Boolean = {
    val supportedJoinTypes = Seq(JoinRelType.INNER, JoinRelType.LEFT)
    supportedJoinTypes.contains(join.getJoinType)
  }

  private def matchesTemporalTableFunctionJoin(join: FlinkLogicalJoin): Boolean = {
    val joinInfo = join.analyzeCondition
    val tableConfig = FlinkRelOptUtil.getTableConfigFromContext(join)
    val (windowBounds, _) = IntervalJoinUtil.extractWindowBoundsFromPredicate(
      joinInfo.getRemaining(join.getCluster.getRexBuilder),
      join.getLeft.getRowType.getFieldCount,
      join.getRowType,
      join.getCluster.getRexBuilder,
      tableConfig)
    windowBounds.isEmpty && join.getJoinType == JoinRelType.INNER
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val join = call.rel[FlinkLogicalJoin](0)
    val left = call.rel[FlinkLogicalRel](1)
    val right = call.rel[FlinkLogicalRel](2)

    val newRight = right match {
      case snapshot: FlinkLogicalSnapshot =>
        snapshot.getInput
      case rel: FlinkLogicalRel => rel
    }

    def toHashTraitByColumns(
        columns: util.Collection[_ <: Number],
        inputTraitSets: RelTraitSet) = {
      val distribution = if (columns.size() == 0) {
        FlinkRelDistribution.SINGLETON
      } else {
        FlinkRelDistribution.hash(columns)
      }
      inputTraitSets.
        replace(FlinkConventions.STREAM_PHYSICAL).
        replace(distribution)
    }

    val joinInfo = join.analyzeCondition
    val (leftRequiredTrait, rightRequiredTrait) = (
      toHashTraitByColumns(joinInfo.leftKeys, left.getTraitSet),
      toHashTraitByColumns(joinInfo.rightKeys, newRight.getTraitSet))

    val convLeft: RelNode = RelOptRule.convert(left, leftRequiredTrait)
    val convRight: RelNode = RelOptRule.convert(newRight, rightRequiredTrait)
    val providedTraitSet: RelTraitSet = join.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)

    val temporalJoin = new StreamExecTemporalJoin(
      join.getCluster,
      providedTraitSet,
      convLeft,
      convRight,
      join.getCondition,
      join.getJoinType)

    call.transformTo(temporalJoin)
  }
}

object StreamExecTemporalJoinRule {
  val INSTANCE: RelOptRule = new StreamExecTemporalJoinRule
}
