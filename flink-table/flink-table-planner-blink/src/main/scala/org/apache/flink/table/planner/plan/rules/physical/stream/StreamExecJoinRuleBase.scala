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

import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.planner.plan.nodes.{FlinkConventions, FlinkRelNode}
import org.apache.flink.table.planner.plan.nodes.logical.{FlinkLogicalJoin, FlinkLogicalRel, FlinkLogicalSnapshot}
import org.apache.flink.table.planner.plan.utils.IntervalJoinUtil.WindowBounds
import org.apache.flink.table.planner.plan.utils.{FlinkRelOptUtil, IntervalJoinUtil}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rex.RexNode

import java.util

/**
 * Base implementation for rules match stream-stream join, including
 * regular stream join, interval join and temporal join.
 */
abstract class StreamExecJoinRuleBase(description: String)
  extends RelOptRule(
    operand(classOf[FlinkLogicalJoin],
      operand(classOf[FlinkLogicalRel], any()),
      operand(classOf[FlinkLogicalRel], any())),
    description) {

  protected def extractWindowBounds(join: FlinkLogicalJoin):
    (Option[WindowBounds], Option[RexNode]) = {
    val tableConfig = FlinkRelOptUtil.getTableConfigFromContext(join)
    IntervalJoinUtil.extractWindowBoundsFromPredicate(
      join.getCondition,
      join.getLeft.getRowType.getFieldCount,
      join.getRowType,
      join.getCluster.getRexBuilder,
      tableConfig)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val join = call.rel[FlinkLogicalJoin](0)
    val left = call.rel[FlinkLogicalRel](1)
    val right = call.rel[FlinkLogicalRel](2)

    def toHashTraitByColumns(
        columns: util.Collection[_ <: Number],
        inputTraitSet: RelTraitSet): RelTraitSet = {
      val distribution = if (columns.size() == 0) {
        FlinkRelDistribution.SINGLETON
      } else {
        FlinkRelDistribution.hash(columns)
      }
      inputTraitSet
          .replace(FlinkConventions.STREAM_PHYSICAL)
          .replace(distribution)
    }

    val newRight = right match {
      case snapshot: FlinkLogicalSnapshot =>
        snapshot.getInput
      case rel: FlinkLogicalRel => rel
    }

    val joinInfo = join.analyzeCondition
    val (leftRequiredTrait, rightRequiredTrait) = (
        toHashTraitByColumns(joinInfo.leftKeys, left.getTraitSet),
        toHashTraitByColumns(joinInfo.rightKeys, newRight.getTraitSet))

    val convertedLeft: RelNode = RelOptRule.convert(left, leftRequiredTrait)
    val convertedRight: RelNode = RelOptRule.convert(newRight, rightRequiredTrait)
    val providedTraitSet: RelTraitSet = join.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)

    val newJoin = transform(join, convertedLeft, convertedRight, providedTraitSet)
    call.transformTo(newJoin)
  }

  protected def transform(
      join: FlinkLogicalJoin,
      convertedLeft: RelNode,
      convertedRight: RelNode,
      providedTraitSet: RelTraitSet): FlinkRelNode
}
