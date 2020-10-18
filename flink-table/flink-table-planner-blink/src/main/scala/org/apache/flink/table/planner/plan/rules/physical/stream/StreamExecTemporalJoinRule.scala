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
import org.apache.flink.table.planner.plan.utils.TemporalJoinUtil

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.JoinRelType

/**
 * Rule that matches a temporal join node and converts it to [[StreamExecTemporalJoin]],
 * the temporal join node is a [[FlinkLogicalJoin]] whose left input is a [[FlinkLogicalRel]]
 * and right input is a [[FlinkLogicalSnapshot]].
 */
class StreamExecTemporalJoinRule
  extends RelOptRule(
    operand(classOf[FlinkLogicalJoin],
      operand(classOf[FlinkLogicalRel], any()),
      operand(classOf[FlinkLogicalSnapshot],
        operand(classOf[FlinkLogicalRel], any()))),
    "StreamExecTemporalJoinRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join = call.rel[FlinkLogicalJoin](0)
    val isTemporalJoin = TemporalJoinUtil.containsTemporalJoinCondition(join.getCondition)

    val supportedJoinTypes = Seq(JoinRelType.INNER, JoinRelType.LEFT)
    isTemporalJoin && supportedJoinTypes.contains(join.getJoinType)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val join = call.rel[FlinkLogicalJoin](0)
    val left = call.rel[FlinkLogicalRel](1)
    val snapshotInput = call.rel[FlinkLogicalRel](3)

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
      toHashTraitByColumns(joinInfo.rightKeys, snapshotInput.getTraitSet))

    val convLeft: RelNode = RelOptRule.convert(left, leftRequiredTrait)
    val convRight: RelNode = RelOptRule.convert(snapshotInput, rightRequiredTrait)
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
