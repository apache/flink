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

import org.apache.flink.table.planner.plan.nodes.FlinkRelNode
import org.apache.flink.table.planner.plan.nodes.logical._
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecTemporalJoin
import org.apache.flink.table.planner.plan.utils.TemporalJoinUtil
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.JoinRelType
import org.apache.flink.util.Preconditions.checkState

/**
 * Rule that matches a temporal join node and converts it to [[StreamExecTemporalJoin]],
 * the temporal join node is a [[FlinkLogicalJoin]] which contains [[TemporalJoinCondition]].
 */
class StreamExecTemporalJoinRule
  extends StreamExecJoinRuleBase("StreamExecJoinRuleBase") {

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
    val (windowBounds, _) = extractWindowBounds(join)
    windowBounds.isEmpty && join.getJoinType == JoinRelType.INNER
  }

  override protected def transform(
      join: FlinkLogicalJoin,
      leftInput: FlinkRelNode,
      leftConversion: RelNode => RelNode,
      rightInput: FlinkRelNode,
      rightConversion: RelNode => RelNode,
      providedTraitSet: RelTraitSet): FlinkRelNode = {
    val newRight = rightInput match {
      case snapshot: FlinkLogicalSnapshot =>
        snapshot.getInput
      case rel: FlinkLogicalRel => rel
    }
    new StreamExecTemporalJoin(
      join.getCluster,
      providedTraitSet,
      leftConversion(leftInput),
      rightConversion(newRight),
      join.getCondition,
      join.getJoinType)
  }
}

object StreamExecTemporalJoinRule {
  val INSTANCE: RelOptRule = new StreamExecTemporalJoinRule
}
