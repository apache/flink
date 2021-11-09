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

import org.apache.flink.table.api.TableException
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.nodes.FlinkRelNode
import org.apache.flink.table.planner.plan.nodes.logical.{FlinkLogicalJoin, FlinkLogicalRel, FlinkLogicalSnapshot}
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalJoin
import org.apache.flink.table.planner.plan.utils.JoinUtil.{accessesTimeAttribute, combineJoinInputsRowType, satisfyRegularJoin}
import org.apache.flink.table.planner.plan.utils.TemporalJoinUtil.containsInitialTemporalJoinCondition
import org.apache.flink.util.Preconditions.checkState

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode

import scala.collection.JavaConversions._

/**
  * Rule that converts [[FlinkLogicalJoin]] without window bounds in join condition
  * to [[StreamPhysicalJoin]].
  */
class StreamPhysicalJoinRule
  extends StreamPhysicalJoinRuleBase("StreamPhysicalJoinRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join: FlinkLogicalJoin = call.rel(0)
    val left: FlinkLogicalRel = call.rel(1).asInstanceOf[FlinkLogicalRel]
    val right: FlinkLogicalRel = call.rel(2).asInstanceOf[FlinkLogicalRel]

    if (!satisfyRegularJoin(join, left, right)) {
      return false
    }

    // validate the join
    if (left.isInstanceOf[FlinkLogicalSnapshot]) {
      throw new TableException(
        "Temporal table join only support apply FOR SYSTEM_TIME AS OF on the right table.")
    }

    // INITIAL_TEMPORAL_JOIN_CONDITION should not appear in physical phase in case which fallback
    // to regular join
    checkState(!containsInitialTemporalJoinCondition(join.getCondition))

    // Time attributes must not be in the output type of a regular join
    val timeAttrInOutput = join.getRowType.getFieldList
      .exists(f => FlinkTypeFactory.isTimeIndicatorType(f.getType))
    checkState(!timeAttrInOutput)

    // Join condition must not access time attributes
    val remainingPredsAccessTime = accessesTimeAttribute(
      join.getCondition, combineJoinInputsRowType(join))
    checkState(!remainingPredsAccessTime)
    true
  }

  override protected def transform(
      join: FlinkLogicalJoin,
      leftInput: FlinkRelNode,
      leftConversion: RelNode => RelNode,
      rightInput: FlinkRelNode,
      rightConversion: RelNode => RelNode,
      providedTraitSet: RelTraitSet): FlinkRelNode = {
    new StreamPhysicalJoin(
      join.getCluster,
      providedTraitSet,
      leftConversion(leftInput),
      rightConversion(rightInput),
      join.getCondition,
      join.getJoinType)
  }
}

object StreamPhysicalJoinRule {
  val INSTANCE: RelOptRule = new StreamPhysicalJoinRule
}
