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
import org.apache.flink.table.planner.plan.utils.{IntervalJoinUtil, TemporalJoinUtil}
import org.apache.flink.table.planner.plan.utils.WindowJoinUtil.containsWindowStartEqualityAndEndEquality

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
    if (!join.getJoinType.projectsRight) {
      // SEMI/ANTI join always converts to StreamExecJoin now
      return true
    }
    val left: FlinkLogicalRel = call.rel(1).asInstanceOf[FlinkLogicalRel]
    val right: FlinkLogicalRel = call.rel(2).asInstanceOf[FlinkLogicalRel]
    val joinRowType = join.getRowType

    if (left.isInstanceOf[FlinkLogicalSnapshot]) {
      throw new TableException(
        "Temporal table join only support apply FOR SYSTEM_TIME AS OF on the right table.")
    }

    // this rule shouldn't match temporal table join
    if (right.isInstanceOf[FlinkLogicalSnapshot] ||
      TemporalJoinUtil.containsTemporalJoinCondition(join.getCondition)) {
      return false
    }

    val (windowBounds, remainingPreds) = extractWindowBounds(join)
    if (windowBounds.isDefined) {
      return false
    }

    if (containsWindowStartEqualityAndEndEquality(join)) {
      return false
    }

    // remaining predicate must not access time attributes
    val remainingPredsAccessTime = remainingPreds.isDefined &&
      IntervalJoinUtil.accessesTimeAttribute(remainingPreds.get, joinRowType)

    val rowTimeAttrInOutput = joinRowType.getFieldList
      .exists(f => FlinkTypeFactory.isRowtimeIndicatorType(f.getType))
    if (rowTimeAttrInOutput) {
      throw new TableException(
        "Rowtime attributes must not be in the input rows of a regular join. " +
          "As a workaround you can cast the time attributes of input tables to TIMESTAMP before.")
    }

    // joins require an equality condition
    // or a conjunctive predicate with at least one equality condition
    // and disable outer joins with non-equality predicates(see FLINK-5520)
    // And do not accept a FlinkLogicalTemporalTableSourceScan as right input
    !remainingPredsAccessTime
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
