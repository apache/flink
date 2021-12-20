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
import org.apache.flink.table.planner.plan.nodes.exec.spec.IntervalJoinSpec.WindowBounds
import org.apache.flink.table.planner.plan.nodes.logical.{FlinkLogicalJoin, FlinkLogicalRel}
import org.apache.flink.table.planner.plan.nodes.{FlinkConventions, FlinkRelNode}
import org.apache.flink.table.planner.plan.utils.{FlinkRelOptUtil, IntervalJoinUtil, JoinUtil}

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rex.RexNode

import java.util

/**
 * Base implementation for rules match stream-stream join, including
 * regular stream join, interval join and temporal join.
 */
abstract class StreamPhysicalJoinRuleBase(description: String)
  extends RelOptRule(
    operand(classOf[FlinkLogicalJoin],
      operand(classOf[FlinkLogicalRel], any()),
      operand(classOf[FlinkLogicalRel], any())),
    description) {

  protected def extractWindowBounds(join: FlinkLogicalJoin):
    (Option[WindowBounds], Option[RexNode]) = {
    val tableConfig = FlinkRelOptUtil.getTableConfigFromContext(join)
    val rowType = JoinUtil.combineJoinInputsRowType(join)
    IntervalJoinUtil.extractWindowBoundsFromPredicate(
      join.getCondition,
      join.getLeft.getRowType.getFieldCount,
      rowType,
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

    def convertInput(input: RelNode, columns: util.Collection[_ <: Number]): RelNode = {
      val requiredTraitSet = toHashTraitByColumns(columns, input.getTraitSet)
      RelOptRule.convert(input, requiredTraitSet)
    }

    val newJoin = transform(
      join,
      left,
      leftInput => {
        convertInput(leftInput, computeJoinLeftKeys(join))
      },
      right,
      rightInput => {
        convertInput(rightInput, computeJoinRightKeys(join))
      },
      join.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)
    )
    call.transformTo(newJoin)
  }

  protected def computeJoinLeftKeys(join: FlinkLogicalJoin): util.Collection[Integer] =
    join.analyzeCondition().leftKeys

  protected def computeJoinRightKeys(join: FlinkLogicalJoin): util.Collection[Integer] =
    join.analyzeCondition().rightKeys

  protected def transform(
      join: FlinkLogicalJoin,
      leftInput: FlinkRelNode,
      leftConversion: RelNode => RelNode,
      rightInput: FlinkRelNode,
      rightConversion: RelNode => RelNode,
      providedTraitSet: RelTraitSet): FlinkRelNode
}
