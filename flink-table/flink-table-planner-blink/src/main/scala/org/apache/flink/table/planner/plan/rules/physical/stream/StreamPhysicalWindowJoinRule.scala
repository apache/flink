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

import org.apache.flink.table.planner.plan.logical.WindowAttachedWindowingStrategy
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.{FlinkLogicalJoin, FlinkLogicalRel}
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowJoin
import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.planner.plan.utils.WindowJoinUtil.{containsWindowStartEqualityAndEndEquality, excludeWindowStartEqualityAndEndEqualityFromJoinCondition, getChildWindowProperties}

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode

import java.util

/**
 * Rule to convert a [[FlinkLogicalJoin]] into a [[StreamPhysicalWindowJoin]].
 */
class StreamPhysicalWindowJoinRule
  extends RelOptRule(
    operand(classOf[FlinkLogicalJoin],
      operand(classOf[FlinkLogicalRel], any()),
      operand(classOf[FlinkLogicalRel], any())),
    "StreamPhysicalWindowJoinRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join = call.rel[FlinkLogicalJoin](0)
    containsWindowStartEqualityAndEndEquality(join)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {

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

    val join = call.rel[FlinkLogicalJoin](0)

    val (
      windowStartEqualityLeftKeys,
      windowEndEqualityLeftKeys,
      windowStartEqualityRightKeys,
      windowEndEqualityRightKeys,
      remainLeftKeys,
      remainRightKeys,
      remainCondition) = excludeWindowStartEqualityAndEndEqualityFromJoinCondition(join)
    val providedTraitSet: RelTraitSet = join.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)

    val left = call.rel[FlinkLogicalRel](1)
    val right = call.rel[FlinkLogicalRel](2)
    val newLeft = convertInput(left, remainLeftKeys)
    val newRight = convertInput(right, remainRightKeys)

    val (leftWindowProperties, rightWindowProperties) = getChildWindowProperties(join)
    // It's safe to directly get first element from windowStartEqualityLeftKeys because window
    // start equality is required in join condition for window join.
    val leftWindowing = new WindowAttachedWindowingStrategy(
      leftWindowProperties.getWindowSpec,
      leftWindowProperties.getTimeAttributeType,
      windowStartEqualityLeftKeys.getInt(0),
      windowEndEqualityLeftKeys.getInt(0))
    val rightWindowing = new WindowAttachedWindowingStrategy(
      rightWindowProperties.getWindowSpec,
      rightWindowProperties.getTimeAttributeType,
      windowStartEqualityRightKeys.getInt(0),
      windowEndEqualityRightKeys.getInt(0))

    val newWindowJoin = new StreamPhysicalWindowJoin(
      join.getCluster,
      providedTraitSet,
      newLeft,
      newRight,
      join.getJoinType,
      remainCondition,
      leftWindowing,
      rightWindowing)
    call.transformTo(newWindowJoin)
  }

}

object StreamPhysicalWindowJoinRule {
  val INSTANCE: RelOptRule = new StreamPhysicalWindowJoinRule
}
