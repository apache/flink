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

package org.apache.flink.table.plan.rules.physical.stream

import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.plan.FlinkJoinRelType
import org.apache.flink.table.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.logical._
import org.apache.flink.table.plan.nodes.physical.stream.StreamExecTemporalTableFunctionJoin
import org.apache.flink.table.plan.schema.BaseRowSchema
import org.apache.flink.table.plan.util.TemporalJoinUtil.containsTemporalJoinCondition
import org.apache.flink.table.runtime.join.WindowJoinUtil

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.JoinRelType

import java.util

class StreamExecTemporalTableFunctionJoinRule
  extends RelOptRule(
    operand(
      classOf[FlinkLogicalJoin],
      operand(classOf[FlinkLogicalRel], any()),
      operand(classOf[FlinkLogicalRel], any())),
    "StreamExecTemporalTableFunctionJoinRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join = call.rel[FlinkLogicalJoin](0)
    val joinInfo = join.analyzeCondition

    val (windowBounds, _) = WindowJoinUtil.extractWindowBoundsFromPredicate(
      joinInfo.getRemaining(join.getCluster.getRexBuilder),
      join.getLeft.getRowType.getFieldCount,
      join.getRowType,
      join.getCluster.getRexBuilder,
      TableConfig.DEFAULT)

    windowBounds.isEmpty && join.getJoinType == JoinRelType.INNER &&
      containsTemporalJoinCondition(join.getCondition)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val join = call.rel[FlinkLogicalJoin](0)
    val left = call.rel[FlinkLogicalRel](1)
    val right = call.rel[FlinkLogicalRel](2)

    val traitSet: RelTraitSet = join.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)
    val joinInfo = join.analyzeCondition

    def toHashTraitByColumns(columns: util.Collection[_ <: Number], inputTraitSets: RelTraitSet) = {
      val distribution = if (columns.size() == 0) {
        FlinkRelDistribution.SINGLETON
      } else {
        FlinkRelDistribution.hash(columns)
      }
      inputTraitSets.
      replace(FlinkConventions.STREAM_PHYSICAL).
      replace(distribution)
    }
    val (leftRequiredTrait, rightRequiredTrait) = (
      toHashTraitByColumns(joinInfo.leftKeys, left.getTraitSet),
      toHashTraitByColumns(joinInfo.rightKeys, right.getTraitSet))

    val convLeft: RelNode = RelOptRule.convert(left, leftRequiredTrait)
    val convRight: RelNode = RelOptRule.convert(right, rightRequiredTrait)

    val leftRowSchema = new BaseRowSchema(convLeft.getRowType)
    val rightRowSchema = new BaseRowSchema(convRight.getRowType)

    val temporalJoin = new StreamExecTemporalTableFunctionJoin(
      join.getCluster,
      traitSet,
      convLeft,
      convRight,
      join.getCondition,
      joinInfo,
      leftRowSchema,
      rightRowSchema,
      new BaseRowSchema(join.getRowType),
      FlinkJoinRelType.toFlinkJoinRelType(join.getJoinType),
      description)

    call.transformTo(temporalJoin)
  }
}

object StreamExecTemporalTableFunctionJoinRule {
  val INSTANCE: RelOptRule = new StreamExecTemporalTableFunctionJoinRule
}
