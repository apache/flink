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

import org.apache.flink.table.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalSemiJoin
import org.apache.flink.table.plan.nodes.physical.stream.StreamExecSemiJoin

import org.apache.calcite.plan.{RelOptRule, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.JoinInfo

import java.util

/**
  * Rule that converts [[FlinkLogicalSemiJoin]] to [[StreamExecSemiJoin]].
  */
class StreamExecSemiJoinRule
  extends ConverterRule(
    classOf[FlinkLogicalSemiJoin],
    FlinkConventions.LOGICAL,
    FlinkConventions.STREAM_PHYSICAL,
    "StreamExecSemiJoinRule") {

  override def convert(rel: RelNode): RelNode = {
    val join: FlinkLogicalSemiJoin = rel.asInstanceOf[FlinkLogicalSemiJoin]
    val joinInfo = JoinInfo.of(join.getLeft, join.getRight, join.getCondition)

    def toHashTraitByColumns(
        columns: util.Collection[_ <: Number],
        inputTraitSet: RelTraitSet): RelTraitSet = {
      val distribution = if (columns.isEmpty) {
        FlinkRelDistribution.SINGLETON
      } else {
        FlinkRelDistribution.hash(columns)
      }
      inputTraitSet
        .replace(FlinkConventions.STREAM_PHYSICAL)
        .replace(distribution)
    }

    val (leftRequiredTrait, rightRequiredTrait) = (
      toHashTraitByColumns(joinInfo.leftKeys, join.getLeft.getTraitSet),
      toHashTraitByColumns(joinInfo.rightKeys, join.getRight.getTraitSet))

    val providedTraitSet = join.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)

    val newLeft: RelNode = RelOptRule.convert(join.getInput(0), leftRequiredTrait)
    val newRight: RelNode = RelOptRule.convert(join.getInput(1), rightRequiredTrait)

    new StreamExecSemiJoin(
      rel.getCluster,
      providedTraitSet,
      newLeft,
      newRight,
      join.getCondition,
      join.isAnti
    )
  }
}

object StreamExecSemiJoinRule {
  val INSTANCE = new StreamExecSemiJoinRule
}
