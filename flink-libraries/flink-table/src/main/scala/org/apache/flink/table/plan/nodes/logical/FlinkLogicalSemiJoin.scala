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

package org.apache.flink.table.plan.nodes.logical

import org.apache.flink.table.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.util.FlinkRexUtil

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.{JoinInfo, JoinRelType, SemiJoin}
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rex.RexNode
import org.apache.calcite.util.ImmutableIntList

class FlinkLogicalSemiJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    left: RelNode,
    right: RelNode,
    condition: RexNode,
    leftKeys: ImmutableIntList,
    rightKeys: ImmutableIntList,
    isAntiJoin: Boolean)
  extends SemiJoin(cluster, traitSet, left, right, condition, leftKeys, rightKeys, isAntiJoin)
  with FlinkLogicalRel {

  override def copy(
    traitSet: RelTraitSet,
    condition: RexNode,
    left: RelNode,
    right: RelNode,
    joinType: JoinRelType,
    semiJoinDone: Boolean): SemiJoin = {
    assert(joinType eq JoinRelType.INNER)
    val joinInfo: JoinInfo = JoinInfo.of(left, right, condition)
    new FlinkLogicalSemiJoin(getCluster, traitSet, left, right, condition,
      joinInfo.leftKeys, joinInfo.rightKeys, isAnti)
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val leftRowCnt = mq.getRowCount(getLeft)
    val leftRowSize = mq.getAverageRowSize(getLeft)
    val rightRowCnt = mq.getRowCount(getRight)
    val cpuCost = leftRowCnt + rightRowCnt
    val ioCost = (leftRowCnt * leftRowSize) + rightRowCnt
    planner.getCostFactory.makeCost(leftRowCnt, cpuCost, ioCost)
  }

  override def isDeterministic: Boolean = FlinkRexUtil.isDeterministicOperator(getCondition)
}

private class FlinkLogicalSemiJoinConverter
  extends ConverterRule(
    classOf[SemiJoin],
    Convention.NONE,
    FlinkConventions.LOGICAL,
    "FlinkLogicalSemiJoinConverter") {

  override def convert(rel: RelNode): RelNode = {
    val join = rel.asInstanceOf[SemiJoin]
    val newLeft = RelOptRule.convert(join.getLeft, FlinkConventions.LOGICAL)
    val newRight = RelOptRule.convert(join.getRight, FlinkConventions.LOGICAL)
    FlinkLogicalSemiJoin.create(
      newLeft,
      newRight,
      join.getCondition,
      join.getLeftKeys,
      join.getRightKeys,
      join.isAnti)
  }
}

object FlinkLogicalSemiJoin {
  val CONVERTER: ConverterRule = new FlinkLogicalSemiJoinConverter()

  def create(
      left: RelNode,
      right: RelNode,
      condition: RexNode,
      leftKeys: ImmutableIntList,
      rightKeys: ImmutableIntList,
      isAntiJoin: Boolean): FlinkLogicalSemiJoin = {
    val cluster = left.getCluster
    val traitSet = cluster.traitSetOf(Convention.NONE)
    // FIXME: FlinkRelMdDistribution requires the current RelNode to compute
    // the distribution trait, so we have to create FlinkLogicalSemiJoin to
    // calculate the distribution trait
    val semiJoin = new FlinkLogicalSemiJoin(
      cluster,
      traitSet,
      left,
      right,
      condition,
      leftKeys,
      rightKeys,
      isAntiJoin)
    val newTraitSet = FlinkRelMetadataQuery.traitSet(semiJoin)
      .replace(FlinkConventions.LOGICAL).simplify()
    semiJoin.copy(newTraitSet, semiJoin.getInputs).asInstanceOf[FlinkLogicalSemiJoin]
  }
}
