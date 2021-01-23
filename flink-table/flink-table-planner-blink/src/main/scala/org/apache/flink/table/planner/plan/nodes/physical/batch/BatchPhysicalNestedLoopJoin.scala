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

package org.apache.flink.table.planner.plan.nodes.physical.batch

import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.cost.{FlinkCost, FlinkCostFactory}
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecNestedLoopJoin
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, InputProperty}
import org.apache.flink.table.planner.plan.utils.JoinTypeUtil
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer

import org.apache.calcite.plan._
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rex.RexNode

/**
  * Batch physical RelNode for nested-loop [[Join]].
  */
class BatchPhysicalNestedLoopJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    leftRel: RelNode,
    rightRel: RelNode,
    condition: RexNode,
    joinType: JoinRelType,
    // true if LHS is build side, else RHS is build side
    val leftIsBuild: Boolean,
    // true if one side returns single row, else false
    val singleRowJoin: Boolean)
  extends BatchPhysicalJoinBase(cluster, traitSet, leftRel, rightRel, condition, joinType) {

  override def copy(
      traitSet: RelTraitSet,
      conditionExpr: RexNode,
      left: RelNode,
      right: RelNode,
      joinType: JoinRelType,
      semiJoinDone: Boolean): Join = {
    new BatchPhysicalNestedLoopJoin(
      cluster,
      traitSet,
      left,
      right,
      conditionExpr,
      joinType,
      leftIsBuild,
      singleRowJoin)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("build", if (leftIsBuild) "left" else "right")
      .itemIf("singleRowJoin", singleRowJoin, singleRowJoin)
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val leftRowCnt = mq.getRowCount(getLeft)
    val rightRowCnt = mq.getRowCount(getRight)
    if (leftRowCnt == null || rightRowCnt == null) {
      return null
    }

    val buildRel = if (leftIsBuild) getLeft else getRight
    val buildRows = mq.getRowCount(buildRel)
    val buildRowSize = mq.getAverageRowSize(buildRel)
    val memoryCost = buildRows *
      (buildRowSize + BinaryRowDataSerializer.LENGTH_SIZE_IN_BYTES) * shuffleBuildCount(mq)
    val cpuCost = leftRowCnt * rightRowCnt
    val costFactory = planner.getCostFactory.asInstanceOf[FlinkCostFactory]
    val cost = costFactory.makeCost(mq.getRowCount(this), cpuCost, 0, 0, memoryCost)
    if (singleRowJoin) {
      // Make single row join more preferable than non-single row join.
      cost.multiplyBy(0.99)
    } else {
      cost
    }
  }

  private def shuffleBuildCount(mq: RelMetadataQuery): Int = {
    val probeRel = if (leftIsBuild) getRight else getLeft
    val rowCount = mq.getRowCount(probeRel)
    if (rowCount == null) {
      1
    } else {
      val probeRowSize = mq.getAverageRowSize(probeRel)
      Math.max(1,
        (rowCount * probeRowSize / FlinkCost.SQL_DEFAULT_PARALLELISM_WORKER_PROCESS_SIZE).toInt)
    }
  }

  override def satisfyTraits(requiredTraitSet: RelTraitSet): Option[RelNode] = {
    // Assume NestedLoopJoin always broadcast data from child which smaller.
    satisfyTraitsOnBroadcastJoin(requiredTraitSet, leftIsBuild)
  }

  override def translateToExecNode(): ExecNode[_] = {
    val (leftInputProperty, rightInputProperty) = getInputProperties
    new BatchExecNestedLoopJoin(
      JoinTypeUtil.getFlinkJoinType(joinType),
      condition,
      leftIsBuild,
      singleRowJoin,
      leftInputProperty,
      rightInputProperty,
      FlinkTypeFactory.toLogicalRowType(getRowType),
      getRelDetailedDescription
    )
  }

  def getInputProperties: (InputProperty, InputProperty) = {
    // this is in sync with BatchExecNestedLoopJoinRuleBase#createNestedLoopJoin
    val (buildRequiredDistribution, probeRequiredDistribution) = if (joinType == JoinRelType.FULL) {
      (InputProperty.SINGLETON_DISTRIBUTION, InputProperty.SINGLETON_DISTRIBUTION)
    } else {
      (InputProperty.BROADCAST_DISTRIBUTION, InputProperty.ANY_DISTRIBUTION)
    }
    val buildInputProperty = InputProperty.builder()
      .requiredDistribution(buildRequiredDistribution)
      .damBehavior(InputProperty.DamBehavior.BLOCKING)
      .priority(0)
      .build()
    val probeInputProperty = InputProperty.builder()
      .requiredDistribution(probeRequiredDistribution)
      .damBehavior(InputProperty.DamBehavior.PIPELINED)
      .priority(1)
      .build()

    if (leftIsBuild) {
      (buildInputProperty, probeInputProperty)
    } else {
      (probeInputProperty, buildInputProperty)
    }
  }
}
