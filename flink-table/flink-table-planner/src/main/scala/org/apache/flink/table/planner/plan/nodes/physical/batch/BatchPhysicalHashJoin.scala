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
import org.apache.flink.table.planner.plan.`trait`.{FlinkRelDistribution, FlinkRelDistributionTraitDef}
import org.apache.flink.table.planner.plan.cost.{FlinkCost, FlinkCostFactory}
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, InputProperty}
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecHashJoin
import org.apache.flink.table.planner.plan.utils.{FlinkRelMdUtil, JoinUtil}
import org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig
import org.apache.flink.table.runtime.operators.join.HashJoinType
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer

import org.apache.calcite.plan._
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rex.RexNode
import org.apache.calcite.util.Util

import scala.collection.JavaConversions._

/** Batch physical RelNode for hash [[Join]]. */
class BatchPhysicalHashJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    leftRel: RelNode,
    rightRel: RelNode,
    condition: RexNode,
    joinType: JoinRelType,
    // true if LHS is build side, else false
    val leftIsBuild: Boolean,
    // true if build side is broadcast, else false
    val isBroadcast: Boolean,
    val tryDistinctBuildRow: Boolean)
  extends BatchPhysicalJoinBase(cluster, traitSet, leftRel, rightRel, condition, joinType) {

  val hashJoinType: HashJoinType = HashJoinType.of(
    leftIsBuild,
    getJoinType.generatesNullsOnRight(),
    getJoinType.generatesNullsOnLeft(),
    getJoinType == JoinRelType.SEMI,
    getJoinType == JoinRelType.ANTI)

  override def copy(
      traitSet: RelTraitSet,
      conditionExpr: RexNode,
      left: RelNode,
      right: RelNode,
      joinType: JoinRelType,
      semiJoinDone: Boolean): Join = {
    new BatchPhysicalHashJoin(
      cluster,
      traitSet,
      left,
      right,
      conditionExpr,
      joinType,
      leftIsBuild,
      isBroadcast,
      tryDistinctBuildRow)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super
      .explainTerms(pw)
      .itemIf("isBroadcast", "true", isBroadcast)
      .item("build", if (leftIsBuild) "left" else "right")
      .itemIf("tryDistinctBuildRow", "true", tryDistinctBuildRow)
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val leftRowCnt = mq.getRowCount(getLeft)
    val rightRowCnt = mq.getRowCount(getRight)
    if (leftRowCnt == null || rightRowCnt == null) {
      return null
    }
    // assume memory is big enough to load into all build size data, spill will not happen.
    // count in network cost of Exchange node before build size child here
    val cpuCost = FlinkCost.HASH_CPU_COST * (leftRowCnt + rightRowCnt)
    val (buildRowCount, buildRowSize) = if (leftIsBuild) {
      (leftRowCnt, FlinkRelMdUtil.binaryRowAverageSize(getLeft))
    } else {
      (rightRowCnt, FlinkRelMdUtil.binaryRowAverageSize(getRight))
    }
    // We aim for a 200% utilization of the bucket table when all the partition buffers are full.
    // TODO use BinaryHashBucketArea.RECORD_BYTES instead of 8
    val bucketSize = buildRowCount * 8 / FlinkCost.HASH_COLLISION_WEIGHT
    val recordSize = buildRowCount * (buildRowSize + BinaryRowDataSerializer.LENGTH_SIZE_IN_BYTES)
    val memCost = (bucketSize + recordSize) * shuffleBuildCount(mq)
    val costFactory = planner.getCostFactory.asInstanceOf[FlinkCostFactory]
    costFactory.makeCost(mq.getRowCount(this), cpuCost, 0, 0, memCost)
  }

  private[flink] def shuffleBuildCount(mq: RelMetadataQuery): Int = {
    val probeRel = if (leftIsBuild) getRight else getLeft
    if (isBroadcast) {
      val rowCount = Util.first(mq.getRowCount(probeRel), 1)
      val shuffleCount = rowCount * mq.getAverageRowSize(probeRel) /
        FlinkCost.SQL_DEFAULT_PARALLELISM_WORKER_PROCESS_SIZE
      Math.max(1, shuffleCount.toInt)
    } else {
      1
    }
  }

  override def satisfyTraits(requiredTraitSet: RelTraitSet): Option[RelNode] = {
    if (!isBroadcast) {
      satisfyTraitsOnNonBroadcastHashJoin(requiredTraitSet)
    } else {
      satisfyTraitsOnBroadcastJoin(requiredTraitSet, leftIsBuild)
    }
  }

  private def satisfyTraitsOnNonBroadcastHashJoin(
      requiredTraitSet: RelTraitSet): Option[RelNode] = {
    val requiredDistribution = requiredTraitSet.getTrait(FlinkRelDistributionTraitDef.INSTANCE)
    val (canSatisfyDistribution, leftRequiredDistribution, rightRequiredDistribution) =
      satisfyHashDistributionOnNonBroadcastJoin(requiredDistribution)
    if (!canSatisfyDistribution) {
      return None
    }

    val toRestrictHashDistributionByKeys = (distribution: FlinkRelDistribution) =>
      getCluster.getPlanner.emptyTraitSet
        .replace(FlinkConventions.BATCH_PHYSICAL)
        .replace(distribution)
    val leftRequiredTraits = toRestrictHashDistributionByKeys(leftRequiredDistribution)
    val rightRequiredTraits = toRestrictHashDistributionByKeys(rightRequiredDistribution)
    val newLeft = RelOptRule.convert(getLeft, leftRequiredTraits)
    val newRight = RelOptRule.convert(getRight, rightRequiredTraits)
    val providedTraits = getTraitSet.replace(requiredDistribution)
    // HashJoin can not satisfy collation.
    Some(copy(providedTraits, Seq(newLeft, newRight)))
  }

  override def translateToExecNode(): ExecNode[_] = {
    JoinUtil.validateJoinSpec(
      joinSpec,
      FlinkTypeFactory.toLogicalRowType(left.getRowType),
      FlinkTypeFactory.toLogicalRowType(right.getRowType))

    val mq = getCluster.getMetadataQuery
    val leftRowSize = Util.first(mq.getAverageRowSize(left), 24).toInt
    val leftRowCount = Util.first(mq.getRowCount(left), 200000).toLong
    val rightRowSize = Util.first(mq.getAverageRowSize(right), 24).toInt
    val rightRowCount = Util.first(mq.getRowCount(right), 200000).toLong
    val (leftEdge, rightEdge) = getInputProperties
    new BatchExecHashJoin(
      unwrapTableConfig(this),
      joinSpec,
      leftRowSize,
      rightRowSize,
      leftRowCount,
      rightRowCount,
      isBroadcast,
      leftIsBuild,
      tryDistinctBuildRow,
      leftEdge,
      rightEdge,
      FlinkTypeFactory.toLogicalRowType(getRowType),
      getRelDetailedDescription)
  }

  private def getInputProperties: (InputProperty, InputProperty) = {
    val (buildRequiredDistribution, probeRequiredDistribution) = if (isBroadcast) {
      (InputProperty.BROADCAST_DISTRIBUTION, InputProperty.ANY_DISTRIBUTION)
    } else {
      val leftKeys = joinSpec.getLeftKeys
      val rightKeys = joinSpec.getRightKeys
      val (buildKeys, probeKeys) = if (leftIsBuild) (leftKeys, rightKeys) else (rightKeys, leftKeys)
      (InputProperty.hashDistribution(buildKeys), InputProperty.hashDistribution(probeKeys))
    }
    val probeDamBehavior = if (hashJoinType.buildLeftSemiOrAnti()) {
      InputProperty.DamBehavior.END_INPUT
    } else {
      InputProperty.DamBehavior.PIPELINED
    }
    val buildEdge = InputProperty
      .builder()
      .requiredDistribution(buildRequiredDistribution)
      .damBehavior(InputProperty.DamBehavior.BLOCKING)
      .priority(0)
      .build()
    val probeEdge = InputProperty
      .builder()
      .requiredDistribution(probeRequiredDistribution)
      .damBehavior(probeDamBehavior)
      .priority(1)
      .build()

    if (leftIsBuild) {
      (buildEdge, probeEdge)
    } else {
      (probeEdge, buildEdge)
    }
  }
}
