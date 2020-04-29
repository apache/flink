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

import org.apache.flink.api.dag.Transformation
import org.apache.flink.configuration.MemorySize
import org.apache.flink.runtime.operators.DamBehavior
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.ProjectionCodeGenerator.generateProjection
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, LongHashJoinGenerator}
import org.apache.flink.table.planner.delegation.BatchPlanner
import org.apache.flink.table.planner.plan.`trait`.{FlinkRelDistribution, FlinkRelDistributionTraitDef}
import org.apache.flink.table.planner.plan.cost.{FlinkCost, FlinkCostFactory}
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode
import org.apache.flink.table.planner.plan.utils.{FlinkRelMdUtil, JoinUtil}
import org.apache.flink.table.runtime.operators.join.{HashJoinOperator, HashJoinType}
import org.apache.flink.table.runtime.typeutils.{RowDataTypeInfo, BinaryRowDataSerializer}
import org.apache.flink.table.types.logical.RowType

import org.apache.calcite.plan._
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rex.RexNode
import org.apache.calcite.util.Util

import java.util

import scala.collection.JavaConversions._

/**
  * Batch physical RelNode for hash [[Join]].
  */
class BatchExecHashJoin(
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
  extends BatchExecJoinBase(cluster, traitSet, leftRel, rightRel, condition, joinType) {

  private val (leftKeys, rightKeys) =
    JoinUtil.checkAndGetJoinKeys(keyPairs, getLeft, getRight, allowEmptyKey = true)
  val (buildKeys, probeKeys) = if (leftIsBuild) (leftKeys, rightKeys) else (rightKeys, leftKeys)

  // Inputs could be changed. See [[BiRel.replaceInput]].
  def buildRel: RelNode = if (leftIsBuild) getLeft else getRight
  def probeRel: RelNode = if (leftIsBuild) getRight else getLeft

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
    new BatchExecHashJoin(
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
    super.explainTerms(pw)
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
      (rightRowCnt,  FlinkRelMdUtil.binaryRowAverageSize(getRight))
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
      getCluster.getPlanner
        .emptyTraitSet
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

  //~ ExecNode methods -----------------------------------------------------------

  override def getDamBehavior: DamBehavior = {
    if (hashJoinType.buildLeftSemiOrAnti()) DamBehavior.FULL_DAM else DamBehavior.MATERIALIZING
  }

  override def getInputNodes: util.List[ExecNode[BatchPlanner, _]] =
    getInputs.map(_.asInstanceOf[ExecNode[BatchPlanner, _]])

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[BatchPlanner, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  override protected def translateToPlanInternal(
      planner: BatchPlanner): Transformation[RowData] = {
    val config = planner.getTableConfig

    val lInput = getInputNodes.get(0).translateToPlan(planner)
        .asInstanceOf[Transformation[RowData]]
    val rInput = getInputNodes.get(1).translateToPlan(planner)
        .asInstanceOf[Transformation[RowData]]

    // get type
    val lType = lInput.getOutputType.asInstanceOf[RowDataTypeInfo].toRowType
    val rType = rInput.getOutputType.asInstanceOf[RowDataTypeInfo].toRowType

    val keyType = RowType.of(leftKeys.map(lType.getTypeAt): _*)

    val condFunc = JoinUtil.generateConditionFunction(
      config, cluster.getRexBuilder, getJoinInfo, lType, rType)

    // projection for equals
    val lProj = generateProjection(
      CodeGeneratorContext(config), "HashJoinLeftProjection", lType, keyType, leftKeys)
    val rProj = generateProjection(
      CodeGeneratorContext(config), "HashJoinRightProjection", rType, keyType, rightKeys)

    val (build, probe, bProj, pProj, bType, pType, reverseJoin) =
      if (leftIsBuild) {
        (lInput, rInput, lProj, rProj, lType, rType, false)
      } else {
        (rInput, lInput, rProj, lProj, rType, lType, true)
      }
    val mq = getCluster.getMetadataQuery

    val buildRowSize = Util.first(mq.getAverageRowSize(buildRel), 24).toInt
    val buildRowCount = Util.first(mq.getRowCount(buildRel), 200000).toLong
    val probeRowCount = Util.first(mq.getRowCount(probeRel), 200000).toLong

    // operator
    val operator = if (LongHashJoinGenerator.support(hashJoinType, keyType, filterNulls)) {
      LongHashJoinGenerator.gen(
        config,
        hashJoinType,
        keyType,
        bType,
        pType,
        buildKeys,
        probeKeys,
        buildRowSize,
        buildRowCount,
        reverseJoin,
        condFunc)
    } else {
      SimpleOperatorFactory.of(HashJoinOperator.newHashJoinOperator(
        hashJoinType,
        condFunc,
        reverseJoin,
        filterNulls,
        bProj,
        pProj,
        tryDistinctBuildRow,
        buildRowSize,
        buildRowCount,
        probeRowCount,
        keyType
      ))
    }

    val managedMemory = MemorySize.parse(config.getConfiguration.getString(
      ExecutionConfigOptions.TABLE_EXEC_RESOURCE_HASH_JOIN_MEMORY)).getBytes
    ExecNode.createTwoInputTransformation(
      build,
      probe,
      getRelDetailedDescription,
      operator,
      RowDataTypeInfo.of(FlinkTypeFactory.toLogicalRowType(getRowType)),
      probe.getParallelism,
      managedMemory)
  }
}
