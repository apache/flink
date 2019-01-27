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
package org.apache.flink.table.plan.nodes.physical.batch

import org.apache.flink.runtime.operators.DamBehavior
import org.apache.flink.streaming.api.transformations.TwoInputTransformation.ReadOrder
import org.apache.flink.streaming.api.transformations.{StreamTransformation, TwoInputTransformation}
import org.apache.flink.table.api.BatchTableEnvironment
import org.apache.flink.table.api.types.{RowType, TypeConverters}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.CodeGeneratorContext
import org.apache.flink.table.codegen.ProjectionCodeGenerator.generateProjection
import org.apache.flink.table.codegen.operator.LongHashJoinGenerator
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.`trait`.{FlinkRelDistribution, FlinkRelDistributionTraitDef}
import org.apache.flink.table.plan.cost.FlinkBatchCost._
import org.apache.flink.table.plan.cost.FlinkCostFactory
import org.apache.flink.table.plan.nodes.exec.batch.BatchExecNodeVisitor
import org.apache.flink.table.plan.nodes.{ExpressionFormat, FlinkConventions}
import org.apache.flink.table.plan.util.JoinUtil
import org.apache.flink.table.runtime.join.batch.hashtable.BinaryHashBucketArea
import org.apache.flink.table.runtime.join.batch.{HashJoinOperator, HashJoinType}
import org.apache.flink.table.typeutils.BinaryRowSerializer
import org.apache.flink.table.util.NodeResourceUtil

import org.apache.calcite.plan._
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rex.RexNode
import org.apache.calcite.util.{ImmutableIntList, Util}

import scala.collection.JavaConversions._

trait BatchExecHashJoinBase extends BatchExecJoinBase {

  val leftIsBuild: Boolean
  val isBroadcast: Boolean
  val tryDistinctBuildRow: Boolean
  var haveInsertRf: Boolean

  private val (leftKeys, rightKeys) =
    JoinUtil.checkAndGetKeys(keyPairs, getLeft, getRight, allowEmpty = true)
  val (buildKeys, probeKeys) = if (leftIsBuild) (leftKeys, rightKeys) else (rightKeys, leftKeys)

  // Inputs could be changed. See [[BiRel.replaceInput]].
  def buildRel: RelNode = if (leftIsBuild) getLeft else getRight
  def probeRel: RelNode = if (leftIsBuild) getRight else getLeft

  val hashJoinType: HashJoinType = HashJoinType.of(flinkJoinType, leftIsBuild)

  def insertRuntimeFilter(): Unit = {
    haveInsertRf = true
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .itemIf("isBroadcast", "true", isBroadcast)
      .item("build", if (leftIsBuild) "left" else "right")
      .itemIf("tryDistinctBuildRow", "true", tryDistinctBuildRow)
  }

  override def satisfyTraitsByInput(requiredTraitSet: RelTraitSet): RelNode = {
    if (!isBroadcast) {
      pushDownTraitsIntoNonBroadcastHashJoin(requiredTraitSet)
    } else {
      pushDownTraitsIntoBroadcastJoin(requiredTraitSet, leftIsBuild)
    }
  }

  private def pushDownTraitsIntoNonBroadcastHashJoin(requiredTraitSet: RelTraitSet): RelNode = {
    val requiredDistribution = requiredTraitSet.getTrait(FlinkRelDistributionTraitDef.INSTANCE)
    val (canPushDown, leftDistribution, rightDistribution) =
      pushDownHashDistributionIntoNonBroadcastJoin(requiredDistribution)
    if (!canPushDown) {
      return null
    }
    val toRestrictHashDistributionByKeys = (distribution: FlinkRelDistribution) =>
      getCluster.getPlanner
        .emptyTraitSet
        .replace(FlinkConventions.BATCH_PHYSICAL)
        .replace(distribution)
    val leftRequiredTrait = toRestrictHashDistributionByKeys(leftDistribution)
    val rightRequiredTrait = toRestrictHashDistributionByKeys(rightDistribution)
    val newLeft = RelOptRule.convert(getLeft, leftRequiredTrait)
    val newRight = RelOptRule.convert(getRight, rightRequiredTrait)
    // Can not push down collation into HashJoin.
    copy(getTraitSet.replace(requiredDistribution), Seq(newLeft, newRight))
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val leftRowCnt = mq.getRowCount(getLeft)
    val rightRowCnt = mq.getRowCount(getRight)
    if (leftRowCnt == null || rightRowCnt == null) {
      return null
    }
    // assume memory is big enough to load into all build size data, spill will not happen.
    // count in network cost of Exchange node before build size child here
    val cpuCost = HASH_CPU_COST * (leftRowCnt + rightRowCnt)
    val (buildRowCount, buildRowSize) = if (leftIsBuild) {
      (leftRowCnt, BatchPhysicalRel.binaryRowAverageSize(getLeft))
    } else {
      (rightRowCnt,  BatchPhysicalRel.binaryRowAverageSize(getRight))
    }
    // We aim for a 200% utilization of the bucket table when all the partition buffers are full.
    val bucketSize =
      buildRowCount * BinaryHashBucketArea.RECORD_BYTES / BatchPhysicalRel.HASH_COLLISION_WEIGHT
    val recordSize = buildRowCount * (buildRowSize + BinaryRowSerializer.LENGTH_SIZE_IN_BYTES)
    val memCost = (bucketSize + recordSize) * shuffleBuildCount(mq)
    val costFactory = planner.getCostFactory.asInstanceOf[FlinkCostFactory]
    costFactory.makeCost(mq.getRowCount(this), cpuCost, 0, 0, memCost)
  }

  private[flink] def shuffleBuildCount(mq: RelMetadataQuery): Int = {
    val probeRel = if (leftIsBuild) getRight else getLeft
    if (isBroadcast) {
      val rowCount = Util.first(mq.getRowCount(probeRel), 1)
      val shuffleCount =
        rowCount * mq.getAverageRowSize(probeRel) / SQL_DEFAULT_PARALLELISM_WORKER_PROCESS_SIZE
      Math.max(1, shuffleCount.toInt)
    } else {
      1
    }
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getDamBehavior: DamBehavior = {
    if (hashJoinType.buildLeftSemiOrAnti()) DamBehavior.FULL_DAM else DamBehavior.MATERIALIZING
  }

  override def accept(visitor: BatchExecNodeVisitor): Unit = visitor.visit(this)

  /**
    * Internal method, translates the [[org.apache.flink.table.plan.nodes.exec.BatchExecNode]]
    * into a Batch operator.
    *
    * @param tableEnv The [[BatchTableEnvironment]] of the translated Table.
    */
  override def translateToPlanInternal(
      tableEnv: BatchTableEnvironment): StreamTransformation[BaseRow] = {

    val config = tableEnv.getConfig

    val lInput = getInputNodes.get(0).translateToPlan(tableEnv)
      .asInstanceOf[StreamTransformation[BaseRow]]
    val rInput = getInputNodes.get(1).translateToPlan(tableEnv)
      .asInstanceOf[StreamTransformation[BaseRow]]

    // get type
    val lType = TypeConverters.createInternalTypeFromTypeInfo(
      lInput.getOutputType).asInstanceOf[RowType]
    val rType = TypeConverters.createInternalTypeFromTypeInfo(
      rInput.getOutputType).asInstanceOf[RowType]

    val keyType = new RowType(leftKeys.map(lType.getFieldTypes()(_)): _*)
    val managedMemorySize = getResource.getReservedManagedMem *
        NodeResourceUtil.SIZE_IN_MB
    val maxMemorySize = getResource.getMaxManagedMem *
        NodeResourceUtil.SIZE_IN_MB
    val condFunc = generateConditionFunction(config, lType, rType)

    // projection for equals
    val lProj = generateProjection(
      CodeGeneratorContext(config), "HashJoinLeftProjection", lType, keyType, leftKeys.toArray)
    val rProj = generateProjection(
      CodeGeneratorContext(config), "HashJoinRightProjection", rType, keyType, rightKeys.toArray)

    val (build, probe, bProj, pProj, bType, pType, reverseJoin) =
      if (leftIsBuild) {
        (lInput, rInput, lProj, rProj, lType, rType, false)
      } else {
        (rInput, lInput, rProj, lProj, rType, lType, true)
      }
    val perRequestSize =
      NodeResourceUtil.getPerRequestManagedMemory(config.getConf) * NodeResourceUtil.SIZE_IN_MB
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
        buildKeys.toArray,
        probeKeys.toArray,
        managedMemorySize,
        maxMemorySize,
        perRequestSize,
        buildRowSize,
        buildRowCount,
        reverseJoin,
        condFunc)
    } else {
      HashJoinOperator.newHashJoinOperator(
        managedMemorySize,
        maxMemorySize,
        perRequestSize,
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
      )
    }

    val transformation = new TwoInputTransformation[BaseRow, BaseRow, BaseRow](
      build,
      probe,
      getOperatorName,
      operator,
      FlinkTypeFactory.toInternalBaseRowTypeInfo(getRowType),
      getResource.getParallelism)
    tableEnv.getRUKeeper.addTransformation(this, transformation)
    transformation.setDamBehavior(getDamBehavior)
    transformation.setReadOrderHint(ReadOrder.INPUT1_FIRST)
    transformation.setResources(getResource.getReservedResourceSpec,
      getResource.getPreferResourceSpec)
    transformation
  }

  private def getOperatorName: String = {
    val inFields = inputDataType.getFieldNames.toList
    val joinExpressionStr = if (getCondition != null) {
      s"where: ${getExpressionString(getCondition, inFields, None, ExpressionFormat.Infix)}, "
    } else {
      ""
    }
    s"HashJoin($joinExpressionStr${if (leftIsBuild) "buildLeft" else "buildRight"})"
  }
}

class BatchExecHashJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    left: RelNode,
    right: RelNode,
    val leftIsBuild: Boolean,
    joinCondition: RexNode,
    joinType: JoinRelType,
    val isBroadcast: Boolean,
    val description: String,
    override var haveInsertRf: Boolean = false)
  extends Join(cluster, traitSet, left, right, joinCondition, Set.empty[CorrelationId], joinType)
  with BatchExecHashJoinBase {

  override val tryDistinctBuildRow = false

  override def copy(
      traitSet: RelTraitSet,
      conditionExpr: RexNode,
      left: RelNode,
      right: RelNode,
      joinType: JoinRelType,
      semiJoinDone: Boolean): Join =
    new BatchExecHashJoin(
      cluster,
      traitSet,
      left,
      right,
      leftIsBuild,
      conditionExpr,
      joinType,
      isBroadcast,
      description,
      haveInsertRf)
}

class BatchExecHashSemiJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    left: RelNode,
    right: RelNode,
    val leftIsBuild: Boolean,
    joinCondition: RexNode,
    leftKeys: ImmutableIntList,
    rightKeys: ImmutableIntList,
    isAntiJoin: Boolean,
    val isBroadcast: Boolean,
    val tryDistinctBuildRow: Boolean,
    val description: String,
    override var haveInsertRf: Boolean = false)
  extends SemiJoin(cluster, traitSet, left, right, joinCondition, leftKeys, rightKeys, isAntiJoin)
  with BatchExecHashJoinBase {

  override def copy(
      traitSet: RelTraitSet,
      condition: RexNode,
      left: RelNode,
      right: RelNode,
      joinType: JoinRelType,
      semiJoinDone: Boolean): SemiJoin = {
    val joinInfo = JoinInfo.of(left, right, condition)
    new BatchExecHashSemiJoin(
      cluster,
      traitSet,
      left,
      right,
      leftIsBuild,
      condition,
      joinInfo.leftKeys,
      joinInfo.rightKeys,
      isAnti,
      isBroadcast,
      tryDistinctBuildRow,
      description,
      haveInsertRf)
  }
}
