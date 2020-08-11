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
import org.apache.flink.table.planner.codegen.CodeGeneratorContext
import org.apache.flink.table.planner.codegen.ProjectionCodeGenerator.generateProjection
import org.apache.flink.table.planner.codegen.sort.SortCodeGenerator
import org.apache.flink.table.planner.delegation.BatchPlanner
import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistributionTraitDef
import org.apache.flink.table.planner.plan.cost.{FlinkCost, FlinkCostFactory}
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode
import org.apache.flink.table.planner.plan.utils.{FlinkRelMdUtil, FlinkRelOptUtil, JoinUtil, SortUtil}
import org.apache.flink.table.runtime.operators.join.{FlinkJoinType, SortMergeJoinOperator}
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.logical.RowType

import org.apache.calcite.plan._
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelCollationTraitDef, RelNode, RelWriter}
import org.apache.calcite.rex.RexNode

import java.util

import scala.collection.JavaConversions._

/**
  * Batch physical RelNode for sort-merge [[Join]].
  */
class BatchExecSortMergeJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    leftRel: RelNode,
    rightRel: RelNode,
    condition: RexNode,
    joinType: JoinRelType,
    // true if LHS is sorted by left join keys, else false
    val leftSorted: Boolean,
    // true if RHS is sorted by right join key, else false
    val rightSorted: Boolean)
  extends BatchExecJoinBase(cluster, traitSet, leftRel, rightRel, condition, joinType) {

  protected lazy val (leftAllKey, rightAllKey) =
    JoinUtil.checkAndGetJoinKeys(keyPairs, getLeft, getRight)

  protected def isMergeJoinSupportedType(joinRelType: FlinkJoinType): Boolean = {
    joinRelType == FlinkJoinType.INNER ||
      joinRelType == FlinkJoinType.LEFT ||
      joinRelType == FlinkJoinType.RIGHT ||
      joinRelType == FlinkJoinType.FULL
  }

  override def copy(
      traitSet: RelTraitSet,
      conditionExpr: RexNode,
      left: RelNode,
      right: RelNode,
      joinType: JoinRelType,
      semiJoinDone: Boolean): Join = {
    new BatchExecSortMergeJoin(
      cluster,
      traitSet,
      left,
      right,
      conditionExpr,
      joinType,
      leftSorted,
      rightSorted)
  }

  override def explainTerms(pw: RelWriter): RelWriter =
    super.explainTerms(pw)
      .itemIf("leftSorted", leftSorted, leftSorted)
      .itemIf("rightSorted", rightSorted, rightSorted)

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val leftRowCnt = mq.getRowCount(getLeft)
    val rightRowCnt = mq.getRowCount(getRight)
    if (leftRowCnt == null || rightRowCnt == null) {
      return null
    }
    val numOfSort = joinInfo.leftKeys.size()
    val leftSortCpuCost: Double = if (leftSorted) {
      // cost of writing lhs data to buffer
      leftRowCnt
    } else {
      // sort cost
      FlinkCost.COMPARE_CPU_COST * numOfSort * leftRowCnt * Math.max(Math.log(leftRowCnt), 1.0)
    }
    val rightSortCpuCost: Double = if (rightSorted) {
      // cost of writing rhs data to buffer
      rightRowCnt
    } else {
      // sort cost
      FlinkCost.COMPARE_CPU_COST * numOfSort * rightRowCnt * Math.max(Math.log(rightRowCnt), 1.0)
    }
    // cost of evaluating each join condition
    val joinConditionCpuCost = FlinkCost.COMPARE_CPU_COST * (leftRowCnt + rightRowCnt)
    val cpuCost = leftSortCpuCost + rightSortCpuCost + joinConditionCpuCost
    val costFactory = planner.getCostFactory.asInstanceOf[FlinkCostFactory]
    // assume memory is big enough, so sort process and mergeJoin process will not spill to disk.
    var sortMemCost = 0D
    if (!leftSorted) {
      sortMemCost += FlinkRelMdUtil.computeSortMemory(mq, getLeft)
    }
    if (!rightSorted) {
      sortMemCost += FlinkRelMdUtil.computeSortMemory(mq, getRight)
    }
    val rowCount = mq.getRowCount(this)
    costFactory.makeCost(rowCount, cpuCost, 0, 0, sortMemCost)
  }

  override def satisfyTraits(requiredTraitSet: RelTraitSet): Option[RelNode] = {
    val requiredDistribution = requiredTraitSet.getTrait(FlinkRelDistributionTraitDef.INSTANCE)
    val (canSatisfyDistribution, leftRequiredDistribution, rightRequiredDistribution) =
      satisfyHashDistributionOnNonBroadcastJoin(requiredDistribution)
    if (!canSatisfyDistribution) {
      return None
    }

    val requiredCollation = requiredTraitSet.getTrait(RelCollationTraitDef.INSTANCE)
    val requiredFieldCollations = requiredCollation.getFieldCollations
    val shuffleKeysSize = leftRequiredDistribution.getKeys.size

    val newLeft = RelOptRule.convert(getLeft, leftRequiredDistribution)
    val newRight = RelOptRule.convert(getRight, rightRequiredDistribution)

    // SortMergeJoin can provide collation trait, check whether provided collation can satisfy
    // required collations
    val canProvideCollation = if (requiredCollation.getFieldCollations.isEmpty) {
      false
    } else if (requiredFieldCollations.size > shuffleKeysSize) {
      // Sort by [a, b] can satisfy [a], but cannot satisfy [a, b, c]
      false
    } else {
      val leftKeys = leftRequiredDistribution.getKeys
      val leftFieldCnt = getLeft.getRowType.getFieldCount
      val rightKeys = rightRequiredDistribution.getKeys.map(_ + leftFieldCnt)
      requiredFieldCollations.zipWithIndex.forall { case (collation, index) =>
        val idxOfCollation = collation.getFieldIndex
        // Full outer join is handled before, so does not need care about it
        if (idxOfCollation < leftFieldCnt && joinType != JoinRelType.RIGHT) {
          val fieldCollationOnLeftSortKey = FlinkRelOptUtil.ofRelFieldCollation(leftKeys.get(index))
          collation == fieldCollationOnLeftSortKey
        } else if (idxOfCollation >= leftFieldCnt &&
          (joinType == JoinRelType.RIGHT || joinType == JoinRelType.INNER)) {
          val fieldCollationOnRightSortKey =
            FlinkRelOptUtil.ofRelFieldCollation(rightKeys.get(index))
          collation == fieldCollationOnRightSortKey
        } else {
          false
        }
      }
    }
    var newProvidedTraitSet = getTraitSet.replace(requiredDistribution)
    if (canProvideCollation) {
      newProvidedTraitSet = newProvidedTraitSet.replace(requiredCollation)
    }
    Some(copy(newProvidedTraitSet, Seq(newLeft, newRight)))
  }

  //~ ExecNode methods -----------------------------------------------------------

  /**
    * Now must be full dam without two input operator chain.
    * TODO two input operator chain will return different value.
    */
  override def getDamBehavior: DamBehavior = DamBehavior.FULL_DAM

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
    val leftInput = getInputNodes.get(0).translateToPlan(planner)
        .asInstanceOf[Transformation[RowData]]
    val rightInput = getInputNodes.get(1).translateToPlan(planner)
        .asInstanceOf[Transformation[RowData]]

    val leftType = leftInput.getOutputType.asInstanceOf[InternalTypeInfo[RowData]].toRowType
    val rightType = rightInput.getOutputType.asInstanceOf[InternalTypeInfo[RowData]].toRowType

    val keyType = RowType.of(leftAllKey.map(leftType.getChildren.get(_)): _*)

    val condFunc = JoinUtil.generateConditionFunction(
      config,
      cluster.getRexBuilder,
      getJoinInfo,
      leftType,
      rightType)

    val externalBufferMemory = MemorySize.parse(config.getConfiguration.getString(
      ExecutionConfigOptions.TABLE_EXEC_RESOURCE_EXTERNAL_BUFFER_MEMORY)).getBytes
    val sortMemory = MemorySize.parse(config.getConfiguration.getString(
      ExecutionConfigOptions.TABLE_EXEC_RESOURCE_SORT_MEMORY)).getBytes
    val externalBufferNum = if (flinkJoinType == FlinkJoinType.FULL) 2 else 1

    val managedMemory = externalBufferMemory * externalBufferNum + sortMemory * 2

    def newSortGen(originalKeys: Array[Int], t: RowType): SortCodeGenerator = {
      val originalOrders = originalKeys.map(_ => true)
      val (keys, orders, nullsIsLast) = SortUtil.deduplicateSortKeys(
        originalKeys,
        originalOrders,
        SortUtil.getNullDefaultOrders(originalOrders))
      val types = keys.map(t.getTypeAt)
      new SortCodeGenerator(config, keys, types, orders, nullsIsLast)
    }

    val leftSortGen = newSortGen(leftAllKey, leftType)
    val rightSortGen = newSortGen(rightAllKey, rightType)

    val operator = new SortMergeJoinOperator(
      externalBufferMemory.toDouble / managedMemory,
      flinkJoinType,
      estimateOutputSize(getLeft) < estimateOutputSize(getRight),
      condFunc,
      generateProjection(
        CodeGeneratorContext(config), "SMJProjection", leftType, keyType, leftAllKey),
      generateProjection(
        CodeGeneratorContext(config), "SMJProjection", rightType, keyType, rightAllKey),
      leftSortGen.generateNormalizedKeyComputer("LeftComputer"),
      leftSortGen.generateRecordComparator("LeftComparator"),
      rightSortGen.generateNormalizedKeyComputer("RightComputer"),
      rightSortGen.generateRecordComparator("RightComparator"),
      newSortGen(leftAllKey.indices.toArray, keyType).generateRecordComparator("KeyComparator"),
      filterNulls)

    ExecNode.createTwoInputTransformation(
      leftInput,
      rightInput,
      getRelDetailedDescription,
      SimpleOperatorFactory.of(operator),
      InternalTypeInfo.of(FlinkTypeFactory.toLogicalRowType(getRowType)),
      rightInput.getParallelism,
      managedMemory)
  }

  private def estimateOutputSize(relNode: RelNode): Double = {
    val mq = relNode.getCluster.getMetadataQuery
    mq.getAverageRowSize(relNode) * mq.getRowCount(relNode)
  }
}
