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
import org.apache.flink.streaming.api.transformations.{StreamTransformation, TwoInputTransformation}
import org.apache.flink.table.`type`.{RowType, TypeConverters}
import org.apache.flink.table.api.{BatchTableEnvironment, TableConfigOptions}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.CodeGeneratorContext
import org.apache.flink.table.codegen.ProjectionCodeGenerator.generateProjection
import org.apache.flink.table.codegen.sort.SortCodeGenerator
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.`trait`.FlinkRelDistributionTraitDef
import org.apache.flink.table.plan.cost.{FlinkCost, FlinkCostFactory}
import org.apache.flink.table.plan.nodes.ExpressionFormat
import org.apache.flink.table.plan.nodes.exec.ExecNode
import org.apache.flink.table.plan.util.{FlinkRelMdUtil, FlinkRelOptUtil, JoinUtil, SortUtil}
import org.apache.flink.table.runtime.join.{FlinkJoinType, SortMergeJoinOperator}

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

  protected lazy val smjType: SortMergeJoinType.Value = {
    (leftSorted, rightSorted) match {
      case (true, true) if isMergeJoinSupportedType(flinkJoinType) =>
        SortMergeJoinType.MergeJoin
      case (false, true) //TODO support more
        if flinkJoinType == FlinkJoinType.INNER || flinkJoinType == FlinkJoinType.RIGHT =>
        SortMergeJoinType.SortLeftJoin
      case (true, false) //TODO support more
        if flinkJoinType == FlinkJoinType.INNER || flinkJoinType == FlinkJoinType.LEFT =>
        SortMergeJoinType.SortRightJoin
      case _ => SortMergeJoinType.SortMergeJoin
    }
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

  override def getInputNodes: util.List[ExecNode[BatchTableEnvironment, _]] =
    getInputs.map(_.asInstanceOf[ExecNode[BatchTableEnvironment, _]])

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[BatchTableEnvironment, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  override def translateToPlanInternal(
      tableEnv: BatchTableEnvironment): StreamTransformation[BaseRow] = {

    val config = tableEnv.getConfig

    val leftInput = getInputNodes.get(0).translateToPlan(tableEnv)
        .asInstanceOf[StreamTransformation[BaseRow]]
    val rightInput = getInputNodes.get(1).translateToPlan(tableEnv)
        .asInstanceOf[StreamTransformation[BaseRow]]

    val leftType = TypeConverters.createInternalTypeFromTypeInfo(
      leftInput.getOutputType).asInstanceOf[RowType]
    val rightType = TypeConverters.createInternalTypeFromTypeInfo(
      rightInput.getOutputType).asInstanceOf[RowType]

    val keyType = new RowType(leftAllKey.map(leftType.getFieldTypes()(_)): _*)

    val condFunc = generateCondition(config, leftType, rightType)

    val externalBufferMemory = config.getConf.getInteger(
      TableConfigOptions.SQL_RESOURCE_EXTERNAL_BUFFER_MEM) * TableConfigOptions.SIZE_IN_MB

    val sortMemory = config.getConf.getInteger(
      TableConfigOptions.SQL_RESOURCE_SORT_BUFFER_MEM) * TableConfigOptions.SIZE_IN_MB

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
      sortMemory,
      sortMemory,
      externalBufferMemory,
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

    new TwoInputTransformation[BaseRow, BaseRow, BaseRow](
      leftInput,
      rightInput,
      getOperatorName,
      operator,
      FlinkTypeFactory.toInternalRowType(getRowType).toTypeInfo,
      tableEnv.getConfig.getConf.getInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM))
  }

  private def estimateOutputSize(relNode: RelNode): Double = {
    val mq = relNode.getCluster.getMetadataQuery
    mq.getAverageRowSize(relNode) * mq.getRowCount(relNode)
  }

  private def getOperatorName: String = if (getCondition != null) {
    val inFields = inputRowType.getFieldNames.toList
    s"SortMergeJoin(where: ${
      getExpressionString(getCondition, inFields, None, ExpressionFormat.Infix)})"
  } else {
    "SortMergeJoin"
  }
}


object SortMergeJoinType extends Enumeration {
  type SortMergeJoinType = Value
  // both LHS and RHS have been sorted
  val MergeJoin,
  // RHS has been sorted, only LHS needs sort
  SortLeftJoin,
  // LHS has been sorted, only RHS needs sort
  SortRightJoin,
  // both LHS and RHS need sort
  SortMergeJoin = Value
}
