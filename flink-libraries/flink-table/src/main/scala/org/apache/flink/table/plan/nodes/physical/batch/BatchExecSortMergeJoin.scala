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
import org.apache.flink.table.api.BatchTableEnvironment
import org.apache.flink.table.api.types.{DataTypes, RowType, TypeConverters}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.{CodeGeneratorContext, GeneratedSorter, ProjectionCodeGenerator, SortCodeGenerator}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.FlinkJoinRelType
import org.apache.flink.table.plan.`trait`.FlinkRelDistributionTraitDef
import org.apache.flink.table.plan.cost.FlinkBatchCost._
import org.apache.flink.table.plan.cost.FlinkCostFactory
import org.apache.flink.table.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.plan.nodes.ExpressionFormat
import org.apache.flink.table.plan.nodes.exec.batch.BatchExecNodeVisitor
import org.apache.flink.table.plan.util.{JoinUtil, SortUtil}
import org.apache.flink.table.runtime.aggregate.RelFieldCollations
import org.apache.flink.table.runtime.join.batch.{MergeJoinOperator, OneSideSortMergeJoinOperator, SortMergeJoinOperator}
import org.apache.flink.table.runtime.sort.BinaryExternalSorter
import org.apache.flink.table.typeutils.TypeUtils
import org.apache.flink.table.util.NodeResourceUtil
import org.apache.flink.table.util.NodeResourceUtil.InferMode

import org.apache.calcite.plan._
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelCollationTraitDef, RelNode, RelWriter}
import org.apache.calcite.rex.RexNode
import org.apache.calcite.util.ImmutableIntList

import scala.collection.JavaConversions._

trait BatchExecSortMergeJoinBase extends BatchExecJoinBase {

  val leftSorted: Boolean
  val rightSorted: Boolean

  def isMergeJoinSupportedType(tpe: FlinkJoinRelType): Boolean =
    tpe == FlinkJoinRelType.INNER ||
      tpe == FlinkJoinRelType.LEFT ||
      tpe == FlinkJoinRelType.RIGHT ||
      tpe == FlinkJoinRelType.FULL

  val smjType: SortMergeJoinType.Value = {
    (leftSorted, rightSorted) match {
      case (true, true) if isMergeJoinSupportedType(flinkJoinType) =>
        SortMergeJoinType.MergeJoin
      case (false, true) //TODO support more
        if flinkJoinType == FlinkJoinRelType.INNER ||
          flinkJoinType == FlinkJoinRelType.RIGHT =>
        SortMergeJoinType.SortLeftJoin
      case (true, false) //TODO support more
        if flinkJoinType == FlinkJoinRelType.INNER ||
          flinkJoinType == FlinkJoinRelType.LEFT =>
        SortMergeJoinType.SortRightJoin
      case _ => SortMergeJoinType.SortMergeJoin
    }
  }

  lazy val (leftAllKey, rightAllKey) = JoinUtil.checkAndGetKeys(keyPairs, getLeft, getRight)

  override def explainTerms(pw: RelWriter): RelWriter =
    super.explainTerms(pw)
      .itemIf("leftSorted", leftSorted, leftSorted)
      .itemIf("rightSorted", rightSorted, rightSorted)

  override def satisfyTraitsByInput(requiredTraitSet: RelTraitSet): RelNode = {
    val requiredDistribution = requiredTraitSet.getTrait(FlinkRelDistributionTraitDef.INSTANCE)
    val (canDistributionPushDown, leftDistribution, rightDistribution) =
      pushDownHashDistributionIntoNonBroadcastJoin(requiredDistribution)
    if (!canDistributionPushDown) {
      return null
    }
    val requiredCollation = requiredTraitSet.getTrait(RelCollationTraitDef.INSTANCE)
    val requiredFieldCollations = requiredCollation.getFieldCollations
    val shuffleKeysSize = leftDistribution.getKeys.size

    val newLeft = RelOptRule.convert(getLeft, leftDistribution)
    val newRight = RelOptRule.convert(getRight, rightDistribution)

    // SortMergeJoin can provide collation trait, check whether provided collation can satisfy
    // required collations
    val canCollationPushDown = if (requiredCollation.getFieldCollations.isEmpty) {
      false
    } else if (requiredFieldCollations.size > shuffleKeysSize) {
      // Sort by [a, b] can satisfy [a], but cannot satisfy [a, b, c]
      false
    } else {
      val leftKeys = leftDistribution.getKeys
      val leftFieldCnt = getLeft.getRowType.getFieldCount
      val rightKeys = rightDistribution.getKeys.map(_ + leftFieldCnt)
      requiredFieldCollations.zipWithIndex.forall { case (fc, index) =>
        val cfi = fc.getFieldIndex
        if (cfi < leftFieldCnt && flinkJoinType != FlinkJoinRelType.RIGHT) {
          val fieldCollationOnLeftSortKey = RelFieldCollations.of(leftKeys.get(index))
          fc == fieldCollationOnLeftSortKey
        } else if (cfi >= leftFieldCnt &&
          (flinkJoinType == FlinkJoinRelType.RIGHT ||
              flinkJoinType == FlinkJoinRelType.INNER)) {
           val fieldCollationOnRightSortKey = RelFieldCollations.of(rightKeys.get(index))
           fc == fieldCollationOnRightSortKey
        } else {
          false
        }
      }
    }
    var newProvidedTraitSet = getTraitSet.replace(requiredDistribution)
    if (canCollationPushDown) {
      newProvidedTraitSet = newProvidedTraitSet.replace(requiredCollation)
    }
    copy(newProvidedTraitSet, Seq(newLeft, newRight))
  }

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
      COMPARE_CPU_COST * numOfSort * leftRowCnt * Math.max(Math.log(leftRowCnt), 1.0)
    }
    val rightSortCpuCost: Double = if (rightSorted) {
      // cost of writing rhs data to buffer
      rightRowCnt
    } else {
      // sort cost
      COMPARE_CPU_COST * numOfSort * rightRowCnt * Math.max(Math.log(rightRowCnt), 1.0)
    }
    // cost of evaluating each join condition
    val joinConditionCpuCost = COMPARE_CPU_COST * (leftRowCnt + rightRowCnt)
    val cpuCost = leftSortCpuCost + rightSortCpuCost + joinConditionCpuCost
    val costFactory = planner.getCostFactory.asInstanceOf[FlinkCostFactory]
    // assume memory is big enough, so sort process and mergeJoin process will not spill to disk.
    var sortMemCost = 0D
    if (!leftSorted) {
      sortMemCost += SortUtil.calcNeedMemoryForSort(mq, getLeft)
    }
    if (!rightSorted) {
      sortMemCost += SortUtil.calcNeedMemoryForSort(mq, getRight)
    }
    costFactory.makeCost(mq.getRowCount(this), cpuCost, 0, 0, sortMemCost)
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getDamBehavior: DamBehavior = {
    if (!leftSorted && !rightSorted) {
      DamBehavior.FULL_DAM
    } else if (!leftSorted || !rightSorted) {
      DamBehavior.MATERIALIZING
    } else {
      DamBehavior.PIPELINED
    }
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

    if (getLeft.isInstanceOf[BatchExecSort] || getRight.isInstanceOf[BatchExecSort]) {
      // SortMergeJoin with inner sort is more efficient than SortMergeJoin with outer sort
      LOG.warn("This will not happen under normal case. The plan is correct, but not efficient. " +
        "Correct the cost model to choose a more efficient plan.")
    }

    val config = tableEnv.getConfig

    val leftInput = getInputNodes.get(0).translateToPlan(tableEnv)
      .asInstanceOf[StreamTransformation[BaseRow]]
    val rightInput = getInputNodes.get(1).translateToPlan(tableEnv)
      .asInstanceOf[StreamTransformation[BaseRow]]

    val leftType = TypeConverters.createInternalTypeFromTypeInfo(
      leftInput.getOutputType).asInstanceOf[RowType]
    val rightType = TypeConverters.createInternalTypeFromTypeInfo(
      rightInput.getOutputType).asInstanceOf[RowType]

    val keyType = new RowType(
      leftAllKey.map(leftType.getFieldTypes()(_)): _*)

    val condFunc = generateConditionFunction(config, leftType, rightType)

    val externalBufferMemory = NodeResourceUtil.getExternalBufferManagedMemory(config.getConf)
    val externalBufferMemorySize = externalBufferMemory * NodeResourceUtil.SIZE_IN_MB

    val perRequestSize =
      NodeResourceUtil.getPerRequestManagedMemory(config.getConf)* NodeResourceUtil.SIZE_IN_MB
    val infer = NodeResourceUtil.getInferMode(config.getConf).equals(InferMode.ALL)

    val totalReservedSortMemory = (getResource.getReservedManagedMem -
      externalBufferMemory * getExternalBufferNum) * NodeResourceUtil.SIZE_IN_MB

    val totalMaxSortMemory = (getResource.getMaxManagedMem -
      externalBufferMemory * getExternalBufferNum) * NodeResourceUtil.SIZE_IN_MB

    val leftRatio = if (infer) inferLeftRowCountRatio else 0.5d

    val leftReservedSortMemorySize = calcSortMemory(leftRatio, totalReservedSortMemory)
    val rightReservedSortMemorySize = totalReservedSortMemory - leftReservedSortMemorySize
    val leftMaxSortMemorySize = calcSortMemory(leftRatio, totalMaxSortMemory)
    val rightMaxSortMemorySize = totalMaxSortMemory - leftMaxSortMemorySize

    // sort code gen
    val operator = smjType match {
      case SortMergeJoinType.MergeJoin =>
        new MergeJoinOperator(
          leftReservedSortMemorySize, rightReservedSortMemorySize,
          flinkJoinType,
          condFunc,
          ProjectionCodeGenerator.generateProjection(
            CodeGeneratorContext(config), "MJProjection",
            leftType, keyType, leftAllKey.toArray),
          ProjectionCodeGenerator.generateProjection(
            CodeGeneratorContext(config), "MJProjection",
            rightType, keyType, rightAllKey.toArray),
          newGeneratedSorter(leftAllKey.indices.toArray, keyType),
          filterNulls)

      case SortMergeJoinType.SortLeftJoin | SortMergeJoinType.SortRightJoin =>
        val (reservedSortMemory, mergeBufferMemory, maxSortMemory, sortKeys) = if (rightSorted) {
          (leftReservedSortMemorySize,
              rightReservedSortMemorySize,
              leftMaxSortMemorySize,
              leftAllKey.toArray)
        } else {
          (rightReservedSortMemorySize,
              leftReservedSortMemorySize,
              rightMaxSortMemorySize,
              rightAllKey.toArray)
        }

        new OneSideSortMergeJoinOperator(
          reservedSortMemory, maxSortMemory,
          perRequestSize, mergeBufferMemory, externalBufferMemorySize,
          flinkJoinType, rightSorted, condFunc,
          ProjectionCodeGenerator.generateProjection(
            CodeGeneratorContext(config), "OneSideSMJProjection",
            leftType, keyType, leftAllKey.toArray),
          ProjectionCodeGenerator.generateProjection(
            CodeGeneratorContext(config), "OneSideSMJProjection",
            rightType, keyType, rightAllKey.toArray),
          newGeneratedSorter(sortKeys, leftType),
          newGeneratedSorter(leftAllKey.indices.toArray, keyType),
          filterNulls)

      case _ =>
        new SortMergeJoinOperator(
          leftReservedSortMemorySize, leftMaxSortMemorySize,
          rightReservedSortMemorySize, rightMaxSortMemorySize,
          perRequestSize, externalBufferMemorySize,
          flinkJoinType, estimateOutputSize(getLeft) < estimateOutputSize(getRight), condFunc,
          ProjectionCodeGenerator.generateProjection(
            CodeGeneratorContext(config), "SMJProjection", leftType, keyType, leftAllKey.toArray),
          ProjectionCodeGenerator.generateProjection(
            CodeGeneratorContext(config), "SMJProjection", rightType, keyType, rightAllKey.toArray),
          newGeneratedSorter(leftAllKey.toArray, leftType),
          newGeneratedSorter(rightAllKey.toArray, rightType),
          newGeneratedSorter(leftAllKey.indices.toArray, keyType),
          filterNulls)
    }

    val transformation = new TwoInputTransformation[BaseRow, BaseRow, BaseRow](
      leftInput,
      rightInput,
      getOperatorName,
      operator,
      FlinkTypeFactory.toInternalBaseRowTypeInfo(getRowType),
      getResource.getParallelism)
    tableEnv.getRUKeeper.addTransformation(this, transformation)
    if (!leftSorted && !rightSorted) {
      transformation.setDamBehavior(DamBehavior.FULL_DAM)
    }
    transformation.setResources(getResource.getReservedResourceSpec,
      getResource.getPreferResourceSpec)
    transformation
  }

  private def inferLeftRowCountRatio: Double = {
    val mq = FlinkRelMetadataQuery.reuseOrCreate(getCluster.getMetadataQuery)
    val leftRowCnt = mq.getRowCount(getLeft)
    val rightRowCnt = mq.getRowCount(getRight)
    if (leftRowCnt == null || rightRowCnt == null) {
      0.5d
    } else {
      leftRowCnt / (rightRowCnt + leftRowCnt)
    }
  }

  private def calcSortMemory(ratio: Double, totalSortMemory: Long): Long = {
    val minGuaranteedMemory = BinaryExternalSorter.SORTER_MIN_NUM_SORT_MEM
    val maxGuaranteedMemory = totalSortMemory - BinaryExternalSorter.SORTER_MIN_NUM_SORT_MEM
    val inferLeftSortMemory = (totalSortMemory * ratio).toLong
    Math.max(Math.min(inferLeftSortMemory, maxGuaranteedMemory), minGuaranteedMemory)
  }

  private[flink] def getExternalBufferNum: Int = {
    if (flinkJoinType == FlinkJoinRelType.FULL) 2 else 1
  }

  private[flink] def getSortNum: Int = {
    (if (leftSorted) 0 else 1) + (if (rightSorted) 0 else 1)
  }

  private def newGeneratedSorter(originalKeys: Array[Int], t: RowType): GeneratedSorter = {
    val originalOrders = originalKeys.map(_ => true)
    val (keys, orders, nullsIsLast) = SortUtil.deduplicateSortKeys(
      originalKeys,
      originalOrders,
      SortUtil.getNullDefaultOrders(originalOrders))

    val types = keys.map(t.getFieldInternalTypes()(_))
    val compAndSers = types.zip(orders).map { case (internalType, order) =>
      (TypeUtils.createInternalComparator(internalType, order),
          DataTypes.createInternalSerializer(internalType))
    }
    val comps = compAndSers.map(_._1)
    val sers = compAndSers.map(_._2)

    val gen = new SortCodeGenerator(keys, types, comps, orders, nullsIsLast)
    GeneratedSorter(
      gen.generateNormalizedKeyComputer("SMJComputer"),
      gen.generateRecordComparator("SMJComparator"),
      sers, comps)
  }

  private def estimateOutputSize(relNode: RelNode): Double = {
    val mq = relNode.getCluster.getMetadataQuery
    mq.getAverageRowSize(relNode) * mq.getRowCount(relNode)
  }

  private def getOperatorName: String = if (getCondition != null) {
    val inFields = inputDataType.getFieldNames.toList
    s"SortMergeJoin(where: ${
      getExpressionString(getCondition, inFields, None, ExpressionFormat.Infix)})"
  } else {
    "SortMergeJoin"
  }
}

object SortMergeJoinType extends Enumeration{
  type SortMergeJoinType = Value
  val MergeJoin, SortLeftJoin, SortRightJoin, SortMergeJoin = Value
}

class BatchExecSortMergeJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    left: RelNode,
    right: RelNode,
    joinCondition: RexNode,
    joinType: JoinRelType,
    override val leftSorted: Boolean,
    override val rightSorted: Boolean,
    val description: String)
  extends Join(cluster, traitSet, left, right, joinCondition, Set.empty[CorrelationId], joinType)
  with BatchExecSortMergeJoinBase {

  override def copy(
      traitSet: RelTraitSet,
      conditionExpr: RexNode,
      left: RelNode,
      right: RelNode,
      joinType: JoinRelType,
      semiJoinDone: Boolean): Join =
    new BatchExecSortMergeJoin(
      cluster,
      traitSet,
      left,
      right,
      conditionExpr,
      joinType,
      leftSorted,
      rightSorted,
      description)
}

class BatchExecSortMergeSemiJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    left: RelNode,
    right: RelNode,
    joinCondition: RexNode,
    leftKeys: ImmutableIntList,
    rightKeys: ImmutableIntList,
    isAntiJoin: Boolean,
    override val leftSorted: Boolean,
    override val rightSorted: Boolean,
    val description: String)
  extends SemiJoin(cluster, traitSet, left, right, joinCondition, leftKeys, rightKeys, isAntiJoin)
  with BatchExecSortMergeJoinBase {

  override def copy(
      traitSet: RelTraitSet,
      condition: RexNode,
      left: RelNode,
      right: RelNode,
      joinType: JoinRelType,
      semiJoinDone: Boolean): SemiJoin = {
    val joinInfo = JoinInfo.of(left, right, condition)
    new BatchExecSortMergeSemiJoin(
      cluster,
      traitSet,
      left,
      right,
      condition,
      joinInfo.leftKeys,
      joinInfo.rightKeys,
      isAnti,
      leftSorted,
      rightSorted,
      description)
  }
}
