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
import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistributionTraitDef
import org.apache.flink.table.planner.plan.cost.{FlinkCost, FlinkCostFactory}
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSortMergeJoin
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, InputProperty}
import org.apache.flink.table.planner.plan.utils.{FlinkRelMdUtil, FlinkRelOptUtil, JoinTypeUtil, JoinUtil}
import org.apache.flink.table.runtime.operators.join.FlinkJoinType

import org.apache.calcite.plan._
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelCollationTraitDef, RelNode, RelWriter}
import org.apache.calcite.rex.RexNode

import scala.collection.JavaConversions._

/**
  * Batch physical RelNode for sort-merge [[Join]].
  */
class BatchPhysicalSortMergeJoin(
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
  extends BatchPhysicalJoinBase(cluster, traitSet, leftRel, rightRel, condition, joinType) {

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
    new BatchPhysicalSortMergeJoin(
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

  override def translateToExecNode(): ExecNode[_] = {
    JoinUtil.validateJoinSpec(
      joinSpec,
      FlinkTypeFactory.toLogicalRowType(left.getRowType),
      FlinkTypeFactory.toLogicalRowType(right.getRowType))

    new BatchExecSortMergeJoin(
      JoinTypeUtil.getFlinkJoinType(joinType),
      joinSpec.getLeftKeys,
      joinSpec.getRightKeys,
      joinSpec.getFilterNulls,
      condition,
      estimateOutputSize(getLeft) < estimateOutputSize(getRight),
      InputProperty.builder().damBehavior(InputProperty.DamBehavior.END_INPUT).build(),
      InputProperty.builder().damBehavior(InputProperty.DamBehavior.END_INPUT).build(),
      FlinkTypeFactory.toLogicalRowType(getRowType),
      getRelDetailedDescription
    )
  }

  private def estimateOutputSize(relNode: RelNode): Double = {
    val mq = relNode.getCluster.getMetadataQuery
    mq.getAverageRowSize(relNode) * mq.getRowCount(relNode)
  }
}
