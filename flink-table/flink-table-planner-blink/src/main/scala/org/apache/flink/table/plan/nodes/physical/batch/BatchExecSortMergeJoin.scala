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

import org.apache.flink.table.plan.FlinkJoinRelType
import org.apache.flink.table.plan.cost.{FlinkCost, FlinkCostFactory}
import org.apache.flink.table.plan.util.{FlinkRelMdUtil, FlinkRelOptUtil}

import org.apache.calcite.plan._
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rex.RexNode

import scala.collection.JavaConversions._

/**
  * Batch physical RelNode for sort-merge [[Join]].
  */
trait BatchExecSortMergeJoinBase extends BatchExecJoinBase {

  // true if LHS is sorted by left join keys, else false
  val leftSorted: Boolean
  // true if RHS is sorted by right join key, else false
  val rightSorted: Boolean

  protected lazy val (leftAllKey, rightAllKey) =
    FlinkRelOptUtil.checkAndGetJoinKeys(keyPairs, getLeft, getRight)

  protected def isMergeJoinSupportedType(joinRelType: FlinkJoinRelType): Boolean = {
    joinRelType == FlinkJoinRelType.INNER ||
      joinRelType == FlinkJoinRelType.LEFT ||
      joinRelType == FlinkJoinRelType.RIGHT ||
      joinRelType == FlinkJoinRelType.FULL
  }

  protected lazy val smjType: SortMergeJoinType.Value = {
    (leftSorted, rightSorted) match {
      case (true, true) if isMergeJoinSupportedType(flinkJoinType) =>
        SortMergeJoinType.MergeJoin
      case (false, true) //TODO support more
        if flinkJoinType == FlinkJoinRelType.INNER || flinkJoinType == FlinkJoinRelType.RIGHT =>
        SortMergeJoinType.SortLeftJoin
      case (true, false) //TODO support more
        if flinkJoinType == FlinkJoinRelType.INNER || flinkJoinType == FlinkJoinRelType.LEFT =>
        SortMergeJoinType.SortRightJoin
      case _ => SortMergeJoinType.SortMergeJoin
    }
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

class BatchExecSortMergeJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    leftRel: RelNode,
    rightRel: RelNode,
    condition: RexNode,
    joinType: JoinRelType,
    override val leftSorted: Boolean,
    override val rightSorted: Boolean)
  extends Join(cluster, traitSet, leftRel, rightRel, condition, Set.empty[CorrelationId], joinType)
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
      rightSorted)
}
