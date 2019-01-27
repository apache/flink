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

package org.apache.flink.table.plan.metadata

import org.apache.flink.table.expressions.ExpressionUtils._
import org.apache.flink.table.plan.logical.{LogicalWindow, SlidingGroupWindow, TumblingGroupWindow}
import org.apache.flink.table.plan.nodes.calcite.{Expand, LogicalWindowAggregate, Rank}
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalWindowAggregate
import org.apache.flink.table.plan.nodes.physical.batch._
import org.apache.flink.table.plan.stats.ValueInterval
import org.apache.flink.table.plan.util.AggregateUtil._
import org.apache.flink.table.plan.util.FlinkRelMdUtil._
import org.apache.flink.table.plan.util.{FlinkRelMdUtil, FlinkRelOptUtil}
import org.apache.flink.table.util.NodeResourceUtil

import org.apache.calcite.adapter.enumerable.EnumerableLimit
import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.metadata._
import org.apache.calcite.rel.{RelNode, SingleRel}
import org.apache.calcite.rex.{RexLiteral, RexNode}
import org.apache.calcite.util._

import java.lang.Double

import scala.collection.JavaConversions._

class FlinkRelMdRowCount private extends MetadataHandler[BuiltInMetadata.RowCount] {

  def getDef: MetadataDef[BuiltInMetadata.RowCount] = BuiltInMetadata.RowCount.DEF

  def getRowCount(rel: Expand, mq: RelMetadataQuery): Double = rel.estimateRowCount(mq)

  def getRowCount(rel: Rank, mq: RelMetadataQuery): Double = rel.estimateRowCount(mq)

  def getRowCount(rel: Aggregate, mq: RelMetadataQuery): Double = {
    getRowCountOfAgg(rel, rel.getGroupSet, rel.getGroupSets.size(), mq)._1
  }

  /**
    * Get output rowCount and input rowCount of agg
    *
    * @param rel           agg relNode
    * @param groupSet      agg groupSet
    * @param groupSetsSize agg groupSets count
    * @param mq            metadata query
    * @return a tuple, the first element is output rowCount, second one is input rowCount
    */
  private def getRowCountOfAgg(
      rel: SingleRel,
      groupSet: ImmutableBitSet,
      groupSetsSize: Int,
      mq: RelMetadataQuery): (Double, Double) = {
    val childRowCount = mq.getRowCount(rel.getInput)
    if (groupSet.cardinality() == 0) {
      return (1.0, childRowCount)
    }

    // rowCount is the cardinality of the group by columns
    val distinctRowCount = mq.getDistinctRowCount(rel.getInput, groupSet, null)
    val groupCount = groupSet.cardinality()
    val d: Double = if (distinctRowCount == null) {
      NumberUtil.multiply(childRowCount,
        FlinkRelMdUtil.getAggregationRatioIfNdvUnavailable(groupCount))
    } else {
      NumberUtil.min(distinctRowCount, childRowCount)
    }

    if (d != null) {
      // Grouping sets multiply
      (d * groupSetsSize, childRowCount)
    } else {
      (null, childRowCount)
    }
  }

  def getRowCount(rel: BatchExecGroupAggregateBase, mq: RelMetadataQuery): Double = {
    getRowCountOfBatchExecAgg(rel, mq)
  }

  private def getRowCountOfBatchExecAgg(rel: SingleRel, mq: RelMetadataQuery): Double = {
    val (grouping, isFinal, isMerge) = rel match {
      case agg: BatchExecGroupAggregateBase =>
        (ImmutableBitSet.of(agg.getGrouping: _*), agg.isFinal, agg.isMerge)
      case windowAgg: BatchExecWindowAggregateBase =>
        (ImmutableBitSet.of(windowAgg.getGrouping: _*), windowAgg.isFinal, windowAgg.isMerge)
      case _ => throw new IllegalArgumentException(s"Unknown node type ${rel.getRelTypeName}!")
    }
    val ndvOfGroupKeysOnGlobalAgg: Double = if (grouping.isEmpty) {
      1.0
    } else {
      // rowCount is the cardinality of the group by columns
      val distinctRowCount = mq.getDistinctRowCount(rel.getInput, grouping, null)
      val childRowCount = mq.getRowCount(rel.getInput)
      if (distinctRowCount == null) {
        if (isFinal && isMerge) {
          // Avoid apply aggregation ratio twice when calculate row count of global agg
          // which has local agg.
          childRowCount
        } else {
          NumberUtil.multiply(childRowCount, getAggregationRatioIfNdvUnavailable(grouping.length))
        }
      } else {
        NumberUtil.min(distinctRowCount, childRowCount)
      }
    }
    if (isFinal) {
      ndvOfGroupKeysOnGlobalAgg
    } else {
      val childRowCount = mq.getRowCount(rel.getInput)
      val tableConfig = FlinkRelOptUtil.getTableConfig(rel)
      val nParallelism = NodeResourceUtil.calOperatorParallelism(childRowCount, tableConfig.getConf)
      if (nParallelism == 1) {
        ndvOfGroupKeysOnGlobalAgg
      } else if (grouping.isEmpty) {
        // output rowcount of local agg is parallelism for agg which has no group keys
        nParallelism.toDouble
      } else {
        val distinctRowCount = mq.getDistinctRowCount(rel.getInput, grouping, null)
        if (distinctRowCount == null) {
          ndvOfGroupKeysOnGlobalAgg
        } else {
          getRowCountOfLocalAgg(nParallelism, childRowCount, ndvOfGroupKeysOnGlobalAgg)
        }
      }
    }
  }

  def getRowCount(rel: FlinkLogicalWindowAggregate, mq: RelMetadataQuery): Double = {
    getRowCountOfWindowAgg(rel, rel.getWindow, rel.getGroupSet, mq)
  }

  def getRowCount(rel: LogicalWindowAggregate, mq: RelMetadataQuery): Double = {
    getRowCountOfWindowAgg(rel, rel.getWindow, rel.getGroupSet, mq)
  }

  def getRowCount(rel: BatchExecWindowAggregateBase, mq: RelMetadataQuery): Double = {
    val ndvOfGroupKeys = getRowCountOfBatchExecAgg(rel, mq)
    val inputRowCount = mq.getRowCount(rel.getInput)
    estimateRowCountOfWindowAgg(ndvOfGroupKeys, inputRowCount, rel.getWindow)
  }

  private def getRowCountOfWindowAgg(
    windowAgg: SingleRel,
    window: LogicalWindow,
    grouping: ImmutableBitSet,
    mq: RelMetadataQuery): Double = {
    val (ndvOfGroupKeys, inputRowCount) = getRowCountOfAgg(windowAgg, grouping, 1, mq)
    estimateRowCountOfWindowAgg(ndvOfGroupKeys, inputRowCount, window)
  }

  private def estimateRowCountOfWindowAgg(
      ndv: Double,
      inputRowCount: Double,
      window: LogicalWindow): Double = {
    if (ndv == null) {
      null
    } else {
      // simply assume expand factor of TumblingWindow/SessionWindow/SlideWindowWithoutOverlap is 2
      // SlideWindowWithOverlap is 4.
      // Introduce expand factor here to distinguish output rowCount of normal agg with all kinds of
      // window aggregates.
      val expandFactorOfTumblingWindow = 2D
      val expandFactorOfNoOverLapSlidingWindow = 2D
      val expandFactorOfOverLapSlidingWindow = 4D
      val expandFactorOfSessionWindow = 2D
      window match {
        case TumblingGroupWindow(_, _, size) if isTimeIntervalLiteral(size) =>
          Math.min(expandFactorOfTumblingWindow * ndv, inputRowCount)
        case SlidingGroupWindow(_, _, size, slide) if isTimeIntervalLiteral(size) =>
          val sizeV = asLong(size)
          val slideV = asLong(slide)
          if (sizeV > slideV) {
            // only slideWindow which has overlap may generates more records than input
            expandFactorOfOverLapSlidingWindow * ndv
          } else {
            Math.min(expandFactorOfNoOverLapSlidingWindow * ndv, inputRowCount)
          }
        case _ => Math.min(expandFactorOfSessionWindow * ndv, inputRowCount)
      }
    }
  }

  def getRowCount(rel: BatchExecOverAggregate, mq: RelMetadataQuery): Double =
    getRowCountOfOverWindow(rel, mq)

  def getRowCount(rel: Window, mq: RelMetadataQuery): Double =
    getRowCountOfOverWindow(rel, mq)

  private def getRowCountOfOverWindow(overWindow: SingleRel, mq: RelMetadataQuery): Double =
    mq.getRowCount(overWindow.getInput)

  def getRowCount(join: Join, mq: RelMetadataQuery): Double = {
    val leftChild = join.getLeft
    val rightChild = join.getRight
    val leftRowCount = mq.getRowCount(leftChild)
    val rightRowCount = mq.getRowCount(rightChild)
    if (leftRowCount == null || rightRowCount == null) {
      return null
    }

    val joinInfo = JoinInfo.of(leftChild, rightChild, join.getCondition)
    if (joinInfo.leftSet().nonEmpty) {
      val innerJoinRowCount = getEquiInnerJoinRowCount(join, mq, leftRowCount, rightRowCount)
      require(innerJoinRowCount != null)
      // Make sure outputRowCount won't be too small based on join type.
      join.getJoinType match {
        case JoinRelType.INNER => innerJoinRowCount
        case JoinRelType.LEFT =>
          // All rows from left side should be in the result.
          math.max(leftRowCount, innerJoinRowCount)
        case JoinRelType.RIGHT =>
          // All rows from right side should be in the result.
          math.max(rightRowCount, innerJoinRowCount)
        case JoinRelType.FULL =>
          // T(A FULL JOIN B) = T(A LEFT JOIN B) + T(A RIGHT JOIN B) - T(A INNER JOIN B)
          math.max(leftRowCount, innerJoinRowCount) +
            math.max(rightRowCount, innerJoinRowCount) - innerJoinRowCount
      }
    } else {
      val rexBuilder = join.getCluster.getRexBuilder
      val crossJoin = copyJoinWithNewCondition(join, rexBuilder.makeLiteral(true))
      val selectivity = mq.getSelectivity(crossJoin, join.getCondition)
      (leftRowCount * rightRowCount) * selectivity
    }
  }

  private def getEquiInnerJoinRowCount(
      join: Join,
      mq: RelMetadataQuery,
      leftRowCount: Double,
      rightRowCount: Double): Double = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val leftChild = join.getLeft
    val rightChild = join.getRight
    val rexBuilder = join.getCluster.getRexBuilder
    val condition = join.getCondition
    val joinInfo = JoinInfo.of(leftChild, rightChild, condition)
    // the leftKeys length equals to rightKeys, so it's ok to only check leftKeys length
    require(joinInfo.leftKeys.nonEmpty)

    val joinKeyDisjoint = joinInfo.leftKeys.zip(joinInfo.rightKeys).exists {
      case (leftKey, rightKey) =>
        val leftInterval = fmq.getColumnInterval(leftChild, leftKey)
        val rightInterval = fmq.getColumnInterval(rightChild, rightKey)
        if (leftInterval != null && rightInterval != null) {
          !ValueInterval.isIntersected(leftInterval, rightInterval)
        } else {
          false
        }
    }
    // One of the join key pairs is disjoint, thus the two sides of join is disjoint.
    if (joinKeyDisjoint) {
      return 0D
    }

    val leftKeySet = joinInfo.leftSet()
    val rightKeySet = joinInfo.rightSet()
    val leftNdv = fmq.getDistinctRowCount(leftChild, leftKeySet, null)
    val rightNdv = fmq.getDistinctRowCount(rightChild, rightKeySet, null)
    // estimate selectivity of non-equi
    val selectivityOfNonEquiPred: Double = if (joinInfo.isEqui) {
      1D
    } else {
      val nonEquiPred = joinInfo.getRemaining(rexBuilder)
      val equiPred = RelMdUtil.minusPreds(rexBuilder, condition, nonEquiPred)
      val joinWithOnlyEquiPred = copyJoinWithNewCondition(join, equiPred)
      fmq.getSelectivity(joinWithOnlyEquiPred, nonEquiPred)
    }

    if (leftNdv != null && rightNdv != null) {
      // selectivity of equi part is 1 / Max(leftNdv, rightNdv)
      val selectivityOfEquiPred = Math.min(1D, 1D / Math.max(leftNdv, rightNdv))
      return leftRowCount * rightRowCount * selectivityOfEquiPred * selectivityOfNonEquiPred
    }

    val leftKeysAreUnique = fmq.areColumnsUnique(leftChild, leftKeySet)
    val rightKeysAreUnique = fmq.areColumnsUnique(rightChild, rightKeySet)
    if (leftKeysAreUnique != null && rightKeysAreUnique != null &&
      (leftKeysAreUnique || rightKeysAreUnique)) {
      val outputRowCount = if (leftKeysAreUnique && rightKeysAreUnique) {
        // if both leftKeys and rightKeys are both unique,
        // rowCount = Min(leftRowCount) * selectivity of non-equi
        Math.min(leftRowCount, rightRowCount) * selectivityOfNonEquiPred
      } else if (leftKeysAreUnique) {
        rightRowCount * selectivityOfNonEquiPred
      } else {
        leftRowCount * selectivityOfNonEquiPred
      }
      return outputRowCount
    }

    // if joinCondition has no ndv stats and no uniqueKeys stats,
    // rowCount = (leftRowCount + rightRowCount) * join condition selectivity
    val crossJoin = copyJoinWithNewCondition(join, rexBuilder.makeLiteral(true))
    val selectivity = fmq.getSelectivity(crossJoin, condition)
    (leftRowCount + rightRowCount) * selectivity
  }

  private def copyJoinWithNewCondition(join: Join, newCondition: RexNode): Join = {
    join.copy(
      join.getTraitSet,
      newCondition,
      join.getLeft,
      join.getRight,
      join.getJoinType,
      join.isSemiJoinDone)
  }

  def getRowCount(rel: SemiJoin, mq: RelMetadataQuery): Double = {
    val semiJoinSelectivity = FlinkRelMdUtil.makeSemiJoinSelectivityRexNode(mq, rel)
    NumberUtil.multiply(
      mq.getSelectivity(rel.getLeft, semiJoinSelectivity),
      mq.getRowCount(rel.getLeft))
  }

  /** Catch-all implementation for
    * [[BuiltInMetadata.RowCount#getRowCount()]],
    * invoked using reflection.
    *
    * @see org.apache.calcite.rel.metadata.RelMetadataQuery#getRowCount(RelNode)
    */
  def getRowCount(rel: RelNode, mq: RelMetadataQuery): Double = rel.estimateRowCount(mq)

  def getRowCount(subset: RelSubset, mq: RelMetadataQuery): Double = {
    if (!Bug.CALCITE_1048_FIXED) {
      return mq.getRowCount(Util.first(subset.getBest, subset.getOriginal))
    }
    val v = subset.getRels.foldLeft(null.asInstanceOf[Double]) {
      (min, r) =>
        try {
          NumberUtil.min(min, mq.getRowCount(r))
        } catch {
          // ignore this rel; there will be other, non-cyclic ones
          case e: CyclicMetadataException => min
          case e: Throwable =>
            e.printStackTrace()
            min
        }
    }
    // if set is empty, estimate large
    Util.first(v, 1e6d)
  }

  def getRowCount(rel: Union, mq: RelMetadataQuery): Double = {
    val rowCounts = rel.getInputs.map(mq.getRowCount)
    if (rowCounts.contains(null)) {
      null
    } else {
      rowCounts.foldLeft(0D)(_ + _)
    }
  }

  def getRowCount(rel: Intersect, mq: RelMetadataQuery): Double = {
    rel.getInputs.foldLeft(null.asInstanceOf[Double])((res, r) => {
      val partialRowCount = mq.getRowCount(r)
      if (res == null || (partialRowCount != null && partialRowCount < res)) {
        partialRowCount
      } else {
        res
      }
    })
  }

  def getRowCount(rel: Minus, mq: RelMetadataQuery): Double = {
    rel.getInputs.foldLeft(null.asInstanceOf[Double])((res, r) => {
      val partialRowCount = mq.getRowCount(r)
      if (res == null || (partialRowCount != null && partialRowCount < res)) {
        partialRowCount
      } else {
        res
      }
    })
  }

  def getRowCount(rel: Filter, mq: RelMetadataQuery): Double =
    RelMdUtil.estimateFilteredRows(rel.getInput, rel.getCondition, mq)

  def getRowCount(rel: Calc, mq: RelMetadataQuery): Double =
    RelMdUtil.estimateFilteredRows(rel.getInput, rel.getProgram, mq)

  def getRowCount(rel: Project, mq: RelMetadataQuery): Double = mq.getRowCount(rel.getInput)

  def getRowCount(rel: Sort, mq: RelMetadataQuery): Double = {
    var rowCount = mq.getRowCount(rel.getInput)
    if (rowCount == null) {
      return null
    }
    val offset = if (rel.offset == null) 0 else RexLiteral.intValue(rel.offset)
    rowCount = Math.max(rowCount - offset, 0D)
    if (rel.fetch != null) {
      val limit = RexLiteral.intValue(rel.fetch)
      if (limit < rowCount) {
        return limit.toDouble
      }
    }
    rowCount
  }

  def getRowCount(rel: EnumerableLimit, mq: RelMetadataQuery): Double = {
    var rowCount: Double = mq.getRowCount(rel.getInput)
    if (rowCount == null) {
      return null
    }
    val offset = if (rel.offset == null) 0 else RexLiteral.intValue(rel.offset)
    rowCount = Math.max(rowCount - offset, 0D)
    if (rel.fetch != null) {
      val limit = RexLiteral.intValue(rel.fetch)
      if (limit < rowCount) {
        return limit.toDouble
      }
    }
    rowCount
  }

  def getRowCount(rel: SingleRel, mq: RelMetadataQuery): Double = mq.getRowCount(rel.getInput)

  def getRowCount(rel: TableScan, mq: RelMetadataQuery): Double = rel.estimateRowCount(mq)

  def getRowCount(rel: Values, mq: RelMetadataQuery): Double = rel.estimateRowCount(mq)

}

object FlinkRelMdRowCount {

  private val INSTANCE = new FlinkRelMdRowCount

  val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
    BuiltInMethod.ROW_COUNT.method, INSTANCE)

}
