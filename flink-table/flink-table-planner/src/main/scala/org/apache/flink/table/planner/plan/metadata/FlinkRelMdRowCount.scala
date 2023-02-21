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
package org.apache.flink.table.planner.plan.metadata

import org.apache.flink.annotation.Experimental
import org.apache.flink.configuration.ConfigOption
import org.apache.flink.configuration.ConfigOptions.key
import org.apache.flink.table.planner.plan.logical.{LogicalWindow, SlidingGroupWindow, TumblingGroupWindow}
import org.apache.flink.table.planner.plan.nodes.calcite.{Expand, Rank, WindowAggregate}
import org.apache.flink.table.planner.plan.nodes.physical.batch._
import org.apache.flink.table.planner.plan.stats.ValueInterval
import org.apache.flink.table.planner.plan.utils.{FlinkRelMdUtil, SortUtil}
import org.apache.flink.table.planner.plan.utils.AggregateUtil.{hasTimeIntervalType, toLong}
import org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig

import org.apache.calcite.adapter.enumerable.EnumerableLimit
import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.{RelNode, SingleRel}
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.metadata._
import org.apache.calcite.rex.{RexLiteral, RexNode}
import org.apache.calcite.util._

import java.lang.{Double => JDouble, Long => JLong}

import scala.collection.JavaConversions._

/**
 * FlinkRelMdRowCount supplies a implementation of [[RelMetadataQuery#getRowCount]] for the standard
 * logical algebra.
 */
class FlinkRelMdRowCount private extends MetadataHandler[BuiltInMetadata.RowCount] {

  def getDef: MetadataDef[BuiltInMetadata.RowCount] = BuiltInMetadata.RowCount.DEF

  def getRowCount(rel: TableScan, mq: RelMetadataQuery): JDouble = rel.estimateRowCount(mq)

  def getRowCount(rel: Values, mq: RelMetadataQuery): JDouble = rel.estimateRowCount(mq)

  def getRowCount(rel: Project, mq: RelMetadataQuery): JDouble = mq.getRowCount(rel.getInput)

  def getRowCount(rel: Filter, mq: RelMetadataQuery): JDouble =
    RelMdUtil.estimateFilteredRows(rel.getInput, rel.getCondition, mq)

  def getRowCount(rel: Calc, mq: RelMetadataQuery): JDouble =
    RelMdUtil.estimateFilteredRows(rel.getInput, rel.getProgram, mq)

  def getRowCount(rel: Expand, mq: RelMetadataQuery): JDouble = rel.estimateRowCount(mq)

  def getRowCount(rel: Exchange, mq: RelMetadataQuery): JDouble = rel.estimateRowCount(mq)

  def getRowCount(rel: Rank, mq: RelMetadataQuery): JDouble = rel.estimateRowCount(mq)

  def getRowCount(rel: Sort, mq: RelMetadataQuery): JDouble = {
    getRowCountOfSort(rel, rel.offset, rel.fetch, mq)
  }

  def getRowCount(rel: EnumerableLimit, mq: RelMetadataQuery): JDouble = {
    getRowCountOfSort(rel, rel.offset, rel.fetch, mq)
  }

  private def getRowCountOfSort(
      rel: SingleRel,
      offset: RexNode,
      fetch: RexNode,
      mq: RelMetadataQuery): JDouble = {
    val inputRowCount = mq.getRowCount(rel.getInput)
    if (inputRowCount == null) {
      return null
    }
    val limitStart = SortUtil.getLimitStart(offset)
    val rowCount = Math.max(inputRowCount - limitStart, 0d)
    if (fetch != null) {
      val limit = RexLiteral.intValue(fetch)
      if (limit < rowCount) {
        return limit.toDouble
      }
    }
    rowCount
  }

  def getRowCount(rel: Aggregate, mq: RelMetadataQuery): JDouble = {
    val (outputRowCnt, _) = getRowCountOfAgg(rel, rel.getGroupSet, rel.getGroupSets.size(), mq)
    outputRowCnt
  }

  /**
   * Get output rowCount and input rowCount of agg
   *
   * @param rel
   *   agg relNode
   * @param groupSet
   *   agg groupSet
   * @param groupSetsSize
   *   agg groupSets count
   * @param mq
   *   metadata query
   * @return
   *   a tuple, the first element is output rowCount, second one is input rowCount
   */
  private def getRowCountOfAgg(
      rel: SingleRel,
      groupSet: ImmutableBitSet,
      groupSetsSize: Int,
      mq: RelMetadataQuery): (JDouble, JDouble) = {
    val input = rel.getInput
    val inputRowCount = mq.getRowCount(input)
    if (groupSet.cardinality() == 0) {
      return (1.0, inputRowCount)
    }

    // rowCount is the cardinality of the group by columns
    val distinctRowCount = mq.getDistinctRowCount(input, groupSet, null)
    val groupCount = groupSet.cardinality()
    val d: JDouble = if (distinctRowCount == null) {
      val ratio = FlinkRelMdUtil.getAggregationRatioIfNdvUnavailable(groupCount)
      NumberUtil.multiply(inputRowCount, ratio)
    } else {
      NumberUtil.min(distinctRowCount, inputRowCount)
    }

    if (d != null) {
      // Grouping sets multiply
      (d * groupSetsSize, inputRowCount)
    } else {
      (null, inputRowCount)
    }
  }

  def getRowCount(rel: BatchPhysicalGroupAggregateBase, mq: RelMetadataQuery): JDouble = {
    getRowCountOfBatchExecAgg(rel, mq)
  }

  private def getRowCountOfBatchExecAgg(rel: SingleRel, mq: RelMetadataQuery): JDouble = {
    val input = rel.getInput
    val (grouping, isFinal, isMerge) = rel match {
      case agg: BatchPhysicalGroupAggregateBase =>
        (ImmutableBitSet.of(agg.grouping: _*), agg.isFinal, agg.isMerge)
      case windowAgg: BatchPhysicalWindowAggregateBase =>
        (ImmutableBitSet.of(windowAgg.grouping: _*), windowAgg.isFinal, windowAgg.isMerge)
      case _ => throw new IllegalArgumentException(s"Unknown aggregate type ${rel.getRelTypeName}!")
    }
    val ndvOfGroupKeysOnGlobalAgg: JDouble = if (grouping.isEmpty) {
      1.0
    } else {
      // rowCount is the cardinality of the group by columns
      val distinctRowCount = mq.getDistinctRowCount(input, grouping, null)
      val childRowCount = mq.getRowCount(input)
      if (distinctRowCount == null) {
        if (isFinal && isMerge) {
          // Avoid apply aggregation ratio twice when calculate row count of global agg
          // which has local agg.
          childRowCount
        } else {
          val ratio = FlinkRelMdUtil.getAggregationRatioIfNdvUnavailable(grouping.length)
          NumberUtil.multiply(childRowCount, ratio)
        }
      } else {
        NumberUtil.min(distinctRowCount, childRowCount)
      }
    }
    if (isFinal) {
      ndvOfGroupKeysOnGlobalAgg
    } else {
      val inputRowCnt = mq.getRowCount(input)
      val tableConfig = unwrapTableConfig(rel)
      val parallelism = (inputRowCnt /
        tableConfig.get(FlinkRelMdRowCount.TABLE_OPTIMIZER_ROWS_PER_LOCALAGG) + 1).toInt
      if (parallelism == 1) {
        ndvOfGroupKeysOnGlobalAgg
      } else if (grouping.isEmpty) {
        // output rowcount of local agg is parallelism for agg which has no group keys
        parallelism.toDouble
      } else {
        val distinctRowCount = mq.getDistinctRowCount(input, grouping, null)
        if (distinctRowCount == null) {
          ndvOfGroupKeysOnGlobalAgg
        } else {
          FlinkRelMdUtil.getRowCountOfLocalAgg(parallelism, inputRowCnt, ndvOfGroupKeysOnGlobalAgg)
        }
      }
    }
  }

  def getRowCount(rel: WindowAggregate, mq: RelMetadataQuery): JDouble = {
    val (ndvOfGroupKeys, inputRowCount) = getRowCountOfAgg(rel, rel.getGroupSet, 1, mq)
    estimateRowCountOfWindowAgg(ndvOfGroupKeys, inputRowCount, rel.getWindow)
  }

  def getRowCount(rel: BatchPhysicalWindowAggregateBase, mq: RelMetadataQuery): JDouble = {
    val ndvOfGroupKeys = getRowCountOfBatchExecAgg(rel, mq)
    val inputRowCount = mq.getRowCount(rel.getInput)
    estimateRowCountOfWindowAgg(ndvOfGroupKeys, inputRowCount, rel.window)
  }

  private def estimateRowCountOfWindowAgg(
      ndv: JDouble,
      inputRowCount: JDouble,
      window: LogicalWindow): JDouble = {
    if (ndv == null) {
      null
    } else {
      // simply assume expand factor of TumblingWindow/SessionWindow/SlideWindowWithoutOverlap is 2
      // SlideWindowWithOverlap is 4.
      // Introduce expand factor here to distinguish output rowCount of normal agg with all kinds of
      // window aggregates.
      val expandFactorOfTumblingWindow = 2d
      val expandFactorOfNoOverLapSlidingWindow = 2d
      val expandFactorOfOverLapSlidingWindow = 4d
      val expandFactorOfSessionWindow = 2d
      window match {
        case TumblingGroupWindow(_, _, size) if hasTimeIntervalType(size) =>
          Math.min(expandFactorOfTumblingWindow * ndv, inputRowCount)
        case SlidingGroupWindow(_, _, size, slide) if hasTimeIntervalType(size) =>
          val sizeValue = toLong(size)
          val slideValue = toLong(slide)
          if (sizeValue > slideValue) {
            // only slideWindow which has overlap may generates more records than input
            expandFactorOfOverLapSlidingWindow * ndv
          } else {
            Math.min(expandFactorOfNoOverLapSlidingWindow * ndv, inputRowCount)
          }
        case _ => Math.min(expandFactorOfSessionWindow * ndv, inputRowCount)
      }
    }
  }

  def getRowCount(rel: Window, mq: RelMetadataQuery): JDouble = getRowCountOfOverAgg(rel, mq)

  def getRowCount(rel: BatchPhysicalOverAggregate, mq: RelMetadataQuery): JDouble =
    getRowCountOfOverAgg(rel, mq)

  private def getRowCountOfOverAgg(overAgg: SingleRel, mq: RelMetadataQuery): JDouble =
    mq.getRowCount(overAgg.getInput)

  def getRowCount(join: Join, mq: RelMetadataQuery): JDouble = {
    join.getJoinType match {
      case JoinRelType.SEMI | JoinRelType.ANTI =>
        val semiJoinSelectivity = FlinkRelMdUtil.makeSemiAntiJoinSelectivityRexNode(mq, join)
        val selectivity = mq.getSelectivity(join.getLeft, semiJoinSelectivity)
        val leftRowCount = mq.getRowCount(join.getLeft)
        return NumberUtil.multiply(leftRowCount, selectivity)
      case _ => // do nothing
    }

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
      leftRowCount: JDouble,
      rightRowCount: JDouble): JDouble = {
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
      return 0d
    }

    val leftKeySet = joinInfo.leftSet()
    val rightKeySet = joinInfo.rightSet()
    val leftNdv = fmq.getDistinctRowCount(leftChild, leftKeySet, null)
    val rightNdv = fmq.getDistinctRowCount(rightChild, rightKeySet, null)
    // estimate selectivity of non-equi
    val selectivityOfNonEquiPred: JDouble = if (joinInfo.isEqui) {
      1d
    } else {
      val nonEquiPred = joinInfo.getRemaining(rexBuilder)
      val equiPred = RelMdUtil.minusPreds(rexBuilder, condition, nonEquiPred)
      val joinWithOnlyEquiPred = copyJoinWithNewCondition(join, equiPred)
      fmq.getSelectivity(joinWithOnlyEquiPred, nonEquiPred)
    }

    if (leftNdv != null && rightNdv != null) {
      // selectivity of equi part is 1 / Max(leftNdv, rightNdv)
      val selectivityOfEquiPred = Math.min(1d, 1d / Math.max(leftNdv, rightNdv))
      return leftRowCount * rightRowCount * selectivityOfEquiPred *
        selectivityOfNonEquiPred
    }

    val leftKeysAreUnique = fmq.areColumnsUnique(leftChild, leftKeySet)
    val rightKeysAreUnique = fmq.areColumnsUnique(rightChild, rightKeySet)
    if (
      leftKeysAreUnique != null && rightKeysAreUnique != null &&
      (leftKeysAreUnique || rightKeysAreUnique)
    ) {
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

  def getRowCount(rel: Union, mq: RelMetadataQuery): JDouble = {
    val rowCounts = rel.getInputs.map(mq.getRowCount)
    if (rowCounts.contains(null)) {
      null
    } else {
      rowCounts.foldLeft(0d)(_ + _)
    }
  }

  def getRowCount(rel: Intersect, mq: RelMetadataQuery): JDouble = {
    rel.getInputs.foldLeft(null.asInstanceOf[JDouble]) {
      (res, input) =>
        val partialRowCount = mq.getRowCount(input)
        if (res == null || (partialRowCount != null && partialRowCount < res)) {
          partialRowCount
        } else {
          res
        }
    }
  }

  def getRowCount(rel: Minus, mq: RelMetadataQuery): JDouble = {
    rel.getInputs.foldLeft(null.asInstanceOf[JDouble]) {
      (res, input) =>
        val partialRowCount = mq.getRowCount(input)
        if (res == null || (partialRowCount != null && partialRowCount < res)) {
          partialRowCount
        } else {
          res
        }
    }
  }

  def getRowCount(subset: RelSubset, mq: RelMetadataQuery): JDouble = {
    if (!Bug.CALCITE_1048_FIXED) {
      val rel = Util.first(subset.getBest, subset.getOriginal)
      return mq.getRowCount(rel)
    }

    val v = subset.getRels.foldLeft(null.asInstanceOf[JDouble]) {
      (min, rel) =>
        try {
          val rowCount = mq.getRowCount(rel)
          NumberUtil.min(min, rowCount)
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

  /**
   * Catch-all implementation for [[BuiltInMetadata.RowCount#getRowCount()]], invoked using
   * reflection.
   *
   * @see
   *   org.apache.calcite.rel.metadata.RelMetadataQuery#getRowCount(RelNode)
   */
  def getRowCount(rel: RelNode, mq: RelMetadataQuery): JDouble = rel.estimateRowCount(mq)

}

object FlinkRelMdRowCount {

  private val INSTANCE = new FlinkRelMdRowCount

  val SOURCE: RelMetadataProvider =
    ReflectiveRelMetadataProvider.reflectiveSource(BuiltInMethod.ROW_COUNT.method, INSTANCE)

  // It is a experimental config, will may be removed later.
  @Experimental
  val TABLE_OPTIMIZER_ROWS_PER_LOCALAGG: ConfigOption[JLong] =
    key("table.optimizer.rows-per-local-agg")
      .longType()
      .defaultValue(JLong.valueOf(1000000L))
      .withDescription("Sets estimated number of records that one local-agg processes. " +
        "Optimizer will infer whether to use local/global aggregate according to it.")

}
