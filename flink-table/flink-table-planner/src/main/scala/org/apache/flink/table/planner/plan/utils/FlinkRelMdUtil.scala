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
package org.apache.flink.table.planner.plan.utils

import org.apache.flink.table.data.binary.BinaryRowData
import org.apache.flink.table.planner.JDouble
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.nodes.calcite.{Expand, Rank, WindowAggregate}
import org.apache.flink.table.planner.plan.nodes.physical.batch.{BatchPhysicalGroupAggregateBase, BatchPhysicalLocalHashWindowAggregate, BatchPhysicalLocalSortWindowAggregate, BatchPhysicalWindowAggregateBase}
import org.apache.flink.table.runtime.groupwindow.NamedWindowProperty
import org.apache.flink.table.runtime.operators.rank.{ConstantRankRange, RankRange}
import org.apache.flink.table.runtime.operators.sort.BinaryIndexedSortable
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer.LENGTH_SIZE_IN_BYTES

import com.google.common.collect.ImmutableList
import org.apache.calcite.avatica.util.TimeUnitRange._
import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rel.{RelNode, SingleRel}
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.metadata.{RelMdUtil, RelMetadataQuery}
import org.apache.calcite.rex._
import org.apache.calcite.sql.`type`.SqlTypeName.{TIME, TIMESTAMP}
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.util.{ImmutableBitSet, NumberUtil}
import org.apache.calcite.util.NumberUtil.multiply

import java.math.BigDecimal
import java.util

import scala.collection.JavaConversions._
import scala.collection.mutable

/** FlinkRelMdUtil provides utility methods used by the metadata provider methods. */
object FlinkRelMdUtil {

  /** Returns an estimate of the number of rows returned by a SEMI/ANTI [[Join]]. */
  def getSemiAntiJoinRowCount(
      mq: RelMetadataQuery,
      left: RelNode,
      right: RelNode,
      joinType: JoinRelType,
      condition: RexNode,
      isAnti: Boolean): JDouble = {
    val leftCount = mq.getRowCount(left)
    if (leftCount == null) {
      return null
    }
    var selectivity = RexUtil.getSelectivity(condition)
    if (isAnti) {
      selectivity = 1d - selectivity
    }
    leftCount * selectivity
  }

  /**
   * Creates a RexNode that stores a selectivity value corresponding to the selectivity of a
   * semi-join/anti-join. This can be added to a filter to simulate the effect of the
   * semi-join/anti-join during costing, but should never appear in a real plan since it has no
   * physical implementation.
   *
   * @param mq
   *   instance of metadata query
   * @param rel
   *   the SEMI/ANTI join of interest
   * @return
   *   constructed rexNode
   */
  def makeSemiAntiJoinSelectivityRexNode(mq: RelMetadataQuery, rel: Join): RexNode = {
    require(rel.getJoinType == JoinRelType.SEMI || rel.getJoinType == JoinRelType.ANTI)
    val joinInfo = rel.analyzeCondition()
    val rexBuilder = rel.getCluster.getRexBuilder
    makeSemiAntiJoinSelectivityRexNode(
      mq,
      joinInfo,
      rel.getLeft,
      rel.getRight,
      rel.getJoinType == JoinRelType.ANTI,
      rexBuilder)
  }

  private def makeSemiAntiJoinSelectivityRexNode(
      mq: RelMetadataQuery,
      joinInfo: JoinInfo,
      left: RelNode,
      right: RelNode,
      isAnti: Boolean,
      rexBuilder: RexBuilder): RexNode = {
    val equiSelectivity: JDouble = if (!joinInfo.leftKeys.isEmpty) {
      RelMdUtil.computeSemiJoinSelectivity(mq, left, right, joinInfo.leftKeys, joinInfo.rightKeys)
    } else {
      1d
    }

    val nonEquiSelectivity = RelMdUtil.guessSelectivity(joinInfo.getRemaining(rexBuilder))
    val semiJoinSelectivity = equiSelectivity * nonEquiSelectivity

    val selectivity = if (isAnti) {
      val antiJoinSelectivity = 1.0 - semiJoinSelectivity
      if (antiJoinSelectivity == 0.0) {
        // we don't expect that anti-join's selectivity is 0.0, so choose a default value 0.1
        0.1
      } else {
        antiJoinSelectivity
      }
    } else {
      semiJoinSelectivity
    }

    rexBuilder.makeCall(
      RelMdUtil.ARTIFICIAL_SELECTIVITY_FUNC,
      rexBuilder.makeApproxLiteral(new BigDecimal(selectivity)))
  }

  /**
   * Estimates new distinctRowCount of currentNode after it applies a condition. The estimation
   * based on one assumption: even distribution of all distinct data
   *
   * @param rowCount
   *   rowcount of node.
   * @param distinctRowCount
   *   distinct rowcount of node.
   * @param selectivity
   *   selectivity of condition expression.
   * @return
   *   new distinctRowCount
   */
  def adaptNdvBasedOnSelectivity(
      rowCount: JDouble,
      distinctRowCount: JDouble,
      selectivity: JDouble): JDouble = {
    val ndv = Math.min(distinctRowCount, rowCount)
    Math.max((1 - Math.pow(1 - selectivity, rowCount / ndv)) * ndv, 1.0)
  }

  /**
   * Estimates ratio outputRowCount/ inputRowCount of agg when ndv of groupKeys is unavailable.
   *
   * the value of `1.0 - math.exp(-0.1 * groupCount)` increases with groupCount from 0.095 until
   * close to 1.0. when groupCount is 1, the formula result is 0.095, when groupCount is 2, the
   * formula result is 0.18, when groupCount is 3, the formula result is 0.25. ...
   *
   * @param groupingLength
   *   grouping keys length of aggregate
   * @return
   *   the ratio outputRowCount/ inputRowCount of agg when ndv of groupKeys is unavailable.
   */
  def getAggregationRatioIfNdvUnavailable(groupingLength: Int): JDouble =
    1.0 - math.exp(-0.1 * groupingLength)

  /**
   * Creates a RexNode that stores a selectivity value corresponding to the selectivity of a
   * NamedProperties predicate.
   *
   * @param winAgg
   *   window aggregate node
   * @param predicate
   *   a RexNode
   * @return
   *   constructed rexNode including non-NamedProperties predicates and a predicate that stores
   *   NamedProperties predicate's selectivity
   */
  def makeNamePropertiesSelectivityRexNode(winAgg: WindowAggregate, predicate: RexNode): RexNode = {
    val fullGroupSet = AggregateUtil.checkAndGetFullGroupSet(winAgg)
    makeNamePropertiesSelectivityRexNode(winAgg, fullGroupSet, winAgg.getNamedProperties, predicate)
  }

  /**
   * Creates a RexNode that stores a selectivity value corresponding to the selectivity of a
   * NamedProperties predicate.
   *
   * @param globalWinAgg
   *   global window aggregate node
   * @param predicate
   *   a RexNode
   * @return
   *   constructed rexNode including non-NamedProperties predicates and a predicate that stores
   *   NamedProperties predicate's selectivity
   */
  def makeNamePropertiesSelectivityRexNode(
      globalWinAgg: BatchPhysicalWindowAggregateBase,
      predicate: RexNode): RexNode = {
    require(globalWinAgg.isFinal, "local window agg does not contain NamedProperties!")
    val fullGrouping = globalWinAgg.grouping ++ globalWinAgg.auxGrouping
    makeNamePropertiesSelectivityRexNode(
      globalWinAgg,
      fullGrouping,
      globalWinAgg.namedWindowProperties,
      predicate)
  }

  /**
   * Creates a RexNode that stores a selectivity value corresponding to the selectivity of a
   * NamedProperties predicate.
   *
   * @param winAgg
   *   window aggregate node
   * @param fullGrouping
   *   full groupSets
   * @param namedProperties
   *   NamedWindowProperty list
   * @param predicate
   *   a RexNode
   * @return
   *   constructed rexNode including non-NamedProperties predicates and a predicate that stores
   *   NamedProperties predicate's selectivity
   */
  def makeNamePropertiesSelectivityRexNode(
      winAgg: SingleRel,
      fullGrouping: Array[Int],
      namedProperties: Seq[NamedWindowProperty],
      predicate: RexNode): RexNode = {
    if (predicate == null || predicate.isAlwaysTrue || namedProperties.isEmpty) {
      return predicate
    }
    val rexBuilder = winAgg.getCluster.getRexBuilder
    val namePropertiesStartIdx = winAgg.getRowType.getFieldCount - namedProperties.size
    // split non-nameProperties predicates and nameProperties predicates
    val pushable = new util.ArrayList[RexNode]
    val notPushable = new util.ArrayList[RexNode]
    RelOptUtil.splitFilters(
      ImmutableBitSet.range(0, namePropertiesStartIdx),
      predicate,
      pushable,
      notPushable)
    if (notPushable.nonEmpty) {
      val pred = RexUtil.composeConjunction(rexBuilder, notPushable, true)
      val selectivity = RelMdUtil.guessSelectivity(pred)
      val fun = rexBuilder.makeCall(
        RelMdUtil.ARTIFICIAL_SELECTIVITY_FUNC,
        rexBuilder.makeApproxLiteral(new BigDecimal(selectivity)))
      pushable.add(fun)
    }
    RexUtil.composeConjunction(rexBuilder, pushable, true)
  }

  /**
   * This method is copied from calcite RelMdUtil and line 324 ~ 328 are changed. This method should
   * be removed once CALCITE-4351 is fixed. See CALCITE-4351 and FLINK-19780.
   *
   * Computes the number of distinct rows for a set of keys returned from a join. Also known as NDV
   * (number of distinct values).
   *
   * @param joinRel
   *   RelNode representing the join
   * @param joinType
   *   type of join
   * @param groupKey
   *   keys that the distinct row count will be computed for
   * @param predicate
   *   join predicate
   * @param useMaxNdv
   *   If true use formula <code>max(left NDV, right NDV)</code>, otherwise use <code>left NDV *
   *   right NDV</code>.
   * @return
   *   number of distinct rows
   */
  def getJoinDistinctRowCount(
      mq: RelMetadataQuery,
      joinRel: RelNode,
      joinType: JoinRelType,
      groupKey: ImmutableBitSet,
      predicate: RexNode,
      useMaxNdv: Boolean): JDouble = {
    if ((predicate == null || predicate.isAlwaysTrue) && groupKey.isEmpty) {
      return 1d
    }
    val join = joinRel.asInstanceOf[Join]
    if (join.isSemiJoin) {
      return RelMdUtil.getSemiJoinDistinctRowCount(join, mq, groupKey, predicate)
    }
    val leftMask = ImmutableBitSet.builder
    val rightMask = ImmutableBitSet.builder
    val left = joinRel.getInputs.get(0)
    val right = joinRel.getInputs.get(1)
    RelMdUtil.setLeftRightBitmaps(groupKey, leftMask, rightMask, left.getRowType.getFieldCount)
    // determine which filters apply to the left vs right
    val (leftPred, rightPred) = if (predicate != null) {
      val leftFilters = new util.ArrayList[RexNode]
      val rightFilters = new util.ArrayList[RexNode]
      val joinFilters = new util.ArrayList[RexNode]
      val predList = RelOptUtil.conjunctions(predicate)
      RelOptUtil.classifyFilters(
        joinRel,
        predList,
        joinType.canPushIntoFromAbove,
        joinType.canPushLeftFromAbove,
        joinType.canPushRightFromAbove,
        joinFilters,
        leftFilters,
        rightFilters)
      val rexBuilder = joinRel.getCluster.getRexBuilder
      val leftResult = RexUtil.composeConjunction(rexBuilder, leftFilters, true)
      val rightResult = RexUtil.composeConjunction(rexBuilder, rightFilters, true)
      (leftResult, rightResult)
    } else {
      (null, null)
    }

    val distRowCount = if (useMaxNdv) {
      NumberUtil.max(
        mq.getDistinctRowCount(left, leftMask.build, leftPred),
        mq.getDistinctRowCount(right, rightMask.build, rightPred))
    } else {
      multiply(
        mq.getDistinctRowCount(left, leftMask.build, leftPred),
        mq.getDistinctRowCount(right, rightMask.build, rightPred))
    }
    val rowCount = mq.getRowCount(joinRel)
    if (distRowCount == null || rowCount == null) {
      null
    } else {
      FlinkRelMdUtil.numDistinctVals(distRowCount, rowCount)
    }
  }

  /**
   * Returns the number of distinct values provided numSelected are selected where there are
   * domainSize distinct values.
   *
   * <p>Current implementation of RelMdUtil#numDistinctVals in Calcite 1.26 has precision problem,
   * so we treat small and large inputs differently here and handle large inputs with the old
   * implementation of RelMdUtil#numDistinctVals in Calcite 1.22.
   *
   * <p>This method should be removed once CALCITE-4351 is fixed. See CALCITE-4351 and FLINK-19780.
   */
  def numDistinctVals(domainSize: Double, numSelected: Double): Double = {
    val EPS = 1e-9
    if (Math.abs(1 / domainSize) < EPS || domainSize < 1) {
      // ln(1+x) ~= x for small x
      val dSize = RelMdUtil.capInfinity(domainSize)
      val numSel = RelMdUtil.capInfinity(numSelected)
      val res = if (dSize > 0) (1.0 - Math.exp(-1 * numSel / dSize)) * dSize else 0
      // fix the boundary cases
      Math.max(0, Math.min(res, Math.min(dSize, numSel)))
    } else {
      RelMdUtil.numDistinctVals(domainSize, numSelected)
    }
  }

  /**
   * Estimates outputRowCount of local aggregate.
   *
   * output rowcount of local agg is (1 - pow((1 - 1/x) , n/m)) * m * x, based on two assumption:
   *   1. even distribution of all distinct data 2. even distribution of all data in each concurrent
   *      local agg worker
   *
   * @param parallelism
   *   number of concurrent worker of local aggregate
   * @param inputRowCount
   *   rowcount of input node of aggregate.
   * @param globalAggRowCount
   *   rowcount of output of global aggregate.
   * @return
   *   outputRowCount of local aggregate.
   */
  def getRowCountOfLocalAgg(
      parallelism: Int,
      inputRowCount: JDouble,
      globalAggRowCount: JDouble): JDouble =
    Math.min(
      (1 - math.pow(1 - 1.0 / parallelism, inputRowCount / globalAggRowCount))
        * globalAggRowCount * parallelism,
      inputRowCount)

  /**
   * Takes a bitmap representing a set of input references and extracts the ones that reference the
   * group by columns in an aggregate.
   *
   * @param groupKey
   *   the original bitmap
   * @param aggRel
   *   the aggregate
   */
  def setAggChildKeys(
      groupKey: ImmutableBitSet,
      aggRel: Aggregate): (ImmutableBitSet, Array[AggregateCall]) = {
    val childKeyBuilder = ImmutableBitSet.builder
    val aggCalls = new mutable.ArrayBuffer[AggregateCall]()
    val groupSet = aggRel.getGroupSet.toArray
    val (auxGroupSet, otherAggCalls) = AggregateUtil.checkAndSplitAggCalls(aggRel)
    val fullGroupSet = groupSet ++ auxGroupSet
    // does not need to take keys in aggregate call into consideration if groupKey contains all
    // groupSet element in aggregate
    val containsAllAggGroupKeys = fullGroupSet.indices.forall(groupKey.get)
    groupKey.foreach(
      bit =>
        if (bit < fullGroupSet.length) {
          childKeyBuilder.set(fullGroupSet(bit))
        } else if (!containsAllAggGroupKeys) {
          // getIndicatorCount return 0 if auxGroupSet is not empty
          val agg = otherAggCalls.get(bit - (fullGroupSet.length + aggRel.getIndicatorCount))
          aggCalls += agg
        })
    (childKeyBuilder.build(), aggCalls.toArray)
  }

  /**
   * Takes a bitmap representing a set of input references and extracts the ones that reference the
   * group by columns in an aggregate.
   *
   * @param groupKey
   *   the original bitmap
   * @param aggRel
   *   the aggregate
   */
  def setAggChildKeys(
      groupKey: ImmutableBitSet,
      aggRel: BatchPhysicalGroupAggregateBase): (ImmutableBitSet, Array[AggregateCall]) = {
    require(!aggRel.isFinal || !aggRel.isMerge, "Cannot handle global agg which has local agg!")
    setChildKeysOfAgg(groupKey, aggRel)
  }

  /**
   * Takes a bitmap representing a set of input references and extracts the ones that reference the
   * group by columns in an aggregate.
   *
   * @param groupKey
   *   the original bitmap
   * @param aggRel
   *   the aggregate
   */
  def setAggChildKeys(
      groupKey: ImmutableBitSet,
      aggRel: BatchPhysicalWindowAggregateBase): (ImmutableBitSet, Array[AggregateCall]) = {
    require(!aggRel.isFinal || !aggRel.isMerge, "Cannot handle global agg which has local agg!")
    setChildKeysOfAgg(groupKey, aggRel)
  }

  private def setChildKeysOfAgg(
      groupKey: ImmutableBitSet,
      agg: SingleRel): (ImmutableBitSet, Array[AggregateCall]) = {
    val (aggCalls, fullGroupSet) = agg match {
      case agg: BatchPhysicalLocalSortWindowAggregate =>
        // grouping + assignTs + auxGrouping
        (agg.getAggCallList, agg.grouping ++ Array(agg.inputTimeFieldIndex) ++ agg.auxGrouping)
      case agg: BatchPhysicalLocalHashWindowAggregate =>
        // grouping + assignTs + auxGrouping
        (agg.getAggCallList, agg.grouping ++ Array(agg.inputTimeFieldIndex) ++ agg.auxGrouping)
      case agg: BatchPhysicalWindowAggregateBase =>
        (agg.getAggCallList, agg.grouping ++ agg.auxGrouping)
      case agg: BatchPhysicalGroupAggregateBase =>
        (agg.getAggCallList, agg.grouping ++ agg.auxGrouping)
      case _ => throw new IllegalArgumentException(s"Unknown aggregate: ${agg.getRelTypeName}")
    }
    // does not need to take keys in aggregate call into consideration if groupKey contains all
    // groupSet element in aggregate
    val containsAllAggGroupKeys = fullGroupSet.indices.forall(groupKey.get)
    val childKeyBuilder = ImmutableBitSet.builder
    val aggs = new mutable.ArrayBuffer[AggregateCall]()
    groupKey.foreach {
      bit =>
        if (bit < fullGroupSet.length) {
          childKeyBuilder.set(fullGroupSet(bit))
        } else if (!containsAllAggGroupKeys) {
          val agg = aggCalls.get(bit - fullGroupSet.length)
          aggs += agg
        }
    }
    (childKeyBuilder.build(), aggs.toArray)
  }

  /**
   * Takes a bitmap representing a set of local window aggregate references.
   *
   * global win-agg output type: groupSet + auxGroupSet + aggCall + namedProperties local win-agg
   * output type: groupSet + assignTs + auxGroupSet + aggCalls
   *
   * Skips `assignTs` when mapping `groupKey` to `childKey`.
   *
   * @param groupKey
   *   the original bitmap
   * @param globalWinAgg
   *   the global window aggregate
   */
  def setChildKeysOfWinAgg(
      groupKey: ImmutableBitSet,
      globalWinAgg: BatchPhysicalWindowAggregateBase): ImmutableBitSet = {
    require(globalWinAgg.isMerge, "Cannot handle global agg which does not have local window agg!")
    val childKeyBuilder = ImmutableBitSet.builder
    groupKey.toArray.foreach {
      key =>
        if (key < globalWinAgg.grouping.length) {
          childKeyBuilder.set(key)
        } else {
          // skips `assignTs`
          childKeyBuilder.set(key + 1)
        }
    }
    childKeyBuilder.build()
  }

  /**
   * Split groupKeys on Aggregate/ BatchPhysicalGroupAggregateBase/ BatchPhysicalWindowAggregateBase
   * into keys on aggregate's groupKey and aggregate's aggregateCalls.
   *
   * @param agg
   *   the aggregate
   * @param groupKey
   *   the original bitmap
   */
  def splitGroupKeysOnAggregate(
      agg: SingleRel,
      groupKey: ImmutableBitSet): (ImmutableBitSet, Array[AggregateCall]) = {

    def removeAuxKey(
        groupKey: ImmutableBitSet,
        groupSet: Array[Int],
        auxGroupSet: Array[Int]): ImmutableBitSet = {
      if (groupKey.contains(ImmutableBitSet.of(groupSet: _*))) {
        // remove auxGroupSet from groupKey if groupKey contain both full-groupSet
        // and (partial-)auxGroupSet
        groupKey.except(ImmutableBitSet.of(auxGroupSet: _*))
      } else {
        groupKey
      }
    }

    agg match {
      case rel: Aggregate =>
        val (auxGroupSet, _) = AggregateUtil.checkAndSplitAggCalls(rel)
        val (childKeys, aggCalls) = setAggChildKeys(groupKey, rel)
        val childKeyExcludeAuxKey = removeAuxKey(childKeys, rel.getGroupSet.toArray, auxGroupSet)
        (childKeyExcludeAuxKey, aggCalls)
      case rel: BatchPhysicalGroupAggregateBase =>
        // set the bits as they correspond to the child input
        val (childKeys, aggCalls) = setAggChildKeys(groupKey, rel)
        val childKeyExcludeAuxKey = removeAuxKey(childKeys, rel.grouping, rel.auxGrouping)
        (childKeyExcludeAuxKey, aggCalls)
      case rel: BatchPhysicalWindowAggregateBase =>
        val (childKeys, aggCalls) = setAggChildKeys(groupKey, rel)
        val childKeyExcludeAuxKey = removeAuxKey(childKeys, rel.grouping, rel.auxGrouping)
        (childKeyExcludeAuxKey, aggCalls)
      case _ => throw new IllegalArgumentException(s"Unknown aggregate: ${agg.getRelTypeName}.")
    }
  }

  /**
   * Split a predicate on Aggregate into two parts, the first one is pushable part, the second one
   * is rest part.
   *
   * @param agg
   *   Aggregate which to analyze
   * @param predicate
   *   Predicate which to analyze
   * @return
   *   a tuple, first element is pushable part, second element is rest part. Note, pushable
   *   condition will be converted based on the input field position.
   */
  def splitPredicateOnAggregate(
      agg: Aggregate,
      predicate: RexNode): (Option[RexNode], Option[RexNode]) = {
    val fullGroupSet = AggregateUtil.checkAndGetFullGroupSet(agg)
    splitPredicateOnAgg(fullGroupSet, agg, predicate)
  }

  /**
   * Split a predicate on BatchExecGroupAggregateBase into two parts, the first one is pushable
   * part, the second one is rest part.
   *
   * @param agg
   *   Aggregate which to analyze
   * @param predicate
   *   Predicate which to analyze
   * @return
   *   a tuple, first element is pushable part, second element is rest part. Note, pushable
   *   condition will be converted based on the input field position.
   */
  def splitPredicateOnAggregate(
      agg: BatchPhysicalGroupAggregateBase,
      predicate: RexNode): (Option[RexNode], Option[RexNode]) = {
    splitPredicateOnAgg(agg.grouping ++ agg.auxGrouping, agg, predicate)
  }

  /**
   * Split a predicate on WindowAggregateBatchExecBase into two parts, the first one is pushable
   * part, the second one is rest part.
   *
   * @param agg
   *   Aggregate which to analyze
   * @param predicate
   *   Predicate which to analyze
   * @return
   *   a tuple, first element is pushable part, second element is rest part. Note, pushable
   *   condition will be converted based on the input field position.
   */
  def splitPredicateOnAggregate(
      agg: BatchPhysicalWindowAggregateBase,
      predicate: RexNode): (Option[RexNode], Option[RexNode]) = {
    splitPredicateOnAgg(agg.grouping ++ agg.auxGrouping, agg, predicate)
  }

  /**
   * Shifts every [[RexInputRef]] in an expression higher than length of full grouping (for skips
   * `assignTs`).
   *
   * global win-agg output type: groupSet + auxGroupSet + aggCall + namedProperties local win-agg
   * output type: groupSet + assignTs + auxGroupSet + aggCalls
   *
   * @param predicate
   *   a RexNode
   * @param globalWinAgg
   *   the global window aggregate
   */
  def setChildPredicateOfWinAgg(
      predicate: RexNode,
      globalWinAgg: BatchPhysicalWindowAggregateBase): RexNode = {
    require(globalWinAgg.isMerge, "Cannot handle global agg which does not have local window agg!")
    if (predicate == null) {
      return null
    }
    // grouping + assignTs + auxGrouping
    val fullGrouping = globalWinAgg.grouping ++ globalWinAgg.auxGrouping
    // skips `assignTs`
    RexUtil.shift(predicate, fullGrouping.length, 1)
  }

  private def splitPredicateOnAgg(
      grouping: Array[Int],
      agg: SingleRel,
      predicate: RexNode): (Option[RexNode], Option[RexNode]) = {
    val notPushable = new util.ArrayList[RexNode]
    val pushable = new util.ArrayList[RexNode]
    val numOfGroupKey = grouping.length
    RelOptUtil.splitFilters(
      ImmutableBitSet.range(0, numOfGroupKey),
      predicate,
      pushable,
      notPushable)
    val rexBuilder = agg.getCluster.getRexBuilder
    val childPred = if (pushable.isEmpty) {
      None
    } else {
      // Converts a list of expressions that are based on the output fields of a
      // Aggregate to equivalent expressions on the Aggregate's input fields.
      val aggOutputFields = agg.getRowType.getFieldList
      val aggInputFields = agg.getInput.getRowType.getFieldList
      val adjustments = new Array[Int](aggOutputFields.size)
      grouping.zipWithIndex.foreach { case (bit, index) => adjustments(index) = bit - index }
      val pushableConditions = pushable.map {
        pushCondition =>
          pushCondition.accept(
            new RelOptUtil.RexInputConverter(
              rexBuilder,
              aggOutputFields,
              aggInputFields,
              adjustments))
      }
      Option(RexUtil.composeConjunction(rexBuilder, pushableConditions, true))
    }
    val restPred = if (notPushable.isEmpty) {
      None
    } else {
      Option(RexUtil.composeConjunction(rexBuilder, notPushable, true))
    }
    (childPred, restPred)
  }

  def binaryRowAverageSize(rel: RelNode): JDouble = {
    val binaryType = FlinkTypeFactory.toLogicalRowType(rel.getRowType)
    // TODO reuse FlinkRelMetadataQuery here
    val mq = rel.getCluster.getMetadataQuery
    val columnSizes = mq.getAverageColumnSizes(rel)
    var length = 0d
    columnSizes.zip(binaryType.getChildren).foreach {
      case (columnSize, internalType) =>
        if (BinaryRowData.isInFixedLengthPart(internalType)) {
          length += 8
        } else {
          if (columnSize == null) {
            // find a better way of computing generic type field variable-length
            // right now we use a small value assumption
            length += 16
          } else {
            // the 8 bytes is used store the length and offset of variable-length part.
            length += columnSize + 8
          }
        }
    }
    length += BinaryRowData.calculateBitSetWidthInBytes(columnSizes.size())
    length
  }

  def computeSortMemory(mq: RelMetadataQuery, inputOfSort: RelNode): JDouble = {
    // TODO It's hard to make sure that the normalized key's length is accurate in optimized stage.
    // use SortCodeGenerator.MAX_NORMALIZED_KEY_LEN instead of 16
    val normalizedKeyBytes = 16
    val rowCount = mq.getRowCount(inputOfSort)
    val averageRowSize = binaryRowAverageSize(inputOfSort)
    val recordAreaInBytes = rowCount * (averageRowSize + LENGTH_SIZE_IN_BYTES)
    val indexAreaInBytes = rowCount * (normalizedKeyBytes + BinaryIndexedSortable.OFFSET_LEN)
    recordAreaInBytes + indexAreaInBytes
  }

  def splitPredicateOnRank(rank: Rank, predicate: RexNode): (Option[RexNode], Option[RexNode]) = {
    val rankFunColumnIndex = RankUtil.getRankNumberColumnIndex(rank).getOrElse(-1)
    if (predicate == null || predicate.isAlwaysTrue || rankFunColumnIndex < 0) {
      return (Some(predicate), None)
    }

    val rankNodes = new util.ArrayList[RexNode]
    val nonRankNodes = new util.ArrayList[RexNode]
    RelOptUtil.splitFilters(
      ImmutableBitSet.range(0, rankFunColumnIndex),
      predicate,
      nonRankNodes,
      rankNodes)
    val rexBuilder = rank.getCluster.getRexBuilder
    val nonRankPred = if (nonRankNodes.isEmpty) {
      None
    } else {
      Option(RexUtil.composeConjunction(rexBuilder, nonRankNodes, true))
    }
    val rankPred = if (rankNodes.isEmpty) {
      None
    } else {
      Option(RexUtil.composeConjunction(rexBuilder, rankNodes, true))
    }
    (nonRankPred, rankPred)
  }

  def getRankRangeNdv(rankRange: RankRange): JDouble = rankRange match {
    case r: ConstantRankRange => (r.getRankEnd - r.getRankStart + 1).toDouble
    case _ => 100d // default value now
  }

  /**
   * Returns [[RexInputRef]] index set of projects corresponding to the given column index. The
   * index will be set as -1 if the given column in project is not a [[RexInputRef]].
   */
  def getInputRefIndices(index: Int, expand: Expand): util.Set[Int] = {
    val inputRefs = new util.HashSet[Int]()
    for (project <- expand.projects) {
      project.get(index) match {
        case inputRef: RexInputRef => inputRefs.add(inputRef.getIndex)
        case _ => inputRefs.add(-1)
      }
    }
    inputRefs
  }

  /** Splits a column set between left and right sets. */
  def splitColumnsIntoLeftAndRight(
      leftCount: Int,
      columns: ImmutableBitSet): (ImmutableBitSet, ImmutableBitSet) = {
    val leftBuilder = ImmutableBitSet.builder
    val rightBuilder = ImmutableBitSet.builder
    columns.foreach {
      bit => if (bit < leftCount) leftBuilder.set(bit) else rightBuilder.set(bit - leftCount)
    }
    (leftBuilder.build, rightBuilder.build)
  }

  /**
   * Computes the cardinality of a particular expression from the projection list.
   *
   * @param mq
   *   metadata query instance
   * @param calc
   *   calc RelNode
   * @param expr
   *   projection expression
   * @return
   *   cardinality
   */
  def cardOfCalcExpr(mq: RelMetadataQuery, calc: Calc, expr: RexNode): JDouble = {
    expr.accept(new CardOfCalcExpr(mq, calc))
  }

  /**
   * Visitor that walks over a scalar expression and computes the cardinality of its result. The
   * code is borrowed from RelMdUtil
   *
   * @param mq
   *   metadata query instance
   * @param calc
   *   calc relnode
   */
  private class CardOfCalcExpr(mq: RelMetadataQuery, calc: Calc)
    extends RexVisitorImpl[JDouble](true) {
    private val program = calc.getProgram

    private val condition = if (program.getCondition != null) {
      program.expandLocalRef(program.getCondition)
    } else {
      null
    }

    override def visitInputRef(inputRef: RexInputRef): JDouble = {
      val col = ImmutableBitSet.of(inputRef.getIndex)
      val distinctRowCount = mq.getDistinctRowCount(calc.getInput, col, condition)
      if (distinctRowCount == null) {
        null
      } else {
        FlinkRelMdUtil.numDistinctVals(distinctRowCount, mq.getAverageRowSize(calc))
      }
    }

    override def visitLiteral(literal: RexLiteral): JDouble = {
      FlinkRelMdUtil.numDistinctVals(1d, mq.getAverageRowSize(calc))
    }

    override def visitCall(call: RexCall): JDouble = {
      val rowCount = mq.getRowCount(calc)
      val distinctRowCount: JDouble = if (call.isA(SqlKind.MINUS_PREFIX)) {
        cardOfCalcExpr(mq, calc, call.getOperands.get(0))
      } else if (call.isA(ImmutableList.of(SqlKind.PLUS, SqlKind.MINUS))) {
        val card0 = cardOfCalcExpr(mq, calc, call.getOperands.get(0))
        if (card0 == null) {
          null
        } else {
          val card1 = cardOfCalcExpr(mq, calc, call.getOperands.get(1))
          if (card1 == null) {
            null
          } else {
            Math.max(card0, card1)
          }
        }
      } else if (call.isA(ImmutableList.of(SqlKind.TIMES, SqlKind.DIVIDE))) {
        NumberUtil.multiply(
          cardOfCalcExpr(mq, calc, call.getOperands.get(0)),
          cardOfCalcExpr(mq, calc, call.getOperands.get(1)))
      } else if (call.isA(SqlKind.EXTRACT)) {
        val extractUnit = call.getOperands.get(0)
        val timeOperand = call.getOperands.get(1)
        extractUnit match {
          // go https://www.postgresql.org/docs/9.1/static/
          // functions-datetime.html#FUNCTIONS-DATETIME-EXTRACT to get the definitions of timeunits
          case unit: RexLiteral =>
            val unitValue = unit.getValue
            val timeOperandType = timeOperand.getType.getSqlTypeName
            // assume min time is 1970-01-01 00:00:00, max time is 2100-12-31 21:59:59
            unitValue match {
              case YEAR => 130d // [1970, 2100]
              case MONTH => 12d
              case DAY => 31d
              case HOUR => 24d
              case MINUTE => 60d
              case SECOND =>
                timeOperandType match {
                  case TIMESTAMP | TIME => 60 * 1000d // [0.000, 59.999]
                  case _ => 60d // [0, 59]
                }
              case QUARTER => 4d
              case WEEK => 53d // [1, 53]
              case MILLISECOND =>
                timeOperandType match {
                  case TIMESTAMP | TIME => 60 * 1000d // [0.000, 59.999]
                  case _ => 60d // [0, 59]
                }
              case MICROSECOND =>
                timeOperandType match {
                  case TIMESTAMP | TIME => 60 * 1000d * 1000d // [0.000, 59.999]
                  case _ => 60d // [0, 59]
                }
              case DOW => 7d // [0, 6]
              case DOY => 366d // [1, 366]
              case EPOCH =>
                timeOperandType match {
                  // the number of seconds since 1970-01-01 00:00:00 UTC
                  case TIMESTAMP | TIME => 130 * 24 * 60 * 60 * 1000d
                  case _ => 130 * 24 * 60 * 60d
                }
              case DECADE => 13d // The year field divided by 10
              case CENTURY => 2d
              case MILLENNIUM => 2d
              case _ => cardOfCalcExpr(mq, calc, timeOperand)
            }
          case _ => cardOfCalcExpr(mq, calc, timeOperand)
        }
      } else if (call.getOperands.size() == 1) {
        cardOfCalcExpr(mq, calc, call.getOperands.get(0))
      } else {
        if (rowCount != null) rowCount / 10 else null
      }
      if (distinctRowCount == null) {
        null
      } else {
        FlinkRelMdUtil.numDistinctVals(distinctRowCount, rowCount)
      }
    }

  }

}
