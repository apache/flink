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

import org.apache.flink.table.api.TableException
import org.apache.flink.table.planner.{JArrayList, JDouble}
import org.apache.flink.table.planner.plan.nodes.calcite.{Expand, Rank, WindowAggregate}
import org.apache.flink.table.planner.plan.nodes.physical.batch._
import org.apache.flink.table.planner.plan.schema.FlinkPreparingTableBase
import org.apache.flink.table.planner.plan.utils.{FlinkRelMdUtil, FlinkRexUtil, RankUtil}
import org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig

import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.{RelNode, SingleRel}
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.logical.LogicalCalc
import org.apache.calcite.rel.metadata._
import org.apache.calcite.rex._
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.util._

import scala.collection.JavaConversions._

/**
 * FlinkRelMdDistinctRowCount supplies a implementation of [[RelMetadataQuery#getDistinctRowCount]]
 * for the standard logical algebra.
 */
class FlinkRelMdDistinctRowCount private extends MetadataHandler[BuiltInMetadata.DistinctRowCount] {

  def getDef: MetadataDef[BuiltInMetadata.DistinctRowCount] = BuiltInMetadata.DistinctRowCount.DEF

  def getDistinctRowCount(
      rel: TableScan,
      mq: RelMetadataQuery,
      groupKey: ImmutableBitSet,
      predicate: RexNode): JDouble = {
    if (predicate == null || predicate.isAlwaysTrue) {
      if (groupKey.isEmpty) {
        return 1d
      }
    }
    val statistic = rel.getTable.asInstanceOf[FlinkPreparingTableBase].getStatistic
    val fields = rel.getRowType.getFieldList
    val isKey = mq.areColumnsUnique(rel, groupKey)
    val isUnique = isKey != null && isKey
    val selectivity: JDouble = if (predicate == null) {
      1d
    } else {
      mq.getSelectivity(rel, predicate)
    }
    if (isUnique) {
      val rowCount = mq.getRowCount(rel)
      NumberUtil.multiply(rowCount, selectivity)
    } else {
      val distinctCount = groupKey.asList().foldLeft(1d) {
        (ndv, g) =>
          val fieldName = fields.get(g).getName
          val colStats = statistic.getColumnStats(fieldName)
          if (colStats != null && colStats.getNdv != null) {
            // Never let ndv of a column go below 1, as it will result in incorrect calculations.
            ndv * Math.max(colStats.getNdv.toDouble, 1d)
          } else {
            return null
          }
      }
      val rowCount = mq.getRowCount(rel)
      FlinkRelMdUtil.adaptNdvBasedOnSelectivity(rowCount, distinctCount, selectivity)
    }
  }

  def getDistinctRowCount(
      rel: Values,
      mq: RelMetadataQuery,
      groupKey: ImmutableBitSet,
      predicate: RexNode): JDouble = {
    if (predicate == null || predicate.isAlwaysTrue) {
      if (groupKey.isEmpty) {
        return 1d
      }
    }
    val selectivity = RelMdUtil.guessSelectivity(predicate)
    val rowCount = mq.getRowCount(rel)
    val nRows = rowCount / 2
    FlinkRelMdUtil.numDistinctVals(nRows, nRows * selectivity)
  }

  def getDistinctRowCount(
      rel: Project,
      mq: RelMetadataQuery,
      groupKey: ImmutableBitSet,
      predicate: RexNode): JDouble = {
    val input = rel.getInput
    val program = RexProgram.create(
      input.getRowType,
      rel.getProjects,
      null,
      rel.getRowType,
      rel.getCluster.getRexBuilder)
    val equivCalc = LogicalCalc.create(input, program)
    getDistinctRowCount(equivCalc, mq, groupKey, predicate)
  }

  def getDistinctRowCount(
      rel: Filter,
      mq: RelMetadataQuery,
      groupKey: ImmutableBitSet,
      predicate: RexNode): JDouble = {
    if (predicate == null || predicate.isAlwaysTrue) {
      if (groupKey.isEmpty) {
        return 1d
      }
    }
    val unionPreds = RelMdUtil.unionPreds(rel.getCluster.getRexBuilder, predicate, rel.getCondition)
    mq.getDistinctRowCount(rel.getInput, groupKey, unionPreds)
  }

  def getDistinctRowCount(
      rel: Calc,
      mq: RelMetadataQuery,
      groupKey: ImmutableBitSet,
      predicate: RexNode): JDouble = {
    if (predicate == null || predicate.isAlwaysTrue) {
      if (groupKey.isEmpty) {
        return 1d
      }
    }
    val program = rel.getProgram
    val projects = program.getProjectList.map(program.expandLocalRef)
    val condition = if (program.getCondition != null) {
      program.expandLocalRef(program.getCondition)
    } else {
      null
    }
    val baseCols = ImmutableBitSet.builder()
    val projCols = ImmutableBitSet.builder()
    RelMdUtil.splitCols(projects, groupKey, baseCols, projCols)
    val pushable = new JArrayList[RexNode]()
    val notPushable = new JArrayList[RexNode]()
    RelOptUtil.splitFilters(
      ImmutableBitSet.range(rel.getRowType.getFieldCount),
      predicate,
      pushable,
      notPushable)
    val rexBuilder = rel.getCluster.getRexBuilder
    val childPred = RexUtil.composeConjunction(rexBuilder, pushable, true)
    val modifiedPred = if (childPred != null) {
      childPred.accept(new RexShuttle() {
        override def visitInputRef(ref: RexInputRef): RexNode = projects.get(ref.getIndex)
      })
    } else {
      null
    }
    val unionPreds = RelMdUtil.unionPreds(rexBuilder, condition, modifiedPred)
    var distinctRowCount = mq.getDistinctRowCount(rel.getInput, baseCols.build(), unionPreds)
    if (distinctRowCount == null) {
      return null
    }
    if (!notPushable.isEmpty) {
      val preds = RexUtil.composeConjunction(rexBuilder, notPushable, true)
      val rowCount = mq.getRowCount(rel)
      val selectivity = RelMdUtil.guessSelectivity(preds)
      distinctRowCount =
        FlinkRelMdUtil.adaptNdvBasedOnSelectivity(rowCount, distinctRowCount, selectivity)
    }
    // No further computation required if the projection expressions are all column references
    if (projCols.cardinality() == 0) {
      return distinctRowCount
    }
    projCols.build().foreach {
      bit =>
        val subRowCount = FlinkRelMdUtil.cardOfCalcExpr(mq, rel, projects.get(bit))
        if (subRowCount == null) {
          return null
        }
        distinctRowCount *= subRowCount
    }
    val rowCount = mq.getRowCount(rel)
    FlinkRelMdUtil.numDistinctVals(distinctRowCount, rowCount)
  }

  def getDistinctRowCount(
      rel: Expand,
      mq: RelMetadataQuery,
      groupKey: ImmutableBitSet,
      predicate: RexNode): JDouble = {
    val newPredicate = if (predicate == null) {
      null
    } else {
      val rexBuilder = rel.getCluster.getRexBuilder
      val tableConfig = unwrapTableConfig(rel)
      val maxCnfNodeCount = tableConfig.get(FlinkRexUtil.TABLE_OPTIMIZER_CNF_NODES_LIMIT)
      val cnf = FlinkRexUtil.toCnf(rexBuilder, maxCnfNodeCount, predicate)
      val conjunctions = RelOptUtil.conjunctions(cnf)
      val conjunctionsWithoutExpandId = conjunctions.filterNot {
        c =>
          val inputRefs = RelOptUtil.InputFinder.bits(c)
          inputRefs.toList.contains(rel.expandIdIndex)
      }
      // ignore expand_id condition if it exists in predicate
      RexUtil.composeConjunction(rexBuilder, conjunctionsWithoutExpandId, false)
    }
    // ndv of expand = ndv of project1 + ndv of project2 + ... + ndv of projectN-1
    if (groupKey.toList.contains(rel.expandIdIndex)) {
      val groupKeySkipExpandId = groupKey.filter(_ != rel.expandIdIndex)
      var ndv = 0d
      rel.projects.foreach {
        project =>
          val groupKeyOfCurrentProject = new JArrayList[Int]()
          groupKeySkipExpandId.foreach {
            key =>
              project.get(key) match {
                case literal: RexLiteral if literal.isNull => // do nothing
                case inputRef: RexInputRef => groupKeyOfCurrentProject.add(inputRef.getIndex)
                case e => throw new TableException(s"Unknown expression ${e.toString}!")
              }
          }
          val ndvOfCurrentProject = mq.getDistinctRowCount(
            rel.getInput,
            ImmutableBitSet.of(groupKeyOfCurrentProject: _*),
            newPredicate)
          if (ndvOfCurrentProject == null) {
            return null
          }
          ndv += ndvOfCurrentProject
      }
      ndv
    } else {
      mq.getDistinctRowCount(rel.getInput, groupKey, newPredicate)
    }
  }

  def getDistinctRowCount(
      rel: Exchange,
      mq: RelMetadataQuery,
      groupKey: ImmutableBitSet,
      predicate: RexNode): JDouble = mq.getDistinctRowCount(rel.getInput, groupKey, predicate)

  def getDistinctRowCount(
      rank: Rank,
      mq: RelMetadataQuery,
      groupKey: ImmutableBitSet,
      predicate: RexNode): JDouble = {
    val rankFunColumnIndex = RankUtil.getRankNumberColumnIndex(rank).getOrElse(-1)
    val newGroupKey = groupKey.clearIf(rankFunColumnIndex, rankFunColumnIndex > 0)
    val (nonRankPred, rankPred) = FlinkRelMdUtil.splitPredicateOnRank(rank, predicate)
    val inputNdv: JDouble = if (newGroupKey.nonEmpty) {
      mq.getDistinctRowCount(rank.getInput, newGroupKey, nonRankPred.orNull)
    } else {
      1d
    }
    val rankSelectivity: JDouble = rankPred match {
      case Some(p) => mq.getSelectivity(rank, p)
      case _ => 1d
    }

    val rankFunNdv: JDouble = if (rankFunColumnIndex > 0 && groupKey.get(rankFunColumnIndex)) {
      FlinkRelMdUtil.getRankRangeNdv(rank.rankRange)
    } else {
      1d // return 1D instead of null for computing directly
    }

    if (inputNdv == null) {
      null
    } else {
      val rowCount = mq.getRowCount(rank)
      FlinkRelMdUtil.adaptNdvBasedOnSelectivity(rowCount, inputNdv * rankFunNdv, rankSelectivity)
    }
  }

  def getDistinctRowCount(
      rel: Sort,
      mq: RelMetadataQuery,
      groupKey: ImmutableBitSet,
      predicate: RexNode): JDouble = {
    val ndv = mq.getDistinctRowCount(rel.getInput, groupKey, predicate)
    if (ndv != null) {
      val rowCount = mq.getRowCount(rel)
      if (rowCount == null) ndv else Math.min(ndv, rowCount)
    } else {
      null
    }
  }

  def getDistinctRowCount(
      rel: Aggregate,
      mq: RelMetadataQuery,
      groupKey: ImmutableBitSet,
      predicate: RexNode): JDouble = {
    if (predicate == null || predicate.isAlwaysTrue) {
      if (groupKey.isEmpty) {
        return 1d
      }
    }
    getDistinctRowCountOfAggregate(rel, mq, groupKey, predicate)
  }

  def getDistinctRowCount(
      rel: BatchPhysicalGroupAggregateBase,
      mq: RelMetadataQuery,
      groupKey: ImmutableBitSet,
      predicate: RexNode): JDouble = {
    if (predicate == null || predicate.isAlwaysTrue) {
      if (groupKey.isEmpty) {
        return 1d
      }
    }

    // for global agg which has inner local agg, it passes the parameters to input directly
    if (rel.isFinal && rel.isMerge) {
      return mq.getDistinctRowCount(rel.getInput, groupKey, predicate)
    }
    getDistinctRowCountOfAggregate(rel, mq, groupKey, predicate)
  }

  private def getDistinctRowCountOfAggregate(
      agg: SingleRel,
      mq: RelMetadataQuery,
      groupKey: ImmutableBitSet,
      predicate: RexNode): JDouble = {
    val (childPred, notPushablePred) = splitPredicateOnAggregate(agg, predicate)
    val (childKey, aggCalls) = FlinkRelMdUtil.splitGroupKeysOnAggregate(agg, groupKey)
    val input = agg.getInput

    val ndvOfColsInGroupKeys = mq.getDistinctRowCount(input, childKey, childPred.orNull)
    if (ndvOfColsInGroupKeys == null) {
      return null
    }
    val inputRowCount = mq.getRowCount(input)
    val factorOfKeyInAggCall = 0.1
    val ndvOfColsInAggCalls = aggCalls.foldLeft(1d) {
      (ndv, aggCall) =>
        val ndvOfAggCall = aggCall.getAggregation.getKind match {
          case SqlKind.COUNT =>
            val inputRowCnt = inputRowCount
            // Assume result of count(c) of each group bucket is different, start with 0, end with
            // N -1 (N is max ndv of count).
            // 0 + 1 + ... + (N - 1) <= rowCount => N ~= Sqrt(2 * rowCnt)
            // Max ndv of count(col) is Sqrt(2 * rowCnt)
            if (inputRowCnt != null) {
              Math.sqrt(2d * inputRowCnt)
            } else {
              return null
            }
          case _ =>
            val argList = aggCall.getArgList
            if (argList.isEmpty) {
              return null
            }
            val approximateNdv =
              mq.getDistinctRowCount(input, ImmutableBitSet.of(argList), childPred.orNull)
            if (approximateNdv != null) {
              approximateNdv * factorOfKeyInAggCall
            } else {
              return null
            }
        }
        ndv * Math.max(ndvOfAggCall, 1d)
    }
    val distinctRowCount = ndvOfColsInGroupKeys * ndvOfColsInAggCalls
    notPushablePred match {
      case Some(p) =>
        val aggCallEstimator =
          new AggCallSelectivityEstimator(agg, FlinkRelMetadataQuery.reuseOrCreate(mq))
        val restSelectivity = aggCallEstimator.evaluate(p) match {
          case Some(s) => s
          case _ => RelMdUtil.guessSelectivity(p)
        }
        val rowCount = mq.getRowCount(agg)
        val newNdv =
          FlinkRelMdUtil.adaptNdvBasedOnSelectivity(rowCount, distinctRowCount, restSelectivity)
        NumberUtil.min(newNdv, inputRowCount)
      case _ =>
        NumberUtil.min(distinctRowCount, inputRowCount)
    }
  }

  private def splitPredicateOnAggregate(
      agg: SingleRel,
      predicate: RexNode): (Option[RexNode], Option[RexNode]) = agg match {
    case rel: Aggregate =>
      FlinkRelMdUtil.splitPredicateOnAggregate(rel, predicate)
    case rel: BatchPhysicalGroupAggregateBase =>
      FlinkRelMdUtil.splitPredicateOnAggregate(rel, predicate)
    case rel: BatchPhysicalWindowAggregateBase =>
      FlinkRelMdUtil.splitPredicateOnAggregate(rel, predicate)
  }

  def getDistinctRowCount(
      rel: WindowAggregate,
      mq: RelMetadataQuery,
      groupKey: ImmutableBitSet,
      predicate: RexNode): JDouble = {
    val newPredicate = FlinkRelMdUtil.makeNamePropertiesSelectivityRexNode(rel, predicate)
    if (newPredicate == null || newPredicate.isAlwaysTrue) {
      if (groupKey.isEmpty) {
        return 1d
      }
    }
    val fieldCnt = rel.getRowType.getFieldCount
    val namedPropertiesCnt = rel.getNamedProperties.size
    val namedWindowStartIndex = fieldCnt - namedPropertiesCnt
    val groupKeyFromNamedWindow = groupKey.toList.exists(_ >= namedWindowStartIndex)
    if (groupKeyFromNamedWindow) {
      // cannot estimate DistinctRowCount result when some group keys are from named windows
      null
    } else {
      getDistinctRowCountOfAggregate(rel, mq, groupKey, newPredicate)
    }
  }

  def getDistinctRowCount(
      rel: BatchPhysicalWindowAggregateBase,
      mq: RelMetadataQuery,
      groupKey: ImmutableBitSet,
      predicate: RexNode): JDouble = {
    if (predicate == null || predicate.isAlwaysTrue) {
      if (groupKey.isEmpty) {
        return 1d
      }
    }

    val newPredicate = if (rel.isFinal) {
      val namedWindowStartIndex = rel.getRowType.getFieldCount - rel.namedWindowProperties.size
      val groupKeyFromNamedWindow = groupKey.toList.exists(_ >= namedWindowStartIndex)
      if (groupKeyFromNamedWindow) {
        // cannot estimate DistinctRowCount result when some group keys are from named windows
        return null
      }
      val newPredicate = FlinkRelMdUtil.makeNamePropertiesSelectivityRexNode(rel, predicate)
      if (rel.isMerge) {
        // set the bits as they correspond to local window aggregate
        val localWinAggGroupKey = FlinkRelMdUtil.setChildKeysOfWinAgg(groupKey, rel)
        val childPredicate = FlinkRelMdUtil.setChildPredicateOfWinAgg(newPredicate, rel)
        return mq.getDistinctRowCount(rel.getInput, localWinAggGroupKey, childPredicate)
      } else {
        newPredicate
      }
    } else {
      // local window aggregate
      val assignTsFieldIndex = rel.grouping.length
      if (groupKey.toList.contains(assignTsFieldIndex)) {
        // groupKey contains `assignTs` fields
        return null
      }
      predicate
    }
    getDistinctRowCountOfAggregate(rel, mq, groupKey, newPredicate)
  }

  def getDistinctRowCount(
      rel: Window,
      mq: RelMetadataQuery,
      groupKey: ImmutableBitSet,
      predicate: RexNode): JDouble = getDistinctRowCountOfOverAgg(rel, mq, groupKey, predicate)

  def getDistinctRowCount(
      rel: BatchPhysicalOverAggregate,
      mq: RelMetadataQuery,
      groupKey: ImmutableBitSet,
      predicate: RexNode): JDouble = getDistinctRowCountOfOverAgg(rel, mq, groupKey, predicate)

  private def getDistinctRowCountOfOverAgg(
      overAgg: SingleRel,
      mq: RelMetadataQuery,
      groupKey: ImmutableBitSet,
      predicate: RexNode): JDouble = {
    if (predicate == null || predicate.isAlwaysTrue) {
      if (groupKey.isEmpty) {
        return 1d
      }
    }
    val input = overAgg.getInput
    val fieldsCountOfInput = input.getRowType.getFieldCount
    val groupKeyContainsAggCall = groupKey.toList.exists(_ >= fieldsCountOfInput)
    // cannot estimate ndv of aggCall result of OverAgg
    if (groupKeyContainsAggCall) {
      null
    } else {
      val notPushable = new JArrayList[RexNode]
      val pushable = new JArrayList[RexNode]
      RelOptUtil.splitFilters(
        ImmutableBitSet.range(0, fieldsCountOfInput),
        predicate,
        pushable,
        notPushable)
      val rexBuilder = overAgg.getCluster.getRexBuilder
      val childPreds = RexUtil.composeConjunction(rexBuilder, pushable, true)
      val distinctRowCount = mq.getDistinctRowCount(input, groupKey, childPreds)
      if (distinctRowCount == null) {
        null
      } else if (notPushable.isEmpty) {
        distinctRowCount
      } else {
        val preds = RexUtil.composeConjunction(rexBuilder, notPushable, true)
        val rowCount = mq.getRowCount(overAgg)
        FlinkRelMdUtil.adaptNdvBasedOnSelectivity(
          rowCount,
          distinctRowCount,
          RelMdUtil.guessSelectivity(preds))
      }
    }
  }

  def getDistinctRowCount(
      rel: Join,
      mq: RelMetadataQuery,
      groupKey: ImmutableBitSet,
      predicate: RexNode): JDouble = {
    if (predicate == null || predicate.isAlwaysTrue) {
      if (groupKey.isEmpty) {
        return 1d
      }
    }
    rel.getJoinType match {
      case JoinRelType.SEMI | JoinRelType.ANTI =>
        // create a RexNode representing the selectivity of the
        // semi-join filter and pass it to getDistinctRowCount
        var newPred = FlinkRelMdUtil.makeSemiAntiJoinSelectivityRexNode(mq, rel)
        if (predicate != null) {
          val rexBuilder = rel.getCluster.getRexBuilder
          newPred = rexBuilder.makeCall(SqlStdOperatorTable.AND, newPred, predicate)
        }
        mq.getDistinctRowCount(rel.getLeft, groupKey, newPred)
      case _ =>
        FlinkRelMdUtil.getJoinDistinctRowCount(mq, rel, rel.getJoinType, groupKey, predicate, false)
    }
  }

  def getDistinctRowCount(
      rel: Union,
      mq: RelMetadataQuery,
      groupKey: ImmutableBitSet,
      predicate: RexNode): JDouble = {
    val adjustments = new Array[Int](rel.getRowType.getFieldCount)
    val rexBuilder = rel.getCluster.getRexBuilder
    val distinctRowCounts: Seq[JDouble] = rel.getInputs.map {
      input =>
        // convert the predicate to reference the types of the union child
        val modifiedPred = if (predicate != null) {
          predicate.accept(
            new RelOptUtil.RexInputConverter(
              rexBuilder,
              null,
              input.getRowType.getFieldList,
              adjustments))
        } else {
          null
        }
        mq.getDistinctRowCount(input, groupKey, modifiedPred)
    }

    if (distinctRowCounts.contains(null)) {
      null
    } else {
      // assume the rows from each input has no same row
      distinctRowCounts.foldLeft(0d)(_ + _)
    }
  }

  def getDistinctRowCount(
      subset: RelSubset,
      mq: RelMetadataQuery,
      groupKey: ImmutableBitSet,
      predicate: RexNode): JDouble = {
    if (!Bug.CALCITE_1048_FIXED) {
      val rel = Util.first(subset.getBest, subset.getOriginal)
      return mq.getDistinctRowCount(rel, groupKey, predicate)
    }

    subset.getRels.foldLeft(null.asInstanceOf[JDouble]) {
      (min, rel) =>
        try {
          NumberUtil.min(min, mq.getDistinctRowCount(rel, groupKey, predicate))
        } catch {
          // Ignore this relational expression; there will be non-cyclic ones
          // in this set.
          case _: CyclicMetadataException => min
        }
    }
  }

  /**
   * Catch-all implementation for
   * [[BuiltInMetadata.DistinctRowCount#getDistinctRowCount(ImmutableBitSet, RexNode)]], invoked
   * using reflection.
   *
   * @see
   *   org.apache.calcite.rel.metadata.RelMetadataQuery#getDistinctRowCount( RelNode,
   *   ImmutableBitSet, RexNode)
   */
  def getDistinctRowCount(
      rel: RelNode,
      mq: RelMetadataQuery,
      groupKey: ImmutableBitSet,
      predicate: RexNode): JDouble = {
    val unique = RelMdUtil.areColumnsDefinitelyUnique(mq, rel, groupKey)
    if (unique) {
      val rowCount = mq.getRowCount(rel)
      val selectivity = mq.getSelectivity(rel, predicate)
      NumberUtil.multiply(rowCount, selectivity)
    } else {
      null
    }
  }
}

object FlinkRelMdDistinctRowCount {

  private val INSTANCE = new FlinkRelMdDistinctRowCount

  val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
    BuiltInMethod.DISTINCT_ROW_COUNT.method,
    INSTANCE)

}
