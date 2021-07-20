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
import org.apache.flink.table.planner.plan.nodes.calcite.{Expand, Rank, WindowAggregate}
import org.apache.flink.table.planner.plan.nodes.physical.batch._
import org.apache.flink.table.planner.plan.utils.{FlinkRelMdUtil, RankUtil}
import org.apache.flink.table.planner.{JArrayList, JDouble}

import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.metadata._
import org.apache.calcite.rel.{RelNode, SingleRel}
import org.apache.calcite.rex.{RexInputRef, RexLiteral}
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.util.{BuiltInMethod, ImmutableBitSet, NumberUtil, Util}

import scala.collection.JavaConversions._

/**
  * [[FlinkRelMdPopulationSize]] supplies a implementation of
  * [[RelMetadataQuery#getPopulationSize]] for the standard logical algebra.
  */
class FlinkRelMdPopulationSize private extends MetadataHandler[BuiltInMetadata.PopulationSize] {

  override def getDef: MetadataDef[BuiltInMetadata.PopulationSize] =
    BuiltInMetadata.PopulationSize.DEF

  def getPopulationSize(
      rel: TableScan,
      mq: RelMetadataQuery,
      groupKey: ImmutableBitSet): JDouble = {
    val unique = RelMdUtil.areColumnsDefinitelyUnique(mq, rel, groupKey)
    if (unique) {
      mq.getRowCount(rel)
    } else {
      mq.getDistinctRowCount(rel, groupKey, null)
    }
  }

  def getPopulationSize(
      rel: Values,
      mq: RelMetadataQuery,
      groupKey: ImmutableBitSet): JDouble = {
    // assume half the rows are duplicates
    // PopulationSize should not be less than 1.0
    val rowCount = rel.estimateRowCount(mq)
    Math.max(rowCount / 2.0, 1.0)
  }

  def getPopulationSize(
      rel: Project,
      mq: RelMetadataQuery,
      groupKey: ImmutableBitSet): JDouble = {
    val baseCols = ImmutableBitSet.builder
    val projCols = ImmutableBitSet.builder
    val projExprs = rel.getProjects
    RelMdUtil.splitCols(projExprs, groupKey, baseCols, projCols)
    var population = mq.getPopulationSize(rel.getInput, baseCols.build)
    if (population == null) {
      return null
    }
    // No further computation required if the projection expressions are
    // all column references
    if (projCols.cardinality == 0) {
      return population
    }
    for (bit <- projCols.build) {
      val subRowCount = RelMdUtil.cardOfProjExpr(mq, rel, projExprs.get(bit))
      if (subRowCount == null) {
        return null
      }
      // subRowCount may be less than 1.0
      population *= Math.max(1.0, subRowCount)
    }
    // REVIEW zfong 6/22/06 - Broadbase did not have the call to
    // numDistinctVals.  This is needed; otherwise, population can be
    // larger than the number of rows in the RelNode.
    val rowCount = mq.getRowCount(rel)
    FlinkRelMdUtil.numDistinctVals(population, rowCount)
  }

  def getPopulationSize(
      rel: Filter,
      mq: RelMetadataQuery,
      groupKey: ImmutableBitSet): JDouble = mq.getPopulationSize(rel.getInput, groupKey)

  def getPopulationSize(
      rel: Calc,
      mq: RelMetadataQuery,
      groupKey: ImmutableBitSet): JDouble = {
    val program = rel.getProgram
    val projects = program.getProjectList.map(program.expandLocalRef)

    val baseCols = ImmutableBitSet.builder
    val projCols = ImmutableBitSet.builder
    RelMdUtil.splitCols(projects, groupKey, baseCols, projCols)

    var population = mq.getPopulationSize(rel.getInput, baseCols.build)
    if (population == null) {
      return null
    }

    // No further computation required if the projection expressions are
    // all column references
    if (projCols.cardinality == 0) {
      return population
    }

    for (bit <- projCols.build) {
      val subRowCount = FlinkRelMdUtil.cardOfCalcExpr(mq, rel, projects.get(bit))
      if (subRowCount == null) {
        return null
      }
      // subRowCount may be less than 1.0
      population *= Math.max(1.0, subRowCount)
    }
    // REVIEW zfong 6/22/06 - Broadbase did not have the call to
    // numDistinctVals.  This is needed; otherwise, population can be
    // larger than the number of rows in the RelNode.
    val rowCount = mq.getRowCount(rel)
    FlinkRelMdUtil.numDistinctVals(population, rowCount)
  }

  def getPopulationSize(
      rel: Expand,
      mq: RelMetadataQuery,
      groupKey: ImmutableBitSet): JDouble = {
    if (groupKey.toList.contains(rel.expandIdIndex)) {
      // populationSize of expand = populationSize of project1 + populationSize of project2 + ...
      // + populationSize of projectN-1
      val groupKeyIgnoreExpandId = groupKey.filter(_ != rel.expandIdIndex)
      var populationSize = 0D
      rel.projects foreach { project =>
        val groupKeyOfCurrentProject = new JArrayList[Int]()
        groupKeyIgnoreExpandId.foreach { key =>
          project.get(key) match {
            case literal: RexLiteral if literal.isNull => // do nothing
            case inputRef: RexInputRef => groupKeyOfCurrentProject.add(inputRef.getIndex)
            case e => throw new TableException(s"Unknown expression ${e.toString}!")
          }
        }
        val populationSizeOfCurrentProject =
          mq.getPopulationSize(rel.getInput, ImmutableBitSet.of(groupKeyOfCurrentProject: _*))
        if (populationSizeOfCurrentProject == null) {
          return null
        }
        populationSize += populationSizeOfCurrentProject
      }
      populationSize
    } else {
      mq.getPopulationSize(rel.getInput, groupKey)
    }
  }

  def getPopulationSize(
      rel: Exchange,
      mq: RelMetadataQuery,
      groupKey: ImmutableBitSet): JDouble = mq.getPopulationSize(rel.getInput, groupKey)

  def getPopulationSize(
      rel: Rank,
      mq: RelMetadataQuery,
      groupKey: ImmutableBitSet): JDouble = {
    val rankFunColumnIndex = RankUtil.getRankNumberColumnIndex(rel).getOrElse(-1)
    if (rankFunColumnIndex < 0 || !groupKey.toArray.contains(rankFunColumnIndex)) {
      mq.getPopulationSize(rel.getInput, groupKey)
    } else {
      val rankFunNdv: JDouble = if (rankFunColumnIndex > 0 &&
        groupKey.toArray.contains(rankFunColumnIndex)) {
        FlinkRelMdUtil.getRankRangeNdv(rel.rankRange)
      } else {
        1D
      }
      val newGroupKey = groupKey.clear(rankFunColumnIndex)
      val inputPopulationSize: JDouble = if (newGroupKey.isEmpty) {
        1D
      } else {
        val size = mq.getPopulationSize(rel.getInput, newGroupKey)
        if (size == null) {
          return null
        }
        size
      }
      val populationSize = inputPopulationSize * rankFunNdv
      val rowCount = mq.getRowCount(rel)
      NumberUtil.min(populationSize, rowCount)
    }
  }

  def getPopulationSize(
      rel: Sort,
      mq: RelMetadataQuery,
      groupKey: ImmutableBitSet): JDouble = mq.getPopulationSize(rel.getInput, groupKey)

  def getPopulationSize(
      rel: Aggregate,
      mq: RelMetadataQuery,
      groupKey: ImmutableBitSet): JDouble = {
    getPopulationSizeOfAggregate(rel, mq, groupKey)
  }

  def getPopulationSize(
      rel: BatchPhysicalGroupAggregateBase,
      mq: RelMetadataQuery,
      groupKey: ImmutableBitSet): JDouble = {
    // for global agg which has inner local agg, it passes the parameters to input directly
    if (rel.isFinal && rel.isMerge) {
      return mq.getPopulationSize(rel.getInput, groupKey)
    }

    getPopulationSizeOfAggregate(rel, mq, groupKey)
  }

  private def getPopulationSizeOfAggregate(
      agg: SingleRel,
      mq: RelMetadataQuery,
      groupKey: ImmutableBitSet): JDouble = {
    val (childKey, aggCalls) = FlinkRelMdUtil.splitGroupKeysOnAggregate(agg, groupKey)
    val popSizeOfColsInGroupKeys = mq.getPopulationSize(agg.getInput, childKey)
    if (popSizeOfColsInGroupKeys == null) {
      return null
    }
    val factorOfKeyInAggCall = 0.1
    val popSizeOfColsInAggCalls = aggCalls.foldLeft(1D) {
      (popSize, aggCall) =>
        val popSizeOfAggCall = aggCall.getAggregation.getKind match {
          case SqlKind.COUNT =>
            val inputRowCnt = mq.getRowCount(agg.getInput)
            // Assume result of count(c) of each group bucket is different, start with 0, end with
            // N -1 (N is max ndv of count).
            // 0 + 1 + ... + (N - 1) <= rowCount => N ~= Sqrt(2 * rowCnt)
            if (inputRowCnt != null) {
              Math.sqrt(2D * inputRowCnt)
            } else {
              return null
            }
          case _ =>
            val argList = aggCall.getArgList
            if (argList.isEmpty) {
              return null
            }
            val approximatePopSize = mq.getPopulationSize(
              agg.getInput,
              ImmutableBitSet.of(argList))
            if (approximatePopSize != null) {
              approximatePopSize * factorOfKeyInAggCall
            } else {
              return null
            }
        }
        popSize * Math.max(popSizeOfAggCall, 1D)
    }
    val inputRowCnt = mq.getRowCount(agg.getInput)
    NumberUtil.min(popSizeOfColsInGroupKeys * popSizeOfColsInAggCalls, inputRowCnt)
  }

  def getPopulationSize(
      rel: WindowAggregate,
      mq: RelMetadataQuery,
      groupKey: ImmutableBitSet): JDouble = {
    val fieldCnt = rel.getRowType.getFieldCount
    val namedPropertiesCnt = rel.getNamedProperties.size
    val namedWindowStartIndex = fieldCnt - namedPropertiesCnt
    val groupKeyFromNamedWindow = groupKey.toList.exists(_ >= namedWindowStartIndex)
    if (groupKeyFromNamedWindow) {
      // cannot estimate PopulationSize result when some group keys are from named windows
      null
    } else {
      // regular aggregate
      getPopulationSize(rel.asInstanceOf[Aggregate], mq, groupKey)
    }
  }

  def getPopulationSize(
      rel: BatchPhysicalWindowAggregateBase,
      mq: RelMetadataQuery,
      groupKey: ImmutableBitSet): JDouble = {
    if (rel.isFinal) {
      val namedWindowStartIndex = rel.getRowType.getFieldCount - rel.namedWindowProperties.size
      val groupKeyFromNamedWindow = groupKey.toList.exists(_ >= namedWindowStartIndex)
      if (groupKeyFromNamedWindow) {
        return null
      }
      if (rel.isMerge) {
        // set the bits as they correspond to local window aggregate
        val localWinAggGroupKey = FlinkRelMdUtil.setChildKeysOfWinAgg(groupKey, rel)
        return mq.getPopulationSize(rel.getInput, localWinAggGroupKey)
      }
    } else {
      // local window aggregate
      val assignTsFieldIndex = rel.grouping.length
      if (groupKey.toList.contains(assignTsFieldIndex)) {
        // groupKey contains `assignTs` fields
        return null
      }
    }
    getPopulationSizeOfAggregate(rel, mq, groupKey)
  }

  def getPopulationSize(
      window: Window,
      mq: RelMetadataQuery,
      groupKey: ImmutableBitSet): JDouble = getPopulationSizeOfOverAgg(window, mq, groupKey)

  def getPopulationSize(
      rel: BatchPhysicalOverAggregate,
      mq: RelMetadataQuery,
      groupKey: ImmutableBitSet): JDouble = getPopulationSizeOfOverAgg(rel, mq, groupKey)

  private def getPopulationSizeOfOverAgg(
      overAgg: SingleRel,
      mq: RelMetadataQuery,
      groupKey: ImmutableBitSet): JDouble = {
    val input = overAgg.getInput
    val fieldsCountOfInput = input.getRowType.getFieldCount
    val groupKeyContainsAggCall = groupKey.toList.exists(_ >= fieldsCountOfInput)
    // cannot estimate population size of aggCall result of OverAgg
    if (groupKeyContainsAggCall) {
      null
    } else {
      mq.getPopulationSize(input, groupKey)
    }
  }

  def getPopulationSize(
      rel: Join,
      mq: RelMetadataQuery,
      groupKey: ImmutableBitSet): JDouble = {
    rel.getJoinType match {
      case JoinRelType.SEMI | JoinRelType.ANTI =>
        mq.getPopulationSize(rel.getLeft, groupKey)
      case _ =>
        RelMdUtil.getJoinPopulationSize(mq, rel, groupKey)
    }
  }

  def getPopulationSize(
      rel: Union,
      mq: RelMetadataQuery,
      groupKey: ImmutableBitSet): JDouble = {
    var population = 0.0
    for (input <- rel.getInputs) {
      val subPop = mq.getPopulationSize(input, groupKey)
      if (subPop == null) {
        return null
      }
      population += subPop
    }
    population
  }

  def getPopulationSize(
      subset: RelSubset,
      mq: RelMetadataQuery,
      groupKey: ImmutableBitSet): JDouble = {
    val rel = Util.first(subset.getBest, subset.getOriginal)
    mq.getPopulationSize(rel, groupKey)
  }

  /**
    * Catch-all implementation for
    * [[BuiltInMetadata.PopulationSize#getPopulationSize(ImmutableBitSet)]],
    * invoked using reflection.
    *
    * @see org.apache.calcite.rel.metadata.RelMetadataQuery#getPopulationSize(RelNode,
    *      ImmutableBitSet)
    */
  def getPopulationSize(
      rel: RelNode,
      mq: RelMetadataQuery,
      groupKey: ImmutableBitSet): JDouble = {
    // if the keys are unique, return the row count; otherwise, we have
    // no further information on which to return any legitimate value
    // REVIEW zfong 4/11/06 - Broadbase code returns the product of each
    // unique key, which would result in the population being larger
    // than the total rows in the relnode
    val unique = RelMdUtil.areColumnsDefinitelyUnique(mq, rel, groupKey)
    if (unique) {
      mq.getRowCount(rel)
    } else {
      null
    }
  }

}

object FlinkRelMdPopulationSize {

  private val INSTANCE = new FlinkRelMdPopulationSize

  val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
    BuiltInMethod.POPULATION_SIZE.method, INSTANCE)

}
