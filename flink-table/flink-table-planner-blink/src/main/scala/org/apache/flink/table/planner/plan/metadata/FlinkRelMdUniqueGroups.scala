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

import org.apache.flink.table.planner.calcite.FlinkRelBuilder.PlannerNamedWindowProperty
import org.apache.flink.table.planner.plan.metadata.FlinkMetadata.UniqueGroups
import org.apache.flink.table.planner.plan.nodes.calcite.{Expand, Rank, WindowAggregate}
import org.apache.flink.table.planner.plan.nodes.physical.batch._
import org.apache.flink.table.planner.plan.utils.{AggregateUtil, FlinkRelMdUtil, RankUtil}

import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.metadata._
import org.apache.calcite.rel.{RelNode, SingleRel}
import org.apache.calcite.rex._
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.util.{Bug, ImmutableBitSet, Util}

import java.util

import scala.collection.JavaConversions._
import scala.collection.mutable

class FlinkRelMdUniqueGroups private extends MetadataHandler[UniqueGroups] {

  override def getDef: MetadataDef[UniqueGroups] = FlinkMetadata.UniqueGroups.DEF

  def getUniqueGroups(
      ts: TableScan,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): ImmutableBitSet = {
    val uniqueKeys = mq.getUniqueKeys(ts)
    if (uniqueKeys == null || uniqueKeys.isEmpty) {
      return columns
    }
    require(columns.forall(_ < ts.getRowType.getFieldCount))
    val none = Option.empty[ImmutableBitSet]
    // find the minimum uniqueKey
    val uniqueGroups = uniqueKeys.foldLeft(none) {
      (groups, uniqueKey) =>
        val containUniqueKey = columns.contains(uniqueKey)
        groups match {
          case Some(g) =>
            if (containUniqueKey && g.cardinality() > uniqueKey.cardinality()) {
              Some(uniqueKey)
            } else {
              groups
            }
          case _ => if (containUniqueKey) Some(uniqueKey) else none
        }
    }
    uniqueGroups.getOrElse(columns)
  }

  def getUniqueGroups(
      project: Project,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): ImmutableBitSet = {
    val projects = project.getProjects
    getUniqueGroupsOfProject(projects, project.getInput, mq, columns)
  }

  def getUniqueGroups(
      filter: Filter,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): ImmutableBitSet = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    fmq.getUniqueGroups(filter.getInput, columns)
  }

  def getUniqueGroups(
      calc: Calc,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): ImmutableBitSet = {
    val projects = calc.getProgram.getProjectList.map(calc.getProgram.expandLocalRef)
    getUniqueGroupsOfProject(projects, calc.getInput, mq, columns)
  }

  private def getUniqueGroupsOfProject(
      projects: util.List[RexNode],
      input: RelNode,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): ImmutableBitSet = {
    val columnList = columns.toList
    val mapInToOutRefPos = new mutable.HashMap[Integer, Integer]()
    // find non-RexInputRef and non-Constant columns
    val outNonRefOrConstantCols = new mutable.ArrayBuffer[Integer]()
    columns.foreach { column =>
      require(column < projects.size)
      projects.get(column) match {
        case ref: RexInputRef => mapInToOutRefPos.putIfAbsent(ref.getIndex, column)
        case call: RexCall if call.getKind.equals(SqlKind.AS) &&
          call.getOperands.head.isInstanceOf[RexInputRef] =>
          val index = call.getOperands.head.asInstanceOf[RexInputRef].getIndex
          mapInToOutRefPos.putIfAbsent(index, column)
        case _: RexLiteral => // do nothing
        case _ => outNonRefOrConstantCols += column
      }
    }

    if (mapInToOutRefPos.isEmpty) {
      val nonConstantCols = columnList.filterNot { column =>
        projects.get(column).isInstanceOf[RexLiteral]
      }
      if (nonConstantCols.isEmpty) {
        // all columns are constant, return first column
        ImmutableBitSet.of(columnList.head)
      } else {
        // return non-constant columns
        ImmutableBitSet.of(nonConstantCols)
      }
    } else {
      val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
      val inputColumns = ImmutableBitSet.of(mapInToOutRefPos.keys.toList)
      val inputUniqueGroups = fmq.getUniqueGroups(input, inputColumns)
      val outputUniqueGroups = inputUniqueGroups.asList.map {
        k => mapInToOutRefPos.getOrElse(k, throw new IllegalArgumentException(s"Illegal index: $k"))
      }
      ImmutableBitSet.of(outputUniqueGroups).union(ImmutableBitSet.of(outNonRefOrConstantCols))
    }
  }

  def getUniqueGroups(
      expand: Expand,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): ImmutableBitSet = {
    val columnList = columns.toList
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val columnsSkipExpandId = columnList.filter(_ != expand.expandIdIndex)
    if (columnsSkipExpandId.isEmpty) {
      return columns
    }

    // mapping input column index to output index for non-null value columns
    val mapInputToOutput = new util.HashMap[Int, Int]()
    columnsSkipExpandId.foreach { column =>
      val inputRefs = FlinkRelMdUtil.getInputRefIndices(column, expand)
      if (inputRefs.size() == 1 && inputRefs.head >= 0) {
        mapInputToOutput.put(inputRefs.head, column)
      }
    }
    if (mapInputToOutput.isEmpty) {
      return columns
    }

    val leftColumns = columnList.filterNot(mapInputToOutput.values().contains)
    val inputUniqueGroups = fmq.getUniqueGroups(
      expand.getInput, ImmutableBitSet.of(mapInputToOutput.keys.toSeq: _*))
    val outputUniqueGroups = inputUniqueGroups.map(mapInputToOutput.get)
    ImmutableBitSet.of(outputUniqueGroups.toSeq: _*).union(ImmutableBitSet.of(leftColumns))
  }

  def getUniqueGroups(
      exchange: Exchange,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): ImmutableBitSet = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    fmq.getUniqueGroups(exchange.getInput, columns)
  }

  def getUniqueGroups(
      rank: Rank,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): ImmutableBitSet = {
    val columnList = columns.toList
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val rankFunColumnIndex = RankUtil.getRankNumberColumnIndex(rank).getOrElse(-1)
    val columnSkipRankCol = columnList.filter(_ != rankFunColumnIndex)
    if (columnSkipRankCol.isEmpty) {
      return columns
    }
    val inputUniqueGroups = fmq.getUniqueGroups(
      rank.getInput, ImmutableBitSet.of(columnSkipRankCol))
    if (columnList.contains(rankFunColumnIndex)) {
      inputUniqueGroups.union(ImmutableBitSet.of(rankFunColumnIndex))
    } else {
      inputUniqueGroups
    }
  }

  def getUniqueGroups(
      sort: Sort,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): ImmutableBitSet = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    fmq.getUniqueGroups(sort.getInput, columns)
  }

  def getUniqueGroups(
      agg: Aggregate,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): ImmutableBitSet = {
    val grouping = agg.getGroupSet.map(_.toInt).toArray
    getUniqueGroupsOfAggregate(agg.getRowType.getFieldCount, grouping, agg.getInput, mq, columns)
  }

  def getUniqueGroups(
      agg: BatchPhysicalGroupAggregateBase,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): ImmutableBitSet = {
    val grouping = agg.grouping
    getUniqueGroupsOfAggregate(agg.getRowType.getFieldCount, grouping, agg.getInput, mq, columns)
  }

  private def getUniqueGroupsOfAggregate(
      outputFiledCount: Int,
      grouping: Array[Int],
      input: RelNode,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): ImmutableBitSet = {
    val columnList = columns.toList
    val groupingInToOutMap = new mutable.HashMap[Integer, Integer]()
    columnList.foreach { column =>
      require(column < outputFiledCount)
      if (column < grouping.length) {
        groupingInToOutMap.put(grouping(column), column)
      }
    }
    if (groupingInToOutMap.isEmpty) {
      columns
    } else {
      val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
      val inputColumns = ImmutableBitSet.of(groupingInToOutMap.keys.toList)
      val inputUniqueGroups = fmq.getUniqueGroups(input, inputColumns)
      val uniqueGroupsFromGrouping = inputUniqueGroups.asList.map { k =>
        groupingInToOutMap.getOrElse(k, throw new IllegalArgumentException(s"Illegal index: $k"))
      }
      val nonGroupingCols = if (inputColumns.toArray.sorted.sameElements(grouping.sorted)) {
        // if values of inputColumns are grouping columns, nonGroupingCols can be dropped.
        // (because grouping columns are unique.)
        Seq.empty[Integer]
      } else {
        val groupingOutColumns = groupingInToOutMap.values
        columnList.filterNot(groupingOutColumns.contains(_))
      }
      ImmutableBitSet.of(uniqueGroupsFromGrouping).union(ImmutableBitSet.of(nonGroupingCols))
    }
  }

  def getUniqueGroups(
      agg: WindowAggregate,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): ImmutableBitSet = {
    val grouping = agg.getGroupSet.map(_.toInt).toArray
    val namedProperties = agg.getNamedProperties
    val (auxGroupSet, _) = AggregateUtil.checkAndSplitAggCalls(agg)
    getUniqueGroupsOfWindowAgg(agg, grouping, auxGroupSet, namedProperties, mq, columns)
  }

  def getUniqueGroups(
      agg: BatchPhysicalWindowAggregateBase,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): ImmutableBitSet = {
    getUniqueGroupsOfWindowAgg(
      agg, agg.grouping, agg.auxGrouping, agg.namedWindowProperties, mq, columns)
  }

  private def getUniqueGroupsOfWindowAgg(
      windowAgg: SingleRel,
      grouping: Array[Int],
      auxGrouping: Array[Int],
      namedProperties: Seq[PlannerNamedWindowProperty],
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): ImmutableBitSet = {
    val fieldCount = windowAgg.getRowType.getFieldCount
    val columnList = columns.toList
    val groupingInToOutMap = new mutable.HashMap[Integer, Integer]()
    columnList.foreach { column =>
      require(column < fieldCount)
      if (column < grouping.length) {
        groupingInToOutMap.put(grouping(column), column)
      }
    }
    if (groupingInToOutMap.isEmpty) {
      columns
    } else {
      val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
      val inputColumns = ImmutableBitSet.of(groupingInToOutMap.keys.toList)
      val inputUniqueGroups = fmq.getUniqueGroups(windowAgg.getInput, inputColumns)
      val uniqueGroupsFromGrouping = inputUniqueGroups.asList.map { i =>
        groupingInToOutMap.getOrElse(i, throw new IllegalArgumentException(s"Illegal index: $i"))
      }
      val fullGroupingOutputIndices =
        grouping.indices ++ auxGrouping.indices.map(_ + grouping.length)
      if (columns.equals(ImmutableBitSet.of(fullGroupingOutputIndices: _*))) {
        return ImmutableBitSet.of(uniqueGroupsFromGrouping)
      }

      val groupingOutCols = groupingInToOutMap.values
      // TODO drop some nonGroupingCols base on FlinkRelMdColumnUniqueness#areColumnsUnique(window)
      val nonGroupingCols = columnList.filterNot(groupingOutCols.contains)
      ImmutableBitSet.of(uniqueGroupsFromGrouping).union(ImmutableBitSet.of(nonGroupingCols))
    }
  }

  def getUniqueGroups(
      over: Window,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): ImmutableBitSet = {
    getUniqueGroupsOfOver(over.getRowType.getFieldCount, over.getInput, mq, columns)
  }

  def getUniqueGroups(
      over: BatchExecOverAggregate,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): ImmutableBitSet = {
    getUniqueGroupsOfOver(over.getRowType.getFieldCount, over.getInput, mq, columns)
  }

  private def getUniqueGroupsOfOver(
      outputFiledCount: Int,
      input: RelNode,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): ImmutableBitSet = {
    val inputFieldCount = input.getRowType.getFieldCount
    val (inputColumns, nonInputColumns) = columns.toList.partition(_ < inputFieldCount)
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val inputUniqueGroups = fmq.getUniqueGroups(input, ImmutableBitSet.of(inputColumns))
    inputUniqueGroups.union(ImmutableBitSet.of(nonInputColumns))
  }

  def getUniqueGroups(
      join: Join,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): ImmutableBitSet = {
    require(join.getSystemFieldList.isEmpty)
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    join.getJoinType match {
      case JoinRelType.SEMI | JoinRelType.ANTI =>
        return fmq.getUniqueGroups(join.getLeft, columns)
      case _ => // do nothing
    }

    val leftFieldCount = join.getLeft.getRowType.getFieldCount
    val (leftColumns, rightColumns) =
      FlinkRelMdUtil.splitColumnsIntoLeftAndRight(leftFieldCount, columns)

    val leftUniqueGroups = fmq.getUniqueGroups(join.getLeft, leftColumns)
    val rightUniqueGroups = fmq.getUniqueGroups(join.getRight, rightColumns)

    val joinType = join.getJoinType
    val joinInfo = join.analyzeCondition()
    val leftJoinKeys = ImmutableBitSet.of(joinInfo.leftKeys)
    val rightJoinKeys = ImmutableBitSet.of(joinInfo.rightKeys)
    // for INNER and LEFT join, returns leftUniqueGroups if the join keys of RHS are unique
    if (leftJoinKeys.nonEmpty
      && leftUniqueGroups.contains(leftJoinKeys)
      && !joinType.generatesNullsOnLeft()) {
      val isRightJoinKeysUnique = fmq.areColumnsUnique(join.getRight, rightJoinKeys)
      if (isRightJoinKeysUnique != null && isRightJoinKeysUnique) {
        return leftUniqueGroups
      }
    }

    val outputRightUniqueGroups =
      rightUniqueGroups.asList.map(c => Integer.valueOf(c + leftFieldCount))
    // for INNER and RIGHT join, returns rightUniqueGroups if the join keys of LHS are unique
    if (rightJoinKeys.nonEmpty
      && rightUniqueGroups.contains(rightJoinKeys)
      && !joinType.generatesNullsOnRight()) {
      val isLeftJoinKeysUnique = fmq.areColumnsUnique(join.getLeft, leftJoinKeys)
      if (isLeftJoinKeysUnique != null && isLeftJoinKeysUnique) {
        return ImmutableBitSet.of(outputRightUniqueGroups)
      }
    }

    leftUniqueGroups.union(ImmutableBitSet.of(outputRightUniqueGroups))
  }

  def getUniqueGroups(
      rel: Correlate,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): ImmutableBitSet = columns

  def getUniqueGroups(
      rel: BatchPhysicalCorrelate,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): ImmutableBitSet = columns

  def getUniqueGroups(
      rel: SetOp,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): ImmutableBitSet = columns

  def getUniqueGroups(
      subset: RelSubset,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): ImmutableBitSet = {
    if (!Bug.CALCITE_1048_FIXED) {
      //if the best node is null, so we can get the uniqueKeys based original node, due to
      //the original node is logically equivalent as the rel.
      val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
      val rel = Util.first(subset.getBest, subset.getOriginal)
      fmq.getUniqueGroups(rel, columns)
    } else {
      throw new RuntimeException("CALCITE_1048 is fixed, so check this method again!")
    }
  }

  // Catch-all rule when none of the others apply.
  def getUniqueGroups(
      rel: RelNode,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): ImmutableBitSet = columns

}

object FlinkRelMdUniqueGroups {

  private val INSTANCE = new FlinkRelMdUniqueGroups

  val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
    FlinkMetadata.UniqueGroups.METHOD, INSTANCE)

}
