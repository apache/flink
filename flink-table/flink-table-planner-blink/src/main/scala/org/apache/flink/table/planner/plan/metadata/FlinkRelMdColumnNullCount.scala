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

import org.apache.flink.table.planner.plan.metadata.FlinkMetadata.ColumnNullCount
import org.apache.flink.table.planner.plan.schema.FlinkPreparingTableBase
import org.apache.flink.table.planner.plan.utils.{FlinkRelOptUtil, FlinkRexUtil}
import org.apache.flink.table.planner.{JDouble, JList}
import org.apache.flink.util.Preconditions

import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.metadata._
import org.apache.calcite.rex.{RexCall, RexInputRef, RexLiteral, RexNode}
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.util.Util

import scala.collection.JavaConversions._

/**
  * FlinkRelMdColumnNullCount supplies a default implementation of
  * [[FlinkRelMetadataQuery.getColumnNullCount]] for the standard logical algebra.
  */
class FlinkRelMdColumnNullCount private extends MetadataHandler[ColumnNullCount] {

  override def getDef: MetadataDef[ColumnNullCount] = FlinkMetadata.ColumnNullCount.DEF

  /**
    * Gets the null count of the given column in TableScan.
    *
    * @param ts    TableScan RelNode
    * @param mq    RelMetadataQuery instance
    * @param index the index of the given column
    * @return the null count of the given column in TableScan
    */
  def getColumnNullCount(ts: TableScan, mq: RelMetadataQuery, index: Int): JDouble = {
    Preconditions.checkArgument(mq.isInstanceOf[FlinkRelMetadataQuery])
    val relOptTable = ts.getTable.asInstanceOf[FlinkPreparingTableBase]
    val fieldNames = relOptTable.getRowType.getFieldNames
    Preconditions.checkArgument(index >= 0 && index < fieldNames.size())
    val fieldName = fieldNames.get(index)
    val statistic = relOptTable.getStatistic
    val colStats = statistic.getColumnStats(fieldName)
    if (colStats != null && colStats.getNullCount != null) {
      colStats.getNullCount.toDouble
    } else {
      null
    }
  }

  /**
    * Gets the null count of the given column on Snapshot.
    *
    * @param snapshot    Snapshot RelNode
    * @param mq    RelMetadataQuery instance
    * @param index the index of the given column
    * @return the null count of the given column on Snapshot.
    */
  def getColumnNullCount(snapshot: Snapshot, mq: RelMetadataQuery, index: Int): JDouble = null

  /**
    * Gets the null count of the given column in Project.
    *
    * @param project Project RelNode
    * @param mq      RelMetadataQuery instance
    * @param index   the index of the given column
    * @return the null count of the given column in Project
    */
  def getColumnNullCount(project: Project, mq: RelMetadataQuery, index: Int): JDouble = {
    val (nullCountOfInput, _) = getColumnNullAfterProjects(
      project.getInput, project.getProjects, mq, index)
    nullCountOfInput
  }

  /**
    * Gets the null count of the given column in Filter.
    *
    * @param filter  Filter RelNode
    * @param mq    RelMetadataQuery instance
    * @param index the index of the given column
    * @return the null count of the given column in Filter
    */
  def getColumnNullCount(filter: Filter, mq: RelMetadataQuery, index: Int): JDouble = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val nullCountOfInput = fmq.getColumnNullCount(filter.getInput, index)
    val predicate = filter.getCondition
    getColumnNullAfterPredicate(predicate, nullCountOfInput, filter, index)
  }

  /**
    * Gets the null count of the given column in Calc.
    *
    * @param calc  Calc RelNode
    * @param mq    RelMetadataQuery instance
    * @param index the index of the given column
    * @return the null count of the given column in Calc
    */
  def getColumnNullCount(calc: Calc, mq: RelMetadataQuery, index: Int): JDouble = {
    val program = calc.getProgram
    val projects = program.getProjectList.map(program.expandLocalRef)
    val (nullCountOfInput, mappingFieldInInput) = getColumnNullAfterProjects(
      calc.getInput, projects, mq, index)
    if (program.getCondition == null) {
      nullCountOfInput
    } else {
      val predicate = program.expandLocalRef(program.getCondition)
      getColumnNullAfterPredicate(predicate, nullCountOfInput, calc, mappingFieldInInput)
    }
  }

  private def getColumnNullAfterProjects(
      input: RelNode,
      projects: JList[RexNode],
      mq: RelMetadataQuery,
      index: Int): (JDouble, Int) = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    projects.get(index) match {
      case inputRef: RexInputRef => (
        fmq.getColumnNullCount(input, inputRef.getIndex),
        inputRef.getIndex)
      case literal: RexLiteral => if (literal.isNull) (1D, -1) else (0D, -1)
      case _ => (null, -1)
    }
  }

  private def getColumnNullAfterPredicate(
      predicate: RexNode,
      nullCountOfInput: JDouble,
      rel: RelNode,
      mappingFieldInInput: Int): JDouble = {
    if (predicate == null) {
      nullCountOfInput
    } else {
      // if null count of input is already 0, then null count must be 0 after predicate.
      if (nullCountOfInput != null && Math.abs(nullCountOfInput) < RelOptUtil.EPSILON) {
        0D
      } else if (mappingFieldInInput == -1) {
        null
      } else {
        // If predicate has $index is not null, null count of index is must be 0 after predicate.
        val rexBuilder = rel.getCluster.getRexBuilder
        val tableConfig = FlinkRelOptUtil.getTableConfigFromContext(rel)
        val maxCnfNodeCount = tableConfig.getConfiguration.getInteger(
          FlinkRexUtil.TABLE_OPTIMIZER_CNF_NODES_LIMIT)
        val cnf = FlinkRexUtil.toCnf(rexBuilder, maxCnfNodeCount, predicate)
        val conjunctions = RelOptUtil.conjunctions(cnf)
        val notNullPredicatesAtIndexField = conjunctions.exists {
          case call: RexCall if call.getOperator == SqlStdOperatorTable.IS_NOT_NULL =>
            call.getOperands.head match {
              case i: RexInputRef => i.getIndex == mappingFieldInInput
              case _ => false
            }
          case _ => false
        }
        if (notNullPredicatesAtIndexField) 0D else null
      }
    }
  }

  def getColumnNullCount(rel: Exchange, mq: RelMetadataQuery, index: Int): JDouble = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    fmq.getColumnNullCount(rel.getInput, index)
  }

  def getColumnNullCount(rel: Sort, mq: RelMetadataQuery, index: Int): JDouble = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    fmq.getColumnNullCount(rel.getInput, index)
  }

  /**
    * Gets the null count of the given column in Join.
    *
    * @param rel   Join RelNode
    * @param mq    RelMetadataQuery instance
    * @param index the index of the given column
    * @return the null count of the given column in Join.
    */
  def getColumnNullCount(rel: Join, mq: RelMetadataQuery, index: Int): JDouble = {

    def isUniqueOnJoinKeys: Boolean = {
      val joinInfo = rel.analyzeCondition()
      if (joinInfo.leftKeys.isEmpty) {
        return false
      }
      val isLeftJoinKeysUnique = mq.areColumnsUnique(rel.getLeft, joinInfo.leftSet())
      if (isLeftJoinKeysUnique == null || !isLeftJoinKeysUnique) {
        return false
      }
      val isRightJoinKeysUnique = mq.areColumnsUnique(rel.getRight, joinInfo.rightSet())
      if (isRightJoinKeysUnique == null || !isRightJoinKeysUnique) {
        return false
      }
      true
    }

    val isLojOrRoj = rel.getJoinType match {
      case JoinRelType.LEFT | JoinRelType.RIGHT => true
      case _ => false
    }
    // Only handle LOJ OR ROJ, and both left joinKeys and right joinKeys are uniqueness.
    if (!isLojOrRoj || !isUniqueOnJoinKeys) {
      return null
    }

    def calRowCountOfNewInnerJoin(): JDouble = {
      val newInnerJoin = rel.copy(
        rel.getTraitSet, rel.getCondition, rel.getLeft, rel.getRight,
        JoinRelType.INNER, rel.isSemiJoinDone)
      mq.getRowCount(newInnerJoin)
    }

    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val nFieldsOnLeft = rel.getLeft.getRowType.getFieldCount
    rel.getJoinType match {
      case JoinRelType.LEFT =>
        if (index < nFieldsOnLeft) {
          // If column comes from left child of LOJ, nullCount is not changed.
          fmq.getColumnNullCount(rel.getLeft, index)
        } else {
          // If column comes from right child of LOJ, and both left joinKeys and right joinKeys are
          // unique in each child, nullCount is
          // RowCount(LeftChild) - RowCount(LeftChild InnerJoin RightChild) + OldNullCount
          val totalRowCount = mq.getRowCount(rel.getLeft)
          val rowCntOfInnerJoin = calRowCountOfNewInnerJoin()
          val oldNullCnt = fmq.getColumnNullCount(rel.getRight, index - nFieldsOnLeft)
          if (totalRowCount == null || rowCntOfInnerJoin == null || oldNullCnt == null) {
            null
          } else {
            Math.max(totalRowCount - rowCntOfInnerJoin, 0D) + oldNullCnt
          }
        }
      case JoinRelType.RIGHT =>
        if (index >= nFieldsOnLeft) {
          // If column comes from right child of ROJ, nullCount is not changed.
          fmq.getColumnNullCount(rel.getRight, index - nFieldsOnLeft)
        } else {
          // If column comes from left child of ROJ, and both left joinKeys and right joinKeys are
          // Unique in each child, nullCount is
          // RowCount(RightChild) - RowCount(LeftChild InnerJoin RightChild) + OldNullCount
          val totalRowCount = mq.getRowCount(rel.getRight)
          val rowCntOfInnerJoin = calRowCountOfNewInnerJoin()
          val oldNullCnt = fmq.getColumnNullCount(rel.getLeft, index)
          if (totalRowCount == null || rowCntOfInnerJoin == null || oldNullCnt == null) {
            null
          } else {
            Math.max(totalRowCount - rowCntOfInnerJoin, 0D) + oldNullCnt
          }
        }
    }
  }

  def getColumnNullCount(subset: RelSubset, mq: RelMetadataQuery, index: Int): JDouble = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val rel = Util.first(subset.getBest, subset.getOriginal)
    fmq.getColumnNullCount(rel, index)
  }

  /**
    * Catches-all rule when none of the others apply.
    *
    * @param rel   RelNode to analyze
    * @param mq    RelMetadataQuery instance
    * @param index the index of the given column
    * @return Always returns null
    */
  def getColumnNullCount(rel: RelNode, mq: RelMetadataQuery, index: Int): JDouble = null

}

object FlinkRelMdColumnNullCount {

  private val INSTANCE = new FlinkRelMdColumnNullCount

  val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
    FlinkMetadata.ColumnNullCount.METHOD, INSTANCE)

}
