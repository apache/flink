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

import org.apache.flink.table.calcite.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.plan.FlinkJoinRelType
import org.apache.flink.table.plan.nodes.FlinkRelNode
import org.apache.flink.table.plan.nodes.calcite.{Expand, LogicalWindowAggregate, Rank}
import org.apache.flink.table.plan.nodes.logical._
import org.apache.flink.table.plan.nodes.physical.batch._
import org.apache.flink.table.plan.nodes.physical.stream._
import org.apache.flink.table.plan.schema.FlinkRelOptTable
import org.apache.flink.table.plan.util.FlinkRelMdUtil.splitColumnsIntoLeftAndRight
import org.apache.flink.table.plan.util.{FlinkRelMdUtil, TemporalJoinUtil}
import org.apache.flink.table.sources.{IndexKey, TableSource}

import org.apache.calcite.plan.RelOptTable
import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.convert.Converter
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.logical.LogicalSnapshot
import org.apache.calcite.rel.metadata._
import org.apache.calcite.rel.{RelNode, SingleRel}
import org.apache.calcite.rex.{RexCall, RexInputRef, RexNode}
import org.apache.calcite.sql.SemiJoinType._
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.util.{Bug, BuiltInMethod, ImmutableBitSet, Util}

import java.lang.{Boolean => JBool}
import java.util

import scala.collection.JavaConversions._

class FlinkRelMdColumnUniqueness private extends MetadataHandler[BuiltInMetadata.ColumnUniqueness] {

  def getDef: MetadataDef[BuiltInMetadata.ColumnUniqueness] = BuiltInMetadata.ColumnUniqueness.DEF

  private def areTableColumnsUnique(
      rel: TableScan,
      tableSource: TableSource,
      relOptTable: RelOptTable,
      columns: ImmutableBitSet): JBool = {
    if (columns.cardinality == 0) {
      return false
    }
    if (tableSource != null) {
      val indexes = TemporalJoinUtil.getTableIndexKeys(tableSource)
      if (null != indexes &&
        indexes.exists(index => index.isUnique && index.isIndex(columns.toArray))) {
        return true
      }
    }

    relOptTable match {
      case flinkRelOptTable: FlinkRelOptTable => flinkRelOptTable.uniqueKeysSet match {
        case Some(keysSet) if keysSet.isEmpty => false
        case Some(keysSet) => keysSet.exists(columns.contains)
        case _ => null
      }
      case _ => rel.getTable.isKey(columns)
    }
  }

  def areColumnsUnique(
      rel: TableScan,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBool = {
    areTableColumnsUnique(rel, null, rel.getTable, columns)
  }

  def areColumnsUnique(
      rel: Window,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBool = areColumnsUniqueOfOverWindow(rel, mq, columns, ignoreNulls)

  def areColumnsUnique(
      rel: BatchExecOverAggregate,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBool = areColumnsUniqueOfOverWindow(rel, mq, columns, ignoreNulls)

  private def areColumnsUniqueOfOverWindow(
      overWindow: SingleRel,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBool = {
    val inputFieldLength = overWindow.getInput.getRowType.getFieldCount
    val columnsBelongsToInput = ImmutableBitSet.of(columns.filter(_ < inputFieldLength).toList)
    val isSubColumnsUnique = mq.areColumnsUnique(
      overWindow.getInput,
      columnsBelongsToInput,
      ignoreNulls)
    if (isSubColumnsUnique != null && isSubColumnsUnique) {
      true
    } else if (columnsBelongsToInput.cardinality() < columns.cardinality()) {
      // We are not sure whether not belongs to input are unique or not
      null
    } else {
      isSubColumnsUnique
    }
  }

  def areColumnsUnique(
      snapshot: LogicalSnapshot,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBool = {
    mq.areColumnsUnique(snapshot.getInput, columns, ignoreNulls)
  }

  def areColumnsUnique(
      rel: FlinkLogicalTableSourceScan,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBool = {
    areTableColumnsUnique(rel, rel.tableSource, rel.getTable, columns)
  }

  def areColumnsUnique(
      rel: FlinkLogicalLastRow,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBool = {
    columns != null && columns.toArray.sameElements(rel.getUniqueKeys)
  }

  def areColumnsUnique(
      rel: Expand,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBool = {
    // values of expand_is are unique in rows expanded from a row,
    // and a input unique key combined with expand_id are also unique
    val expandIdIndex = rel.expandIdIndex
    if (!columns.get(expandIdIndex)) {
      return false
    }
    val columnsSkipExpandId = ImmutableBitSet.builder().addAll(columns).clear(expandIdIndex).build()
    if (columnsSkipExpandId.cardinality == 0) {
      return false
    }
    val inputRefColumns = columnsSkipExpandId.flatMap {
      column =>
        val inputRefs = FlinkRelMdUtil.getInputRefIndices(column, rel)
        if (inputRefs.size() == 1 && inputRefs.head >= 0) {
          Array(inputRefs.head)
        } else {
          Array.empty[Int]
        }
    }.toSeq

    if (inputRefColumns.isEmpty) {
      return false
    }
    mq.areColumnsUnique(rel.getInput, ImmutableBitSet.of(inputRefColumns: _*), ignoreNulls)
  }

  /**
    * Determines whether a specified set of columns from a Calc relational expression are unique.
    *
    * @param rel         the Calc relational expression
    * @param mq          metadata query instance
    * @param columns     column mask representing the subset of columns for which
    * uniqueness will be determined
    * @param ignoreNulls if true, ignore null values when determining column
    * uniqueness
    * @return whether the columns are unique, or
    *         null if not enough information is available to make that determination
    */
  def areColumnsUnique(
      rel: Calc,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBool = {
    val program = rel.getProgram
    // Calc is composed by projects and conditions. conditions does no change unique property;
    // while projects  maps a set of rows to a different set.
    // Without knowledge of the mapping function(whether it
    // preserves uniqueness), it is only safe to derive uniqueness
    // info from the child of a project when the mapping is f(a) => a.
    //
    // Also need to map the input column set to the corresponding child
    // references
    areColumnsUniqueOfProject(
      program.getProjectList.map(program.expandLocalRef),
      mq, columns, ignoreNulls, rel)
  }

  /**
    * Determines whether a specified set of columns from a RelSubSet relational expression are
    * unique.
    *
    * FIX BUG in <a href="https://issues.apache.org/jira/browse/CALCITE-2134">[CALCITE-2134] </a>
    *
    * @param rel         the RelSubSet relational expression
    * @param mq          metadata query instance
    * @param columns     column mask representing the subset of columns for which
    * uniqueness will be determined
    * @param ignoreNulls if true, ignore null values when determining column
    * uniqueness
    * @return whether the columns are unique, or
    *         null if not enough information is available to make that determination
    */
  def areColumnsUnique(
      rel: RelSubset,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBool = {
    if (!Bug.CALCITE_1048_FIXED) {
      return mq.areColumnsUnique(Util.first(rel.getBest, rel.getOriginal), columns, ignoreNulls)
    }
    var nullCount = 0
    for (rel2 <- rel.getRels) {
      rel2 match {
        // NOTE: If add estimation uniqueness for new RelNode type e.g. Rank / Expand,
        // add the RelNode to pattern matching in RelSubset.
        case _: Aggregate | _: Filter | _: Values | _: TableScan | _: Project | _: Correlate |
             _: Join | _: Exchange | _: Sort | _: SetOp | _: Calc | _: Converter | _: Window |
             _: Expand | _: Rank | _: FlinkRelNode =>
          try {
            val unique = mq.areColumnsUnique(rel2, columns, ignoreNulls)
            if (unique != null) {
              if (unique) {
                return true
              }
            } else {
              nullCount += 1
            }
          }
          catch {
            case _: CyclicMetadataException =>
            // Ignore this relational expression; there will be non-cyclic ones in this set.
          }
        case _ => // skip
      }
    }

    if (nullCount == 0) false else null
  }

  def areColumnsUnique(
      rel: Filter,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBool = mq.areColumnsUnique(rel.getInput, columns, ignoreNulls)

  def areColumnsUnique(
      rel: SetOp,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBool = {
    // If not ALL then the rows are distinct.
    // Therefore the set of all columns is a key.
    !rel.all && columns.nextClearBit(0) >= rel.getRowType.getFieldCount
  }

  def areColumnsUnique(
      rel: Intersect,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBool = {
    if (areColumnsUnique(rel.asInstanceOf[SetOp], mq, columns, ignoreNulls)) {
      return true
    }
    rel.getInputs foreach { input =>
      val b = mq.areColumnsUnique(input, columns, ignoreNulls)
      if (b != null && b) {
        return true
      }
    }
    false
  }

  def areColumnsUnique(
      rel: Minus,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBool = {
    if (areColumnsUnique(rel.asInstanceOf[SetOp], mq, columns, ignoreNulls)) {
      true
    } else {
      mq.areColumnsUnique(rel.getInput(0), columns, ignoreNulls)
    }
  }

  def areColumnsUnique(
      rel: Sort,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBool = mq.areColumnsUnique(rel.getInput, columns, ignoreNulls)

  def areColumnsUnique(
      rel: Exchange,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBool = mq.areColumnsUnique(rel.getInput, columns, ignoreNulls)

  def areColumnsUnique(
      rel: Correlate,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBool = {
    rel.getJoinType match {
      case ANTI | SEMI => mq.areColumnsUnique(rel.getLeft, columns, ignoreNulls)
      case LEFT | INNER =>
        val leftFieldCount = rel.getLeft.getRowType.getFieldCount
        val (leftColumns, rightColumns) = splitColumnsIntoLeftAndRight(leftFieldCount, columns)
        val left = rel.getLeft
        val right = rel.getRight
        if (leftColumns.cardinality > 0 && rightColumns.cardinality > 0) {
          val leftUnique = mq.areColumnsUnique(left, leftColumns, ignoreNulls)
          val rightUnique = mq.areColumnsUnique(right, rightColumns, ignoreNulls)
          if (leftUnique == null || rightUnique == null) null else leftUnique && rightUnique
        }
        else {
          null
        }
      case _ => throw new IllegalStateException(
        s"Unknown join type ${rel.getJoinType} for correlate relation $rel")
    }
  }

  def areColumnsUnique(
      rel: BatchExecCorrelate,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBool = null

  def areColumnsUnique(
      rel: Project,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBool = {
    // LogicalProject maps a set of rows to a different set;
    // Without knowledge of the mapping function(whether it
    // preserves uniqueness), it is only safe to derive uniqueness
    // info from the child of a project when the mapping is f(a) => a.
    //
    // Also need to map the input column set to the corresponding child
    // references
    areColumnsUniqueOfProject(rel.getProjects, mq, columns, ignoreNulls, rel)
  }

  private def areColumnsUniqueOfProject(
      projExprs: util.List[RexNode],
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean,
      originalNode: SingleRel): JBool = {
    val childColumns = ImmutableBitSet.builder
    columns.foreach { bit =>
      val projExpr = projExprs.get(bit)
      projExpr match {
        case inputRef: RexInputRef => childColumns.set(inputRef.getIndex)
        case a: RexCall if a.getKind.equals(SqlKind.AS) &&
          a.getOperands.get(0).isInstanceOf[RexInputRef] =>
          childColumns.set(a.getOperands.get(0).asInstanceOf[RexInputRef].getIndex)
        case call: RexCall if ignoreNulls =>
          // If the expression is a cast such that the types are the same
          // except for the nullability, then if we're ignoring nulls,
          // it doesn't matter whether the underlying column reference
          // is nullable.  Check that the types are the same by making a
          // nullable copy of both types and then comparing them.
          if (call.getOperator eq SqlStdOperatorTable.CAST) {
            val castOperand = call.getOperands.get(0)
            castOperand match {
              case castRef: RexInputRef =>
                val typeFactory = originalNode.getCluster.getTypeFactory
                val castType = typeFactory.createTypeWithNullability(projExpr.getType, true)
                val origType = typeFactory.createTypeWithNullability(castOperand.getType, true)
                if (castType == origType) {
                  childColumns.set(castRef.getIndex)
                }
              case _ => // ignore
            }
          }
        case _ =>
        // If the expression will not influence uniqueness of the
        // projection, then skip it.
      }
    }

    // If no columns can affect uniqueness, then return unknown
    if (childColumns.cardinality == 0) {
      null
    } else {
      mq.areColumnsUnique(originalNode.getInput(), childColumns.build, ignoreNulls)
    }
  }

  def areJoinColumnsUnique(
      joinInfo: JoinInfo,
      joinRelType: JoinRelType,
      leftType: RelDataType,
      isleftUnique: (ImmutableBitSet) => JBool,
      isRightUnique: (ImmutableBitSet) => JBool,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): JBool = {
    if (columns.cardinality == 0) {
      return false
    }
    // Divide up the input column mask into column masks for the left and
    // right sides of the join
    val (leftColumns, rightColumns) = splitColumnsIntoLeftAndRight(leftType.getFieldCount, columns)

    // If the original column mask contains columns from both the left and
    // right hand side, then the columns are unique if and only if they're
    // unique for their respective join inputs
    val leftUnique = isleftUnique(leftColumns)
    val rightUnique = isRightUnique(rightColumns)
    if ((leftColumns.cardinality > 0) && (rightColumns.cardinality > 0)) {
      if ((leftUnique == null) || (rightUnique == null)) {
        return null
      }
      else {
        return leftUnique && rightUnique
      }
    }

    // If we're only trying to determine uniqueness for columns that
    // originate from one join input, then determine if the equijoin
    // columns from the other join input are unique.  If they are, then
    // the columns are unique for the entire join if they're unique for
    // the corresponding join input, provided that input is not null
    // generating.
    if (leftColumns.cardinality > 0) {
      if (joinRelType.generatesNullsOnLeft) {
        false
      } else {
        val rightJoinColsUnique = isRightUnique(joinInfo.rightSet)
        if ((rightJoinColsUnique == null) || (leftUnique == null)) {
          null
        } else {
          rightJoinColsUnique && leftUnique
        }
      }
    } else if (rightColumns.cardinality > 0) {
      if (joinRelType.generatesNullsOnRight) {
        false
      } else {
        val leftJoinColsUnique = isleftUnique(joinInfo.leftSet)
        if ((leftJoinColsUnique == null) || (rightUnique == null)) {
          null
        } else {
          leftJoinColsUnique && rightUnique
        }
      }
    } else {
      throw new AssertionError
    }
  }

  def areColumnsUnique(
      rel: StreamExecWindowJoin,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBool = {
    val joinInfo = JoinInfo.of(rel.getLeft, rel.getRight, rel.joinCondition)
    areJoinColumnsUnique(
      joinInfo, rel.joinType, rel.getLeft.getRowType,
      (leftSet: ImmutableBitSet) => mq.areColumnsUnique(rel.getLeft, leftSet, ignoreNulls),
      (rightSet: ImmutableBitSet) => mq.areColumnsUnique(rel.getRight, rightSet, ignoreNulls),
      mq, columns
    )
  }

  def areColumnsUnique(
      rel: StreamExecJoin,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBool = {
    rel.joinType match {
      case FlinkJoinRelType.ANTI | FlinkJoinRelType.SEMI =>
        mq.areColumnsUnique(rel.getLeft, columns, ignoreNulls)
      case _ =>
        val joinInfo = JoinInfo.of(rel.getLeft, rel.getRight, rel.joinCondition)
        areJoinColumnsUnique(
          joinInfo, FlinkJoinRelType.toJoinRelType(rel.joinType), rel.getLeft.getRowType,
          (leftSet: ImmutableBitSet) => mq.areColumnsUnique(rel.getLeft, leftSet, ignoreNulls),
          (rightSet: ImmutableBitSet) => mq.areColumnsUnique(rel.getRight, rightSet, ignoreNulls),
          mq, columns)
    }
  }

  def areColumnsUnique(
      rel: StreamExecLastRow,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBool = {

    columns != null && columns.toArray.equals(rel.getUniqueKeys)
  }

  def areColumnsUnique(
      rel: Join,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBool = {
    areJoinColumnsUnique(
      rel.analyzeCondition(), rel.getJoinType, rel.getLeft.getRowType,
      (leftSet: ImmutableBitSet) => mq.areColumnsUnique(rel.getLeft, leftSet, ignoreNulls),
      (rightSet: ImmutableBitSet) => mq.areColumnsUnique(rel.getRight, rightSet, ignoreNulls),
      mq, columns
    )
  }

  def areColumnsUnique(
      rel: SemiJoin,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBool = {
    // only return the unique keys from the LHS since a semijoin only
    // returns the LHS
    mq.areColumnsUnique(rel.getLeft, columns, ignoreNulls)
  }

  def areColumnsUnique(
      rel: Aggregate,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBool = {
    // group by keys form a unique key
    val groupKey = ImmutableBitSet.range(rel.getGroupCount)
    columns.contains(groupKey)
  }

  def areColumnsUnique(
      rel: BatchExecGroupAggregateBase,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBool = {
    if (rel.isFinal) {
      columns.contains(ImmutableBitSet.of(rel.getGrouping: _*))
    } else {
      null
    }
  }

  def areColumnsUnique(
      rel: StreamExecGroupAggregate,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBool = {
    columns.contains(ImmutableBitSet.of(rel.getGroupings: _*))
  }

  def areColumnsUnique(
      rel: StreamExecGlobalGroupAggregate,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBool = {
    columns.contains(ImmutableBitSet.of(rel.getGroupings.indices.toArray: _*))
  }

  def areColumnsUnique(
      rel: StreamExecLocalGroupAggregate,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBool = null

  def areColumnsUnique(
      rel: StreamExecGroupWindowAggregate,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBool = {
    areGroupWindowColumnsUnique(
      columns,
      rel.getRowType.getFieldCount,
      rel.getWindowProperties,
      rel.getGroupings,
      mq,
      ignoreNulls)
  }

  def areColumnsUnique(
      rel: FlinkLogicalWindowAggregate,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBool = {
    areGroupWindowColumnsUnique(
      columns,
      rel.getRowType.getFieldCount,
      rel.getNamedProperties,
      rel.getGroupSet.toArray,
      mq,
      ignoreNulls)
  }

  def areColumnsUnique(
      rel: LogicalWindowAggregate,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBool = {
    areGroupWindowColumnsUnique(
      columns,
      rel.getRowType.getFieldCount,
      rel.getNamedProperties,
      rel.getGroupSet.toArray,
      mq,
      ignoreNulls)
  }

  def areColumnsUnique(
      rel: BatchExecWindowAggregateBase,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBool = {
    if (rel.isFinal) {
      areGroupWindowColumnsUnique(
        columns,
        rel.getRowType.getFieldCount,
        rel.getNamedProperties,
        rel.getGrouping,
        mq,
        ignoreNulls)
    } else {
      null
    }
  }

  def areColumnsUnique(
      rel: StreamExecCorrelate,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBool = null

  private def areGroupWindowColumnsUnique(
      columns: ImmutableBitSet,
      fieldCount: Int,
      namedProperties: Seq[NamedWindowProperty],
      grouping: Array[Int],
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): JBool = {
    if (namedProperties.nonEmpty) {
      val begin = fieldCount - namedProperties.size
      val end = fieldCount - 1
      val keys = ImmutableBitSet.of(grouping: _*)
      (begin to end).map {
        i => keys.union(ImmutableBitSet.of(i))
      }.exists(columns.contains)
    } else {
      false
    }
  }

  def areJoinTableColumnsUnique(
      joinInfo: JoinInfo,
      joinRelType: JoinRelType,
      left: RelNode,
      joinedIndex: Option[IndexKey],
      columns: ImmutableBitSet,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): JBool = {
    areJoinColumnsUnique(
      joinInfo, joinRelType, left.getRowType,
      (leftSet: ImmutableBitSet) => mq.areColumnsUnique(left, leftSet, ignoreNulls),
      (rightSet: ImmutableBitSet) => {
        null != joinedIndex &&
          joinedIndex.exists(index => index.isUnique && index.isIndex(rightSet.toArray))
      },
      mq, columns
    )
  }

  def areColumnsUnique(
      join: BatchExecTemporalTableJoin,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBool = {
    areJoinTableColumnsUnique(
      join.joinInfo,
      join.joinType,
      join.getInput,
      join.joinedIndex,
      columns,
      mq,
      ignoreNulls
    )
  }

  def areColumnsUnique(
      join: StreamExecTemporalTableJoin,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBool = {
    areJoinTableColumnsUnique(
      join.joinInfo,
      join.joinType,
      join.getInput,
      join.joinedIndex,
      columns,
      mq,
      ignoreNulls
    )
  }

  def areColumnsUnique(
      rank: Rank,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBool = {
    val rankFunColumnIndex = FlinkRelMdUtil.getRankFunColumnIndex(rank)
    if (rankFunColumnIndex < 0) {
      mq.areColumnsUnique(rank.getInput, columns, ignoreNulls)
    } else {
      val childColumns = columns.clear(rankFunColumnIndex)
      val isChildColumnsUnique = mq.areColumnsUnique(rank.getInput, childColumns, ignoreNulls)
      if (isChildColumnsUnique != null && isChildColumnsUnique) {
        true
      } else {
        rank.rankFunction.getKind match {
          case SqlKind.ROW_NUMBER =>
            val fields = columns.toArray
            (rank.partitionKey.toArray :+ rankFunColumnIndex).forall(fields.contains(_))
          case _ => false
        }
      }
    }
  }

  def areColumnsUnique(
      rel: Values,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBool = {
    if (rel.tuples.size < 2) {
      return true
    }
    columns foreach { bit =>
      val columnValues = rel.tuples map { tuple =>
        val literal = tuple.get(bit)
        if (literal.isNull) {
          NullSentinel.INSTANCE
        } else {
          literal.getValueAs(classOf[Comparable[_]])
        }
      }
      if (columnValues.toSet.size == columnValues.size) {
        return true
      }
    }
    false
  }

  def areColumnsUnique(
      rel: Converter,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBool = {
    mq.areColumnsUnique(rel.getInput, columns, ignoreNulls)
  }

  /**
    * Catch-all implementation for
    * [[BuiltInMetadata.ColumnUniqueness#areColumnsUnique(ImmutableBitSet, boolean)]],
    * invoked using reflection, for any relational expression not
    * handled by a more specific method.
    *
    * @param rel         Relational expression
    * @param mq          Metadata query
    * @param columns     column mask representing the subset of columns for which
    * uniqueness will be determined
    * @param ignoreNulls if true, ignore null values when determining column
    * uniqueness
    * @return whether the columns are unique, or
    *         null if not enough information is available to make that determination
    * @see org.apache.calcite.rel.metadata.RelMetadataQuery#areColumnsUnique(
    *      RelNode, ImmutableBitSet, boolean)
    */
  def areColumnsUnique(
      rel: RelNode,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBool = null

}

object FlinkRelMdColumnUniqueness {

  private val INSTANCE = new FlinkRelMdColumnUniqueness

  val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
    BuiltInMethod.COLUMN_UNIQUENESS.method, INSTANCE)

}
