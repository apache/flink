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
import org.apache.flink.table.planner.JBoolean
import org.apache.flink.table.planner.calcite.FlinkRelBuilder.PlannerNamedWindowProperty
import org.apache.flink.table.planner.plan.nodes.FlinkRelNode
import org.apache.flink.table.planner.plan.nodes.calcite.{Expand, Rank, WindowAggregate}
import org.apache.flink.table.planner.plan.nodes.common.CommonLookupJoin
import org.apache.flink.table.planner.plan.nodes.logical._
import org.apache.flink.table.planner.plan.nodes.physical.batch._
import org.apache.flink.table.planner.plan.nodes.physical.stream._
import org.apache.flink.table.planner.plan.schema.FlinkPreparingTableBase
import org.apache.flink.table.planner.plan.utils.{FlinkRelMdUtil, RankUtil}
import org.apache.flink.table.runtime.operators.rank.RankType
import org.apache.flink.table.sources.TableSource

import org.apache.calcite.plan.RelOptTable
import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.convert.Converter
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.metadata._
import org.apache.calcite.rel.{RelNode, SingleRel}
import org.apache.calcite.rex.{RexCall, RexInputRef, RexNode}
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.util.{Bug, BuiltInMethod, ImmutableBitSet, Util}

import java.util

import scala.collection.JavaConversions._

/**
  * FlinkRelMdColumnUniqueness supplies a implementation of
  * [[RelMetadataQuery#areColumnsUnique]] for the standard logical algebra.
  */
class FlinkRelMdColumnUniqueness private extends MetadataHandler[BuiltInMetadata.ColumnUniqueness] {

  def getDef: MetadataDef[BuiltInMetadata.ColumnUniqueness] = BuiltInMetadata.ColumnUniqueness.DEF

  def areColumnsUnique(
      rel: TableScan,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBoolean = {
    areTableColumnsUnique(rel, null, rel.getTable, columns)
  }

  def areColumnsUnique(
      rel: FlinkLogicalLegacyTableSourceScan,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBoolean = {
    areTableColumnsUnique(rel, rel.tableSource, rel.getTable, columns)
  }

  private def areTableColumnsUnique(
      rel: TableScan,
      tableSource: TableSource[_],
      relOptTable: RelOptTable,
      columns: ImmutableBitSet): JBoolean = {
    if (columns.cardinality == 0) {
      return false
    }

    // TODO get uniqueKeys from TableSchema of TableSource

    relOptTable match {
      case table: FlinkPreparingTableBase => {
        val ukOptional = table.uniqueKeysSet
        if (ukOptional.isPresent) {
          if (ukOptional.get().isEmpty) {
            false
          } else {
            ukOptional.get().exists(columns.contains)
          }
        } else {
          null
        }
      }
      case _ => rel.getTable.isKey(columns)
    }
  }

  def areColumnsUnique(
      rel: Values,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBoolean = {
    if (rel.tuples.size < 2) {
      return true
    }
    columns.foreach { idx =>
      val columnValues = rel.tuples map { tuple =>
        val literal = tuple.get(idx)
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
      rel: Project,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBoolean = {
    // LogicalProject maps a set of rows to a different set;
    // Without knowledge of the mapping function(whether it
    // preserves uniqueness), it is only safe to derive uniqueness
    // info from the child of a project when the mapping is f(a) => a.
    //
    // Also need to map the input column set to the corresponding child
    // references
    areColumnsUniqueOfProject(rel.getProjects, mq, columns, ignoreNulls, rel)
  }

  def areColumnsUnique(
      rel: Filter,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBoolean = mq.areColumnsUnique(rel.getInput, columns, ignoreNulls)

  /**
    * Determines whether a specified set of columns from a Calc relational expression are unique.
    *
    * @param rel         the Calc relational expression
    * @param mq          metadata query instance
    * @param columns     column mask representing the subset of columns for which
    *                    uniqueness will be determined
    * @param ignoreNulls if true, ignore null values when determining column
    *                    uniqueness
    * @return whether the columns are unique, or
    *         null if not enough information is available to make that determination
    */
  def areColumnsUnique(
      rel: Calc,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBoolean = {
    // Calc is composed by projects and conditions. conditions does no change unique property;
    // while projects  maps a set of rows to a different set.
    // Without knowledge of the mapping function(whether it
    // preserves uniqueness), it is only safe to derive uniqueness
    // info from the child of a project when the mapping is f(a) => a.
    //
    // Also need to map the input column set to the corresponding child
    // references
    val program = rel.getProgram
    val projects = program.getProjectList.map(program.expandLocalRef)
    areColumnsUniqueOfProject(projects, mq, columns, ignoreNulls, rel)
  }

  private def areColumnsUniqueOfProject(
      projects: util.List[RexNode],
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean,
      originalNode: SingleRel): JBoolean = {
    val childColumns = ImmutableBitSet.builder
    columns.foreach { idx =>
      val project = projects.get(idx)
      project match {
        case inputRef: RexInputRef => childColumns.set(inputRef.getIndex)
        case asCall: RexCall if asCall.getKind.equals(SqlKind.AS) &&
          asCall.getOperands.get(0).isInstanceOf[RexInputRef] =>
          childColumns.set(asCall.getOperands.get(0).asInstanceOf[RexInputRef].getIndex)
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
                val castType = typeFactory.createTypeWithNullability(project.getType, true)
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

  def areColumnsUnique(
      rel: Expand,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBoolean = {
    // values of expand_id are unique in rows expanded from a row,
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
        if (inputRefs.size == 1 && inputRefs.head >= 0) {
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

  def areColumnsUnique(
      rel: Converter,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBoolean = mq.areColumnsUnique(rel.getInput, columns, ignoreNulls)

  def areColumnsUnique(
      rel: Exchange,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBoolean = mq.areColumnsUnique(rel.getInput, columns, ignoreNulls)

  def areColumnsUnique(
      rank: Rank,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBoolean = {
    val input = rank.getInput
    val rankFunColumnIndex = RankUtil.getRankNumberColumnIndex(rank).getOrElse(-1)
    if (rankFunColumnIndex < 0) {
      mq.areColumnsUnique(input, columns, ignoreNulls)
    } else {
      val childColumns = columns.clear(rankFunColumnIndex)
      val isChildColumnsUnique = mq.areColumnsUnique(input, childColumns, ignoreNulls)
      if (isChildColumnsUnique != null && isChildColumnsUnique) {
        true
      } else {
        rank.rankType match {
          case RankType.ROW_NUMBER =>
            val fields = columns.toArray
            (rank.partitionKey.toArray :+ rankFunColumnIndex).forall(fields.contains(_))
          case _ => false
        }
      }
    }
  }

  def areColumnsUnique(
      rel: Sort,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBoolean = mq.areColumnsUnique(rel.getInput, columns, ignoreNulls)

  def areColumnsUnique(
      rel: StreamExecDeduplicate,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBoolean = {
    columns != null && util.Arrays.equals(columns.toArray, rel.getUniqueKeys)
  }

  def areColumnsUnique(
      rel: StreamExecChangelogNormalize,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBoolean = {
    columns != null && ImmutableBitSet.of(rel.uniqueKeys: _*).equals(columns)
  }

  def areColumnsUnique(
      rel: StreamExecDropUpdateBefore,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBoolean = {
    mq.areColumnsUnique(rel.getInput, columns, ignoreNulls)
  }

  def areColumnsUnique(
      rel: Aggregate,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBoolean = {
    areColumnsUniqueOnAggregate(rel.getGroupSet.toArray, mq, columns, ignoreNulls)
  }

  def areColumnsUnique(
      rel: BatchExecGroupAggregateBase,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBoolean = {
    if (rel.isFinal) {
      areColumnsUniqueOnAggregate(rel.getGrouping, mq, columns, ignoreNulls)
    } else {
      null
    }
  }

  def areColumnsUnique(
      rel: StreamExecGroupAggregate,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBoolean = {
    areColumnsUniqueOnAggregate(rel.grouping, mq, columns, ignoreNulls)
  }

  def areColumnsUnique(
      rel: StreamExecGlobalGroupAggregate,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBoolean = {
    areColumnsUniqueOnAggregate(rel.grouping, mq, columns, ignoreNulls)
  }

  def areColumnsUnique(
      rel: StreamExecLocalGroupAggregate,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBoolean = null

  private  def areColumnsUniqueOnAggregate(
      grouping: Array[Int],
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBoolean = {
    // group key of agg output always starts from 0
    val outputGroupKey = ImmutableBitSet.of(grouping.indices: _*)
    columns.contains(outputGroupKey)
  }

  def areColumnsUnique(
      rel: WindowAggregate,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBoolean = {
    areColumnsUniqueOnWindowAggregate(
      rel.getGroupSet.toArray,
      rel.getNamedProperties,
      rel.getRowType.getFieldCount,
      mq,
      columns,
      ignoreNulls)
  }

  def areColumnsUnique(
      rel: BatchExecWindowAggregateBase,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBoolean = {
    if (rel.isFinal) {
      areColumnsUniqueOnWindowAggregate(
        rel.getGrouping,
        rel.getNamedProperties,
        rel.getRowType.getFieldCount,
        mq,
        columns,
        ignoreNulls)
    } else {
      null
    }
  }

  def areColumnsUnique(
      rel: StreamExecGroupWindowAggregate,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBoolean = {
    areColumnsUniqueOnWindowAggregate(
      rel.getGrouping,
      rel.getWindowProperties,
      rel.getRowType.getFieldCount,
      mq,
      columns,
      ignoreNulls)
  }

  private def areColumnsUniqueOnWindowAggregate(
      grouping: Array[Int],
      namedProperties: Seq[PlannerNamedWindowProperty],
      outputFieldCount: Int,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBoolean = {
    if (namedProperties.nonEmpty) {
      val begin = outputFieldCount - namedProperties.size
      val end = outputFieldCount - 1
      val keys = ImmutableBitSet.of(grouping.indices: _*)
      (begin to end).map {
        i => keys.union(ImmutableBitSet.of(i))
      }.exists(columns.contains)
    } else {
      false
    }
  }

  def areColumnsUnique(
      rel: Window,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBoolean = areColumnsUniqueOfOverAgg(rel, mq, columns, ignoreNulls)

  def areColumnsUnique(
      rel: BatchExecOverAggregate,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBoolean = areColumnsUniqueOfOverAgg(rel, mq, columns, ignoreNulls)

  def areColumnsUnique(
      rel: StreamExecOverAggregate,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBoolean = areColumnsUniqueOfOverAgg(rel, mq, columns, ignoreNulls)

  private def areColumnsUniqueOfOverAgg(
      overAgg: SingleRel,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBoolean = {
    val input = overAgg.getInput
    val inputFieldLength = input.getRowType.getFieldCount
    val columnsBelongsToInput = ImmutableBitSet.of(columns.filter(_ < inputFieldLength).toList)
    val isSubColumnsUnique = mq.areColumnsUnique(
      input,
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
      rel: Join,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBoolean = {
    rel.getJoinType match {
      case JoinRelType.SEMI | JoinRelType.ANTI =>
        // only return the unique keys from the LHS since a SEMI/ANTI join only
        // returns the LHS
        mq.areColumnsUnique(rel.getLeft, columns, ignoreNulls)
      case _ =>
        areColumnsUniqueOfJoin(
          rel.analyzeCondition(),
          rel.getJoinType,
          rel.getLeft.getRowType,
          (leftSet: ImmutableBitSet) => mq.areColumnsUnique(rel.getLeft, leftSet, ignoreNulls),
          (rightSet: ImmutableBitSet) => mq.areColumnsUnique(rel.getRight, rightSet, ignoreNulls),
          mq,
          columns
        )
    }
  }

  def areColumnsUnique(
      rel: StreamExecIntervalJoin,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBoolean = {
    val joinInfo = JoinInfo.of(rel.getLeft, rel.getRight, rel.joinCondition)
    areColumnsUniqueOfJoin(
      joinInfo,
      rel.joinType,
      rel.getLeft.getRowType,
      (leftSet: ImmutableBitSet) => mq.areColumnsUnique(rel.getLeft, leftSet, ignoreNulls),
      (rightSet: ImmutableBitSet) => mq.areColumnsUnique(rel.getRight, rightSet, ignoreNulls),
      mq,
      columns
    )
  }

  def areColumnsUnique(
      join: CommonLookupJoin,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBoolean = {
    val left = join.getInput
    areColumnsUniqueOfJoin(
      join.joinInfo, join.joinType, left.getRowType,
      (leftSet: ImmutableBitSet) => mq.areColumnsUnique(left, leftSet, ignoreNulls),
      // TODO get uniqueKeys from TableSchema of TableSource
      (_: ImmutableBitSet) => null,
      mq, columns
    )
  }

  def areColumnsUniqueOfJoin(
      joinInfo: JoinInfo,
      joinRelType: JoinRelType,
      leftRowType: RelDataType,
      isLeftUnique: ImmutableBitSet => JBoolean,
      isRightUnique: ImmutableBitSet => JBoolean,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): JBoolean = {
    if (columns.cardinality == 0) {
      return false
    }
    // Divide up the input column mask into column masks for the left and
    // right sides of the join
    val (leftColumns, rightColumns) =
    FlinkRelMdUtil.splitColumnsIntoLeftAndRight(leftRowType.getFieldCount, columns)

    // If the original column mask contains columns from both the left and
    // right hand side, then the columns are unique if and only if they're
    // unique for their respective join inputs
    val leftUnique = isLeftUnique(leftColumns)
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
        val leftJoinColsUnique = isLeftUnique(joinInfo.leftSet)
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
      rel: Correlate,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBoolean = {
    rel.getJoinType match {
      case JoinRelType.ANTI | JoinRelType.SEMI =>
        mq.areColumnsUnique(rel.getLeft, columns, ignoreNulls)
      case JoinRelType.LEFT | JoinRelType.INNER =>
        val left = rel.getLeft
        val right = rel.getRight
        val leftFieldCount = left.getRowType.getFieldCount
        val (leftColumns, rightColumns) =
          FlinkRelMdUtil.splitColumnsIntoLeftAndRight(leftFieldCount, columns)
        if (leftColumns.cardinality > 0 && rightColumns.cardinality > 0) {
          val leftUnique = mq.areColumnsUnique(left, leftColumns, ignoreNulls)
          val rightUnique = mq.areColumnsUnique(right, rightColumns, ignoreNulls)
          if (leftUnique == null || rightUnique == null) null else leftUnique && rightUnique
        } else {
          null
        }
      case _ => throw new TableException(
        s"Unknown join type ${rel.getJoinType} for correlate relation $rel")
    }
  }

  def areColumnsUnique(
      rel: BatchExecCorrelate,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBoolean = null

  def areColumnsUnique(
      rel: StreamExecCorrelate,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBoolean = null

  def areColumnsUnique(
      rel: SetOp,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBoolean = {
    // If not ALL then the rows are distinct.
    // Therefore the set of all columns is a key.
    !rel.all && columns.nextClearBit(0) >= rel.getRowType.getFieldCount
  }

  def areColumnsUnique(
      rel: Intersect,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBoolean = {
    if (areColumnsUnique(rel.asInstanceOf[SetOp], mq, columns, ignoreNulls)) {
      return true
    }
    rel.getInputs foreach { input =>
      val unique = mq.areColumnsUnique(input, columns, ignoreNulls)
      if (unique != null && unique) {
        return true
      }
    }
    false
  }

  def areColumnsUnique(
      rel: Minus,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBoolean = {
    if (areColumnsUnique(rel.asInstanceOf[SetOp], mq, columns, ignoreNulls)) {
      true
    } else {
      mq.areColumnsUnique(rel.getInput(0), columns, ignoreNulls)
    }
  }

  /**
    * Determines whether a specified set of columns from a RelSubSet relational expression are
    * unique.
    *
    * FIX BUG in <a href="https://issues.apache.org/jira/browse/CALCITE-2134">[CALCITE-2134] </a>
    *
    * @param subset      the RelSubSet relational expression
    * @param mq          metadata query instance
    * @param columns     column mask representing the subset of columns for which
    *                    uniqueness will be determined
    * @param ignoreNulls if true, ignore null values when determining column uniqueness
    * @return whether the columns are unique, or
    *         null if not enough information is available to make that determination
    */
  def areColumnsUnique(
      subset: RelSubset,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBoolean = {
    if (!Bug.CALCITE_1048_FIXED) {
      val rel = Util.first(subset.getBest, subset.getOriginal)
      return mq.areColumnsUnique(rel, columns, ignoreNulls)
    }
    var nullCount = 0
    for (rel <- subset.getRels) {
      rel match {
        // NOTE: If add estimation uniqueness for new RelNode type e.g. Rank / Expand,
        // add the RelNode to pattern matching in RelSubset.
        case _: Aggregate | _: Filter | _: Values | _: TableScan | _: Project | _: Correlate |
             _: Join | _: Exchange | _: Sort | _: SetOp | _: Calc | _: Converter | _: Window |
             _: Expand | _: Rank | _: FlinkRelNode =>
          try {
            val unique = mq.areColumnsUnique(rel, columns, ignoreNulls)
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

  /**
    * Catch-all implementation for
    * [[BuiltInMetadata.ColumnUniqueness#areColumnsUnique(ImmutableBitSet, boolean)]],
    * invoked using reflection, for any relational expression not
    * handled by a more specific method.
    *
    * @param rel         Relational expression
    * @param mq          Metadata query
    * @param columns     column mask representing the subset of columns for which
    *                    uniqueness will be determined
    * @param ignoreNulls if true, ignore null values when determining column uniqueness
    * @return whether the columns are unique, or
    *         null if not enough information is available to make that determination
    * @see org.apache.calcite.rel.metadata.RelMetadataQuery#areColumnsUnique(
    *      RelNode, ImmutableBitSet, boolean)
    */
  def areColumnsUnique(
      rel: RelNode,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBoolean = null

}

object FlinkRelMdColumnUniqueness {

  private val INSTANCE = new FlinkRelMdColumnUniqueness

  val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
    BuiltInMethod.COLUMN_UNIQUENESS.method, INSTANCE)

}
