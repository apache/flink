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
import org.apache.flink.table.plan.nodes.calcite.{Expand, LogicalWindowAggregate, Rank}
import org.apache.flink.table.plan.nodes.logical._
import org.apache.flink.table.plan.nodes.physical.batch._
import org.apache.flink.table.plan.nodes.physical.stream._
import org.apache.flink.table.plan.schema.FlinkRelOptTable
import org.apache.flink.table.plan.util.{FlinkRelMdUtil, TemporalJoinUtil}
import org.apache.flink.table.sources.{IndexKey, TableSource}

import com.google.common.collect.ImmutableSet
import org.apache.calcite.plan.RelOptTable
import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{JoinRelType, _}
import org.apache.calcite.rel.logical.LogicalSnapshot
import org.apache.calcite.rel.metadata._
import org.apache.calcite.rel.{RelNode, SingleRel}
import org.apache.calcite.rex.{RexCall, RexInputRef, RexNode}
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.util.{Bug, BuiltInMethod, ImmutableBitSet, Util}

import java.lang.{Boolean => JBool}
import java.util
import java.util.{ArrayList => JArrayList, List => JList}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable

class FlinkRelMdUniqueKeys private extends MetadataHandler[BuiltInMetadata.UniqueKeys] {

  def getDef: MetadataDef[BuiltInMetadata.UniqueKeys] = BuiltInMetadata.UniqueKeys.DEF

  private def getTableUniqueKeys(
      tableSource: TableSource,
      relOptTable: RelOptTable): util.Set[ImmutableBitSet] = {
    if (tableSource != null) {
      val indexes = TemporalJoinUtil.getTableIndexKeys(tableSource)
      if (indexes.nonEmpty) {
        return indexKeysToUniqueBitSet(indexes)
      }
    }
    relOptTable match {
      case flinkRelOptTable: FlinkRelOptTable => flinkRelOptTable.uniqueKeysSet.orNull
      case _ => null
    }
  }

  def getUniqueKeys(
      rel: TableScan,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): util.Set[ImmutableBitSet] = {
    getTableUniqueKeys(null, rel.getTable)
  }

  def getUniqueKeys(
      snapshot: LogicalSnapshot,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): util.Set[ImmutableBitSet] = {
    mq.getUniqueKeys(snapshot.getInput, ignoreNulls)
  }

  def getUniqueKeys(
      rel: FlinkLogicalTableSourceScan,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): util.Set[ImmutableBitSet] = {
    getTableUniqueKeys(rel.tableSource, rel.getTable)
  }

  def getUniqueKeys(
      rel: Expand,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): util.Set[ImmutableBitSet] = {
    // mapping input column index to output index for non-null value columns
    val mapInputToOutput = new util.HashMap[Int, Int]()
    (0 until rel.getRowType.getFieldCount).filter(_ != rel.expandIdIndex).foreach { column =>
      val inputRefs = FlinkRelMdUtil.getInputRefIndices(column, rel)
      // expand columns corresponding to a given index should be same input ref.
      if (inputRefs.size() == 1 && inputRefs.head >= 0) {
        mapInputToOutput.put(inputRefs.head, column)
      }
    }
    if (mapInputToOutput.isEmpty) {
      return null
    }

    val inputUniqueKeys = mq.getUniqueKeys(rel.getInput, ignoreNulls)
    if (inputUniqueKeys == null || inputUniqueKeys.isEmpty) {
      return inputUniqueKeys
    }

    // values of expand_is are unique in rows expanded from a row,
    // and a input unique key combined with expand_id are also unique
    val outputUniqueKeys = new util.HashSet[ImmutableBitSet]()
    inputUniqueKeys.foreach { uniqueKey =>
      val outputUniqueKeyBuilder = ImmutableBitSet.builder()
      // a input unique key can be output only its values are all in `mapInputToOutput`
      val canOutput = uniqueKey.toList.forall { key =>
        if (mapInputToOutput.contains(key)) {
          outputUniqueKeyBuilder.set(mapInputToOutput.get(key))
          true
        } else {
          false
        }
      }
      if (canOutput) {
        // unique key from input combined with expand id are unique
        outputUniqueKeyBuilder.set(rel.expandIdIndex)
        outputUniqueKeys.add(outputUniqueKeyBuilder.build())
      }
    }
    if (outputUniqueKeys.isEmpty) null else outputUniqueKeys
  }

  def getUniqueKeys(
      rel: Filter,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): util.Set[ImmutableBitSet] = mq.getUniqueKeys(rel.getInput, ignoreNulls)

  def getUniqueKeys(
      rel: Sort,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): util.Set[ImmutableBitSet] = mq.getUniqueKeys(rel.getInput, ignoreNulls)

  def getUniqueKeys(
      rel: Correlate,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): util.Set[ImmutableBitSet] = null

  def getUniqueKeys(
      rel: BatchExecCorrelate,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): util.Set[ImmutableBitSet] = null

  def getUniqueKeys(
      rel: Project,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): util.Set[ImmutableBitSet] =
    getProjectUniqueKeys(rel.getProjects, rel.getInput, mq, ignoreNulls)


  private def getProjectUniqueKeys(
      projects: JList[RexNode],
      input: RelNode,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): util.Set[ImmutableBitSet] = {
    // LogicalProject maps a set of rows to a different set;
    // Without knowledge of the mapping function(whether it
    // preserves uniqueness), it is only safe to derive uniqueness
    // info from the child of a project when the mapping is f(a) => a.
    //
    // Further more, the unique bitset coming from the child needs
    val projUniqueKeySet = mutable.HashSet[ImmutableBitSet]()
    val mapInToOutPos = mutable.HashMap[Int, mutable.ArrayBuffer[Int]]()

    def appendMapInToOutPos(inIndex: Int, outIndex: Int): Unit = {
      if (mapInToOutPos.contains(inIndex)) {
        mapInToOutPos(inIndex) += outIndex
      } else {
        val arrayBuffer = mutable.ArrayBuffer[Int]()
        arrayBuffer += outIndex
        mapInToOutPos += (inIndex -> arrayBuffer)
      }
    }
    // Build an input to output position map.
    projects.zipWithIndex.foreach {
      case (projExpr, i) =>
        projExpr match {
          case ref: RexInputRef => appendMapInToOutPos(ref.getIndex, i)
          case a: RexCall if ignoreNulls && a.getOperator.equals(SqlStdOperatorTable.CAST) =>
            val castOperand = a.getOperands.get(0)
            castOperand match {
              case castRef: RexInputRef =>
                val typeFactory = input.getCluster.getTypeFactory
                val castType = typeFactory.createTypeWithNullability(projExpr.getType, true)
                val origType = typeFactory.createTypeWithNullability(castOperand.getType, true)
                if (castType == origType) {
                  appendMapInToOutPos(castRef.getIndex, i)
                }
              case _ => // ignore
            }
          //rename
          case a: RexCall if a.getKind.equals(SqlKind.AS) &&
            a.getOperands.get(0).isInstanceOf[RexInputRef] =>
            appendMapInToOutPos(a.getOperands.get(0).asInstanceOf[RexInputRef].getIndex, i)
          case _ => // ignore
        }
    }
    if (mapInToOutPos.isEmpty) {
      // if there's no RexInputRef in the projected expressions
      // return empty set.
      return projUniqueKeySet
    }

    val childUniqueKeySet = mq.getUniqueKeys(input, ignoreNulls)
    if (childUniqueKeySet != null) {
      // Now add to the projUniqueKeySet the child keys that are fully
      // projected.
      childUniqueKeySet.foreach { colMask =>
        val filerInToOutPos = mapInToOutPos.filter { entry =>
          colMask.asList().contains(entry._1)
        }
        val keys = filerInToOutPos.keys
        if (colMask.forall(keys.contains(_))) {
          val total = filerInToOutPos.map(_._2.size).product
          for (i <- 0 to total) {
            val tmpMask = ImmutableBitSet.builder()
            filerInToOutPos.foreach { entry =>
              tmpMask.set(entry._2.get(i % entry._2.size))
            }
            projUniqueKeySet += tmpMask.build()
          }
        }
      }
    }
    projUniqueKeySet
  }

  def getUniqueKeys(
      join: Join,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): util.Set[ImmutableBitSet] = {
    getJoinUniqueKeys(
      join.analyzeCondition(), join.getJoinType, join.getLeft, join.getRight, mq, ignoreNulls)
  }

  private def getJoinUniqueKeys(
      joinInfo: JoinInfo,
      joinRelType: JoinRelType,
      left: RelNode,
      right: RelNode,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): util.Set[ImmutableBitSet] = {
    val leftUniqueKeys = mq.getUniqueKeys(left, ignoreNulls)
    val rightUniqueKeys = mq.getUniqueKeys(right, ignoreNulls)
    getJoinUniqueKeys(
      joinInfo, joinRelType, left.getRowType, leftUniqueKeys, rightUniqueKeys,
      mq.areColumnsUnique(left, joinInfo.leftSet, ignoreNulls),
      mq.areColumnsUnique(right, joinInfo.rightSet, ignoreNulls),
      mq)
  }

  private def getJoinUniqueKeys(
      joinInfo: JoinInfo,
      joinRelType: JoinRelType,
      leftType: RelDataType,
      leftUniqueKeys: util.Set[ImmutableBitSet],
      rightUniqueKeys: util.Set[ImmutableBitSet],
      isleftUnique: JBool,
      isRightUnique: JBool,
      mq: RelMetadataQuery): util.Set[ImmutableBitSet] = {

    // first add the different combinations of concatenated unique keys
    // from the left and the right, adjusting the right hand side keys to
    // reflect the addition of the left hand side
    //
    // NOTE zfong 12/18/06 - If the number of tables in a join is large,
    // the number of combinations of unique key sets will explode.  If
    // that is undesirable, use RelMetadataQuery.areColumnsUnique() as
    // an alternative way of getting unique key information.
    val retSet = new util.HashSet[ImmutableBitSet]
    val nFieldsOnLeft = leftType.getFieldCount
    val rightSet = if (rightUniqueKeys != null) {
      val res = new util.HashSet[ImmutableBitSet]
      rightUniqueKeys.foreach { colMask =>
        val tmpMask = ImmutableBitSet.builder
        colMask.foreach(bit => tmpMask.set(bit + nFieldsOnLeft))
        res.add(tmpMask.build())
      }
      if (leftUniqueKeys != null) {
        res.foreach { colMaskRight =>
          leftUniqueKeys.foreach(colMaskLeft => retSet.add(colMaskLeft.union(colMaskRight)))
        }
      }
      res
    } else {
      null
    }

    // determine if either or both the LHS and RHS are unique on the
    // equijoin columns
    val leftUnique = isleftUnique
    val rightUnique = isRightUnique

    // if the right hand side is unique on its equijoin columns, then we can
    // add the unique keys from left if the left hand side is not null
    // generating
    if (rightUnique != null
      && rightUnique
      && (leftUniqueKeys != null)
      && !joinRelType.generatesNullsOnLeft) {
      retSet.addAll(leftUniqueKeys)
    }

    // same as above except left and right are reversed
    if (leftUnique != null
      && leftUnique
      && (rightSet != null)
      && !joinRelType.generatesNullsOnRight) {
      retSet.addAll(rightSet)
    }
    retSet
  }

  def getUniqueKeys(
      rel: SemiJoin,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): util.Set[ImmutableBitSet] =
  // only return the unique keys from the LHS since a semijoin only
  // returns the LHS
    mq.getUniqueKeys(rel.getLeft, ignoreNulls)

  def getUniqueKeys(
      rel: Aggregate,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): util.Set[ImmutableBitSet] =
  // group by keys form a unique key
    ImmutableSet.of(ImmutableBitSet.range(rel.getGroupCount))

  def getUniqueKeys(
      rel: BatchExecGroupAggregateBase,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): util.Set[ImmutableBitSet] = {
    if (rel.isFinal) {
      // group by keys form a unique key
      ImmutableSet.of(ImmutableBitSet.of(rel.getGrouping: _*))
    } else {
      null
    }
  }

  def getUniqueKeys(
      rel: SetOp,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): util.Set[ImmutableBitSet] = {
    if (!rel.all) {
      ImmutableSet.of(ImmutableBitSet.range(rel.getRowType.getFieldCount))
    } else {
      ImmutableSet.of()
    }
  }

  def getUniqueKeys(
      calc: Calc,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): util.Set[ImmutableBitSet] = {
    val input = calc.getInput
    val projects = calc.getProgram.getProjectList.map(calc.getProgram.expandLocalRef)
    getProjectUniqueKeys(projects, input, mq, ignoreNulls)
  }

  def getUniqueKeys(
      rel: StreamExecCorrelate,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): util.Set[ImmutableBitSet] = null

  def getUniqueKeys(
      rel: Exchange,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): util.Set[ImmutableBitSet] = mq.getUniqueKeys(rel.getInput, ignoreNulls)

  def getUniqueKeys(
      rel: StreamExecGroupAggregate,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): util.Set[ImmutableBitSet] = {
    // group by keys form a unique key
    toImmutableSet(rel.getGroupings.indices.toArray)
  }

  def getUniqueKeys(
      rel: StreamExecGlobalGroupAggregate,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): util.Set[ImmutableBitSet] = {
    ImmutableSet.of(ImmutableBitSet.of(rel.getGroupings.indices.toArray: _*))
  }

  def getUniqueKeys(
      rel: StreamExecLocalGroupAggregate,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): util.Set[ImmutableBitSet] = null

  private def getGroupWindowUniqueKeys(
      fieldCount: Int,
      namedProperties: Seq[NamedWindowProperty],
      grouping: Array[Int],
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): util.Set[ImmutableBitSet] = {
    if (namedProperties.nonEmpty) {
      val begin = fieldCount - namedProperties.size
      val end = fieldCount - 1
      //namedProperties's indexes is at the end of output record
      val keys = ImmutableBitSet.of(grouping: _*)
      (begin to end).map {
        i => keys.union(ImmutableBitSet.of(i))
      }.toSet[ImmutableBitSet]
    } else {
      null
    }
  }

  def getUniqueKeys(
      rel: StreamExecGroupWindowAggregate,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): util.Set[ImmutableBitSet] = {
    getGroupWindowUniqueKeys(
      rel.getRowType.getFieldCount, rel.getWindowProperties, rel.getGroupings, mq, ignoreNulls)
  }

  def getUniqueKeys(
      rel: FlinkLogicalWindowAggregate,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): util.Set[ImmutableBitSet] = {
    getGroupWindowUniqueKeys(
      rel.getRowType.getFieldCount,
      rel.getNamedProperties,
      rel.getGroupSet.toArray,
      mq,
      ignoreNulls)
  }

  def getUniqueKeys(
      rel: LogicalWindowAggregate,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): util.Set[ImmutableBitSet] = {
    getGroupWindowUniqueKeys(
      rel.getRowType.getFieldCount,
      rel.getNamedProperties,
      rel.getGroupSet.toArray,
      mq,
      ignoreNulls)
  }

  def getUniqueKeys(
      rel: BatchExecWindowAggregateBase,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): util.Set[ImmutableBitSet] = {
    if (rel.isFinal) {
      getGroupWindowUniqueKeys(
        rel.getRowType.getFieldCount,
        rel.getNamedProperties,
        rel.getGrouping,
        mq,
        ignoreNulls)
    } else {
      null
    }
  }

  private def getTemporalTableJoinUniqueKeys(
      joinInfo: JoinInfo,
      joinRelType: JoinRelType,
      left: RelNode,
      joinedIndex: Option[IndexKey],
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): util.Set[ImmutableBitSet] = {
    val leftUniqueKeys = mq.getUniqueKeys(left, ignoreNulls)
    val leftType = left.getRowType
    val rightUniqueKeys = indexKeyToUniqueBitSet(joinedIndex)
    getJoinUniqueKeys(
      joinInfo, joinRelType, leftType, leftUniqueKeys, rightUniqueKeys,
      mq.areColumnsUnique(left, joinInfo.leftSet, ignoreNulls),
      null != rightUniqueKeys && rightUniqueKeys.exists { uniqueKey =>
        uniqueKey.forall(key => joinInfo.rightKeys.contains(key))
      },
      mq)
  }

  def getUniqueKeys(
      join: StreamExecTemporalTableJoin,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): util.Set[ImmutableBitSet] = {
    getTemporalTableJoinUniqueKeys(
      join.joinInfo,
      join.joinType,
      join.getInput,
      join.joinedIndex,
      mq,
      ignoreNulls)
  }

  def getUniqueKeys(
      join: BatchExecTemporalTableJoin,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): util.Set[ImmutableBitSet] = {
    getTemporalTableJoinUniqueKeys(
      join.joinInfo,
      join.joinType,
      join.getInput,
      join.joinedIndex,
      mq,
      ignoreNulls)
  }

  def getUniqueKeys(
      rel: FlinkLogicalLastRow,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): util.Set[ImmutableBitSet] = {
    mq.getUniqueKeys(rel.getInput, ignoreNulls)
    ImmutableSet.of(ImmutableBitSet.of(rel.getUniqueKeys.map(Integer.valueOf(_)).toIterable.asJava))
  }

  def getUniqueKeys(
      rel: StreamExecLastRow,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): util.Set[ImmutableBitSet] = {
    ImmutableSet.of(ImmutableBitSet.of(rel.getUniqueKeys.map(Integer.valueOf(_)).toIterable.asJava))
  }

  def getUniqueKeys(
      rel: Rank,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): util.Set[ImmutableBitSet] = {
    val inputUniqueKeys = mq.getUniqueKeys(rel.getInput, ignoreNulls)
    val rankFunColumnIndex = FlinkRelMdUtil.getRankFunColumnIndex(rel)
    if (rankFunColumnIndex < 0) {
      inputUniqueKeys
    } else {
      val retSet = new util.HashSet[ImmutableBitSet]
      rel.rankFunction.kind match {
        case SqlKind.ROW_NUMBER =>
          retSet.add(rel.partitionKey.union(ImmutableBitSet.of(rankFunColumnIndex)))
        case _ => // do nothing
      }
      if (inputUniqueKeys != null && inputUniqueKeys.nonEmpty) {
        inputUniqueKeys.foreach {
          uniqueKey => retSet.add(uniqueKey)
        }
      }
      retSet
    }
  }

  def getUniqueKeys(
      rel: StreamExecWindowJoin,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): util.Set[ImmutableBitSet] = {
    val joinInfo = JoinInfo.of(rel.getLeft, rel.getRight, rel.joinCondition)
    getJoinUniqueKeys(joinInfo, rel.joinType, rel.getLeft, rel.getRight, mq, ignoreNulls)
  }

  def getUniqueKeys(
      rel: StreamExecJoin,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): util.Set[ImmutableBitSet] = {
    rel.joinType match {
      case FlinkJoinRelType.ANTI | FlinkJoinRelType.SEMI =>
        mq.getUniqueKeys(rel.getLeft, ignoreNulls)
      case _ =>
        val joinInfo = JoinInfo.of(rel.getLeft, rel.getRight, rel.joinCondition)
        getJoinUniqueKeys(
          joinInfo, FlinkJoinRelType.toJoinRelType(rel.joinType), rel.getLeft, rel.getRight, mq,
          ignoreNulls)
    }
  }

  def getUniqueKeys(
      rel: BatchExecOverAggregate,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): util.Set[ImmutableBitSet] =
    getUniqueKeysOfOverWindow(rel, mq, ignoreNulls)

  def getUniqueKeys(
      rel: Window,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): util.Set[ImmutableBitSet] =
    getUniqueKeysOfOverWindow(rel, mq, ignoreNulls)

  private def getUniqueKeysOfOverWindow(
      window: SingleRel,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): util.Set[ImmutableBitSet] =
    mq.getUniqueKeys(window.getInput, ignoreNulls)

  // Catch-all rule when none of the others apply.
  def getUniqueKeys(
      rel: RelNode,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): util.Set[ImmutableBitSet] = null

  def getUniqueKeys(
      rel: RelSubset,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): util.Set[ImmutableBitSet] = {
    if (!Bug.CALCITE_1048_FIXED) {
      //if the best node is null, so we can get the uniqueKeys based original node, due to
      //the original node is logically equivalent as the rel.
      mq.getUniqueKeys(Util.first(rel.getBest, rel.getOriginal), ignoreNulls)
    } else {
      throw new RuntimeException("CALCITE_1048 is fixed, so check this method again!")
    }
  }

  private def toImmutableSet(array: Array[Int]): util.Set[ImmutableBitSet] = {
    if (array.nonEmpty) {
      val keys = new JArrayList[Integer]()
      array.foreach(keys.add(_))
      ImmutableSet.of(ImmutableBitSet.of(keys))
    } else {
      null
    }
  }

  private def indexKeyToUniqueBitSet(
      index: Option[IndexKey]): util.Set[ImmutableBitSet] = index match {
    case Some(idx) if idx.isUnique =>
      toImmutableSet(idx.toArray)
    case _ => null
  }

  private def indexKeysToUniqueBitSet(
    indexes: util.Collection[IndexKey]): util.Set[ImmutableBitSet] = {
    if (null != indexes && indexes.nonEmpty) {
      val retSet = new util.HashSet[ImmutableBitSet]
      indexes.filter(_.isUnique).map(_.toArray).foreach { uk => retSet.addAll(toImmutableSet(uk)) }
      if (retSet.isEmpty) {
        null
      } else {
        retSet
      }
    } else {
      null
    }
  }

}

object FlinkRelMdUniqueKeys {

  private val INSTANCE = new FlinkRelMdUniqueKeys

  val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
    BuiltInMethod.UNIQUE_KEYS.method, INSTANCE)

}
