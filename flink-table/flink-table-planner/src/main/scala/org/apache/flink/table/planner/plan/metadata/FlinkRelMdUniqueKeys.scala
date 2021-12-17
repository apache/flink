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

import org.apache.flink.table.catalog.CatalogTable
import org.apache.flink.table.planner._
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.expressions.PlannerNamedWindowProperty
import org.apache.flink.table.planner.plan.nodes.calcite.{Expand, Rank, WatermarkAssigner, WindowAggregate}
import org.apache.flink.table.planner.plan.nodes.physical.batch._
import org.apache.flink.table.planner.plan.nodes.physical.common.CommonPhysicalLookupJoin
import org.apache.flink.table.planner.plan.nodes.physical.stream._
import org.apache.flink.table.planner.plan.schema.{FlinkPreparingTableBase, TableSourceTable}
import org.apache.flink.table.planner.plan.utils.{FlinkRelMdUtil, RankUtil}
import org.apache.flink.table.runtime.operators.rank.{ConstantRankRange, RankType}
import org.apache.flink.table.types.logical.utils.LogicalTypeCasts

import com.google.common.collect.ImmutableSet
import org.apache.calcite.plan.RelOptTable
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.metadata._
import org.apache.calcite.rel.{RelNode, SingleRel}
import org.apache.calcite.rex.{RexCall, RexInputRef, RexNode}
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.util.{Bug, BuiltInMethod, ImmutableBitSet, Util}

import java.util

import scala.collection.JavaConversions._

class FlinkRelMdUniqueKeys private extends MetadataHandler[BuiltInMetadata.UniqueKeys] {

  def getDef: MetadataDef[BuiltInMetadata.UniqueKeys] = BuiltInMetadata.UniqueKeys.DEF

  def getUniqueKeys(
      rel: TableScan,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): JSet[ImmutableBitSet] = {
    getTableUniqueKeys(rel.getTable)
  }

  private def getTableUniqueKeys(relOptTable: RelOptTable): JSet[ImmutableBitSet] = {
    relOptTable match {
      case sourceTable: TableSourceTable =>
        val catalogTable = sourceTable.catalogTable
        catalogTable match {
          case act: CatalogTable =>
            val builder = ImmutableSet.builder[ImmutableBitSet]()

            val schema = act.getResolvedSchema
            if (schema.getPrimaryKey.isPresent) {
              // use relOptTable's type which may be projected based on original schema
              val columns = relOptTable.getRowType.getFieldNames
              val primaryKeyColumns = schema.getPrimaryKey.get().getColumns
              // we check this because a portion of a composite primary key is not unique
              if (columns.containsAll(primaryKeyColumns)) {
                val columnIndices = primaryKeyColumns.map(c => columns.indexOf(c))
                builder.add(ImmutableBitSet.of(columnIndices: _*))
              }
            }

            val uniqueSet = sourceTable.uniqueKeysSet.orElse(null)
            if (uniqueSet != null) {
              builder.addAll(uniqueSet)
            }

            val result = builder.build()
            if (result.isEmpty) null else result
        }
      case table: FlinkPreparingTableBase => table.uniqueKeysSet.orElse(null)
      case _ => null
    }
  }

  def getUniqueKeys(
      rel: Project,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): JSet[ImmutableBitSet] =
    getProjectUniqueKeys(rel.getProjects, rel.getInput, mq, ignoreNulls)

  def getUniqueKeys(
      rel: Filter,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): JSet[ImmutableBitSet] = mq.getUniqueKeys(rel.getInput, ignoreNulls)

  def getUniqueKeys(
      calc: Calc,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): JSet[ImmutableBitSet] = {
    val input = calc.getInput
    val projects = calc.getProgram.getProjectList.map(calc.getProgram.expandLocalRef)
    getProjectUniqueKeys(projects, input, mq, ignoreNulls)
  }

  private def getProjectUniqueKeys(
      projects: JList[RexNode],
      input: RelNode,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): JSet[ImmutableBitSet] = {
    getProjectUniqueKeys(
      projects,
      input.getCluster.getTypeFactory,
      () => mq.getUniqueKeys(input, ignoreNulls),
      ignoreNulls)
  }

  def getProjectUniqueKeys(
      projects: JList[RexNode],
      typeFactory: RelDataTypeFactory,
      getInputUniqueKeys :() => util.Set[ImmutableBitSet],
      ignoreNulls: Boolean): JSet[ImmutableBitSet] = {
    // LogicalProject maps a set of rows to a different set;
    // Without knowledge of the mapping function(whether it
    // preserves uniqueness), it is only safe to derive uniqueness
    // info from the child of a project when the mapping is f(a) => a.
    //
    // Further more, the unique bitset coming from the child needs
    val projUniqueKeySet = new JHashSet[ImmutableBitSet]()
    val mapInToOutPos = new JHashMap[Int, JArrayList[Int]]()

    def appendMapInToOutPos(inIndex: Int, outIndex: Int): Unit = {
      if (mapInToOutPos.contains(inIndex)) {
        mapInToOutPos(inIndex).add(outIndex)
      } else {
        val arrayBuffer = new JArrayList[Int]()
        arrayBuffer.add(outIndex)
        mapInToOutPos.put(inIndex, arrayBuffer)
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
                val castType = typeFactory.createTypeWithNullability(projExpr.getType, true)
                val origType = typeFactory.createTypeWithNullability(castOperand.getType, true)
                if (castType == origType) {
                  appendMapInToOutPos(castRef.getIndex, i)
                }
              case _ => // ignore
            }
          //rename or cast
          case a: RexCall if (a.getKind.equals(SqlKind.AS) || isFidelityCast(a)) &&
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

    val childUniqueKeySet = getInputUniqueKeys()
    if (childUniqueKeySet != null) {
      // Now add to the projUniqueKeySet the child keys that are fully
      // projected.
      childUniqueKeySet.foreach { colMask =>
        val filerInToOutPos = mapInToOutPos.filter { inToOut =>
          colMask.asList().contains(inToOut._1)
        }
        val keys = filerInToOutPos.keys
        if (colMask.forall(keys.contains(_))) {
          val total = filerInToOutPos.map(_._2.size).product
          for (i <- 0 to total) {
            val tmpMask = ImmutableBitSet.builder()
            filerInToOutPos.foreach { inToOut =>
              val outs = inToOut._2
              tmpMask.set(outs.get(i % outs.size))
            }
            projUniqueKeySet.add(tmpMask.build())
          }
        }
      }
    }
    projUniqueKeySet
  }

  /**
   * Whether the [[RexCall]] is a cast that doesn't lose any information.
   */
  private def isFidelityCast(call: RexCall): Boolean = {
    if (call.getKind != SqlKind.CAST) {
      return false
    }
    val originalType = FlinkTypeFactory.toLogicalType(call.getOperands.get(0).getType)
    val newType = FlinkTypeFactory.toLogicalType(call.getType)
    LogicalTypeCasts.supportsImplicitCast(originalType, newType)
  }

  def getUniqueKeys(
      rel: Expand,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): JSet[ImmutableBitSet] = {
    getExpandUniqueKeys(rel, () => mq.getUniqueKeys(rel.getInput, ignoreNulls))
  }

  def getExpandUniqueKeys(
      rel: Expand, getInputUniqueKeys :() => util.Set[ImmutableBitSet]): JSet[ImmutableBitSet] = {
    // mapping input column index to output index for non-null value columns
    val mapInputToOutput = new JHashMap[Int, Int]()
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

    val inputUniqueKeys = getInputUniqueKeys()
    if (inputUniqueKeys == null || inputUniqueKeys.isEmpty) {
      return inputUniqueKeys
    }

    // values of expand_is are unique in rows expanded from a row,
    // and a input unique key combined with expand_id are also unique
    val outputUniqueKeys = new JHashSet[ImmutableBitSet]()
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
      rel: Exchange,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): JSet[ImmutableBitSet] = mq.getUniqueKeys(rel.getInput, ignoreNulls)

  def getUniqueKeys(
      rel: Rank,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): JSet[ImmutableBitSet] = {
    getRankUniqueKeys(rel, mq.getUniqueKeys(rel.getInput, ignoreNulls))
  }

  def getRankUniqueKeys(rel: Rank, inputKeys: JSet[ImmutableBitSet]): JSet[ImmutableBitSet] = {
    val rankFunColumnIndex = RankUtil.getRankNumberColumnIndex(rel).getOrElse(-1)
    // for Rank node that can convert to Deduplicate, unique key is partition key
    val canConvertToDeduplicate: Boolean = {
      val rankRange = rel.rankRange
      val isRowNumberType = rel.rankType == RankType.ROW_NUMBER
      val isLimit1 = rankRange match {
        case rankRange: ConstantRankRange =>
          rankRange.getRankStart == 1 && rankRange.getRankEnd == 1
        case _ => false
      }
      isRowNumberType && isLimit1
    }

    if (canConvertToDeduplicate) {
      val retSet = new JHashSet[ImmutableBitSet]
      retSet.add(rel.partitionKey)
      retSet
    }
    else if (rankFunColumnIndex < 0) {
      inputKeys
    } else {
      val retSet = new JHashSet[ImmutableBitSet]
      rel.rankType match {
        case RankType.ROW_NUMBER =>
          retSet.add(rel.partitionKey.union(ImmutableBitSet.of(rankFunColumnIndex)))
        case _ => // do nothing
      }
      if (inputKeys != null && inputKeys.nonEmpty) {
        inputKeys.foreach {
          uniqueKey => retSet.add(uniqueKey)
        }
      }
      retSet
    }
  }

  def getUniqueKeys(
      rel: Sort,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): JSet[ImmutableBitSet] = mq.getUniqueKeys(rel.getInput, ignoreNulls)

  def getUniqueKeys(
      rel: StreamPhysicalDeduplicate,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): JSet[ImmutableBitSet] = {
    ImmutableSet.of(ImmutableBitSet.of(rel.getUniqueKeys.map(Integer.valueOf).toList))
  }

  def getUniqueKeys(
      rel: StreamPhysicalChangelogNormalize,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): JSet[ImmutableBitSet] = {
    ImmutableSet.of(ImmutableBitSet.of(rel.uniqueKeys.map(Integer.valueOf).toList))
  }

  def getUniqueKeys(
      rel: StreamPhysicalDropUpdateBefore,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): JSet[ImmutableBitSet] = {
    mq.getUniqueKeys(rel.getInput, ignoreNulls)
  }

  def getUniqueKeys(
      rel: Aggregate,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): JSet[ImmutableBitSet] = {
    getUniqueKeysOnAggregate(rel.getGroupSet.toArray)
  }

  def getUniqueKeys(
      rel: BatchPhysicalGroupAggregateBase,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): JSet[ImmutableBitSet] = {
    if (rel.isFinal) {
      getUniqueKeysOnAggregate(rel.grouping)
    } else {
      null
    }
  }

  def getUniqueKeys(
      rel: StreamPhysicalGroupAggregate,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): JSet[ImmutableBitSet] = {
    getUniqueKeysOnAggregate(rel.grouping)
  }

  def getUniqueKeys(
      rel: StreamPhysicalLocalGroupAggregate,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): JSet[ImmutableBitSet] = null

  def getUniqueKeys(
      rel: StreamPhysicalGlobalGroupAggregate,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): JSet[ImmutableBitSet] = {
    getUniqueKeysOnAggregate(rel.grouping)
  }

  def getUniqueKeysOnAggregate(grouping: Array[Int]): util.Set[ImmutableBitSet] = {
    // group by keys form a unique key
    ImmutableSet.of(ImmutableBitSet.of(grouping.indices: _*))
  }

  def getUniqueKeys(
      rel: WindowAggregate,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): util.Set[ImmutableBitSet] = {
    getUniqueKeysOnWindowAgg(
      rel.getRowType.getFieldCount,
      rel.getNamedProperties,
      rel.getGroupSet.toArray)
  }

  def getUniqueKeys(
      rel: BatchPhysicalWindowAggregateBase,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): util.Set[ImmutableBitSet] = {
    if (rel.isFinal) {
      getUniqueKeysOnWindowAgg(
        rel.getRowType.getFieldCount,
        rel.namedWindowProperties,
        rel.grouping)
    } else {
      null
    }
  }

  def getUniqueKeys(
      rel: StreamPhysicalGroupWindowAggregate,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): util.Set[ImmutableBitSet] = {
    getUniqueKeysOnWindowAgg(
      rel.getRowType.getFieldCount, rel.namedWindowProperties, rel.grouping)
  }

  def getUniqueKeysOnWindowAgg(
      fieldCount: Int,
      namedProperties: Seq[PlannerNamedWindowProperty],
      grouping: Array[Int]): util.Set[ImmutableBitSet] = {
    if (namedProperties.nonEmpty) {
      val begin = fieldCount - namedProperties.size
      val end = fieldCount - 1
      //namedProperties's indexes is at the end of output record
      val keys = ImmutableBitSet.of(grouping.indices: _*)
      (begin to end).map {
        i => keys.union(ImmutableBitSet.of(i))
      }.toSet[ImmutableBitSet]
    } else {
      null
    }
  }

  def getUniqueKeys(
      rel: Window,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): JSet[ImmutableBitSet] = {
    getUniqueKeysOfOverAgg(rel, mq, ignoreNulls)
  }

  def getUniqueKeys(
      rel: BatchPhysicalOverAggregate,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): JSet[ImmutableBitSet] = {
    getUniqueKeysOfOverAgg(rel, mq, ignoreNulls)
  }

  def getUniqueKeys(
      rel: StreamPhysicalOverAggregate,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): JSet[ImmutableBitSet] = {
    getUniqueKeysOfOverAgg(rel, mq, ignoreNulls)
  }

  private def getUniqueKeysOfOverAgg(
      window: SingleRel,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): JSet[ImmutableBitSet] = {
    mq.getUniqueKeys(window.getInput, ignoreNulls)
  }

  def getUniqueKeys(
      join: Join,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): JSet[ImmutableBitSet] = {
    join.getJoinType match {
      case JoinRelType.SEMI | JoinRelType.ANTI =>
        // only return the unique keys from the LHS since a SEMI/ANTI join only
        // returns the LHS
        mq.getUniqueKeys(join.getLeft, ignoreNulls)
      case _ =>
        getJoinUniqueKeys(
          join.analyzeCondition(), join.getJoinType, join.getLeft, join.getRight, mq, ignoreNulls)
    }
  }

  def getUniqueKeys(
      rel: StreamPhysicalIntervalJoin,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): JSet[ImmutableBitSet] = {
    val joinInfo = JoinInfo.of(rel.getLeft, rel.getRight, rel.originalCondition)
    getJoinUniqueKeys(joinInfo, rel.getJoinType, rel.getLeft, rel.getRight, mq, ignoreNulls)
  }

  def getUniqueKeys(
      join: CommonPhysicalLookupJoin,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): util.Set[ImmutableBitSet] = {
    val left = join.getInput
    val leftUniqueKeys = mq.getUniqueKeys(left, ignoreNulls)
    val leftType = left.getRowType
    getJoinUniqueKeys(
      join.joinType, leftType, leftUniqueKeys, null,
      mq.areColumnsUnique(left, join.joinInfo.leftSet, ignoreNulls),
      // TODO get uniqueKeys from TableSchema of TableSource
      null)
  }

  private def getJoinUniqueKeys(
      joinInfo: JoinInfo,
      joinRelType: JoinRelType,
      left: RelNode,
      right: RelNode,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): JSet[ImmutableBitSet] = {
    val leftUniqueKeys = mq.getUniqueKeys(left, ignoreNulls)
    val rightUniqueKeys = mq.getUniqueKeys(right, ignoreNulls)
    getJoinUniqueKeys(
      joinRelType, left.getRowType, leftUniqueKeys, rightUniqueKeys,
      mq.areColumnsUnique(left, joinInfo.leftSet, ignoreNulls),
      mq.areColumnsUnique(right, joinInfo.rightSet, ignoreNulls))
  }

  def getJoinUniqueKeys(
      joinRelType: JoinRelType,
      leftType: RelDataType,
      leftUniqueKeys: JSet[ImmutableBitSet],
      rightUniqueKeys: JSet[ImmutableBitSet],
      isLeftUnique: JBoolean,
      isRightUnique: JBoolean): JSet[ImmutableBitSet] = {

    // first add the different combinations of concatenated unique keys
    // from the left and the right, adjusting the right hand side keys to
    // reflect the addition of the left hand side
    //
    // NOTE zfong 12/18/06 - If the number of tables in a join is large,
    // the number of combinations of unique key sets will explode.  If
    // that is undesirable, use RelMetadataQuery.areColumnsUnique() as
    // an alternative way of getting unique key information.
    val retSet = new JHashSet[ImmutableBitSet]
    val nFieldsOnLeft = leftType.getFieldCount
    val rightSet = if (rightUniqueKeys != null) {
      val res = new JHashSet[ImmutableBitSet]
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
    // equi-join columns
    val leftUnique = isLeftUnique
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
      rel: Correlate,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): util.Set[ImmutableBitSet] = null

  def getUniqueKeys(
      rel: BatchPhysicalCorrelate,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): util.Set[ImmutableBitSet] = null

  def getUniqueKeys(
      rel: SetOp,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): JSet[ImmutableBitSet] = {
    if (!rel.all) {
      ImmutableSet.of(ImmutableBitSet.range(rel.getRowType.getFieldCount))
    } else {
      ImmutableSet.of()
    }
  }

  def getUniqueKeys(
      subset: RelSubset,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): JSet[ImmutableBitSet] = {
    if (!Bug.CALCITE_1048_FIXED) {
      //if the best node is null, so we can get the uniqueKeys based original node, due to
      //the original node is logically equivalent as the rel.
      val rel = Util.first(subset.getBest, subset.getOriginal)
      mq.getUniqueKeys(rel, ignoreNulls)
    } else {
      throw new RuntimeException("CALCITE_1048 is fixed, so check this method again!")
    }
  }

  def getUniqueKeys(
      subset: HepRelVertex,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): JSet[ImmutableBitSet] = {
    mq.getUniqueKeys(subset.getCurrentRel, ignoreNulls)
  }

  def getUniqueKeys(
      subset: WatermarkAssigner,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): JSet[ImmutableBitSet] = {
    mq.getUniqueKeys(subset.getInput, ignoreNulls)
  }

  // Catch-all rule when none of the others apply.
  def getUniqueKeys(
      rel: RelNode,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): JSet[ImmutableBitSet] = null

}

object FlinkRelMdUniqueKeys {

  val INSTANCE = new FlinkRelMdUniqueKeys

  val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
    BuiltInMethod.UNIQUE_KEYS.method, INSTANCE)

}
