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
import org.apache.flink.table.planner.plan.schema.FlinkPreparingTableBase
import org.apache.flink.table.planner.plan.utils.AggregateUtil
import org.apache.flink.table.planner.{JArrayList, JDouble, JList}

import com.google.common.collect.ImmutableList
import org.apache.calcite.avatica.util.ByteString
import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.metadata._
import org.apache.calcite.rel.{RelNode, SingleRel}
import org.apache.calcite.rex.{RexCall, RexInputRef, RexLiteral, RexNode}
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.util.{BuiltInMethod, ImmutableNullableList, NlsString, Util}

import scala.collection.JavaConversions._

/**
  * FlinkRelMdSize supplies a implementation of
  * [[RelMetadataQuery#getAverageRowSize]] and
  * [[RelMetadataQuery#getAverageColumnSizes]] for the standard logical algebra.
  */
class FlinkRelMdSize private extends MetadataHandler[BuiltInMetadata.Size] {

  def getDef: MetadataDef[BuiltInMetadata.Size] = BuiltInMetadata.Size.DEF

  // --------------- averageRowSize ----------------------------------------------------------------

  def averageRowSize(rel: TableScan, mq: RelMetadataQuery): JDouble = {
    val averageColumnSizes = mq.getAverageColumnSizes(rel)
    require(averageColumnSizes != null && !averageColumnSizes.contains(null))
    averageColumnSizes.foldLeft(0D)(_ + _)
  }

  def averageRowSize(rel: RelNode, mq: RelMetadataQuery): JDouble = {
    val averageColumnSizes = mq.getAverageColumnSizes(rel)
    if (averageColumnSizes == null) {
      FlinkRelMdSize.estimateRowSize(rel.getRowType)
    } else {
      val fields = rel.getRowType.getFieldList
      val columnSizes = averageColumnSizes.zip(fields) map {
        case (columnSize, field) =>
          if (columnSize == null) FlinkRelMdSize.averageTypeValueSize(field.getType) else columnSize
      }
      columnSizes.foldLeft(0D)(_ + _)
    }
  }

  // --------------- averageColumnSizes ------------------------------------------------------------

  def averageColumnSizes(rel: TableScan, mq: RelMetadataQuery): JList[JDouble] = {
    val statistic = rel.getTable.asInstanceOf[FlinkPreparingTableBase].getStatistic
    rel.getRowType.getFieldList.map { field =>
      val colStats = statistic.getColumnStats(field.getName)
      if (colStats != null && colStats.getAvgLen != null) {
        colStats.getAvgLen
      } else {
        FlinkRelMdSize.averageTypeValueSize(field.getType)
      }
    }
  }

  def averageColumnSizes(rel: Values, mq: RelMetadataQuery): JList[JDouble] = {
    val fields = rel.getRowType.getFieldList
    val list = ImmutableList.builder[JDouble]()
    fields.zipWithIndex.foreach {
      case (field, index) =>
        val d: JDouble = if (rel.getTuples().isEmpty) {
          FlinkRelMdSize.averageTypeValueSize(field.getType)
        } else {
          val sumSize = rel.getTuples().foldLeft(0D) { (acc, literals) =>
            val size = typeValueSize(field.getType,
              literals.get(index).getValueAs(classOf[Comparable[_]]))
            acc + size
          }
          sumSize / rel.getTuples.size()
        }
        list.add(d)
    }
    list.build
  }

  def averageColumnSizes(rel: Project, mq: RelMetadataQuery): JList[JDouble] = {
    val inputColumnSizes = mq.getAverageColumnSizesNotNull(rel.getInput)
    val sizesBuilder = ImmutableNullableList.builder[JDouble]()
    rel.getProjects.foreach(p => sizesBuilder.add(averageRexSize(p, inputColumnSizes)))
    sizesBuilder.build
  }

  def averageColumnSizes(rel: Filter, mq: RelMetadataQuery): JList[JDouble] =
    mq.getAverageColumnSizes(rel.getInput)

  def averageColumnSizes(rel: Calc, mq: RelMetadataQuery): JList[JDouble] = {
    val inputColumnSizes = mq.getAverageColumnSizesNotNull(rel.getInput())
    val sizesBuilder = ImmutableNullableList.builder[JDouble]()
    val projects = rel.getProgram.split().left
    projects.foreach(p => sizesBuilder.add(averageRexSize(p, inputColumnSizes)))
    sizesBuilder.build()
  }

  def averageColumnSizes(rel: Expand, mq: RelMetadataQuery): JList[JDouble] = {
    val fieldCount = rel.getRowType.getFieldCount
    // get each column's RexNode (RexLiteral, RexInputRef or null)
    val projectNodes = (0 until fieldCount).map { i =>
      val initNode: RexNode = rel.getCluster.getRexBuilder.constantNull()
      rel.projects.foldLeft(initNode) {
        (mergeNode, project) =>
          (mergeNode, project.get(i)) match {
            case (l1: RexLiteral, l2: RexLiteral) =>
              // choose non-null one
              if (l1.getValueAs(classOf[Comparable[_]]) == null) l2 else l1
            case (_: RexLiteral, r: RexInputRef) => r
            case (r: RexInputRef, _: RexLiteral) => r
            case (r1: RexInputRef, r2: RexInputRef) =>
              // if reference different columns, return null (using default value)
              if (r1.getIndex == r2.getIndex) r1 else null
            case (_, _) => null
          }
      }
    }

    val inputColumnSizes = mq.getAverageColumnSizesNotNull(rel.getInput())
    val sizesBuilder = ImmutableNullableList.builder[JDouble]()
    projectNodes.zipWithIndex.foreach {
      case (p, i) =>
        val size = if (p == null || i == rel.expandIdIndex) {
          // use default value
          FlinkRelMdSize.averageTypeValueSize(rel.getRowType.getFieldList.get(i).getType)
        } else {
          // use value from input
          averageRexSize(p, inputColumnSizes)
        }
        sizesBuilder.add(size)
    }
    sizesBuilder.build()
  }

  def averageColumnSizes(rel: Exchange, mq: RelMetadataQuery): JList[JDouble] =
    mq.getAverageColumnSizes(rel.getInput)

  def averageColumnSizes(rel: Rank, mq: RelMetadataQuery): JList[JDouble] = {
    val inputColumnSizes = mq.getAverageColumnSizes(rel.getInput)
    if (rel.getRowType.getFieldCount != rel.getInput.getRowType.getFieldCount) {
      // if outputs rank function value, rank function column is the last one
      val rankFunColumnSize =
        FlinkRelMdSize.averageTypeValueSize(rel.getRowType.getFieldList.last.getType)
      inputColumnSizes ++ List(rankFunColumnSize)
    } else {
      inputColumnSizes
    }
  }

  def averageColumnSizes(rel: Sort, mq: RelMetadataQuery): JList[JDouble] =
    mq.getAverageColumnSizes(rel.getInput)

  def averageColumnSizes(rel: Aggregate, mq: RelMetadataQuery): JList[JDouble] = {
    val inputColumnSizes = mq.getAverageColumnSizesNotNull(rel.getInput)
    val sizesBuilder = ImmutableList.builder[JDouble]()
    val (auxGroupSet, otherAggCalls) = AggregateUtil.checkAndSplitAggCalls(rel)
    val fullGrouping = rel.getGroupSet.toArray ++ auxGroupSet
    fullGrouping.foreach(i => sizesBuilder.add(inputColumnSizes.get(i)))
    otherAggCalls.foreach(aggCall => sizesBuilder.add(
      FlinkRelMdSize.averageTypeValueSize(aggCall.getType)))
    sizesBuilder.build
  }

  def averageColumnSizes(
      rel: BatchPhysicalGroupAggregateBase,
      mq: RelMetadataQuery): JList[JDouble] = {
    // note: the logical to estimate column sizes of AggregateBatchExecBase is different from
    // Calcite Aggregate because AggregateBatchExecBase's rowTypes is not composed by
    // grouping columns + aggFunctionCall results
    val mapInputToOutput = (rel.grouping ++ rel.auxGrouping).zipWithIndex.toMap
    getColumnSizesFromInputOrType(rel, mq, mapInputToOutput)
  }

  def averageColumnSizes(rel: WindowAggregate, mq: RelMetadataQuery): JList[JDouble] = {
    averageColumnSizesOfWindowAgg(rel, mq)
  }

  def averageColumnSizes(
      rel: BatchPhysicalWindowAggregateBase,
      mq: RelMetadataQuery): JList[JDouble] = {
    averageColumnSizesOfWindowAgg(rel, mq)
  }

  private def averageColumnSizesOfWindowAgg(
      windowAgg: SingleRel,
      mq: RelMetadataQuery): JList[JDouble] = {
    val mapInputToOutput: Map[Int, Int] = windowAgg match {
      case agg: WindowAggregate =>
        AggregateUtil.checkAndGetFullGroupSet(agg).zipWithIndex.toMap
      case agg: BatchPhysicalLocalHashWindowAggregate =>
        // local win-agg output type: grouping + assignTs + auxGrouping + aggCalls
        agg.grouping.zipWithIndex.toMap ++
          agg.auxGrouping.zipWithIndex.map {
            case (k, v) => k -> (agg.grouping.length + 1 + v)
          }.toMap
      case agg: BatchPhysicalLocalSortWindowAggregate =>
        // local win-agg output type: grouping + assignTs + auxGrouping + aggCalls
        agg.grouping.zipWithIndex.toMap ++
          agg.auxGrouping.zipWithIndex.map {
            case (k, v) => k -> (agg.grouping.length + 1 + v)
          }.toMap
      case agg: BatchPhysicalWindowAggregateBase =>
        (agg.grouping ++ agg.auxGrouping).zipWithIndex.toMap
      case _ => throw new IllegalArgumentException(s"Unknown node type ${windowAgg.getRelTypeName}")
    }
    getColumnSizesFromInputOrType(windowAgg, mq, mapInputToOutput)
  }

  def averageColumnSizes(overWindow: Window, mq: RelMetadataQuery): JList[JDouble] =
    averageColumnSizesOfOverAgg(overWindow, mq)

  def averageColumnSizes(rel: BatchPhysicalOverAggregate, mq: RelMetadataQuery): JList[JDouble] =
    averageColumnSizesOfOverAgg(rel, mq)

  private def averageColumnSizesOfOverAgg(
      overAgg: SingleRel,
      mq: RelMetadataQuery): JList[JDouble] = {
    val inputFieldCount = overAgg.getInput.getRowType.getFieldCount
    getColumnSizesFromInputOrType(overAgg, mq, (0 until inputFieldCount).zipWithIndex.toMap)
  }

  def averageColumnSizes(rel: Join, mq: RelMetadataQuery): JList[JDouble] = {
    val acsOfLeft = mq.getAverageColumnSizes(rel.getLeft)
    val acsOfRight = rel.getJoinType match {
      case JoinRelType.SEMI | JoinRelType.ANTI => null
      case _ => mq.getAverageColumnSizes(rel.getRight)
    }
    if (acsOfLeft == null && acsOfRight == null) {
      null
    } else if (acsOfRight == null) {
      acsOfLeft
    } else if (acsOfLeft == null) {
      acsOfRight
    } else {
      val sizesBuilder = ImmutableNullableList.builder[JDouble]()
      sizesBuilder.addAll(acsOfLeft)
      sizesBuilder.addAll(acsOfRight)
      sizesBuilder.build()
    }
  }

  def averageColumnSizes(rel: Union, mq: RelMetadataQuery): JList[JDouble] = {
    val inputColumnSizeList = new JArrayList[JList[JDouble]]()
    rel.getInputs.foreach { input =>
      val inputSizes = mq.getAverageColumnSizes(input)
      if (inputSizes != null) {
        inputColumnSizeList.add(inputSizes)
      }
    }
    inputColumnSizeList.length match {
      case 0 => null // all were null
      case 1 => inputColumnSizeList.get(0) // all but one were null
      case _ =>
        val sizes = ImmutableNullableList.builder[JDouble]()
        var nn = 0
        val fieldCount: Int = rel.getRowType.getFieldCount
        (0 until fieldCount).foreach { i =>
          var d = 0D
          var n = 0
          inputColumnSizeList.foreach { inputColumnSizes =>
            val d2 = inputColumnSizes.get(i)
            if (d2 != null) {
              d += d2
              n += 1
              nn += 1
            }
          }
          val size: JDouble = if (n > 0) d / n else null
          sizes.add(size)
        }
        if (nn == 0) {
          null // all columns are null
        } else {
          sizes.build()
        }
    }
  }

  def averageColumnSizes(rel: Intersect, mq: RelMetadataQuery): JList[JDouble] =
    mq.getAverageColumnSizes(rel.getInput(0))

  def averageColumnSizes(rel: Minus, mq: RelMetadataQuery): JList[JDouble] =
    mq.getAverageColumnSizes(rel.getInput(0))

  def averageColumnSizes(subset: RelSubset, mq: RelMetadataQuery): JList[JDouble] = {
    val rel = Util.first(subset.getBest, subset.getOriginal)
    mq.getAverageColumnSizes(rel)
  }

  def averageColumnSizes(rel: RelNode, mq: RelMetadataQuery): JList[JDouble] =
    rel.getRowType.getFieldList.map(f => FlinkRelMdSize.averageTypeValueSize(f.getType)).toList

  private def averageRexSize(node: RexNode, inputColumnSizes: JList[JDouble]): JDouble = {
    node match {
      case ref: RexInputRef => inputColumnSizes.get(ref.getIndex)
      case lit: RexLiteral => typeValueSize(node.getType, lit.getValueAs(classOf[Comparable[_]]))
      case call: RexCall =>
        val nodeSqlTypeName = node.getType.getSqlTypeName
        val matchedOps = call.getOperands.filter(op => op.getType.getSqlTypeName eq nodeSqlTypeName)
        matchedOps.headOption match {
          case Some(op) => averageRexSize(op, inputColumnSizes)
          case _ => FlinkRelMdSize.averageTypeValueSize(node.getType)
        }
      case _ => FlinkRelMdSize.averageTypeValueSize(node.getType)
    }
  }

  /**
    * Estimates the average size (in bytes) of a value of a type.
    *
    * Nulls count as 1 byte.
    */
  private def typeValueSize(t: RelDataType, value: Comparable[_]): JDouble = {
    if (value == null) {
      return 1D
    }
    t.getSqlTypeName match {
      case SqlTypeName.BINARY | SqlTypeName.VARBINARY =>
        value.asInstanceOf[ByteString].length().toDouble
      case SqlTypeName.CHAR | SqlTypeName.VARCHAR =>
        value.asInstanceOf[NlsString].getValue.length * FlinkRelMdSize.BYTES_PER_CHARACTER.toDouble
      case _ => FlinkRelMdSize.averageTypeValueSize(t)
    }
  }

  /**
    * Gets each column size of rel output from input column size or from column type.
    * column size is from input column size if the column index is in `mapInputToOutput` keys,
    * otherwise from column type.
    */
  private def getColumnSizesFromInputOrType(
      rel: SingleRel,
      mq: RelMetadataQuery,
      mapInputToOutput: Map[Int, Int]): JList[JDouble] = {
    val outputIndices = mapInputToOutput.values
    require(outputIndices.forall(idx => rel.getRowType.getFieldCount > idx && idx >= 0))
    val inputIndices = mapInputToOutput.keys
    val input = rel.getInput
    inputIndices.forall(idx => input.getRowType.getFieldCount > idx && idx >= 0)

    val mapOutputToInput = mapInputToOutput.map(_.swap)
    val acsOfInput = mq.getAverageColumnSizesNotNull(input)
    val sizesBuilder = ImmutableList.builder[JDouble]()
    rel.getRowType.getFieldList.zipWithIndex.foreach {
      case (f, idx) =>
        val size = mapOutputToInput.get(idx) match {
          case Some(inputIdx) => acsOfInput.get(inputIdx)
          case _ => FlinkRelMdSize.averageTypeValueSize(f.getType)
        }
        sizesBuilder.add(size)
    }
    sizesBuilder.build()
  }
}

object FlinkRelMdSize {

  private val INSTANCE = new FlinkRelMdSize

  // Bytes per character (2).
  val BYTES_PER_CHARACTER: Int = Character.SIZE / java.lang.Byte.SIZE

  val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
    INSTANCE,
    BuiltInMethod.AVERAGE_COLUMN_SIZES.method,
    BuiltInMethod.AVERAGE_ROW_SIZE.method)

  def averageTypeValueSize(t: RelDataType): JDouble = t.getSqlTypeName match {
    case SqlTypeName.ROW | SqlTypeName.STRUCTURED =>
      estimateRowSize(t)
    case SqlTypeName.ARRAY =>
      // 16 is an arbitrary estimate
      averageTypeValueSize(t.getComponentType) * 16
    case SqlTypeName.MAP =>
      // 16 is an arbitrary estimate
      (averageTypeValueSize(t.getKeyType) + averageTypeValueSize(t.getValueType)) * 16
    case SqlTypeName.MULTISET =>
      // 16 is an arbitrary estimate
      (averageTypeValueSize(t.getComponentType) + averageTypeValueSize(SqlTypeName.INTEGER)) * 16
    case _ => averageTypeValueSize(t.getSqlTypeName)
  }

  private def estimateRowSize(rowType: RelDataType): JDouble = {
    val fieldList = rowType.getFieldList
    fieldList.map(_.getType).foldLeft(0.0) {
      (s, t) =>
        s + averageTypeValueSize(t)
    }
  }

  def averageTypeValueSize(sqlType: SqlTypeName): JDouble = sqlType match {
    case SqlTypeName.TINYINT => 1D
    case SqlTypeName.SMALLINT => 2D
    case SqlTypeName.INTEGER => 4D
    case SqlTypeName.BIGINT => 8D
    case SqlTypeName.BOOLEAN => 1D
    case SqlTypeName.FLOAT => 4D
    case SqlTypeName.DOUBLE => 8D
    case SqlTypeName.VARCHAR => 12D
    case SqlTypeName.CHAR => 1D
    case SqlTypeName.DECIMAL => 12D
    case typeName if SqlTypeName.YEAR_INTERVAL_TYPES.contains(typeName) => 8D
    case typeName if SqlTypeName.DAY_INTERVAL_TYPES.contains(typeName) => 4D
    // TODO after time/date => int, timestamp => long, this estimate value should update
    case SqlTypeName.TIME | SqlTypeName.TIMESTAMP |
         SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE | SqlTypeName.DATE => 12D
    case SqlTypeName.ANY | SqlTypeName.OTHER => 128D // 128 is an arbitrary estimate
    case SqlTypeName.BINARY | SqlTypeName.VARBINARY => 16D // 16 is an arbitrary estimate
    case _ => throw new TableException(s"Unsupported data type encountered: $sqlType")
  }

}
