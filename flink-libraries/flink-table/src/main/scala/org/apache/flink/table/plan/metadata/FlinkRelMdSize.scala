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

import org.apache.flink.table.api.TableException
import org.apache.flink.table.plan.nodes.calcite.{Expand, LogicalWindowAggregate, Rank}
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalWindowAggregate
import org.apache.flink.table.plan.nodes.physical.batch._
import org.apache.flink.table.plan.schema.FlinkRelOptTable
import org.apache.flink.table.plan.util.FlinkRelOptUtil.{checkAndGetFullGroupSet, checkAndSplitAggCalls}

import org.apache.calcite.avatica.util.ByteString
import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.metadata._
import org.apache.calcite.rel.{BiRel, RelNode, SingleRel}
import org.apache.calcite.rex.{RexCall, RexInputRef, RexLiteral, RexNode}
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.util.{BuiltInMethod, ImmutableNullableList, NlsString, Util}

import com.google.common.collect.ImmutableList

import java.lang.Double
import java.util.{List => JList}

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * FlinkRelMdSize supplies a default implementation of
  * [[RelMetadataQuery#getAverageColumnSizes]] for the standard logical algebra.
  */
class FlinkRelMdSize private extends MetadataHandler[BuiltInMetadata.Size] {

  def getDef: MetadataDef[BuiltInMetadata.Size] = BuiltInMetadata.Size.DEF

  def averageRowSize(rel: TableScan, mq: RelMetadataQuery): Double = {
    val averageColumnSizes = mq.getAverageColumnSizes(rel)
    assert(averageColumnSizes != null && !averageColumnSizes.contains(null))
    averageColumnSizes.foldLeft(0D)(_ + _)
  }

  def averageRowSize(rel: RelNode, mq: RelMetadataQuery): Double = {
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

  def averageColumnSizes(rel: TableScan, mq: RelMetadataQuery): JList[Double] = {
    val statistic = rel.getTable.asInstanceOf[FlinkRelOptTable].getFlinkStatistic
    rel.getRowType.getFieldList.map { f =>
      val colStats = statistic.getColumnStats(f.getName)
      if (colStats != null && colStats.avgLen != null) {
        colStats.avgLen
      } else {
        FlinkRelMdSize.averageTypeValueSize(f.getType)
      }
    }
  }

  def averageColumnSizes(rel: RelNode, mq: RelMetadataQuery): JList[Double] =
    rel.getRowType.getFieldList.map(f => FlinkRelMdSize.averageTypeValueSize(f.getType)).toList

  def averageColumnSizes(rel: Calc, mq: RelMetadataQuery): JList[Double] = {
    val inputColumnSizes = mq.getAverageColumnSizesNotNull(rel.getInput())
    val sizesBuilder = ImmutableNullableList.builder[Double]()
    val projects = rel.getProgram.split().left
    projects.foreach(p => sizesBuilder.add(averageRexSize(p, inputColumnSizes)))
    sizesBuilder.build()
  }

  def averageColumnSizes(rel: BatchExecOverAggregate, mq: RelMetadataQuery): JList[Double] =
    averageColumnSizesOfOverWindow(rel, mq)

  def averageColumnSizes(overWindow: Window, mq: RelMetadataQuery): JList[Double] =
    averageColumnSizesOfOverWindow(overWindow, mq)

  private def averageColumnSizesOfOverWindow(
      overWindow: SingleRel,
      mq: RelMetadataQuery): JList[Double] = {
    val inputFieldCount = overWindow.getInput.getRowType.getFieldCount
    getColumnSizesFromInputOrType(overWindow, mq, (0 until inputFieldCount).zipWithIndex.toMap)
  }

  def averageColumnSizes(rel: FlinkLogicalWindowAggregate, mq: RelMetadataQuery): JList[Double] = {
    averageColumnSizesOfWindowAgg(rel, mq)
  }

  def averageColumnSizes(rel: LogicalWindowAggregate, mq: RelMetadataQuery): JList[Double] = {
    averageColumnSizesOfWindowAgg(rel, mq)
  }

  def averageColumnSizes(rel: BatchExecWindowAggregateBase, mq: RelMetadataQuery): JList[Double] = {
    averageColumnSizesOfWindowAgg(rel, mq)
  }

  private def averageColumnSizesOfWindowAgg(
      windowAgg: SingleRel,
      mq: RelMetadataQuery): JList[Double] = {
    val mapInputToOutput: Map[Int, Int] = windowAgg match {
      case agg: FlinkLogicalWindowAggregate => checkAndGetFullGroupSet(agg).zipWithIndex.toMap
      case agg: LogicalWindowAggregate => checkAndGetFullGroupSet(agg).zipWithIndex.toMap
      case agg: BatchExecLocalHashWindowAggregate =>
        // local win-agg output type: grouping + assignTs + auxGrouping + aggCalls
        agg.getGrouping.zipWithIndex.toMap ++
          agg.getAuxGrouping.zipWithIndex.map {
            case (k, v) => k -> (agg.getGrouping.length + 1 + v)
          }.toMap
      case agg: BatchExecLocalSortWindowAggregate =>
        // local win-agg output type: grouping + assignTs + auxGrouping + aggCalls
        agg.getGrouping.zipWithIndex.toMap ++
          agg.getAuxGrouping.zipWithIndex.map {
            case (k, v) => k -> (agg.getGrouping.length + 1 + v)
          }.toMap
      case agg: BatchExecWindowAggregateBase =>
        (agg.getGrouping ++ agg.getAuxGrouping).zipWithIndex.toMap
      case _ => throw new IllegalArgumentException(s"Unknown node type ${windowAgg.getRelTypeName}")
    }
    getColumnSizesFromInputOrType(windowAgg, mq, mapInputToOutput)
  }

  def averageColumnSizes(rel: BatchExecGroupAggregateBase, mq: RelMetadataQuery): JList[Double] = {
    // note: the logical to estimate column sizes of AggregateBatchExecBase is different from
    // Calcite Aggregate because AggregateBatchExecBase's rowTypes is not composed by
    // grouping columns + aggFunctionCall results
    val mapInputToOutput = (rel.getGrouping ++ rel.getAuxGrouping).zipWithIndex.toMap
    getColumnSizesFromInputOrType(rel, mq, mapInputToOutput)
  }

  def averageColumnSizes(rel: Union, mq: RelMetadataQuery): JList[Double] =
    averageColumnSizesOfUnion(rel, mq)

  private def averageColumnSizesOfUnion(rel: RelNode, mq: RelMetadataQuery): JList[Double] = {
    val inputColumnSizeList = mutable.ArrayBuffer[JList[Double]]()
    rel.getInputs.foreach { i =>
      val inputSizes = mq.getAverageColumnSizes(i)
      if (inputSizes != null) {
        inputColumnSizeList += inputSizes
      }
    }
    inputColumnSizeList.length match {
      case 0 => null // all were null
      case 1 => inputColumnSizeList.get(0) // all but one were null
      case _ =>
        val sizes = ImmutableNullableList.builder[Double]()
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
          val size: Double = if (n > 0) d / n else null
          sizes.add(size)
        }
        if (nn == 0) {
          null // all columns are null
        } else {
          sizes.build()
        }
    }
  }

  def averageColumnSizes(rel: Expand, mq: RelMetadataQuery): JList[Double] = {
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
    val sizesBuilder = ImmutableNullableList.builder[Double]()
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

  def averageColumnSizes(subset: RelSubset, mq: RelMetadataQuery): JList[Double] =
    mq.getAverageColumnSizes(Util.first(subset.getBest, subset.getOriginal))

  def averageColumnSizes(rel: Rank, mq: RelMetadataQuery): JList[Double] = {
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

  def averageColumnSizes(rel: Filter, mq: RelMetadataQuery): JList[Double] =
    mq.getAverageColumnSizes(rel.getInput)

  def averageColumnSizes(rel: Sort, mq: RelMetadataQuery): JList[Double] =
    mq.getAverageColumnSizes(rel.getInput)

  def averageColumnSizes(rel: Exchange, mq: RelMetadataQuery): JList[Double] =
    mq.getAverageColumnSizes(rel.getInput)

  def averageColumnSizes(rel: Project, mq: RelMetadataQuery): JList[Double] = {
    val inputColumnSizes = mq.getAverageColumnSizesNotNull(rel.getInput)
    val sizesBuilder = ImmutableNullableList.builder[Double]()
    rel.getProjects.foreach(p => sizesBuilder.add(averageRexSize(p, inputColumnSizes)))
    sizesBuilder.build
  }

  def averageColumnSizes(rel: Values, mq: RelMetadataQuery): JList[Double] = {
    val fields = rel.getRowType.getFieldList
    val list = ImmutableList.builder[Double]()
    fields.zipWithIndex.foreach {
      case (f, index) =>
        val d: Double = if (rel.getTuples().isEmpty) {
          FlinkRelMdSize.averageTypeValueSize(f.getType)
        } else {
          val sumSize = rel.getTuples().foldLeft(0D)((acc, literals) =>
            acc + typeValueSize(f.getType, literals.get(index).getValueAs(classOf[Comparable[_]]))
          )
          sumSize / rel.getTuples.size()
        }
        list.add(d)
    }
    list.build
  }

  def averageColumnSizes(rel: Aggregate, mq: RelMetadataQuery): JList[Double] = {
    val inputColumnSizes = mq.getAverageColumnSizesNotNull(rel.getInput)
    val sizesBuilder = ImmutableList.builder[Double]()
    val (auxGroupSet, otherAggCalls) = checkAndSplitAggCalls(rel)
    val fullGrouping = rel.getGroupSet.toArray ++ auxGroupSet
    fullGrouping.foreach(i => sizesBuilder.add(inputColumnSizes.get(i)))
    otherAggCalls.foreach(aggCall => sizesBuilder.add(
      FlinkRelMdSize.averageTypeValueSize(aggCall.getType)))
    sizesBuilder.build
  }

  def averageColumnSizes(rel: SemiJoin, mq: RelMetadataQuery): JList[Double] =
    averageJoinColumnSizes(rel, mq, isSemi = true)

  def averageColumnSizes(rel: Join, mq: RelMetadataQuery): JList[Double] =
    averageJoinColumnSizes(rel, mq, isSemi = false)

  private def averageJoinColumnSizes(
      join: BiRel,
      mq: RelMetadataQuery,
      isSemi: Boolean): JList[Double] = {
    val acsOfLeft = mq.getAverageColumnSizes(join.getLeft)
    val acsOfRight = if (isSemi) null else mq.getAverageColumnSizes(join.getRight)
    if (acsOfLeft == null && acsOfRight == null) {
      null
    } else if (acsOfRight == null) {
      acsOfLeft
    } else if (acsOfLeft == null) {
      acsOfRight
    } else {
      val sizesBuilder = ImmutableNullableList.builder[Double]()
      sizesBuilder.addAll(acsOfLeft)
      sizesBuilder.addAll(acsOfRight)
      sizesBuilder.build()
    }
  }

  def averageColumnSizes(rel: Intersect, mq: RelMetadataQuery): JList[Double] =
    mq.getAverageColumnSizes(rel.getInput(0))

  def averageColumnSizes(rel: Minus, mq: RelMetadataQuery): JList[Double] =
    mq.getAverageColumnSizes(rel.getInput(0))

  def averageRexSize(node: RexNode, inputColumnSizes: JList[Double]): Double = {
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
  def typeValueSize(t: RelDataType, value: Comparable[_]): Double = {
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
      mapInputToOutput: Map[Int, Int]): JList[Double] = {
    val outputIndices = mapInputToOutput.values
    require(outputIndices.forall(idx => rel.getRowType.getFieldCount > idx && idx >= 0))
    val inputIndices = mapInputToOutput.keys
    val input = rel.getInput
    inputIndices.forall(idx => input.getRowType.getFieldCount > idx && idx >= 0)

    val mapOutputToInput = mapInputToOutput.map(_.swap)
    val acsOfInput = mq.getAverageColumnSizesNotNull(input)
    val sizesBuilder = ImmutableList.builder[Double]()
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

  def averageTypeValueSize(t: RelDataType): Double = t.getSqlTypeName match {
    case SqlTypeName.ROW =>
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

  private def estimateRowSize(rowType: RelDataType): Double = {
    val fieldList = rowType.getFieldList
    fieldList.map(_.getType).foldLeft(0.0) {
      (s, t) =>
        s + averageTypeValueSize(t)
    }
  }

  def averageTypeValueSize(sqlType: SqlTypeName): Double = sqlType match {
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
    case SqlTypeName.TIME | SqlTypeName.TIMESTAMP | SqlTypeName.DATE => 12D
    case SqlTypeName.ANY => 128D // 128 is an arbitrary estimate
    case SqlTypeName.BINARY | SqlTypeName.VARBINARY => 16D // 16 is an arbitrary estimate
    case _ => throw new TableException(s"Unsupported data type encountered: $sqlType")
  }

}
