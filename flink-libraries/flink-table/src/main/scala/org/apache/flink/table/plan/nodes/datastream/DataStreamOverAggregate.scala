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
package org.apache.flink.table.plan.nodes.datastream

import java.util.{List => JList}

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{AggregateCall, Window}
import org.apache.calcite.rel.core.Window.Group
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.rel.RelFieldCollation.Direction.ASCENDING
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.{StreamTableEnvironment, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.nodes.OverAggregate
import org.apache.flink.table.runtime.aggregate._
import org.apache.flink.api.java.functions.NullByteKeySelector
import org.apache.flink.table.codegen.CodeGenerator
import org.apache.flink.table.functions.{ProcTimeType, RowTimeType}
import org.apache.flink.table.runtime.aggregate.AggregateUtil.CalcitePair
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}

class DataStreamOverAggregate(
    logicWindow: Window,
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    rowRelDataType: RelDataType,
    inputType: RelDataType)
  extends SingleRel(cluster, traitSet, inputNode)
  with OverAggregate
  with DataStreamRel {

  override def deriveRowType(): RelDataType = rowRelDataType

  override def needsUpdatesAsRetraction = true

  override def consumesRetractions = true

  override def copy(traitSet: RelTraitSet, inputs: JList[RelNode]): RelNode = {
    new DataStreamOverAggregate(
      logicWindow,
      cluster,
      traitSet,
      inputs.get(0),
      getRowType,
      inputType)
  }

  override def toString: String = {
    s"OverAggregate($aggOpName)"
  }

  override def explainTerms(pw: RelWriter): RelWriter = {

    val overWindow: Group = logicWindow.groups.get(0)
    val partitionKeys: Array[Int] = overWindow.keys.toArray
    val namedAggregates: Seq[CalcitePair[AggregateCall, String]] = generateNamedAggregates

    super.explainTerms(pw)
      .itemIf("partitionBy", partitionToString(inputType, partitionKeys), partitionKeys.nonEmpty)
      .item("orderBy",orderingToString(inputType, overWindow.orderKeys.getFieldCollations))
      .itemIf("rows", windowRange(logicWindow, overWindow, getInput), overWindow.isRows)
      .itemIf("range", windowRange(logicWindow, overWindow, getInput), !overWindow.isRows)
      .item(
        "select", aggregationToString(
          inputType,
          getRowType,
          namedAggregates))
  }

  override def translateToPlan(tableEnv: StreamTableEnvironment): DataStream[CRow] = {
    if (logicWindow.groups.size > 1) {
      throw new TableException(
        "Unsupported use of OVER windows. All aggregates must be computed on the same window.")
    }

    val overWindow: org.apache.calcite.rel.core.Window.Group = logicWindow.groups.get(0)

    val orderKeys = overWindow.orderKeys.getFieldCollations

    if (orderKeys.size() != 1) {
      throw new TableException(
        "Unsupported use of OVER windows. The window can only be ordered by a single time column.")
    }
    val orderKey = orderKeys.get(0)

    if (!orderKey.direction.equals(ASCENDING)) {
      throw new TableException(
        "Unsupported use of OVER windows. The window can only be ordered in ASCENDING mode.")
    }

    val inputDS = input.asInstanceOf[DataStreamRel].translateToPlan(tableEnv)

    val consumeRetraction = DataStreamRetractionRules.isAccRetract(input)

    val generator = new CodeGenerator(
      tableEnv.getConfig,
      false,
      inputDS.getType)

    val timeType = inputType
      .getFieldList
      .get(orderKey.getFieldIndex)
      .getValue

    if (consumeRetraction) {
      throw new TableException(
        "Retraction on over window is not supported yet. Note: Currently, over windows shoud not" +
          " follow a non-windowed GroupBy.")
    }

    timeType match {
      case _: ProcTimeType =>
        // proc-time OVER window
        if (overWindow.lowerBound.isUnbounded && overWindow.upperBound.isCurrentRow) {
          // unbounded OVER window
          createUnboundedAndCurrentRowOverWindow(
            generator,
            inputDS,
            isRowTimeType = false,
            isRowsClause = overWindow.isRows,
            consumeRetraction)
        } else if (
          overWindow.lowerBound.isPreceding && !overWindow.lowerBound.isUnbounded &&
            overWindow.upperBound.isCurrentRow) {
          // bounded OVER window
          createBoundedAndCurrentRowOverWindow(
            generator,
            inputDS,
            isRowTimeType = false,
            isRowsClause = overWindow.isRows
          )
        } else {
          throw new TableException(
            "OVER RANGE FOLLOWING windows are not supported yet.")
        }
      case _: RowTimeType =>
        // row-time OVER window
        if (overWindow.lowerBound.isPreceding &&
          overWindow.lowerBound.isUnbounded && overWindow.upperBound.isCurrentRow) {
          // unbounded OVER window
          createUnboundedAndCurrentRowOverWindow(
            generator,
            inputDS,
            isRowTimeType = true,
            isRowsClause = overWindow.isRows,
            consumeRetraction
          )
        } else if (overWindow.lowerBound.isPreceding && overWindow.upperBound.isCurrentRow) {
          // bounded OVER window
          createBoundedAndCurrentRowOverWindow(
            generator,
            inputDS,
            isRowTimeType = true,
            isRowsClause = overWindow.isRows
            )
        } else {
          throw new TableException(
            "OVER RANGE FOLLOWING windows are not supported yet.")
        }
      case _ =>
        throw new TableException(
          "Unsupported time type {$timeType}. " +
            "OVER windows do only support RowTimeType and ProcTimeType.")
    }

  }

  def createUnboundedAndCurrentRowOverWindow(
    generator: CodeGenerator,
    inputDS: DataStream[CRow],
    isRowTimeType: Boolean,
    isRowsClause: Boolean,
    consumeRetraction: Boolean): DataStream[CRow] = {

    val overWindow: Group = logicWindow.groups.get(0)
    val partitionKeys: Array[Int] = overWindow.keys.toArray
    val namedAggregates: Seq[CalcitePair[AggregateCall, String]] = generateNamedAggregates

    // get the output types
    val retrunTypeInfo = CRowTypeInfo(FlinkTypeFactory.toInternalRowTypeInfo(getRowType))

    val processFunction = AggregateUtil.createUnboundedOverProcessFunction(
      generator,
      namedAggregates,
      inputType,
      isRowTimeType,
      partitionKeys.nonEmpty,
      isRowsClause,
      consumeRetraction)

    val result: DataStream[CRow] =
    // partitioned aggregation
      if (partitionKeys.nonEmpty) {
        inputDS
          .keyBy(partitionKeys: _*)
          .process(processFunction)
          .returns(retrunTypeInfo)
          .name(aggOpName)
          .asInstanceOf[DataStream[CRow]]
      }
      // non-partitioned aggregation
      else {
        if (isRowTimeType) {
          inputDS.keyBy(new NullByteKeySelector[CRow])
            .process(processFunction).setParallelism(1).setMaxParallelism(1)
            .returns(retrunTypeInfo)
            .name(aggOpName)
            .asInstanceOf[DataStream[CRow]]
        } else {
          inputDS
            .process(processFunction).setParallelism(1).setMaxParallelism(1)
            .returns(retrunTypeInfo)
            .name(aggOpName)
            .asInstanceOf[DataStream[CRow]]
        }
      }
    result
  }

  def createBoundedAndCurrentRowOverWindow(
    generator: CodeGenerator,
    inputDS: DataStream[CRow],
    isRowTimeType: Boolean,
    isRowsClause: Boolean): DataStream[CRow] = {

    val overWindow: Group = logicWindow.groups.get(0)
    val partitionKeys: Array[Int] = overWindow.keys.toArray
    val namedAggregates: Seq[CalcitePair[AggregateCall, String]] = generateNamedAggregates

    val precedingOffset =
      getLowerBoundary(logicWindow, overWindow, getInput()) + (if (isRowsClause) 1 else 0)

    // get the output types
    val retrunTypeInfo = CRowTypeInfo(FlinkTypeFactory.toInternalRowTypeInfo(getRowType))

    val processFunction = AggregateUtil.createBoundedOverProcessFunction[CRow](
      generator,
      namedAggregates,
      inputType,
      precedingOffset,
      isRowsClause,
      isRowTimeType
    )
    val result: DataStream[CRow] =
    // partitioned aggregation
      if (partitionKeys.nonEmpty) {
        inputDS
          .keyBy(partitionKeys: _*)
          .process(processFunction)
          .returns(retrunTypeInfo)
          .name(aggOpName)
          .asInstanceOf[DataStream[CRow]]
      }
      // non-partitioned aggregation
      else {
        inputDS
          .keyBy(new NullByteKeySelector[CRow])
          .process(processFunction).setParallelism(1).setMaxParallelism(1)
          .returns(retrunTypeInfo)
          .name(aggOpName)
          .asInstanceOf[DataStream[CRow]]
      }
    result
  }

  private def generateNamedAggregates: Seq[CalcitePair[AggregateCall, String]] = {
    val overWindow: Group = logicWindow.groups.get(0)

    val aggregateCalls = overWindow.getAggregateCalls(logicWindow)
    for (i <- 0 until aggregateCalls.size())
      yield new CalcitePair[AggregateCall, String](aggregateCalls.get(i), "w0$o" + i)
  }

  private def aggOpName = {
    val overWindow: Group = logicWindow.groups.get(0)
    val partitionKeys: Array[Int] = overWindow.keys.toArray
    val namedAggregates: Seq[CalcitePair[AggregateCall, String]] = generateNamedAggregates

    s"over: (${
      if (!partitionKeys.isEmpty) {
        s"PARTITION BY: ${partitionToString(inputType, partitionKeys)}, "
      } else {
        ""
      }
    }ORDER BY: ${orderingToString(inputType, overWindow.orderKeys.getFieldCollations)}, " +
      s"${if (overWindow.isRows) "ROWS" else "RANGE"}" +
      s"${windowRange(logicWindow, overWindow, getInput)}, " +
      s"select: (${
        aggregationToString(
          inputType,
          getRowType,
          namedAggregates)
      }))"
  }

}

