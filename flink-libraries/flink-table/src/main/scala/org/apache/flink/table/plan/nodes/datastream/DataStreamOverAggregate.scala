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

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.{StreamTableEnvironment, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.runtime.aggregate._
import org.apache.flink.table.plan.nodes.OverAggregate
import org.apache.flink.types.Row
import org.apache.calcite.rel.core.Window
import org.apache.calcite.rel.core.Window.Group
import java.util.{List => JList}

import org.apache.flink.table.functions.{ProcTimeType, RowTimeType}
import org.apache.flink.table.runtime.aggregate.AggregateUtil.CalcitePair

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
      .itemIf("rows", windowRange(overWindow), overWindow.isRows)
      .itemIf("range", windowRange(overWindow), !overWindow.isRows)
      .item(
        "select", aggregationToString(
          inputType,
          getRowType,
          namedAggregates))
  }

  override def translateToPlan(tableEnv: StreamTableEnvironment): DataStream[Row] = {
    if (logicWindow.groups.size > 1) {
      throw new TableException(
        "Unsupported use of OVER windows. All aggregates must be computed on the same window.")
    }

    val overWindow: org.apache.calcite.rel.core.Window.Group = logicWindow.groups.get(0)

    val inputDS = input.asInstanceOf[DataStreamRel].translateToPlan(tableEnv)

    if (overWindow.orderKeys.getFieldCollations.size() != 1) {
      throw new TableException(
        "Unsupported use of OVER windows. The window may only be ordered by a single time column.")
    }

    val timeType = inputType
      .getFieldList
      .get(overWindow.orderKeys.getFieldCollations.get(0).getFieldIndex)
      .getValue

    timeType match {
      case _: ProcTimeType =>
        // both ROWS and RANGE clause with UNBOUNDED PRECEDING and CURRENT ROW condition.
        if (overWindow.lowerBound.isUnbounded &&
          overWindow.upperBound.isCurrentRow) {
          createUnboundedAndCurrentRowProcessingTimeOverWindow(inputDS)
        } else {
          throw new TableException(
              "OVER window only support ProcessingTime UNBOUNDED PRECEDING and CURRENT ROW " +
              "condition.")
        }
      case _: RowTimeType =>
        throw new TableException("OVER Window of the EventTime type is not currently supported.")
      case _ =>
        throw new TableException(s"Unsupported time type {$timeType}")
    }

  }

  def createUnboundedAndCurrentRowProcessingTimeOverWindow(
    inputDS: DataStream[Row]): DataStream[Row]  = {

    val overWindow: Group = logicWindow.groups.get(0)
    val partitionKeys: Array[Int] = overWindow.keys.toArray
    val namedAggregates: Seq[CalcitePair[AggregateCall, String]] = generateNamedAggregates

    // get the output types
    val rowTypeInfo = FlinkTypeFactory.toInternalRowTypeInfo(getRowType).asInstanceOf[RowTypeInfo]

    val result: DataStream[Row] =
        // partitioned aggregation
        if (partitionKeys.nonEmpty) {
          val processFunction = AggregateUtil.CreateUnboundedProcessingOverProcessFunction(
            namedAggregates,
            inputType)

          inputDS
          .keyBy(partitionKeys: _*)
          .process(processFunction)
          .returns(rowTypeInfo)
          .name(aggOpName)
          .asInstanceOf[DataStream[Row]]
        }
        // non-partitioned aggregation
        else {
          val processFunction = AggregateUtil.CreateUnboundedProcessingOverProcessFunction(
            namedAggregates,
            inputType,
            false)

          inputDS
            .process(processFunction).setParallelism(1).setMaxParallelism(1)
            .returns(rowTypeInfo)
            .name(aggOpName)
            .asInstanceOf[DataStream[Row]]
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
      s"${windowRange(overWindow)}, " +
      s"select: (${
        aggregationToString(
          inputType,
          getRowType,
          namedAggregates)
      }))"
  }

}

