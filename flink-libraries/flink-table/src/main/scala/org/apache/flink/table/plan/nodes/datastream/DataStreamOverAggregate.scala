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
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFieldImpl}
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.{StreamTableEnvironment, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.runtime.aggregate.AggregateUtil.{CalcitePair, _}
import org.apache.flink.table.runtime.aggregate._
import org.apache.flink.table.plan.nodes.CommonAggregate
import org.apache.flink.types.Row
import org.apache.calcite.rel.core.Window
import org.apache.calcite.rel.core.Window.Group
import java.util.{List => JList}

import org.apache.flink.table.functions.{ProcTimeType, RowTime, RowTimeType, TimeModeType}

import scala.collection.JavaConverters._
import scala.collection.immutable.IndexedSeq

class DataStreamOverAggregate(
    logicWindow: Window,
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    rowRelDataType: RelDataType,
    inputType: RelDataType)
  extends SingleRel(cluster, traitSet, inputNode)
  with CommonAggregate
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
    val (
      overWindow: Group,
      partitionKeys: Array[Int],
      namedAggregates: IndexedSeq[CalcitePair[AggregateCall, String]]
      ) = genPartitionKeysAndNamedAggregates

    s"Aggregate(${
      if (!partitionKeys.isEmpty) {
        s"partitionBy: (${groupingToString(inputType, partitionKeys)}), "
      } else {
        ""
      }
    }window: ($overWindow), " +
      s"select: (${
        aggregationToString(
          inputType,
          partitionKeys,
          getRowType,
          namedAggregates,
          Seq())
      }))"
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val (
      overWindow: Group,
      partitionKeys: Array[Int],
      namedAggregates: IndexedSeq[CalcitePair[AggregateCall, String]]
      ) = genPartitionKeysAndNamedAggregates

    super.explainTerms(pw)
      .itemIf("partitionBy", groupingToString(inputType, partitionKeys), !partitionKeys.isEmpty)
      .item("overWindow", overWindow)
      .item(
        "select", aggregationToString(
          inputType,
          partitionKeys,
          getRowType,
          namedAggregates,
          Seq()))
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
        "Unsupported use of OVER windows. All aggregates must be ordered on a time mode column.")
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

    val (
      overWindow: Group,
      partitionKeys: Array[Int],
      namedAggregates: IndexedSeq[CalcitePair[AggregateCall, String]]
      ) = genPartitionKeysAndNamedAggregates

    val inputIndicies = (0 until inputType.getFieldCount).toArray

    // get the output types
    val fieldTypes: Array[TypeInformation[_]] = getRowType
      .getFieldList.asScala
      .map(field => FlinkTypeFactory.toTypeInfo(field.getType)).toArray

    val rowTypeInfo = new RowTypeInfo(fieldTypes: _*)

    val aggString = aggregationToString(
      inputType,
      inputIndicies,
      getRowType,
      namedAggregates,
      Seq())

    val prepareOpName = s"prepare select: ($aggString)"
    val keyedAggOpName = s"partitionBy: (${groupingToString(inputType, partitionKeys)}), " +
      s"overWindow: ($overWindow), " +
      s"select: ($aggString)"

    val mapFunction = AggregateUtil.createPrepareMapFunction(
      namedAggregates,
      inputIndicies,
      inputType)

    val mappedInput = inputDS.map(mapFunction).name(prepareOpName)

    val result: DataStream[Row] =
    // check whether all aggregates support partial aggregate
      if (AggregateUtil.doAllSupportPartialAggregation(
        namedAggregates.map(_.getKey),
        inputType,
        partitionKeys.length)) {

        // partitioned aggregation
        if (partitionKeys.length > 0) {

          val processFunction = AggregateUtil.CreateUnboundedProcessingOverProcessFunction(
            namedAggregates,
            inputType,
            getRowType,
            inputIndicies)

          mappedInput
          .keyBy(partitionKeys: _*)
          .process(processFunction)
          .returns(rowTypeInfo)
          .name(keyedAggOpName)
          .asInstanceOf[DataStream[Row]]
        }
        // global non-partitioned aggregation
        else {
          throw TableException("non-partitioned over Aggregation is currently not supported...")
        }
      }
      else {
        throw TableException("non-Incremental over Aggregation is currently not supported...")
      }
    result
  }

  private def genPartitionKeysAndNamedAggregates = {
    val overWindow: Group = logicWindow.groups.get(0)
    val partitionKeys = overWindow.keys.toArray

    val aggregateCalls = overWindow.getAggregateCalls(logicWindow)
    val namedAggregates = for (i <- 0 until aggregateCalls.size())
      yield new CalcitePair[AggregateCall, String](aggregateCalls.get(i), "w0$o" + i)
    (overWindow, partitionKeys, namedAggregates)
  }
}

