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

import java.util.{ List => JList }

import org.apache.flink.table.runtime.aggregate.AggregateUtil.CalcitePair
import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.RelWriter
import org.apache.calcite.rel.SingleRel
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.core.Window
import org.apache.calcite.rel.core.Window.Group
import org.apache.calcite.sql.`type`.BasicSqlType
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.StreamTableEnvironment
import org.apache.flink.table.api.TableException
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.functions.ProcTimeType
import org.apache.flink.table.functions.RowTimeType
import org.apache.flink.table.plan.nodes.OverAggregate
import org.apache.flink.table.runtime.aggregate.AggregateUtil
import org.apache.flink.types.Row
import org.apache.calcite.sql.`type`.IntervalSqlType
import org.apache.calcite.rex.RexInputRef
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.util.Collector
import java.util.concurrent.TimeUnit
import org.apache.calcite.plan.{RelOptCluster, RelTraitSet} 
import org.apache.flink.api.java.functions.NullByteKeySelector




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
    val fieldCount = input.getRowType.getFieldCount
 
    if(overWindow.lowerBound.isUnbounded){
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
    else {
      super.explainTerms(pw)
      .itemIf("partitionBy", partitionToString(inputType, partitionKeys), partitionKeys.nonEmpty)
      .item("orderBy",orderingToString(inputType, overWindow.orderKeys.getFieldCollations))
      .itemIf("rows", windowRange(overWindow), overWindow.isRows)
      .itemIf("range", windowRangeValue(overWindow,logicWindow,fieldCount), !overWindow.isRows)
      .item(
        "select", aggregationToString(
          inputType,
          getRowType,
          namedAggregates))
    }
      
    
    
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
        } else if (overWindow.lowerBound.isPreceding && !overWindow.lowerBound.isUnbounded && 
             overWindow.upperBound.isCurrentRow && // until current row
             !overWindow.isRows){
          createTimeBoundedProcessingTimeOverWindow(inputDS)
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

  def createTimeBoundedProcessingTimeOverWindow(inputDS: DataStream[Row]): DataStream[Row] = {

    val overWindow: Group = logicWindow.groups.get(0)
    val partitionKeys: Array[Int] = overWindow.keys.toArray
    val namedAggregates: Seq[CalcitePair[AggregateCall, String]] = generateNamedAggregates

    val index = overWindow.lowerBound.getOffset.asInstanceOf[RexInputRef].getIndex
    val count = input.getRowType.getFieldCount
    val lowerBoundIndex = index - count
    
    
    val timeBoundary = logicWindow.constants.get(lowerBoundIndex).getValue2 match {
      case bd: java.math.BigDecimal => bd.longValue()
      case _ => throw new TableException("OVER Window boundaries must be numeric")
    }

     // get the output types
    val rowTypeInfo = FlinkTypeFactory.toInternalRowTypeInfo(getRowType).asInstanceOf[RowTypeInfo]
         
    val result: DataStream[Row] =
        // partitioned aggregation
        if (partitionKeys.nonEmpty) {
          
          val processFunction = AggregateUtil.createTimeBoundedProcessingOverProcessFunction(
            namedAggregates,
            inputType,
            timeBoundary)
          
          inputDS
          .keyBy(partitionKeys: _*)
          .process(processFunction)
          .returns(rowTypeInfo)
          .name(aggOpName)
          .asInstanceOf[DataStream[Row]]
        } else { // non-partitioned aggregation
          val processFunction = AggregateUtil.createTimeBoundedProcessingOverProcessFunction(
            namedAggregates,
            inputType,
            timeBoundary,
            false)
          
          inputDS
            .keyBy(new NullByteKeySelector[Row])
            .process(processFunction).setParallelism(1).setMaxParallelism(1)
            .returns(rowTypeInfo)
            .name(aggOpName)
            .asInstanceOf[DataStream[Row]]
        }
    result         
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

