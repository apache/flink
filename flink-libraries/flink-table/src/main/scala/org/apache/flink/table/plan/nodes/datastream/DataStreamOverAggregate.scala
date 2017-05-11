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
import org.apache.calcite.rel.core.Window.Group
import org.apache.calcite.rel.core.{AggregateCall, Window}
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.rel.RelFieldCollation.Direction.ASCENDING
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.{StreamQueryConfig, StreamTableEnvironment, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.nodes.OverAggregate
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.runtime.aggregate._
import org.apache.flink.api.java.functions.NullByteKeySelector
import org.apache.flink.table.codegen.CodeGenerator
import org.apache.flink.table.plan.rules.datastream.DataStreamRetractionRules
import org.apache.flink.table.runtime.aggregate.AggregateUtil.CalcitePair
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
import org.slf4j.LoggerFactory

class DataStreamOverAggregate(
    logicWindow: Window,
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    schema: RowSchema,
    inputSchema: RowSchema)
  extends SingleRel(cluster, traitSet, inputNode)
  with OverAggregate
  with DataStreamRel {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  override def deriveRowType(): RelDataType = schema.logicalType

  override def needsUpdatesAsRetraction = true

  override def consumesRetractions = true

  override def copy(traitSet: RelTraitSet, inputs: JList[RelNode]): RelNode = {
    new DataStreamOverAggregate(
      logicWindow,
      cluster,
      traitSet,
      inputs.get(0),
      schema,
      inputSchema)
  }

  override def toString: String = {
    s"OverAggregate($aggOpName)"
  }

  override def explainTerms(pw: RelWriter): RelWriter = {

    val overWindow: Group = logicWindow.groups.get(0)
    val partitionKeys: Array[Int] = overWindow.keys.toArray
    val namedAggregates: Seq[CalcitePair[AggregateCall, String]] = generateNamedAggregates

    super.explainTerms(pw)
      .itemIf("partitionBy",
        partitionToString(schema.logicalType, partitionKeys), partitionKeys.nonEmpty)
      .item("orderBy",
        orderingToString(schema.logicalType, overWindow.orderKeys.getFieldCollations))
      .itemIf("rows", windowRange(logicWindow, overWindow, inputNode), overWindow.isRows)
      .itemIf("range", windowRange(logicWindow, overWindow, inputNode), !overWindow.isRows)
      .item(
        "select", aggregationToString(
          inputSchema.logicalType,
          schema.logicalType,
          namedAggregates))
  }

  override def translateToPlan(
      tableEnv: StreamTableEnvironment,
      queryConfig: StreamQueryConfig): DataStream[CRow] = {

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

    val inputDS = input.asInstanceOf[DataStreamRel].translateToPlan(tableEnv, queryConfig)

    val consumeRetraction = DataStreamRetractionRules.isAccRetract(input)

    if (consumeRetraction) {
      throw new TableException(
        "Retraction on Over window aggregation is not supported yet. " +
        "Note: Over window aggregation should not follow a non-windowed GroupBy aggregation.")
    }

    if (overWindow.lowerBound.isUnbounded) {
      if (queryConfig.getMinIdleStateRetentionTime < 0
        || queryConfig.getMaxIdleStateRetentionTime < 0) {
        LOG.warn(
          "No state retention interval configured for a query which accumulates state. " +
          "Please provide a query configuration with valid retention interval to prevent " +
          "excessive state size. You may specify a retention time of 0 to not clean up the state.")
      }
    }

    val generator = new CodeGenerator(
      tableEnv.getConfig,
      false,
      inputDS.getType)

    val timeType = schema.logicalType
      .getFieldList
      .get(orderKey.getFieldIndex)
      .getType

    timeType match {
      case _ if FlinkTypeFactory.isProctimeIndicatorType(timeType)  =>
        // proc-time OVER window
        if (overWindow.lowerBound.isUnbounded && overWindow.upperBound.isCurrentRow) {
          // unbounded OVER window
          createUnboundedAndCurrentRowOverWindow(
            queryConfig,
            generator,
            inputDS,
            isRowTimeType = false,
            isRowsClause = overWindow.isRows)
        } else if (
          overWindow.lowerBound.isPreceding && !overWindow.lowerBound.isUnbounded &&
            overWindow.upperBound.isCurrentRow) {

          // bounded OVER window
          createBoundedAndCurrentRowOverWindow(
            queryConfig,
            generator,
            inputDS,
            isRowTimeType = false,
            isRowsClause = overWindow.isRows)
        } else {
          throw new TableException(
            "OVER RANGE FOLLOWING windows are not supported yet.")
        }

      case _ if FlinkTypeFactory.isRowtimeIndicatorType(timeType) =>
        // row-time OVER window
        if (overWindow.lowerBound.isPreceding &&
          overWindow.lowerBound.isUnbounded && overWindow.upperBound.isCurrentRow) {
          // unbounded OVER window
          createUnboundedAndCurrentRowOverWindow(
            queryConfig,
            generator,
            inputDS,
            isRowTimeType = true,
            isRowsClause = overWindow.isRows)
        } else if (overWindow.lowerBound.isPreceding && overWindow.upperBound.isCurrentRow) {
          // bounded OVER window
          createBoundedAndCurrentRowOverWindow(
            queryConfig,
            generator,
            inputDS,
            isRowTimeType = true,
            isRowsClause = overWindow.isRows)
        } else {
          throw new TableException(
            "OVER RANGE FOLLOWING windows are not supported yet.")
        }

      case _ =>
        throw new TableException(
          s"OVER windows can only be applied on time attributes.")
    }
  }

  def createUnboundedAndCurrentRowOverWindow(
    queryConfig: StreamQueryConfig,
    generator: CodeGenerator,
    inputDS: DataStream[CRow],
    isRowTimeType: Boolean,
    isRowsClause: Boolean): DataStream[CRow] = {



    val overWindow: Group = logicWindow.groups.get(0)

    val partitionKeys: Array[Int] = overWindow.keys.toArray.map(schema.mapIndex)

    val namedAggregates: Seq[CalcitePair[AggregateCall, String]] = generateNamedAggregates.map {
      namedAggregate =>
        new CalcitePair[AggregateCall, String](
          schema.mapAggregateCall(namedAggregate.left),
          namedAggregate.right)
    }

    // get the output types
    val returnTypeInfo = CRowTypeInfo(schema.physicalTypeInfo)

    val processFunction = AggregateUtil.createUnboundedOverProcessFunction(
      generator,
      namedAggregates,
      inputSchema.physicalType,
      inputSchema.physicalTypeInfo,
      inputSchema.physicalFieldTypeInfo,
      queryConfig,
      isRowTimeType,
      partitionKeys.nonEmpty,
      isRowsClause)

    val result: DataStream[CRow] =
    // partitioned aggregation
      if (partitionKeys.nonEmpty) {
        inputDS
          .keyBy(partitionKeys: _*)
          .process(processFunction)
          .returns(returnTypeInfo)
          .name(aggOpName)
          .asInstanceOf[DataStream[CRow]]
      }
      // non-partitioned aggregation
      else {
        if (isRowTimeType) {
          inputDS.keyBy(new NullByteKeySelector[CRow])
            .process(processFunction).setParallelism(1).setMaxParallelism(1)
            .returns(returnTypeInfo)
            .name(aggOpName)
        } else {
          inputDS
            .process(processFunction).setParallelism(1).setMaxParallelism(1)
            .returns(returnTypeInfo)
            .name(aggOpName)
        }
      }
    result
  }

  def createBoundedAndCurrentRowOverWindow(
    queryConfig: StreamQueryConfig,
    generator: CodeGenerator,
    inputDS: DataStream[CRow],
    isRowTimeType: Boolean,
    isRowsClause: Boolean): DataStream[CRow] = {

    val overWindow: Group = logicWindow.groups.get(0)
    val partitionKeys: Array[Int] = overWindow.keys.toArray.map(schema.mapIndex)
    val namedAggregates: Seq[CalcitePair[AggregateCall, String]] = generateNamedAggregates.map {
      namedAggregate =>
        new CalcitePair[AggregateCall, String](
          schema.mapAggregateCall(namedAggregate.left),
          namedAggregate.right)
    }

    val precedingOffset =
      getLowerBoundary(logicWindow, overWindow, getInput()) + (if (isRowsClause) 1 else 0)

    // get the output types
    val returnTypeInfo = CRowTypeInfo(schema.physicalTypeInfo)

    val processFunction = AggregateUtil.createBoundedOverProcessFunction(
      generator,
      namedAggregates,
      inputSchema.physicalType,
      inputSchema.physicalTypeInfo,
      inputSchema.physicalFieldTypeInfo,
      precedingOffset,
      queryConfig,
      isRowsClause,
      isRowTimeType
    )
    val result: DataStream[CRow] =
    // partitioned aggregation
      if (partitionKeys.nonEmpty) {
        inputDS
          .keyBy(partitionKeys: _*)
          .process(processFunction)
          .returns(returnTypeInfo)
          .name(aggOpName)
      }
      // non-partitioned aggregation
      else {
        inputDS
          .keyBy(new NullByteKeySelector[CRow])
          .process(processFunction).setParallelism(1).setMaxParallelism(1)
          .returns(returnTypeInfo)
          .name(aggOpName)
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
        s"PARTITION BY: ${partitionToString(inputSchema.logicalType, partitionKeys)}, "
      } else {
        ""
      }
    }ORDER BY: ${orderingToString(inputSchema.logicalType,
        overWindow.orderKeys.getFieldCollations)}, " +
      s"${if (overWindow.isRows) "ROWS" else "RANGE"}" +
      s"${windowRange(logicWindow, overWindow, inputNode)}, " +
      s"select: (${
        aggregationToString(
          inputSchema.logicalType,
          schema.logicalType,
          namedAggregates)
      }))"
  }

}

