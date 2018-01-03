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
import org.apache.calcite.rel.RelFieldCollation.Direction.ASCENDING
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.Window.Group
import org.apache.calcite.rel.core.{AggregateCall, Window}
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.rex.RexLiteral
import org.apache.flink.api.java.functions.NullByteKeySelector
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.{StreamQueryConfig, StreamTableEnvironment, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.AggregationCodeGenerator
import org.apache.flink.table.plan.nodes.OverAggregate
import org.apache.flink.table.plan.rules.datastream.DataStreamRetractionRules
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.runtime.CRowKeySelector
import org.apache.flink.table.runtime.aggregate.AggregateUtil.CalcitePair
import org.apache.flink.table.runtime.aggregate._
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
import org.apache.flink.table.util.Logging

import scala.collection.JavaConverters._

class DataStreamOverAggregate(
    logicWindow: Window,
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    schema: RowSchema,
    inputSchema: RowSchema)
  extends SingleRel(cluster, traitSet, inputNode)
  with OverAggregate
  with DataStreamRel
  with Logging {

  override def deriveRowType(): RelDataType = schema.relDataType

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
    val constants: Seq[RexLiteral] = logicWindow.constants.asScala
    val partitionKeys: Array[Int] = overWindow.keys.toArray
    val namedAggregates: Seq[CalcitePair[AggregateCall, String]] = generateNamedAggregates

    super.explainTerms(pw)
      .itemIf("partitionBy",
        partitionToString(schema.relDataType, partitionKeys), partitionKeys.nonEmpty)
      .item("orderBy",
        orderingToString(schema.relDataType, overWindow.orderKeys.getFieldCollations))
      .itemIf("rows", windowRange(logicWindow, overWindow, inputNode), overWindow.isRows)
      .itemIf("range", windowRange(logicWindow, overWindow, inputNode), !overWindow.isRows)
      .item(
        "select", aggregationToString(
          inputSchema.relDataType,
          constants,
          schema.relDataType,
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

    val inputIsAccRetract = DataStreamRetractionRules.isAccRetract(input)

    if (inputIsAccRetract) {
      throw new TableException(
        "Retraction on Over window aggregation is not supported yet. " +
        "Note: Over window aggregation should not follow a non-windowed GroupBy aggregation.")
    }

    if (!logicWindow.groups.get(0).keys.isEmpty && queryConfig.getMinIdleStateRetentionTime < 0) {
      LOG.warn(
        "No state retention interval configured for a query which accumulates state. " +
        "Please provide a query configuration with valid retention interval to prevent " +
        "excessive state size. You may specify a retention time of 0 to not clean up the state.")
    }

    val constants: Seq[RexLiteral] = logicWindow.constants.asScala

    val generator = new AggregationCodeGenerator(
      tableEnv.getConfig,
      false,
      inputSchema.typeInfo,
      Some(constants))

    val constantTypes = constants.map(_.getType)
    val fieldTypes = input.getRowType.getFieldList.asScala.map(_.getType)
    val aggInTypes = fieldTypes ++ constantTypes
    val aggInNames = aggInTypes.indices.map("f" + _)

    val aggregateInputType =
      getCluster.getTypeFactory.createStructType(aggInTypes.asJava, aggInNames.asJava)

    val timeType = schema.relDataType
      .getFieldList
      .get(orderKey.getFieldIndex)
      .getType

    // identify window rowtime attribute
    val rowTimeIdx: Option[Int] = if (FlinkTypeFactory.isRowtimeIndicatorType(timeType)) {
      Some(orderKey.getFieldIndex)
    } else if (FlinkTypeFactory.isProctimeIndicatorType(timeType)) {
      None
    } else {
      throw new TableException(s"OVER windows can only be applied on time attributes.")
    }

    if (overWindow.lowerBound.isPreceding && overWindow.lowerBound.isUnbounded &&
        overWindow.upperBound.isCurrentRow) {
      // unbounded OVER window
      createUnboundedAndCurrentRowOverWindow(
        queryConfig,
        generator,
        inputDS,
        rowTimeIdx,
        aggregateInputType,
        isRowsClause = overWindow.isRows)
    } else if (
      overWindow.lowerBound.isPreceding && !overWindow.lowerBound.isUnbounded &&
        overWindow.upperBound.isCurrentRow) {

      // bounded OVER window
      createBoundedAndCurrentRowOverWindow(
        queryConfig,
        generator,
        inputDS,
        rowTimeIdx,
        aggregateInputType,
        isRowsClause = overWindow.isRows)
    } else {
      throw new TableException("OVER RANGE FOLLOWING windows are not supported yet.")
    }
  }

  def createUnboundedAndCurrentRowOverWindow(
    queryConfig: StreamQueryConfig,
    generator: AggregationCodeGenerator,
    inputDS: DataStream[CRow],
    rowTimeIdx: Option[Int],
    aggregateInputType: RelDataType,
    isRowsClause: Boolean): DataStream[CRow] = {

    val overWindow: Group = logicWindow.groups.get(0)

    val partitionKeys: Array[Int] = overWindow.keys.toArray

    val namedAggregates: Seq[CalcitePair[AggregateCall, String]] = generateNamedAggregates

    // get the output types
    val returnTypeInfo = CRowTypeInfo(schema.typeInfo)

    val processFunction = AggregateUtil.createUnboundedOverProcessFunction(
      generator,
      namedAggregates,
      aggregateInputType,
      inputSchema.relDataType,
      inputSchema.typeInfo,
      inputSchema.fieldTypeInfos,
      queryConfig,
      rowTimeIdx,
      partitionKeys.nonEmpty,
      isRowsClause)

    val result: DataStream[CRow] =
    // partitioned aggregation
      if (partitionKeys.nonEmpty) {
        inputDS
          .keyBy(new CRowKeySelector(partitionKeys, inputSchema.projectedTypeInfo(partitionKeys)))
          .process(processFunction)
          .returns(returnTypeInfo)
          .name(aggOpName)
          .asInstanceOf[DataStream[CRow]]
      }
      // non-partitioned aggregation
      else {
        inputDS.keyBy(new NullByteKeySelector[CRow])
          .process(processFunction).setParallelism(1).setMaxParallelism(1)
          .returns(returnTypeInfo)
          .name(aggOpName)
      }
    result
  }

  def createBoundedAndCurrentRowOverWindow(
    queryConfig: StreamQueryConfig,
    generator: AggregationCodeGenerator,
    inputDS: DataStream[CRow],
    rowTimeIdx: Option[Int],
    aggregateInputType: RelDataType,
    isRowsClause: Boolean): DataStream[CRow] = {

    val overWindow: Group = logicWindow.groups.get(0)

    val partitionKeys: Array[Int] = overWindow.keys.toArray

    val namedAggregates: Seq[CalcitePair[AggregateCall, String]] = generateNamedAggregates

    val precedingOffset =
      getLowerBoundary(logicWindow, overWindow, getInput()) + (if (isRowsClause) 1 else 0)

    // get the output types
    val returnTypeInfo = CRowTypeInfo(schema.typeInfo)

    val processFunction = AggregateUtil.createBoundedOverProcessFunction(
      generator,
      namedAggregates,
      aggregateInputType,
      inputSchema.relDataType,
      inputSchema.typeInfo,
      inputSchema.fieldTypeInfos,
      precedingOffset,
      queryConfig,
      isRowsClause,
      rowTimeIdx
    )
    val result: DataStream[CRow] =
    // partitioned aggregation
      if (partitionKeys.nonEmpty) {
        inputDS
          .keyBy(new CRowKeySelector(partitionKeys, inputSchema.projectedTypeInfo(partitionKeys)))
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
    val constants: Seq[RexLiteral] = logicWindow.constants.asScala
    val partitionKeys: Array[Int] = overWindow.keys.toArray
    val namedAggregates: Seq[CalcitePair[AggregateCall, String]] = generateNamedAggregates

    s"over: (${
      if (!partitionKeys.isEmpty) {
        s"PARTITION BY: ${partitionToString(inputSchema.relDataType, partitionKeys)}, "
      } else {
        ""
      }
    }ORDER BY: ${orderingToString(inputSchema.relDataType,
        overWindow.orderKeys.getFieldCollations)}, " +
      s"${if (overWindow.isRows) "ROWS" else "RANGE"}" +
      s"${windowRange(logicWindow, overWindow, inputNode)}, " +
      s"select: (${
        aggregationToString(
          inputSchema.relDataType,
          constants,
          schema.relDataType,
          namedAggregates)
      }))"
  }

}

