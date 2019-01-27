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
package org.apache.flink.table.plan.nodes.physical.stream

import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import org.apache.flink.table.api.{StreamTableEnvironment, TableConfig, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.CodeGeneratorContext
import org.apache.flink.table.codegen.agg.AggsHandlerCodeGenerator
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.errorcode.TableErrors
import org.apache.flink.table.plan.nodes.exec.RowStreamExecNode
import org.apache.flink.table.plan.nodes.physical.FlinkPhysicalRel
import org.apache.flink.table.plan.rules.physical.stream.StreamExecRetractionRules
import org.apache.flink.table.plan.schema.BaseRowSchema
import org.apache.flink.table.plan.util.AggregateUtil.{CalcitePair, transformToStreamAggregateInfoList}
import org.apache.flink.table.plan.util.{OverAggregateUtil, StreamExecUtil}
import org.apache.flink.table.runtime.KeyedProcessOperator
import org.apache.flink.table.runtime.aggregate._
import org.apache.flink.table.runtime.functions.ProcessFunction
import org.apache.flink.table.typeutils.BaseRowTypeInfo

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.RelFieldCollation.Direction.ASCENDING
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.Window.Group
import org.apache.calcite.rel.core.{AggregateCall, Window}
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.rex.RexLiteral
import org.apache.calcite.tools.RelBuilder

import java.util.{List => JList}

import scala.collection.JavaConverters._

class StreamExecOverAggregate(
    logicWindow: Window,
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    outputSchema: BaseRowSchema,
    inputSchema: BaseRowSchema)
  extends SingleRel(cluster, traitSet, inputNode)
  with StreamPhysicalRel
  with RowStreamExecNode {

  override def needsUpdatesAsRetraction(input: RelNode) = true

  override def consumesRetractions = true

  override def deriveRowType(): RelDataType = outputSchema.relDataType

  override def copy(traitSet: RelTraitSet, inputs: JList[RelNode]): RelNode = {
    new StreamExecOverAggregate(
      logicWindow,
      cluster,
      traitSet,
      inputs.get(0),
      outputSchema,
      inputSchema)
  }

  override def estimateRowCount(mq: RelMetadataQuery): Double = {
    // over window: one input at least one output (do not introduce retract amplification)
    mq.getRowCount(getInput)
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    // by default, assume cost is proportional to number of rows
    val rowCnt: Double = estimateRowCount(mq)
    val count = (getRowType.getFieldCount - 1) * 1.0 / inputNode.getRowType.getFieldCount
    planner.getCostFactory.makeCost(rowCnt, rowCnt * count, 0)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {

    val overWindow: Group = logicWindow.groups.get(0)
    val constants: Seq[RexLiteral] = logicWindow.constants.asScala
    val partitionKeys: Array[Int] = overWindow.keys.toArray
    val namedAggregates: Seq[CalcitePair[AggregateCall, String]] = generateNamedAggregates

    super.explainTerms(pw)
      .itemIf("partitionBy", OverAggregateUtil.partitionToString(
        outputSchema.relDataType, partitionKeys), partitionKeys.nonEmpty)
      .item("orderBy",
        OverAggregateUtil.orderingToString(
          outputSchema.relDataType, overWindow.orderKeys.getFieldCollations))
      .item("window", OverAggregateUtil.windowRangeToString(logicWindow, overWindow))
      .item(
        "select", OverAggregateUtil.aggregationToString(
          inputSchema.relDataType,
          constants,
          outputSchema.relDataType,
          namedAggregates))
  }

  private def generateNamedAggregates: Seq[CalcitePair[AggregateCall, String]] = {
    val overWindow: Group = logicWindow.groups.get(0)

    val aggregateCalls = overWindow.getAggregateCalls(logicWindow)
    for (i <- 0 until aggregateCalls.size())
      yield new CalcitePair[AggregateCall, String](aggregateCalls.get(i), "w0$o" + i)
  }

  override def isDeterministic: Boolean = OverAggregateUtil.isDeterministic(logicWindow.groups)

  //~ ExecNode methods -----------------------------------------------------------

  override def getFlinkPhysicalRel: FlinkPhysicalRel = this

  override def translateToPlanInternal(
      tableEnv: StreamTableEnvironment): StreamTransformation[BaseRow] = {
    val tableConfig = tableEnv.getConfig

    if (logicWindow.groups.size > 1) {
      throw new TableException(
        TableErrors.INST.sqlOverAggInvalidUseOfOverWindow(
          "All aggregates must be computed on the same window."))
    }

    val overWindow: org.apache.calcite.rel.core.Window.Group = logicWindow.groups.get(0)

    val orderKeys = overWindow.orderKeys.getFieldCollations

    if (orderKeys.size() != 1) {
      throw new TableException(
        TableErrors.INST.sqlOverAggInvalidUseOfOverWindow(
          "The window can only be ordered by a single time column."))
    }
    val orderKey = orderKeys.get(0)

    if (!orderKey.direction.equals(ASCENDING)) {
      throw new TableException(
        TableErrors.INST.sqlOverAggInvalidUseOfOverWindow(
          "The window can only be ordered in ASCENDING mode."))
    }

    val inputDS = getInputNodes.get(0).translateToPlan(tableEnv)
      .asInstanceOf[StreamTransformation[BaseRow]]

    val inputIsAccRetract = StreamExecRetractionRules.isAccRetract(input)

    if (inputIsAccRetract) {
      throw new TableException(
        TableErrors.INST.sqlOverAggInvalidUseOfOverWindow(
          "Retraction on Over window aggregation is not supported yet. " +
            "Note: Over window aggregation should not follow a non-windowed GroupBy aggregation."))
    }

    if (!logicWindow.groups.get(0).keys.isEmpty && tableConfig.getMinIdleStateRetentionTime < 0) {
      LOG.warn(
        "No state retention interval configured for a query which accumulates state. " +
        "Please provide a query configuration with valid retention interval to prevent " +
        "excessive state size. You may specify a retention time of 0 to not clean up the state.")
    }

    val timeType = outputSchema.fieldTypeInfos(orderKey.getFieldIndex)

    // check time field
    if (!FlinkTypeFactory.isRowtimeIndicatorType(timeType)
        && !FlinkTypeFactory.isProctimeIndicatorType(timeType)) {
      throw new TableException(
        "OVER windows' ordering in stream mode must be defined on a time attribute.")
    }

    // identify window rowtime attribute
    val rowTimeIdx: Option[Int] = if (FlinkTypeFactory.isRowtimeIndicatorType(timeType)) {
      Some(orderKey.getFieldIndex)
    } else if (FlinkTypeFactory.isProctimeIndicatorType(timeType)) {
      None
    } else {
      throw new TableException(
        TableErrors.INST.sqlOverAggInvalidUseOfOverWindow(
          "OVER windows can only be applied on time attributes."))
    }

    val config = tableEnv.getConfig
    val codeGenCtx = CodeGeneratorContext(config, supportReference = true)
    val aggregateCalls = logicWindow.groups.get(0).getAggregateCalls(logicWindow).asScala
    val isRowsClause = overWindow.isRows
    val constants = logicWindow.constants.asScala
    val constantTypes = constants.map(c => FlinkTypeFactory.toInternalType(c.getType))
    val aggInputType = tableEnv.getTypeFactory.buildRelDataType(
      inputSchema.fieldNames ++ constants.indices.map(i => "TMP" + i),
      inputSchema.fieldTypes ++ constantTypes)

    val overProcessFunction = if (overWindow.lowerBound.isPreceding
      && overWindow.lowerBound.isUnbounded
      && overWindow.upperBound.isCurrentRow) {

      // unbounded OVER window
      createUnboundedOverProcessFunction(
        codeGenCtx,
        aggregateCalls,
        constants,
        aggInputType,
        rowTimeIdx,
        isRowsClause,
        tableConfig,
        tableEnv.getRelBuilder,
        config.getNullCheck)

    } else if (overWindow.lowerBound.isPreceding
      && !overWindow.lowerBound.isUnbounded
      && overWindow.upperBound.isCurrentRow) {

      val boundValue = OverAggregateUtil.getBoundary(logicWindow, overWindow.lowerBound)

      if (boundValue.isInstanceOf[BigDecimal]) {
        throw new TableException(
          TableErrors.INST.sqlOverAggInvalidUseOfOverWindow(
            "the specific value is decimal which haven not supported yet."))
      }
      // bounded OVER window
      val precedingOffset = -1 * boundValue.asInstanceOf[Long] + (if (isRowsClause) 1 else 0)

      createBoundedOverProcessFunction(
        codeGenCtx,
        aggregateCalls,
        constants,
        aggInputType,
        rowTimeIdx,
        isRowsClause,
        precedingOffset,
        tableConfig,
        tableEnv.getRelBuilder,
        config.getNullCheck)

    } else {
      throw new TableException(
        TableErrors.INST.sqlOverAggInvalidUseOfOverWindow(
          "OVER RANGE FOLLOWING windows are not supported yet."))
    }

    val partitionKeys: Array[Int] = overWindow.keys.toArray
    val inputTypeInfo = inputSchema.typeInfo()

    val selector = StreamExecUtil.getKeySelector(partitionKeys, inputTypeInfo)

    val returnTypeInfo = outputSchema.typeInfo()
      .asInstanceOf[BaseRowTypeInfo]
    // partitioned aggregation

    val operator = new KeyedProcessOperator(overProcessFunction)
    operator.setRequireState(true)
    val ret = new OneInputTransformation(
      inputDS,
      getOperatorName,
      operator,
      returnTypeInfo,
      inputDS.getParallelism)

    if (partitionKeys.isEmpty) {
      ret.setParallelism(1)
      ret.setMaxParallelism(1)
    }
    ret.setResources(getResource.getReservedResourceSpec,
      getResource.getPreferResourceSpec)

    // set KeyType and Selector for state
    ret.setStateKeySelector(selector)
    ret.setStateKeyType(selector.getProducedType)
    ret
  }


  /**
    * Create an ProcessFunction for unbounded OVER window to evaluate final aggregate value.
    *
    * @param ctx            code generator context
    * @param aggregateCalls physical calls to aggregate functions and their output field names
    * @param constants      the constants in aggregates parameters, such as sum(1)
    * @param aggInputType   physical type of the input row which consist of input and constants.
    * @param rowTimeIdx     the index of the rowtime field or None in case of processing time.
    * @param isRowsClause   it is a tag that indicates whether the OVER clause is ROWS clause
    */
  private def createUnboundedOverProcessFunction(
      ctx: CodeGeneratorContext,
      aggregateCalls: Seq[AggregateCall],
      constants: Seq[RexLiteral],
      aggInputType: RelDataType,
      rowTimeIdx: Option[Int],
      isRowsClause: Boolean,
      tableConfig: TableConfig,
      relBuilder: RelBuilder,
      nullCheck: Boolean): ProcessFunction[BaseRow, BaseRow] = {

    val needRetraction = false
    val aggInfoList = transformToStreamAggregateInfoList(
      aggregateCalls,
      // use aggInputType which considers constants as input instead of inputSchema.relDataType
      aggInputType,
      Array.fill(aggregateCalls.size)(needRetraction),
      needInputCount = needRetraction,
      isStateBackendDataViews = true)

    val generator = new AggsHandlerCodeGenerator(
      ctx,
      relBuilder,
      inputSchema.fieldTypes,
      needRetraction,
      needMerge = false,
      nullCheck,
      copyInputField = false)

    val genAggsHandler = generator
      // over agg code gen must pass the constants
      .withConstants(constants)
      .generateAggsHandler("UnboundedOverAggregateHelper", aggInfoList)

    val flattenAccTypes = aggInfoList.getAccTypes.map(_.toInternalType)

    if (rowTimeIdx.isDefined) {
      if (isRowsClause) {
        // ROWS unbounded over process function
        new RowTimeUnboundedRowsOver(
          genAggsHandler,
          flattenAccTypes,
          inputSchema.fieldTypes,
          rowTimeIdx.get,
          tableConfig)
      } else {
        // RANGE unbounded over process function
        new RowTimeUnboundedRangeOver(
          genAggsHandler,
          flattenAccTypes,
          inputSchema.fieldTypes,
          rowTimeIdx.get,
          tableConfig)
      }
    } else {
      new ProcTimeUnboundedOver(
        genAggsHandler,
        flattenAccTypes,
        tableConfig)
    }
  }

  /**
    * Create an ProcessFunction for ROWS clause bounded OVER window to evaluate final
    * aggregate value.
    *
    * @param ctx            code generator context
    * @param aggregateCalls physical calls to aggregate functions and their output field names
    * @param constants      the constants in aggregates parameters, such as sum(1)
    * @param aggInputType   physical type of the input row which consist of input and constants.
    * @param rowTimeIdx     the index of the rowtime field or None in case of processing time.
    * @param isRowsClause   it is a tag that indicates whether the OVER clause is ROWS clause
    */
  private def createBoundedOverProcessFunction(
      ctx: CodeGeneratorContext,
      aggregateCalls: Seq[AggregateCall],
      constants: Seq[RexLiteral],
      aggInputType: RelDataType,
      rowTimeIdx: Option[Int],
      isRowsClause: Boolean,
      precedingOffset: Long,
      tableConfig: TableConfig,
      relBuilder: RelBuilder,
      nullCheck: Boolean): ProcessFunction[BaseRow, BaseRow] = {

    val needRetraction = true
    val aggInfoList = transformToStreamAggregateInfoList(
      aggregateCalls,
      // use aggInputType which considers constants as input instead of inputSchema.relDataType
      aggInputType,
      Array.fill(aggregateCalls.size)(needRetraction),
      needInputCount = needRetraction,
      isStateBackendDataViews = true)

    val generator = new AggsHandlerCodeGenerator(
      ctx,
      relBuilder,
      inputSchema.fieldTypes,
      needRetraction,
      needMerge = false,
      nullCheck,
      copyInputField = false)

    val genAggsHandler = generator
      // over agg code gen must pass the constants
      .withConstants(constants)
      .generateAggsHandler("BoundedOverAggregateHelper", aggInfoList)

    val flattenAccTypes = aggInfoList.getAccTypes.map(_.toInternalType)

    if (rowTimeIdx.isDefined) {
      if (isRowsClause) {
        new RowTimeBoundedRowsOver(
          genAggsHandler,
          flattenAccTypes,
          inputSchema.fieldTypes,
          precedingOffset,
          rowTimeIdx.get,
          tableConfig)
      } else {
        new RowTimeBoundedRangeOver(
          genAggsHandler,
          flattenAccTypes,
          inputSchema.fieldTypes,
          precedingOffset,
          rowTimeIdx.get,
          tableConfig)
      }
    } else {
      if (isRowsClause) {
        new ProcTimeBoundedRowsOver(
          genAggsHandler,
          flattenAccTypes,
          inputSchema.fieldTypes,
          precedingOffset,
          tableConfig)
      } else {
        new ProcTimeBoundedRangeOver(
          genAggsHandler,
          flattenAccTypes,
          inputSchema.fieldTypes,
          precedingOffset,
          tableConfig)
      }
    }
  }

  private def getOperatorName = {
    val overWindow: Group = logicWindow.groups.get(0)
    val constants: Seq[RexLiteral] = logicWindow.constants.asScala
    val partitionKeys: Array[Int] = overWindow.keys.toArray
    val namedAggregates: Seq[CalcitePair[AggregateCall, String]] = generateNamedAggregates

    s"over: (${
      if (!partitionKeys.isEmpty) {
        s"PARTITION BY: ${OverAggregateUtil.partitionToString(
          inputSchema.relDataType, partitionKeys)}, "
      } else {
        ""
      }
    }ORDER BY: ${OverAggregateUtil.orderingToString(
      inputSchema.relDataType, overWindow.orderKeys.getFieldCollations)}, " +
      s"${OverAggregateUtil.windowRangeToString(logicWindow, overWindow)}, " +
      s"select: (${
        OverAggregateUtil.aggregationToString(
          inputSchema.relDataType,
          constants,
          outputSchema.relDataType,
          namedAggregates)
      }))"
  }
}

