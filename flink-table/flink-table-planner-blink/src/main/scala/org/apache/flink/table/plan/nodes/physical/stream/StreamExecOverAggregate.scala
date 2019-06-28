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

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.operators.KeyedProcessOperator
import org.apache.flink.streaming.api.transformations.OneInputTransformation
import org.apache.flink.table.CalcitePair
import org.apache.flink.table.api.{StreamTableEnvironment, TableConfig, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.CodeGeneratorContext
import org.apache.flink.table.codegen.agg.AggsHandlerCodeGenerator
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.plan.rules.physical.stream.StreamExecRetractionRules
import org.apache.flink.table.plan.util.AggregateUtil.transformToStreamAggregateInfoList
import org.apache.flink.table.plan.util.{KeySelectorUtil, OverAggregateUtil, RelExplainUtil}
import org.apache.flink.table.runtime.over._
import org.apache.flink.table.types.LogicalTypeDataTypeConverter
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
import java.util

import org.apache.flink.api.dag.Transformation

import scala.collection.JavaConverters._

/**
  * Stream physical RelNode for time-based over [[Window]].
  */
class StreamExecOverAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    outputRowType: RelDataType,
    inputRowType: RelDataType,
    logicWindow: Window)
  extends SingleRel(cluster, traitSet, inputRel)
  with StreamPhysicalRel
  with StreamExecNode[BaseRow] {

  override def producesUpdates: Boolean = false

  override def needsUpdatesAsRetraction(input: RelNode) = true

  override def consumesRetractions = true

  override def producesRetractions: Boolean = false

  override def requireWatermark: Boolean = {
    if (logicWindow.groups.size() != 1
      || logicWindow.groups.get(0).orderKeys.getFieldCollations.size() != 1) {
      return false
    }
    val orderKey = logicWindow.groups.get(0).orderKeys.getFieldCollations.get(0)
    val timeType = outputRowType.getFieldList.get(orderKey.getFieldIndex).getType
    FlinkTypeFactory.isRowtimeIndicatorType(timeType)
  }

  override def deriveRowType(): RelDataType = outputRowType

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new StreamExecOverAggregate(
      cluster,
      traitSet,
      inputs.get(0),
      outputRowType,
      inputRowType,
      logicWindow
    )
  }

  override def estimateRowCount(mq: RelMetadataQuery): Double = {
    // over window: one input at least one output (do not introduce retract amplification)
    mq.getRowCount(getInput)
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    // by default, assume cost is proportional to number of rows
    val rowCnt: Double = mq.getRowCount(this)
    val count = (getRowType.getFieldCount - 1) * 1.0 / inputRel.getRowType.getFieldCount
    planner.getCostFactory.makeCost(rowCnt, rowCnt * count, 0)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val overWindow: Group = logicWindow.groups.get(0)
    val constants: Seq[RexLiteral] = logicWindow.constants.asScala
    val partitionKeys: Array[Int] = overWindow.keys.toArray
    val namedAggregates: Seq[CalcitePair[AggregateCall, String]] = generateNamedAggregates

    super.explainTerms(pw)
      .itemIf("partitionBy", RelExplainUtil.fieldToString(partitionKeys, inputRowType),
        partitionKeys.nonEmpty)
      .item("orderBy", RelExplainUtil.collationToString(overWindow.orderKeys, inputRowType))
      .item("window", RelExplainUtil.windowRangeToString(logicWindow, overWindow))
      .item("select", RelExplainUtil.overAggregationToString(
        inputRowType,
        outputRowType,
        constants,
        namedAggregates))
  }

  private def generateNamedAggregates: Seq[CalcitePair[AggregateCall, String]] = {
    val overWindow: Group = logicWindow.groups.get(0)

    val aggregateCalls = overWindow.getAggregateCalls(logicWindow)
    for (i <- 0 until aggregateCalls.size())
      yield new CalcitePair[AggregateCall, String](aggregateCalls.get(i), "w0$o" + i)
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getInputNodes: util.List[ExecNode[StreamTableEnvironment, _]] = {
    getInputs.asScala.map(_.asInstanceOf[ExecNode[StreamTableEnvironment, _]]).asJava
  }

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[StreamTableEnvironment, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  override protected def translateToPlanInternal(
      tableEnv: StreamTableEnvironment): Transformation[BaseRow] = {
    val tableConfig = tableEnv.getConfig

    if (logicWindow.groups.size > 1) {
      throw new TableException(
          "All aggregates must be computed on the same window.")
    }

    val overWindow: Group = logicWindow.groups.get(0)

    val orderKeys = overWindow.orderKeys.getFieldCollations

    if (orderKeys.size() != 1) {
      throw new TableException(
          "The window can only be ordered by a single time column.")
    }
    val orderKey = orderKeys.get(0)

    if (!orderKey.direction.equals(ASCENDING)) {
      throw new TableException(
          "The window can only be ordered in ASCENDING mode.")
    }

    val inputDS = getInputNodes.get(0).translateToPlan(tableEnv)
      .asInstanceOf[Transformation[BaseRow]]

    val inputIsAccRetract = StreamExecRetractionRules.isAccRetract(input)

    if (inputIsAccRetract) {
      throw new TableException(
          "Retraction on Over window aggregation is not supported yet. " +
            "Note: Over window aggregation should not follow a non-windowed GroupBy aggregation.")
    }

    if (!logicWindow.groups.get(0).keys.isEmpty && tableConfig.getMinIdleStateRetentionTime < 0) {
      LOG.warn(
        "No state retention interval configured for a query which accumulates state. " +
          "Please provide a query configuration with valid retention interval to prevent " +
          "excessive state size. You may specify a retention time of 0 to not clean up the state.")
    }

    val timeType = outputRowType.getFieldList.get(orderKey.getFieldIndex).getType

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
          "OVER windows can only be applied on time attributes.")
    }

    val codeGenCtx = CodeGeneratorContext(tableConfig)
    val aggregateCalls = logicWindow.groups.get(0).getAggregateCalls(logicWindow).asScala
    val isRowsClause = overWindow.isRows
    val constants = logicWindow.constants.asScala
    val constantTypes = constants.map(c => FlinkTypeFactory.toLogicalType(c.getType))

    val fieldNames = inputRowType.getFieldNames.asScala
    val fieldTypes = inputRowType.getFieldList.asScala
      .map(c => FlinkTypeFactory.toLogicalType(c.getType))

    val inRowType = FlinkTypeFactory.toLogicalRowType(inputRel.getRowType)
    val outRowType = FlinkTypeFactory.toLogicalRowType(outputRowType)

    val aggInputType = tableEnv.getTypeFactory.buildRelNodeRowType(
      fieldNames ++ constants.indices.map(i => "TMP" + i),
      fieldTypes ++ constantTypes)

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
        tableConfig.getNullCheck)

    } else if (overWindow.lowerBound.isPreceding
      && !overWindow.lowerBound.isUnbounded
      && overWindow.upperBound.isCurrentRow) {

      val boundValue = OverAggregateUtil.getBoundary(logicWindow, overWindow.lowerBound)

      if (boundValue.isInstanceOf[BigDecimal]) {
        throw new TableException(
            "the specific value is decimal which haven not supported yet.")
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
        tableConfig.getNullCheck)

    } else {
      throw new TableException(
          "OVER RANGE FOLLOWING windows are not supported yet.")
    }

    val partitionKeys: Array[Int] = overWindow.keys.toArray
    val inputTypeInfo = BaseRowTypeInfo.of(inRowType)

    val selector = KeySelectorUtil.getBaseRowSelector(partitionKeys, inputTypeInfo)

    val returnTypeInfo = BaseRowTypeInfo.of(outRowType)
      .asInstanceOf[BaseRowTypeInfo]
    // partitioned aggregation

    val operator = new KeyedProcessOperator(overProcessFunction)

    val ret = new OneInputTransformation(
      inputDS,
      "OverAggregate",
      operator,
      returnTypeInfo,
      getResource.getParallelism)

    if (getResource.getMaxParallelism > 0) {
      ret.setMaxParallelism(getResource.getMaxParallelism)
    }

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
      nullCheck: Boolean): KeyedProcessFunction[BaseRow, BaseRow, BaseRow] = {

    val needRetraction = false
    val aggInfoList = transformToStreamAggregateInfoList(
      aggregateCalls,
      // use aggInputType which considers constants as input instead of inputSchema.relDataType
      aggInputType,
      Array.fill(aggregateCalls.size)(needRetraction),
      needInputCount = needRetraction,
      isStateBackendDataViews = true)

    val fieldTypes = inputRowType.getFieldList.asScala.
      map(c => FlinkTypeFactory.toLogicalType(c.getType)).toArray

    val generator = new AggsHandlerCodeGenerator(
      ctx,
      relBuilder,
      fieldTypes,
      copyInputField = false)

    val genAggsHandler = generator
      .needAccumulate()
      // over agg code gen must pass the constants
      .withConstants(constants)
      .generateAggsHandler("UnboundedOverAggregateHelper", aggInfoList)

    val flattenAccTypes = aggInfoList.getAccTypes.map(
      LogicalTypeDataTypeConverter.fromDataTypeToLogicalType)

    if (rowTimeIdx.isDefined) {
      if (isRowsClause) {
        // ROWS unbounded over process function
        new RowTimeRowsUnboundedPrecedingFunction(
          tableConfig.getMinIdleStateRetentionTime,
          tableConfig.getMaxIdleStateRetentionTime,
          genAggsHandler,
          flattenAccTypes,
          fieldTypes,
          rowTimeIdx.get)
      } else {
        // RANGE unbounded over process function
        new RowTimeRangeUnboundedPrecedingFunction(
          tableConfig.getMinIdleStateRetentionTime,
          tableConfig.getMaxIdleStateRetentionTime,
          genAggsHandler,
          flattenAccTypes,
          fieldTypes,
          rowTimeIdx.get)
      }
    } else {
      new ProcTimeUnboundedPrecedingFunction(
        tableConfig.getMinIdleStateRetentionTime,
        tableConfig.getMaxIdleStateRetentionTime,
        genAggsHandler,
        flattenAccTypes)
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
      nullCheck: Boolean): KeyedProcessFunction[BaseRow, BaseRow, BaseRow] = {

    val needRetraction = true
    val aggInfoList = transformToStreamAggregateInfoList(
      aggregateCalls,
      // use aggInputType which considers constants as input instead of inputSchema.relDataType
      aggInputType,
      Array.fill(aggregateCalls.size)(needRetraction),
      needInputCount = needRetraction,
      isStateBackendDataViews = true)

    val fieldTypes = inputRowType.getFieldList.asScala.
      map(c => FlinkTypeFactory.toLogicalType(c.getType)).toArray

    val generator = new AggsHandlerCodeGenerator(
      ctx,
      relBuilder,
      fieldTypes,
      copyInputField = false)


    val genAggsHandler = generator
      .needRetract()
      .needAccumulate()
      // over agg code gen must pass the constants
      .withConstants(constants)
      .generateAggsHandler("BoundedOverAggregateHelper", aggInfoList)

    val flattenAccTypes = aggInfoList.getAccTypes.map(
      LogicalTypeDataTypeConverter.fromDataTypeToLogicalType)

    if (rowTimeIdx.isDefined) {
      if (isRowsClause) {
        new RowTimeRowsBoundedPrecedingFunction(
          tableConfig.getMinIdleStateRetentionTime,
          tableConfig.getMaxIdleStateRetentionTime,
          genAggsHandler,
          flattenAccTypes,
          fieldTypes,
          precedingOffset,
          rowTimeIdx.get)
      } else {
        new RowTimeRangeBoundedPrecedingFunction(
          tableConfig.getMinIdleStateRetentionTime,
          tableConfig.getMaxIdleStateRetentionTime,
          genAggsHandler,
          flattenAccTypes,
          fieldTypes,
          precedingOffset,
          rowTimeIdx.get)
      }
    } else {
      if (isRowsClause) {
        new ProcTimeRowsBoundedPrecedingFunction(
          tableConfig.getMinIdleStateRetentionTime,
          tableConfig.getMaxIdleStateRetentionTime,
          genAggsHandler,
          flattenAccTypes,
          fieldTypes,
          precedingOffset)
      } else {
        new ProcTimeRangeBoundedPrecedingFunction(
          tableConfig.getMinIdleStateRetentionTime,
          tableConfig.getMaxIdleStateRetentionTime,
          genAggsHandler,
          flattenAccTypes,
          fieldTypes,
          precedingOffset)
      }
    }
  }
}
