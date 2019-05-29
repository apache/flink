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
import org.apache.flink.table.calcite.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.logical._
import org.apache.flink.table.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.plan.rules.physical.stream.StreamExecRetractionRules
import org.apache.flink.table.plan.util.AggregateUtil.{isProctimeIndicatorType, isRowIntervalType, isRowtimeIndicatorType, isTimeIntervalType, toDuration, toLong, transformToStreamAggregateInfoList}
import org.apache.flink.table.plan.util.{AggregateInfoList, KeySelectorUtil, RelExplainUtil, WindowEmitStrategy}
import org.apache.flink.table.`type`.TypeConverters.createInternalTypeFromTypeInfo
import org.apache.flink.table.api.window.{CountWindow, TimeWindow}
import org.apache.flink.table.codegen.agg.AggsHandlerCodeGenerator
import org.apache.flink.table.codegen.{CodeGeneratorContext, EqualiserCodeGenerator}
import org.apache.flink.table.`type`.InternalType
import org.apache.flink.table.generated.{GeneratedNamespaceAggsHandleFunction, GeneratedRecordEqualiser}
import org.apache.flink.table.runtime.window.{WindowOperator, WindowOperatorBuilder}
import org.apache.flink.table.typeutils.BaseRowTypeInfo

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.tools.RelBuilder

import java.time.Duration
import java.util
import java.util.Calendar

import scala.collection.JavaConversions._

/**
  * Streaming group window aggregate physical node which will be translate to window operator.
  */
class StreamExecGroupWindowAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    outputRowType: RelDataType,
    inputRowType: RelDataType,
    grouping: Array[Int],
    val aggCalls: Seq[AggregateCall],
    val window: LogicalWindow,
    namedProperties: Seq[NamedWindowProperty],
    inputTimeFieldIndex: Int,
    val emitStrategy: WindowEmitStrategy)
  extends SingleRel(cluster, traitSet, inputRel)
  with StreamPhysicalRel
  with StreamExecNode[BaseRow] {

  override def producesUpdates: Boolean = emitStrategy.produceUpdates

  override def consumesRetractions = true

  override def needsUpdatesAsRetraction(input: RelNode) = true

  override def producesRetractions: Boolean = false

  override def requireWatermark: Boolean = window match {
    case TumblingGroupWindow(_, timeField, size)
      if isRowtimeIndicatorType(timeField.getResultType) && isTimeIntervalType(size.getType) => true
    case SlidingGroupWindow(_, timeField, size, _)
      if isRowtimeIndicatorType(timeField.getResultType) && isTimeIntervalType(size.getType) => true
    case SessionGroupWindow(_, timeField, _)
      if isRowtimeIndicatorType(timeField.getResultType) => true
    case _ => false
  }

  def getGrouping: Array[Int] = grouping

  def getWindowProperties: Seq[NamedWindowProperty] = namedProperties

  override def deriveRowType(): RelDataType = outputRowType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new StreamExecGroupWindowAggregate(
      cluster,
      traitSet,
      inputs.get(0),
      outputRowType,
      inputRowType,
      grouping,
      aggCalls,
      window,
      namedProperties,
      inputTimeFieldIndex,
      emitStrategy)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .itemIf("groupBy", RelExplainUtil.fieldToString(grouping, inputRowType), grouping.nonEmpty)
      .item("window", window)
      .itemIf("properties", namedProperties.map(_.name).mkString(", "), namedProperties.nonEmpty)
      .item("select", RelExplainUtil.streamWindowAggregationToString(
        inputRowType,
        grouping,
        outputRowType,
        aggCalls,
        namedProperties))
      .itemIf("emit", emitStrategy, !emitStrategy.toString.isEmpty)
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getInputNodes: util.List[ExecNode[StreamTableEnvironment, _]] = {
    getInputs.map(_.asInstanceOf[ExecNode[StreamTableEnvironment, _]])
  }

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[StreamTableEnvironment, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  override protected def translateToPlanInternal(
      tableEnv: StreamTableEnvironment): StreamTransformation[BaseRow] = {
    val config = tableEnv.getConfig

    val inputTransform = getInputNodes.get(0).translateToPlan(tableEnv)
      .asInstanceOf[StreamTransformation[BaseRow]]

    val inputRowTypeInfo = inputTransform.getOutputType.asInstanceOf[BaseRowTypeInfo]
    val outRowType = FlinkTypeFactory.toInternalRowType(outputRowType).toTypeInfo

    val inputIsAccRetract = StreamExecRetractionRules.isAccRetract(input)

    if (inputIsAccRetract) {
      throw new TableException(
        "Group Window Agg: Retraction on windowed GroupBy aggregation is not supported yet. \n" +
          "please re-check sql grammar. \n" +
          "Note: Windowed GroupBy aggregation should not follow a" +
          "non-windowed GroupBy aggregation.")
    }

    val isCountWindow = window match {
      case TumblingGroupWindow(_, _, size) if isRowIntervalType(size.getType) => true
      case SlidingGroupWindow(_, _, size, _) if isRowIntervalType(size.getType) => true
      case _ => false
    }

    if (isCountWindow && grouping.length > 0 && config.getMinIdleStateRetentionTime < 0) {
      LOG.warn(
        "No state retention interval configured for a query which accumulates state. " +
          "Please provide a query configuration with valid retention interval to prevent " +
          "excessive state size. You may specify a retention time of 0 to not clean up the state.")
    }

    // validation
    emitStrategy.checkValidation()

    val aggString = RelExplainUtil.streamWindowAggregationToString(
      inputRowType,
      grouping,
      outputRowType,
      aggCalls,
      namedProperties)

    val timeIdx = if (isRowtimeIndicatorType(window.timeAttribute.getResultType)) {
      if (inputTimeFieldIndex < 0) {
        throw new TableException(
          "Group window aggregate must defined on a time attribute, " +
            "but the time attribute can't be found.\n" +
          "This should never happen. Please file an issue."
        )
      }
      inputTimeFieldIndex
    } else {
      -1
    }

    val needRetraction = StreamExecRetractionRules.isAccRetract(getInput)
    val aggInfoList = transformToStreamAggregateInfoList(
      aggCalls,
      inputRowType,
      Array.fill(aggCalls.size)(needRetraction),
      needInputCount = needRetraction,
      isStateBackendDataViews = true)

    val aggsHandler = createAggsHandler(
      aggInfoList,
      config,
      tableEnv.getRelBuilder,
      inputRowTypeInfo.getInternalTypes,
      needRetraction)

    val aggResultTypes = aggInfoList.getActualValueTypes.map(createInternalTypeFromTypeInfo)
    val windowPropertyTypes = namedProperties.map(_.property.resultType).toArray
    val generator = new EqualiserCodeGenerator(aggResultTypes ++ windowPropertyTypes)
    val equaliser = generator.generateRecordEqualiser("WindowValueEqualiser")

    val aggValueTypes = aggInfoList.getActualValueTypes.map(createInternalTypeFromTypeInfo)
    val accTypes = aggInfoList.getAccTypes.map(createInternalTypeFromTypeInfo)
    val operator = createWindowOperator(
      config,
      aggsHandler,
      equaliser,
      accTypes,
      windowPropertyTypes,
      aggValueTypes,
      inputRowTypeInfo.getInternalTypes,
      timeIdx)

    val operatorName = if (grouping.nonEmpty) {
      s"window: ($window), " +
        s"groupBy: (${RelExplainUtil.fieldToString(grouping, inputRowType)}), " +
        s"select: ($aggString)"
    } else {
      s"window: ($window), select: ($aggString)"
    }

    val transformation = new OneInputTransformation(
      inputTransform,
      operatorName,
      operator,
      outRowType,
      inputTransform.getParallelism)

    if (grouping.isEmpty) {
      transformation.setParallelism(1)
      transformation.setMaxParallelism(1)
    }

    val selector = KeySelectorUtil.getBaseRowSelector(grouping, inputRowTypeInfo)

    // set KeyType and Selector for state
    transformation.setStateKeySelector(selector)
    transformation.setStateKeyType(selector.getProducedType)
    transformation
  }

  private def createAggsHandler(
      aggInfoList: AggregateInfoList,
      config: TableConfig,
      relBuilder: RelBuilder,
      fieldTypeInfos: Seq[InternalType],
      needRetraction: Boolean): GeneratedNamespaceAggsHandleFunction[_] = {

    val needMerge = window match {
      case SlidingGroupWindow(_, _, size, _) if isTimeIntervalType(size.getType) => true
      case SessionGroupWindow(_, _, _) => true
      case _ => false
    }
    val windowClass = window match {
      case TumblingGroupWindow(_, _, size) if isRowIntervalType(size.getType) =>
        classOf[CountWindow]
      case SlidingGroupWindow(_, _, size, _) if isRowIntervalType(size.getType) =>
        classOf[CountWindow]
      case _ => classOf[TimeWindow]
    }

    val generator = new AggsHandlerCodeGenerator(
      CodeGeneratorContext(config),
      relBuilder,
      fieldTypeInfos,
      copyInputField = false)

    generator.needAccumulate()
    if (needMerge) {
      generator.needMerge(mergedAccOffset = 0, mergedAccOnHeap = false)
    }
    if (needRetraction) {
      generator.needRetract()
    }

    generator.generateNamespaceAggsHandler(
      "GroupingWindowAggsHandler",
      aggInfoList,
      namedProperties.map(_.property),
      windowClass)
  }

  private def createWindowOperator(
      config: TableConfig,
      aggsHandler: GeneratedNamespaceAggsHandleFunction[_],
      recordEqualiser: GeneratedRecordEqualiser,
      accTypes: Array[InternalType],
      windowPropertyTypes: Array[InternalType],
      aggValueTypes: Array[InternalType],
      inputFields: Seq[InternalType],
      timeIdx: Int): WindowOperator[_, _] = {

    val builder = WindowOperatorBuilder
      .builder()
      .withInputFields(inputFields.toArray)
    val timeZoneOffset = -config.getTimeZone.getOffset(Calendar.ZONE_OFFSET)

    val newBuilder = window match {
      case TumblingGroupWindow(_, timeField, size)
        if isProctimeIndicatorType(timeField.getResultType) && isTimeIntervalType(size.getType) =>
        builder.tumble(toDuration(size), timeZoneOffset).withProcessingTime()

      case TumblingGroupWindow(_, timeField, size)
        if isRowtimeIndicatorType(timeField.getResultType) && isTimeIntervalType(size.getType) =>
        builder.tumble(toDuration(size), timeZoneOffset).withEventTime(timeIdx)

      case TumblingGroupWindow(_, timeField, size)
        if isProctimeIndicatorType(timeField.getResultType) && isRowIntervalType(size.getType) =>
        builder.countWindow(toLong(size))

      case TumblingGroupWindow(_, _, _) =>
        // TODO: EventTimeTumblingGroupWindow should sort the stream on event time
        // before applying the  windowing logic. Otherwise, this would be the same as a
        // ProcessingTimeTumblingGroupWindow
        throw new UnsupportedOperationException(
          "Event-time grouping windows on row intervals are currently not supported.")

      case SlidingGroupWindow(_, timeField, size, slide)
        if isProctimeIndicatorType(timeField.getResultType) && isTimeIntervalType(size.getType) =>
        builder.sliding(toDuration(size), toDuration(slide), timeZoneOffset)
          .withProcessingTime()

      case SlidingGroupWindow(_, timeField, size, slide)
        if isRowtimeIndicatorType(timeField.getResultType) && isTimeIntervalType(size.getType) =>
        builder.sliding(toDuration(size), toDuration(slide), timeZoneOffset)
          .withEventTime(timeIdx)

      case SlidingGroupWindow(_, timeField, size, slide)
        if isProctimeIndicatorType(timeField.getResultType) && isRowIntervalType(size.getType) =>
        builder.countWindow(toLong(size), toLong(slide))

      case SlidingGroupWindow(_, _, _, _) =>
        // TODO: EventTimeTumblingGroupWindow should sort the stream on event time
        // before applying the  windowing logic. Otherwise, this would be the same as a
        // ProcessingTimeTumblingGroupWindow
        throw new UnsupportedOperationException(
          "Event-time grouping windows on row intervals are currently not supported.")

      case SessionGroupWindow(_, timeField, gap)
        if isProctimeIndicatorType(timeField.getResultType) =>
        builder.session(toDuration(gap)).withProcessingTime()

      case SessionGroupWindow(_, timeField, gap)
        if isRowtimeIndicatorType(timeField.getResultType) =>
        builder.session(toDuration(gap)).withEventTime(timeIdx)
    }

    if (emitStrategy.produceUpdates) {
      // mark this operator will send retraction and set new trigger
      newBuilder
        .withSendRetraction()
        .triggering(emitStrategy.getTrigger)
    }

    newBuilder
      .aggregate(aggsHandler, recordEqualiser, accTypes, aggValueTypes, windowPropertyTypes)
      .withAllowedLateness(Duration.ofMillis(emitStrategy.getAllowLateness))
      .build()
  }
}
