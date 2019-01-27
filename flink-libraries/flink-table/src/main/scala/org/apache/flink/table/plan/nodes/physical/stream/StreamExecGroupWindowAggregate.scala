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
import org.apache.flink.table.api.types.InternalType
import org.apache.flink.table.api.window.{CountWindow, TimeWindow}
import org.apache.flink.table.api.{StreamTableEnvironment, TableConfig, TableException}
import org.apache.flink.table.calcite.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.codegen._
import org.apache.flink.table.codegen.agg.AggsHandlerCodeGenerator
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.errorcode.TableErrors
import org.apache.flink.table.expressions.ExpressionUtils.{isTimeIntervalLiteral, _}
import org.apache.flink.table.plan.logical._
import org.apache.flink.table.plan.nodes.exec.RowStreamExecNode
import org.apache.flink.table.plan.nodes.physical.FlinkPhysicalRel
import org.apache.flink.table.plan.rules.physical.stream.StreamExecRetractionRules
import org.apache.flink.table.plan.schema.BaseRowSchema
import org.apache.flink.table.plan.util._
import org.apache.flink.table.runtime.window.{WindowOperator, WindowOperatorBuilder}
import org.apache.flink.table.typeutils.BaseRowTypeInfo

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.tools.RelBuilder

import java.time.Duration

import scala.collection.JavaConversions._

class StreamExecGroupWindowAggregate(
    val window: LogicalWindow,
    namedProperties: Seq[NamedWindowProperty],
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    val aggCalls: Seq[AggregateCall],
    outputSchema: BaseRowSchema,
    inputSchema: BaseRowSchema,
    grouping: Array[Int],
    inputTimestampIndex: Int,
    emitStrategy: EmitStrategy)
  extends SingleRel(cluster, traitSet, inputNode)
  with StreamPhysicalRel
  with RowStreamExecNode {

  override def deriveRowType(): RelDataType = outputSchema.relDataType

  override def producesUpdates: Boolean = emitStrategy.produceUpdates

  override def consumesRetractions = true

  override def needsUpdatesAsRetraction(input: RelNode) = true

  def getGroupings: Array[Int] = grouping

  def getWindowProperties: Seq[NamedWindowProperty] = namedProperties

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new StreamExecGroupWindowAggregate(
      window,
      namedProperties,
      cluster,
      traitSet,
      inputs.get(0),
      aggCalls,
      outputSchema,
      inputSchema,
      grouping,
      inputTimestampIndex,
      emitStrategy)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .itemIf("groupBy",
        AggregateNameUtil.groupingToString(inputSchema.relDataType, grouping), grouping.nonEmpty)
      .item("window", window)
      .itemIf("properties", namedProperties.map(_.name).mkString(", "), namedProperties.nonEmpty)
      .item(
        "select", AggregateNameUtil.aggregationToString(
          inputSchema.relDataType,
          grouping,
          outputSchema.relDataType,
          aggCalls,
          namedProperties))
      .itemIf("emit", emitStrategy, !emitStrategy.toString.isEmpty)
  }

  override def isDeterministic: Boolean = AggregateUtil.isDeterministic(aggCalls)

  //~ ExecNode methods -----------------------------------------------------------

  override def getFlinkPhysicalRel: FlinkPhysicalRel = this

  override def translateToPlanInternal(
      tableEnv: StreamTableEnvironment): StreamTransformation[BaseRow] = {

    val config = tableEnv.getConfig

    val inputTransform = getInputNodes.get(0).translateToPlan(tableEnv)
      .asInstanceOf[StreamTransformation[BaseRow]]

    val inputIsAccRetract = StreamExecRetractionRules.isAccRetract(input)

    if (inputIsAccRetract) {
      throw new TableException(
        TableErrors.INST.sqlGroupWindowAggTranslateRetractNotSupported())
    }

    val isCountWindow = window match {
      case TumblingGroupWindow(_, _, size) if isRowCountLiteral(size) => true
      case SlidingGroupWindow(_, _, size, _) if isRowCountLiteral(size) => true
      case _ => false
    }

    if (isCountWindow && grouping.length > 0 &&
      tableEnv.getConfig.getMinIdleStateRetentionTime < 0) {
      LOG.warn(
        "No state retention interval configured for a query which accumulates state. " +
          "Please provide a query configuration with valid retention interval to prevent " +
          "excessive state size. You may specify a retention time of 0 to not clean up the state.")
    }

    // validation
    emitStrategy.checkValidation()

    val aggString = AggregateNameUtil.aggregationToString(
      inputSchema.relDataType,
      grouping,
      outputSchema.relDataType,
      aggCalls,
      namedProperties)

    val operatorName = if (grouping.nonEmpty) {
      s"window: ($window), " +
        s"groupBy: (${AggregateNameUtil.groupingToString(inputSchema.relDataType, grouping)}), " +
        s"select: ($aggString)"
    } else {
      s"window: ($window), select: ($aggString)"
    }

    val timeIdx = if (isRowtimeAttribute(window.timeAttribute)) {
      if (inputTimestampIndex < 0) {
        throw new TableException(
          TableErrors.INST.sqlGroupWindowAggTranslateTimeAttrNotFound())
      }
      inputTimestampIndex
    } else {
      -1
    }

    val needRetraction = StreamExecRetractionRules.isAccRetract(getInput)
    val aggInfoList = AggregateUtil.transformToStreamAggregateInfoList(
      aggCalls,
      inputSchema.relDataType,
      Array.fill(aggCalls.size)(needRetraction),
      needInputCount = needRetraction,
      isStateBackendDataViews = true)

    val aggsHandler = createAggsHandler(
      aggInfoList,
      config,
      tableEnv.getRelBuilder,
      inputSchema.fieldTypes,
      needRetraction)

    val accTypes = aggInfoList.getAccTypes.map(_.toInternalType)
    val aggResultTypes = aggInfoList.getActualValueTypes.map(_.toInternalType)
    val windowPropertyTypes = namedProperties
      .map(_.property.resultType)
      .toArray
    val equaliser = createEqualiser(aggResultTypes, windowPropertyTypes)

    val aggValueTypes = aggInfoList.getActualValueTypes.map(_.toInternalType)

    val operator = createWindowOperator(
      config,
      aggsHandler,
      equaliser,
      accTypes,
      windowPropertyTypes,
      aggValueTypes,
      timeIdx)

    val inputRowType = inputTransform.getOutputType.asInstanceOf[BaseRowTypeInfo]
    val selector = StreamExecUtil.getKeySelector(grouping, inputRowType)

    val outRowType = outputSchema.typeInfo()
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
    transformation.setResources(getResource.getReservedResourceSpec,
      getResource.getPreferResourceSpec)

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
      needRetraction: Boolean): GeneratedSubKeyedAggsHandleFunction[_] = {

    val ctx = CodeGeneratorContext(config, supportReference = true)
    val needMerge = window match {
      case SlidingGroupWindow(_, _, size, _) if isTimeIntervalLiteral(size) => true
      case SessionGroupWindow(_, _, _) => true
      case _ => false
    }
    val windowClass = window match {
      case TumblingGroupWindow(_, _, size) if isRowCountLiteral(size) => classOf[CountWindow]
      case SlidingGroupWindow(_, _, size, _) if isRowCountLiteral(size) => classOf[CountWindow]
      case _ => classOf[TimeWindow]
    }

    val aggsGenerator = new AggsHandlerCodeGenerator(
      ctx,
      relBuilder,
      inputSchema.fieldTypes,
      needRetraction,
      needMerge,
      config.getNullCheck,
      copyInputField = false)
    aggsGenerator.generateSubKeyedAggsHandler(
      "GroupingWindowAggsHandler",
      aggInfoList,
      namedProperties.map(_.property),
      windowClass)
  }

  private def createEqualiser(
      aggResultTypes: Array[InternalType],
      windowPropertyTypes: Array[InternalType]): GeneratedRecordEqualiser = {
    val generator = new EqualiserCodeGenerator(aggResultTypes ++ windowPropertyTypes)
    generator.generateRecordEqualiser("WindowValueEqualiser")
  }

  private def createWindowOperator(
      config: TableConfig,
      aggsHandler: GeneratedSubKeyedAggsHandleFunction[_],
      recordEqualiser: GeneratedRecordEqualiser,
      accTypes: Array[InternalType],
      windowPropertyTypes: Array[InternalType],
      aggValueTypes: Array[InternalType],
      timeIdx: Int): WindowOperator[_, _] = {

    val builder = WindowOperatorBuilder
      .builder()
      .withInputFields(inputSchema.fieldTypes.toArray)

    val newBuilder = window match {
      case TumblingGroupWindow(_, timeField, size)
        if isProctimeAttribute(timeField) && isTimeIntervalLiteral(size) =>
        builder.tumble(toDuration(size)).withProcessingTime()

      case TumblingGroupWindow(_, timeField, size)
        if isRowtimeAttribute(timeField) && isTimeIntervalLiteral(size) =>
        builder.tumble(toDuration(size)).withEventTime(timeIdx)

      case TumblingGroupWindow(_, timeField, size)
        if isProctimeAttribute(timeField) && isRowCountLiteral(size) =>
        builder.countWindow(toLong(size))

      case TumblingGroupWindow(_, _, _) =>
        // TODO: EventTimeTumblingGroupWindow should sort the stream on event time
        // before applying the  windowing logic. Otherwise, this would be the same as a
        // ProcessingTimeTumblingGroupWindow
        throw new UnsupportedOperationException(
          "Event-time grouping windows on row intervals are currently not supported.")

      case SlidingGroupWindow(_, timeField, size, slide)
        if isProctimeAttribute(timeField) && isTimeIntervalLiteral(slide) =>
        builder.sliding(toDuration(size), toDuration(slide)).withProcessingTime()

      case SlidingGroupWindow(_, timeField, size, slide)
        if isRowtimeAttribute(timeField) && isTimeIntervalLiteral(size) =>
        builder.sliding(toDuration(size), toDuration(slide)).withEventTime(timeIdx)

      case SlidingGroupWindow(_, timeField, size, slide)
        if isProctimeAttribute(timeField) && isRowCountLiteral(size) =>
        builder.countWindow(toLong(size), toLong(slide))

      case SlidingGroupWindow(_, _, _, _) =>
        // TODO: EventTimeTumblingGroupWindow should sort the stream on event time
        // before applying the  windowing logic. Otherwise, this would be the same as a
        // ProcessingTimeTumblingGroupWindow
        throw new UnsupportedOperationException(
          "Event-time grouping windows on row intervals are currently not supported.")

      case SessionGroupWindow(_, timeField, gap)
        if isProctimeAttribute(timeField) =>
        builder.session(toDuration(gap)).withProcessingTime()

      case SessionGroupWindow(_, timeField, gap)
        if isRowtimeAttribute(timeField) =>
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
