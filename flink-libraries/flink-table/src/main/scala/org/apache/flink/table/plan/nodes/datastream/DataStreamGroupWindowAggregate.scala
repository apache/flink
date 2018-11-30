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
import org.apache.flink.streaming.api.datastream.{AllWindowedStream, DataStream, KeyedStream, WindowedStream}
import org.apache.flink.streaming.api.windowing.assigners._
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger
import org.apache.flink.streaming.api.windowing.windows.{Window => DataStreamWindow}
import org.apache.flink.table.api.{StreamQueryConfig, StreamTableEnvironment, TableException}
import org.apache.flink.table.calcite.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.codegen.AggregationCodeGenerator
import org.apache.flink.table.expressions.ExpressionUtils._
import org.apache.flink.table.expressions.ResolvedFieldReference
import org.apache.flink.table.plan.logical._
import org.apache.flink.table.plan.nodes.CommonAggregate
import org.apache.flink.table.plan.nodes.datastream.DataStreamGroupWindowAggregate._
import org.apache.flink.table.plan.rules.datastream.DataStreamRetractionRules
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.runtime.aggregate.AggregateUtil._
import org.apache.flink.table.runtime.aggregate._
import org.apache.flink.table.runtime.triggers.StateCleaningCountTrigger
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
import org.apache.flink.table.runtime.{CRowKeySelector, RowtimeProcessFunction}
import org.apache.flink.table.typeutils.TypeCheckUtils.isTimeInterval
import org.apache.flink.table.util.Logging
import org.apache.flink.types.Row

class DataStreamGroupWindowAggregate(
    window: LogicalWindow,
    namedProperties: Seq[NamedWindowProperty],
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    namedAggregates: Seq[CalcitePair[AggregateCall, String]],
    schema: RowSchema,
    inputSchema: RowSchema,
    grouping: Array[Int])
  extends SingleRel(cluster, traitSet, inputNode)
    with CommonAggregate
    with DataStreamRel
    with Logging {

  override def deriveRowType(): RelDataType = schema.relDataType

  override def needsUpdatesAsRetraction = true

  override def consumesRetractions = true

  def getGroupings: Array[Int] = grouping

  def getWindowProperties: Seq[NamedWindowProperty] = namedProperties

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataStreamGroupWindowAggregate(
      window,
      namedProperties,
      cluster,
      traitSet,
      inputs.get(0),
      namedAggregates,
      schema,
      inputSchema,
      grouping)
  }

  override def toString: String = {
    s"Aggregate(${
      if (!grouping.isEmpty) {
        s"groupBy: (${groupingToString(inputSchema.relDataType, grouping)}), "
      } else {
        ""
      }
    }window: ($window), " +
      s"select: (${
        aggregationToString(
          inputSchema.relDataType,
          grouping,
          getRowType,
          namedAggregates,
          namedProperties)
      }))"
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .itemIf("groupBy", groupingToString(inputSchema.relDataType, grouping), !grouping.isEmpty)
      .item("window", window)
      .item(
        "select", aggregationToString(
          inputSchema.relDataType,
          grouping,
          schema.relDataType,
          namedAggregates,
          namedProperties))
  }

  override def translateToPlan(
      tableEnv: StreamTableEnvironment,
      queryConfig: StreamQueryConfig): DataStream[CRow] = {

    val inputDS = input.asInstanceOf[DataStreamRel].translateToPlan(tableEnv, queryConfig)

    val inputIsAccRetract = DataStreamRetractionRules.isAccRetract(input)

    if (inputIsAccRetract) {
      throw new TableException(
        "Retraction on windowed GroupBy aggregation is not supported yet. " +
          "Note: Windowed GroupBy aggregation should not follow a " +
          "non-windowed GroupBy aggregation.")
    }

    val isCountWindow = window match {
      case TumblingGroupWindow(_, _, size) if isRowCountLiteral(size) => true
      case SlidingGroupWindow(_, _, size, _) if isRowCountLiteral(size) => true
      case _ => false
    }

    if (isCountWindow && grouping.length > 0 && queryConfig.getMinIdleStateRetentionTime < 0) {
      LOG.warn(
        "No state retention interval configured for a query which accumulates state. " +
        "Please provide a query configuration with valid retention interval to prevent excessive " +
        "state size. You may specify a retention time of 0 to not clean up the state.")
    }

    val timestampedInput = if (isRowtimeAttribute(window.timeAttribute)) {
      // copy the window rowtime attribute into the StreamRecord timestamp field
      val timeAttribute = window.timeAttribute.asInstanceOf[ResolvedFieldReference].name
      val timeIdx = inputSchema.fieldNames.indexOf(timeAttribute)
      if (timeIdx < 0) {
        throw new TableException("Time attribute could not be found. This is a bug.")
      }

      inputDS
        .process(
          new RowtimeProcessFunction(timeIdx, CRowTypeInfo(inputSchema.typeInfo)))
        .setParallelism(inputDS.getParallelism)
        .name(s"time attribute: ($timeAttribute)")
    } else {
      inputDS
    }

    val outRowType = CRowTypeInfo(schema.typeInfo)

    val aggString = aggregationToString(
      inputSchema.relDataType,
      grouping,
      schema.relDataType,
      namedAggregates,
      namedProperties)

    val keyedAggOpName = s"groupBy: (${groupingToString(inputSchema.relDataType, grouping)}), " +
      s"window: ($window), " +
      s"select: ($aggString)"
    val nonKeyedAggOpName = s"window: ($window), select: ($aggString)"

    val generator = new AggregationCodeGenerator(
      tableEnv.getConfig,
      false,
      inputSchema.typeInfo,
      None)

    val needMerge = window match {
      case SessionGroupWindow(_, _, _) => true
      case _ => false
    }
    // grouped / keyed aggregation
    if (grouping.length > 0) {
      val windowFunction = AggregateUtil.createAggregationGroupWindowFunction(
        window,
        grouping.length,
        namedAggregates.size,
        schema.arity,
        namedProperties)

      val keySelector = new CRowKeySelector(grouping, inputSchema.projectedTypeInfo(grouping))

      val keyedStream = timestampedInput.keyBy(keySelector)
      val windowedStream =
        createKeyedWindowedStream(queryConfig, window, keyedStream)
          .asInstanceOf[WindowedStream[CRow, Row, DataStreamWindow]]

      val (aggFunction, accumulatorRowType, aggResultRowType) =
        AggregateUtil.createDataStreamAggregateFunction(
          generator,
          namedAggregates,
          inputSchema.relDataType,
          inputSchema.fieldTypeInfos,
          schema.relDataType,
          grouping,
          needMerge,
          tableEnv.getConfig)

      windowedStream
        .aggregate(aggFunction, windowFunction, accumulatorRowType, aggResultRowType, outRowType)
        .name(keyedAggOpName)
    }
    // global / non-keyed aggregation
    else {
      val windowFunction = AggregateUtil.createAggregationAllWindowFunction(
        window,
        schema.arity,
        namedProperties)

      val windowedStream =
        createNonKeyedWindowedStream(queryConfig, window, timestampedInput)
          .asInstanceOf[AllWindowedStream[CRow, DataStreamWindow]]

      val (aggFunction, accumulatorRowType, aggResultRowType) =
        AggregateUtil.createDataStreamAggregateFunction(
          generator,
          namedAggregates,
          inputSchema.relDataType,
          inputSchema.fieldTypeInfos,
          schema.relDataType,
          Array[Int](),
          needMerge,
          tableEnv.getConfig)

      windowedStream
        .aggregate(aggFunction, windowFunction, accumulatorRowType, aggResultRowType, outRowType)
        .name(nonKeyedAggOpName)
    }
  }
}

object DataStreamGroupWindowAggregate {

  private def createKeyedWindowedStream(
      queryConfig: StreamQueryConfig,
      groupWindow: LogicalWindow,
      stream: KeyedStream[CRow, Row]):
    WindowedStream[CRow, Row, _ <: DataStreamWindow] = groupWindow match {

    case TumblingGroupWindow(_, timeField, size)
        if isProctimeAttribute(timeField) && isTimeIntervalLiteral(size)=>
      stream.window(TumblingProcessingTimeWindows.of(toTime(size)))

    case TumblingGroupWindow(_, timeField, size)
        if isProctimeAttribute(timeField) && isRowCountLiteral(size)=>
      stream.countWindow(toLong(size))
      .trigger(PurgingTrigger.of(StateCleaningCountTrigger.of(queryConfig, toLong(size))));

    case TumblingGroupWindow(_, timeField, size)
        if isRowtimeAttribute(timeField) && isTimeIntervalLiteral(size) =>
      stream.window(TumblingEventTimeWindows.of(toTime(size)))

    case TumblingGroupWindow(_, _, size) =>
      // TODO: EventTimeTumblingGroupWindow should sort the stream on event time
      // before applying the  windowing logic. Otherwise, this would be the same as a
      // ProcessingTimeTumblingGroupWindow
      throw new UnsupportedOperationException(
        "Event-time grouping windows on row intervals are currently not supported.")

    case SlidingGroupWindow(_, timeField, size, slide)
        if isProctimeAttribute(timeField) && isTimeIntervalLiteral(slide) =>
      stream.window(SlidingProcessingTimeWindows.of(toTime(size), toTime(slide)))

    case SlidingGroupWindow(_, timeField, size, slide)
        if isProctimeAttribute(timeField) && isRowCountLiteral(size) =>
      stream.countWindow(toLong(size), toLong(slide))
      .trigger(StateCleaningCountTrigger.of(queryConfig, toLong(slide)));

    case SlidingGroupWindow(_, timeField, size, slide)
        if isRowtimeAttribute(timeField) && isTimeIntervalLiteral(size)=>
      stream.window(SlidingEventTimeWindows.of(toTime(size), toTime(slide)))

    case SlidingGroupWindow(_, _, size, slide) =>
      // TODO: EventTimeTumblingGroupWindow should sort the stream on event time
      // before applying the  windowing logic. Otherwise, this would be the same as a
      // ProcessingTimeTumblingGroupWindow
      throw new UnsupportedOperationException(
        "Event-time grouping windows on row intervals are currently not supported.")

    case SessionGroupWindow(_, timeField, gap)
        if isProctimeAttribute(timeField) =>
      stream.window(ProcessingTimeSessionWindows.withGap(toTime(gap)))

    case SessionGroupWindow(_, timeField, gap)
        if isRowtimeAttribute(timeField) =>
      stream.window(EventTimeSessionWindows.withGap(toTime(gap)))
  }

  private def createNonKeyedWindowedStream(
      queryConfig: StreamQueryConfig,
      groupWindow: LogicalWindow,
      stream: DataStream[CRow]):
    AllWindowedStream[CRow, _ <: DataStreamWindow] = groupWindow match {

    case TumblingGroupWindow(_, timeField, size)
        if isProctimeAttribute(timeField) && isTimeIntervalLiteral(size) =>
      stream.windowAll(TumblingProcessingTimeWindows.of(toTime(size)))

    case TumblingGroupWindow(_, timeField, size)
        if isProctimeAttribute(timeField) && isRowCountLiteral(size)=>
      stream.countWindowAll(toLong(size))
      .trigger(PurgingTrigger.of(StateCleaningCountTrigger.of(queryConfig, toLong(size))));

    case TumblingGroupWindow(_, _, size) if isTimeInterval(size.resultType) =>
      stream.windowAll(TumblingEventTimeWindows.of(toTime(size)))

    case TumblingGroupWindow(_, _, size) =>
      // TODO: EventTimeTumblingGroupWindow should sort the stream on event time
      // before applying the  windowing logic. Otherwise, this would be the same as a
      // ProcessingTimeTumblingGroupWindow
      throw new UnsupportedOperationException(
        "Event-time grouping windows on row intervals are currently not supported.")

    case SlidingGroupWindow(_, timeField, size, slide)
        if isProctimeAttribute(timeField) && isTimeIntervalLiteral(size) =>
      stream.windowAll(SlidingProcessingTimeWindows.of(toTime(size), toTime(slide)))

    case SlidingGroupWindow(_, timeField, size, slide)
        if isProctimeAttribute(timeField) && isRowCountLiteral(size)=>
      stream.countWindowAll(toLong(size), toLong(slide))
      .trigger(StateCleaningCountTrigger.of(queryConfig, toLong(slide)));

    case SlidingGroupWindow(_, timeField, size, slide)
        if isRowtimeAttribute(timeField) && isTimeIntervalLiteral(size)=>
      stream.windowAll(SlidingEventTimeWindows.of(toTime(size), toTime(slide)))

    case SlidingGroupWindow(_, _, size, slide) =>
      // TODO: EventTimeTumblingGroupWindow should sort the stream on event time
      // before applying the  windowing logic. Otherwise, this would be the same as a
      // ProcessingTimeTumblingGroupWindow
      throw new UnsupportedOperationException(
        "Event-time grouping windows on row intervals are currently not supported.")

    case SessionGroupWindow(_, timeField, gap)
        if isProctimeAttribute(timeField) && isTimeIntervalLiteral(gap) =>
      stream.windowAll(ProcessingTimeSessionWindows.withGap(toTime(gap)))

    case SessionGroupWindow(_, timeField, gap)
        if isRowtimeAttribute(timeField) && isTimeIntervalLiteral(gap) =>
      stream.windowAll(EventTimeSessionWindows.withGap(toTime(gap)))
  }

}

