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
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.datastream.{AllWindowedStream, DataStream, KeyedStream, WindowedStream}
import org.apache.flink.streaming.api.windowing.assigners._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{Window => DataStreamWindow}
import org.apache.flink.table.api.StreamTableEnvironment
import org.apache.flink.table.calcite.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.expressions._
import org.apache.flink.table.plan.logical._
import org.apache.flink.table.plan.nodes.CommonAggregate
import org.apache.flink.table.plan.nodes.datastream.DataStreamAggregate._
import org.apache.flink.table.runtime.aggregate.AggregateUtil._
import org.apache.flink.table.runtime.aggregate._
import org.apache.flink.table.typeutils.TypeCheckUtils.isTimeInterval
import org.apache.flink.table.typeutils.{RowIntervalTypeInfo, TimeIntervalTypeInfo}
import org.apache.flink.types.Row

class DataStreamAggregate(
    window: LogicalWindow,
    namedProperties: Seq[NamedWindowProperty],
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    namedAggregates: Seq[CalcitePair[AggregateCall, String]],
    rowRelDataType: RelDataType,
    inputType: RelDataType,
    grouping: Array[Int])
  extends SingleRel(cluster, traitSet, inputNode) with CommonAggregate with DataStreamRel {

  override def deriveRowType(): RelDataType = rowRelDataType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataStreamAggregate(
      window,
      namedProperties,
      cluster,
      traitSet,
      inputs.get(0),
      namedAggregates,
      getRowType,
      inputType,
      grouping)
  }

  override def toString: String = {
    s"Aggregate(${
      if (!grouping.isEmpty) {
        s"groupBy: (${groupingToString(inputType, grouping)}), "
      } else {
        ""
      }
    }window: ($window), " +
      s"select: (${
        aggregationToString(
          inputType,
          grouping,
          getRowType,
          namedAggregates,
          namedProperties)
      }))"
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .itemIf("groupBy", groupingToString(inputType, grouping), !grouping.isEmpty)
      .item("window", window)
      .item(
        "select", aggregationToString(
          inputType,
          grouping,
          getRowType,
          namedAggregates,
          namedProperties))
  }

  override def translateToPlan(tableEnv: StreamTableEnvironment): DataStream[Row] = {

    val groupingKeys = grouping.indices.toArray
    val inputDS = input.asInstanceOf[DataStreamRel].translateToPlan(tableEnv)

    val rowTypeInfo = FlinkTypeFactory.toInternalRowTypeInfo(getRowType)

    val aggString = aggregationToString(
      inputType,
      grouping,
      getRowType,
      namedAggregates,
      namedProperties)

    val keyedAggOpName = s"groupBy: (${groupingToString(inputType, grouping)}), " +
      s"window: ($window), " +
      s"select: ($aggString)"
    val nonKeyedAggOpName = s"window: ($window), select: ($aggString)"

    // grouped / keyed aggregation
    if (groupingKeys.length > 0) {
      val windowFunction = AggregateUtil.createAggregationGroupWindowFunction(
        window,
        groupingKeys.length,
        namedAggregates.size,
        rowRelDataType.getFieldCount,
        namedProperties)

      val keyedStream = inputDS.keyBy(groupingKeys: _*)
      val windowedStream =
        createKeyedWindowedStream(window, keyedStream)
          .asInstanceOf[WindowedStream[Row, Tuple, DataStreamWindow]]

      val (aggFunction, accumulatorRowType, aggResultRowType) =
        AggregateUtil.createDataStreamAggregateFunction(
          namedAggregates,
          inputType,
          rowRelDataType,
          grouping)

      windowedStream
        .aggregate(aggFunction, windowFunction, accumulatorRowType, aggResultRowType, rowTypeInfo)
        .name(keyedAggOpName)
    }
    // global / non-keyed aggregation
    else {
      val windowFunction = AggregateUtil.createAggregationAllWindowFunction(
        window,
        rowRelDataType.getFieldCount,
        namedProperties)

      val windowedStream =
        createNonKeyedWindowedStream(window, inputDS)
          .asInstanceOf[AllWindowedStream[Row, DataStreamWindow]]

      val (aggFunction, accumulatorRowType, aggResultRowType) =
        AggregateUtil.createDataStreamAggregateFunction(
          namedAggregates,
          inputType,
          rowRelDataType,
          grouping)

      windowedStream
        .aggregate(aggFunction, windowFunction, accumulatorRowType, aggResultRowType, rowTypeInfo)
        .name(nonKeyedAggOpName)
    }
  }
}

object DataStreamAggregate {


  private def createKeyedWindowedStream(groupWindow: LogicalWindow, stream: KeyedStream[Row, Tuple])
    : WindowedStream[Row, Tuple, _ <: DataStreamWindow] = groupWindow match {

    case ProcessingTimeTumblingGroupWindow(_, size) if isTimeInterval(size.resultType) =>
      stream.window(TumblingProcessingTimeWindows.of(asTime(size)))

    case ProcessingTimeTumblingGroupWindow(_, size) =>
      stream.countWindow(asCount(size))

    case EventTimeTumblingGroupWindow(_, _, size) if isTimeInterval(size.resultType) =>
      stream.window(TumblingEventTimeWindows.of(asTime(size)))

    case EventTimeTumblingGroupWindow(_, _, size) =>
      // TODO: EventTimeTumblingGroupWindow should sort the stream on event time
      // before applying the  windowing logic. Otherwise, this would be the same as a
      // ProcessingTimeTumblingGroupWindow
      throw new UnsupportedOperationException(
        "Event-time grouping windows on row intervals are currently not supported.")

    case ProcessingTimeSlidingGroupWindow(_, size, slide) if isTimeInterval(size.resultType) =>
      stream.window(SlidingProcessingTimeWindows.of(asTime(size), asTime(slide)))

    case ProcessingTimeSlidingGroupWindow(_, size, slide) =>
      stream.countWindow(asCount(size), asCount(slide))

    case EventTimeSlidingGroupWindow(_, _, size, slide) if isTimeInterval(size.resultType) =>
      stream.window(SlidingEventTimeWindows.of(asTime(size), asTime(slide)))

    case EventTimeSlidingGroupWindow(_, _, size, slide) =>
      // TODO: EventTimeTumblingGroupWindow should sort the stream on event time
      // before applying the  windowing logic. Otherwise, this would be the same as a
      // ProcessingTimeTumblingGroupWindow
      throw new UnsupportedOperationException(
        "Event-time grouping windows on row intervals are currently not supported.")

    case ProcessingTimeSessionGroupWindow(_, gap: Expression) =>
      stream.window(ProcessingTimeSessionWindows.withGap(asTime(gap)))

    case EventTimeSessionGroupWindow(_, _, gap) =>
      stream.window(EventTimeSessionWindows.withGap(asTime(gap)))
  }

  private def createNonKeyedWindowedStream(groupWindow: LogicalWindow, stream: DataStream[Row])
    : AllWindowedStream[Row, _ <: DataStreamWindow] = groupWindow match {

    case ProcessingTimeTumblingGroupWindow(_, size) if isTimeInterval(size.resultType) =>
      stream.windowAll(TumblingProcessingTimeWindows.of(asTime(size)))

    case ProcessingTimeTumblingGroupWindow(_, size) =>
      stream.countWindowAll(asCount(size))

    case EventTimeTumblingGroupWindow(_, _, size) if isTimeInterval(size.resultType) =>
      stream.windowAll(TumblingEventTimeWindows.of(asTime(size)))

    case EventTimeTumblingGroupWindow(_, _, size) =>
      // TODO: EventTimeTumblingGroupWindow should sort the stream on event time
      // before applying the  windowing logic. Otherwise, this would be the same as a
      // ProcessingTimeTumblingGroupWindow
      throw new UnsupportedOperationException(
        "Event-time grouping windows on row intervals are currently not supported.")

    case ProcessingTimeSlidingGroupWindow(_, size, slide) if isTimeInterval(size.resultType) =>
      stream.windowAll(SlidingProcessingTimeWindows.of(asTime(size), asTime(slide)))

    case ProcessingTimeSlidingGroupWindow(_, size, slide) =>
      stream.countWindowAll(asCount(size), asCount(slide))

    case EventTimeSlidingGroupWindow(_, _, size, slide) if isTimeInterval(size.resultType) =>
      stream.windowAll(SlidingEventTimeWindows.of(asTime(size), asTime(slide)))

    case EventTimeSlidingGroupWindow(_, _, size, slide) =>
      // TODO: EventTimeTumblingGroupWindow should sort the stream on event time
      // before applying the  windowing logic. Otherwise, this would be the same as a
      // ProcessingTimeTumblingGroupWindow
      throw new UnsupportedOperationException(
        "Event-time grouping windows on row intervals are currently not supported.")

    case ProcessingTimeSessionGroupWindow(_, gap) =>
      stream.windowAll(ProcessingTimeSessionWindows.withGap(asTime(gap)))

    case EventTimeSessionGroupWindow(_, _, gap) =>
      stream.windowAll(EventTimeSessionWindows.withGap(asTime(gap)))
  }

  def asTime(expr: Expression): Time = expr match {
    case Literal(value: Long, TimeIntervalTypeInfo.INTERVAL_MILLIS) => Time.milliseconds(value)
    case _ => throw new IllegalArgumentException()
  }

  def asCount(expr: Expression): Long = expr match {
    case Literal(value: Long, RowIntervalTypeInfo.INTERVAL_ROWS) => value
    case _ => throw new IllegalArgumentException()
  }
}

