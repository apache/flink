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

package org.apache.flink.api.table.plan.nodes.datastream

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.flink.api.common.functions.RichGroupReduceFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.table.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.api.table.expressions._
import org.apache.flink.api.table.plan.logical._
import org.apache.flink.api.table.plan.nodes.FlinkAggregate
import org.apache.flink.api.table.plan.nodes.datastream.DataStreamAggregate._
import org.apache.flink.api.table.runtime.aggregate.AggregateUtil._
import org.apache.flink.api.table.runtime.aggregate._
import org.apache.flink.api.table.typeutils.TypeCheckUtils.isTimeInterval
import org.apache.flink.api.table.typeutils.{RowIntervalTypeInfo, RowTypeInfo, TimeIntervalTypeInfo, TypeConverter}
import org.apache.flink.api.table.{TableException, FlinkTypeFactory, Row, StreamTableEnvironment}
import org.apache.flink.streaming.api.datastream.{AllWindowedStream, DataStream, KeyedStream, WindowedStream}
import org.apache.flink.streaming.api.functions.windowing.{WindowFunction, AllWindowFunction}
import org.apache.flink.streaming.api.windowing.assigners._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{Window => DataStreamWindow}

import scala.collection.JavaConverters._

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
  extends SingleRel(cluster, traitSet, inputNode)
  with FlinkAggregate
  with DataStreamRel {

  override def deriveRowType() = rowRelDataType

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
      .item("select", aggregationToString(
        inputType,
        grouping,
        getRowType,
        namedAggregates,
        namedProperties))
  }

  override def translateToPlan(
      tableEnv: StreamTableEnvironment,
      expectedType: Option[TypeInformation[Any]])
    : DataStream[Any] = {
    
    val config = tableEnv.getConfig

    val groupingKeys = grouping.indices.toArray
    // add grouping fields, position keys in the input, and input type
    val aggregateResult = AggregateUtil.createOperatorFunctionsForAggregates(
      namedAggregates,
      inputType,
      getRowType,
      grouping)

    val inputDS = input.asInstanceOf[DataStreamRel].translateToPlan(
      tableEnv,
      // tell the input operator that this operator currently only supports Rows as input
      Some(TypeConverter.DEFAULT_ROW_TYPE))

    // get the output types
    val fieldTypes: Array[TypeInformation[_]] = getRowType.getFieldList.asScala
      .map(field => FlinkTypeFactory.toTypeInfo(field.getType))
      .toArray

    val aggString = aggregationToString(
      inputType,
      grouping,
      getRowType,
      namedAggregates,
      namedProperties)

    val prepareOpName = s"prepare select: ($aggString)"
    val mappedInput = inputDS
      .map(aggregateResult._1)
      .name(prepareOpName)

    val groupReduceFunction = aggregateResult._2
    val rowTypeInfo = new RowTypeInfo(fieldTypes)

    val result = {
      // grouped / keyed aggregation
      if (groupingKeys.length > 0) {
        val aggOpName = s"groupBy: (${groupingToString(inputType, grouping)}), " +
          s"window: ($window), " +
          s"select: ($aggString)"
        val aggregateFunction =
          createWindowAggregationFunction(window, namedProperties, groupReduceFunction)

        val keyedStream = mappedInput.keyBy(groupingKeys: _*)

        val windowedStream = createKeyedWindowedStream(window, keyedStream)
          .asInstanceOf[WindowedStream[Row, Tuple, DataStreamWindow]]

        windowedStream
          .apply(aggregateFunction)
          .returns(rowTypeInfo)
          .name(aggOpName)
          .asInstanceOf[DataStream[Any]]
      }
      // global / non-keyed aggregation
      else {
        val aggOpName = s"window: ($window), select: ($aggString)"
        val aggregateFunction =
          createAllWindowAggregationFunction(window, namedProperties, groupReduceFunction)

        val windowedStream = createNonKeyedWindowedStream(window, mappedInput)
          .asInstanceOf[AllWindowedStream[Row, DataStreamWindow]]

        windowedStream
          .apply(aggregateFunction)
          .returns(rowTypeInfo)
          .name(aggOpName)
          .asInstanceOf[DataStream[Any]]
      }
    }

    // if the expected type is not a Row, inject a mapper to convert to the expected type
    expectedType match {
      case Some(typeInfo) if typeInfo.getTypeClass != classOf[Row] =>
        val mapName = s"convert: (${getRowType.getFieldNames.asScala.toList.mkString(", ")})"
        result.map(getConversionMapper(
          config = config,
          nullableInput = false,
          inputType = rowTypeInfo.asInstanceOf[TypeInformation[Any]],
          expectedType = expectedType.get,
          conversionOperatorName = "DataStreamAggregateConversion",
          fieldNames = getRowType.getFieldNames.asScala
        ))
          .name(mapName)
      case _ => result
    }
  }
}

object DataStreamAggregate {

  private def createAllWindowAggregationFunction(
      window: LogicalWindow,
      properties: Seq[NamedWindowProperty],
      aggFunction: RichGroupReduceFunction[Row, Row])
    : AllWindowFunction[Row, Row, DataStreamWindow] = {

    if (isTimeWindow(window)) {
      val (startPos, endPos) = computeWindowStartEndPropertyPos(properties)
      new AggregateAllTimeWindowFunction(aggFunction, startPos, endPos)
        .asInstanceOf[AllWindowFunction[Row, Row, DataStreamWindow]]
    } else {
      new AggregateAllWindowFunction(aggFunction)
    }

  }

  private def createWindowAggregationFunction(
      window: LogicalWindow,
      properties: Seq[NamedWindowProperty],
      aggFunction: RichGroupReduceFunction[Row, Row])
    : WindowFunction[Row, Row, Tuple, DataStreamWindow] = {

    if (isTimeWindow(window)) {
      val (startPos, endPos) = computeWindowStartEndPropertyPos(properties)
      new AggregateTimeWindowFunction(aggFunction, startPos, endPos)
        .asInstanceOf[WindowFunction[Row, Row, Tuple, DataStreamWindow]]
    } else {
      new AggregateWindowFunction(aggFunction)
    }

  }

  private def isTimeWindow(window: LogicalWindow) = {
    window match {
      case ProcessingTimeTumblingGroupWindow(_, size) => isTimeInterval(size.resultType)
      case ProcessingTimeSlidingGroupWindow(_, size, _) => isTimeInterval(size.resultType)
      case ProcessingTimeSessionGroupWindow(_, _) => true
      case EventTimeTumblingGroupWindow(_, _, size) => isTimeInterval(size.resultType)
      case EventTimeSlidingGroupWindow(_, _, size, _) => isTimeInterval(size.resultType)
      case EventTimeSessionGroupWindow(_, _, _) => true
    }
  }

  def computeWindowStartEndPropertyPos(properties: Seq[NamedWindowProperty])
      : (Option[Int], Option[Int]) = {

    val propPos = properties.foldRight((None: Option[Int], None: Option[Int], 0)) {
      (p, x) => p match {
        case NamedWindowProperty(name, prop) =>
          prop match {
            case WindowStart(_) if x._1.isDefined =>
              throw new TableException("Duplicate WindowStart property encountered. This is a bug.")
            case WindowStart(_) =>
              (Some(x._3), x._2, x._3 - 1)
            case WindowEnd(_) if x._2.isDefined =>
              throw new TableException("Duplicate WindowEnd property encountered. This is a bug.")
            case WindowEnd(_) =>
              (x._1, Some(x._3), x._3 - 1)
          }
      }
    }
    (propPos._1, propPos._2)
  }

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
      throw new UnsupportedOperationException("Event-time grouping windows on row intervals are " +
        "currently not supported.")

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
      throw new UnsupportedOperationException("Event-time grouping windows on row intervals are " +
        "currently not supported.")

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
      throw new UnsupportedOperationException("Event-time grouping windows on row intervals are " +
        "currently not supported.")

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
      throw new UnsupportedOperationException("Event-time grouping windows on row intervals are " +
        "currently not supported.")

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

