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
package org.apache.flink.table.runtime.aggregate

import java.util

import org.apache.calcite.rel.`type`._
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.`type`.SqlTypeName._
import org.apache.calcite.sql.fun._
import org.apache.calcite.sql.{SqlAggFunction, SqlKind}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.common.functions.{AggregateFunction => DataStreamAggFunction, _}
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.streaming.api.functions.windowing.{AllWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.windows.{Window => DataStreamWindow}
import org.apache.flink.table.api.TableException
import org.apache.flink.table.calcite.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.CodeGenerator
import org.apache.flink.table.expressions._
import org.apache.flink.table.functions.aggfunctions._
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils._
import org.apache.flink.table.functions.{AggregateFunction => TableAggregateFunction}
import org.apache.flink.table.plan.logical._
import org.apache.flink.table.typeutils.TypeCheckUtils._
import org.apache.flink.table.typeutils.{RowIntervalTypeInfo, TimeIntervalTypeInfo}
import org.apache.flink.types.Row

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

object AggregateUtil {

  type CalcitePair[T, R] = org.apache.calcite.util.Pair[T, R]
  type JavaList[T] = java.util.List[T]

  /**
    * Create an [[org.apache.flink.streaming.api.functions.ProcessFunction]] for unbounded OVER
    * window to evaluate final aggregate value.
    *
    * @param generator       code generator instance
    * @param namedAggregates List of calls to aggregate functions and their output field names
    * @param inputType Input row type
    * @param isRowTimeType It is a tag that indicates whether the time type is rowTimeType
    * @param isPartitioned It is a tag that indicate whether the input is partitioned
    * @param isRowsClause It is a tag that indicates whether the OVER clause is ROWS clause
    */
  private[flink] def createUnboundedOverProcessFunction(
    generator: CodeGenerator,
    namedAggregates: Seq[CalcitePair[AggregateCall, String]],
    inputType: RelDataType,
    isRowTimeType: Boolean,
    isPartitioned: Boolean,
    isRowsClause: Boolean): ProcessFunction[Row, Row] = {

    val (aggFields, aggregates) =
      transformToAggregateFunctions(
        namedAggregates.map(_.getKey),
        inputType,
        needRetraction = false)

    val aggregationStateType: RowTypeInfo =
      createDataSetAggregateBufferDataType(Array(), aggregates, inputType)

    val forwardMapping = (0 until inputType.getFieldCount).map(x => (x, x)).toArray
    val aggMapping = aggregates.indices.map(x => x + inputType.getFieldCount).toArray
    val outputArity = inputType.getFieldCount + aggregates.length

    val genFunction = generator.generateAggregations(
      "UnboundedProcessingOverAggregateHelper",
      generator,
      inputType,
      aggregates,
      aggFields,
      aggMapping,
      forwardMapping,
      outputArity
    )

    if (isRowTimeType) {
      if (isRowsClause) {
        // ROWS unbounded over process function
        new RowTimeUnboundedRowsOver(
          genFunction,
          aggregationStateType,
          FlinkTypeFactory.toInternalRowTypeInfo(inputType))
      } else {
        // RANGE unbounded over process function
        new RowTimeUnboundedRangeOver(
          genFunction,
          aggregationStateType,
          FlinkTypeFactory.toInternalRowTypeInfo(inputType))
      }
    } else {
      if (isPartitioned) {
        new ProcTimeUnboundedPartitionedOver(
          genFunction,
          aggregationStateType)
      } else {
        new ProcTimeUnboundedNonPartitionedOver(
          genFunction,
          aggregationStateType)
      }
    }
  }

  /**
    * Create an [[org.apache.flink.streaming.api.functions.ProcessFunction]] for
    * bounded OVER window to evaluate final aggregate value.
    *
    * @param generator       code generator instance
    * @param namedAggregates List of calls to aggregate functions and their output field names
    * @param inputType       Input row type
    * @param precedingOffset the preceding offset
    * @param isRowsClause   It is a tag that indicates whether the OVER clause is ROWS clause
    * @param isRowTimeType   It is a tag that indicates whether the time type is rowTimeType
    * @return [[org.apache.flink.streaming.api.functions.ProcessFunction]]
    */
  private[flink] def createBoundedOverProcessFunction(
    generator: CodeGenerator,
    namedAggregates: Seq[CalcitePair[AggregateCall, String]],
    inputType: RelDataType,
    precedingOffset: Long,
    isRowsClause: Boolean,
    isRowTimeType: Boolean): ProcessFunction[Row, Row] = {

    val (aggFields, aggregates) =
      transformToAggregateFunctions(
        namedAggregates.map(_.getKey),
        inputType,
        needRetraction = true)

    val aggregationStateType: RowTypeInfo = createAccumulatorRowType(aggregates)
    val inputRowType = FlinkTypeFactory.toInternalRowTypeInfo(inputType).asInstanceOf[RowTypeInfo]

    val forwardMapping = (0 until inputType.getFieldCount).map(x => (x, x)).toArray
    val aggMapping = aggregates.indices.map(x => x + inputType.getFieldCount).toArray
    val outputArity = inputType.getFieldCount + aggregates.length

    val genFunction = generator.generateAggregations(
      "BoundedOverAggregateHelper",
      generator,
      inputType,
      aggregates,
      aggFields,
      aggMapping,
      forwardMapping,
      outputArity
    )

    if (isRowTimeType) {
      if (isRowsClause) {
        new RowTimeBoundedRowsOver(
          genFunction,
          aggregationStateType,
          inputRowType,
          precedingOffset
        )
      } else {
        new RowTimeBoundedRangeOver(
          genFunction,
          aggregationStateType,
          inputRowType,
          precedingOffset
        )
      }
    } else {
      if (isRowsClause) {
        new ProcTimeBoundedRowsOver(
          genFunction,
          precedingOffset,
          aggregationStateType,
          inputRowType)
      } else {
        new ProcTimeBoundedRangeOver(
          genFunction,
          precedingOffset,
          aggregationStateType,
          inputRowType)
      }
    }
  }


  /**
    * Create a [[org.apache.flink.api.common.functions.MapFunction]] that prepares for aggregates.
    * The output of the function contains the grouping keys and the timestamp and the intermediate
    * aggregate values of all aggregate function. The timestamp field is aligned to time window
    * start and used to be a grouping key in case of time window. In case of count window on
    * event-time, the timestamp is not aligned and used to sort.
    *
    * The output is stored in Row by the following format:
    * {{{
    *                      avg(x)                           count(z)
    *                       |                                 |
    *                       v                                 v
    *   +---------+---------+----------------+----------------+------------------+-------+
    *   |groupKey1|groupKey2| AvgAccumulator | SumAccumulator | CountAccumulator |rowtime|
    *   +---------+---------+----------------+----------------+------------------+-------+
    *                                        ^                                   ^
    *                                        |                                   |
    *                                       sum(y)                        rowtime to group or sort
    * }}}
    *
    * NOTE: this function is only used for time based window on batch tables.
    */
  def createDataSetWindowPrepareMapFunction(
      window: LogicalWindow,
      namedAggregates: Seq[CalcitePair[AggregateCall, String]],
      groupings: Array[Int],
      inputType: RelDataType,
      isParserCaseSensitive: Boolean)
  : MapFunction[Row, Row] = {

    val (aggFieldIndexes, aggregates) = transformToAggregateFunctions(
      namedAggregates.map(_.getKey),
      inputType,
      needRetraction = false)

    val mapReturnType: RowTypeInfo =
      createDataSetAggregateBufferDataType(
        groupings,
        aggregates,
        inputType,
        Some(Array(BasicTypeInfo.LONG_TYPE_INFO)))

    val (timeFieldPos, tumbleTimeWindowSize) = window match {
      case EventTimeTumblingGroupWindow(_, time, size) if isTimeInterval(size.resultType) =>
        val timeFieldPos = getTimeFieldPosition(time, inputType, isParserCaseSensitive)
        (timeFieldPos, Some(asLong(size)))

      case EventTimeTumblingGroupWindow(_, time, size) =>
        val timeFieldPos = getTimeFieldPosition(time, inputType, isParserCaseSensitive)
        (timeFieldPos, None)

      case EventTimeSessionGroupWindow(_, time, _) =>
        val timeFieldPos = getTimeFieldPosition(time, inputType, isParserCaseSensitive)
        (timeFieldPos, None)

      case EventTimeSlidingGroupWindow(_, time, size, slide)
          if isTimeInterval(time.resultType) && doAllSupportPartialMerge(aggregates) =>
        // pre-tumble incremental aggregates on time-windows
        val timeFieldPos = getTimeFieldPosition(time, inputType, isParserCaseSensitive)
        val preTumblingSize = determineLargestTumblingSize(asLong(size), asLong(slide))
        (timeFieldPos, Some(preTumblingSize))

      case EventTimeSlidingGroupWindow(_, time, _, _) =>
        val timeFieldPos = getTimeFieldPosition(time, inputType, isParserCaseSensitive)
        (timeFieldPos, None)

      case _ =>
        throw new UnsupportedOperationException(s"$window is currently not supported on batch")
    }

    new DataSetWindowAggMapFunction(
      aggregates,
      aggFieldIndexes,
      groupings,
      timeFieldPos,
      tumbleTimeWindowSize,
      mapReturnType)
  }

  /**
    * Create a [[org.apache.flink.api.common.functions.GroupReduceFunction]] that prepares for
    * partial aggregates of sliding windows (time and count-windows).
    * It requires a prepared input (with intermediate aggregate fields and aligned rowtime for
    * pre-tumbling in case of time-windows), pre-aggregates (pre-tumbles) rows, aligns the
    * window-start, and replicates or omits records for different panes of a sliding window.
    *
    * The output of the function contains the grouping keys, the intermediate aggregate values of
    * all aggregate function and the aligned window start. Window start must not be a timestamp,
    * but can also be a count value for count-windows.
    *
    * The output is stored in Row by the following format:
    *
    * {{{
    *                      avg(x) aggOffsetInRow = 2      count(z) aggOffsetInRow = 5
    *                            |                          |
    *                            v                          v
    *        +---------+---------+--------+--------+--------+--------+-------------+
    *        |groupKey1|groupKey2|  sum1  | count1 |  sum2  | count2 | windowStart |
    *        +---------+---------+--------+--------+--------+--------+-------------+
    *                                              ^                 ^
    *                                              |                 |
    *                                 sum(y) aggOffsetInRow = 4    window start for pane mapping
    * }}}
    *
    * NOTE: this function is only used for sliding windows with partial aggregates on batch tables.
    */
  def createDataSetSlideWindowPrepareGroupReduceFunction(
      window: LogicalWindow,
      namedAggregates: Seq[CalcitePair[AggregateCall, String]],
      groupings: Array[Int],
      inputType: RelDataType,
      isParserCaseSensitive: Boolean)
    : RichGroupReduceFunction[Row, Row] = {

    val aggregates = transformToAggregateFunctions(
      namedAggregates.map(_.getKey),
      inputType,
      needRetraction = false)._2

    val returnType: RowTypeInfo = createDataSetAggregateBufferDataType(
      groupings,
      aggregates,
      inputType,
      Some(Array(BasicTypeInfo.LONG_TYPE_INFO)))

    window match {
      case EventTimeSlidingGroupWindow(_, _, size, slide) if isTimeInterval(size.resultType) =>
        // sliding time-window for partial aggregations
        new DataSetSlideTimeWindowAggReduceGroupFunction(
          aggregates,
          groupings.length,
          returnType.getArity - 1,
          asLong(size),
          asLong(slide),
          returnType)

      case _ =>
        throw new UnsupportedOperationException(s"$window is currently not supported on batch.")
    }
  }

  /**
    * Create a [[org.apache.flink.api.common.functions.FlatMapFunction]] that prepares for
    * non-incremental aggregates of sliding windows (time-windows).
    *
    * It requires a prepared input (with intermediate aggregate fields), aligns the
    * window-start, and replicates or omits records for different panes of a sliding window.
    *
    * The output of the function contains the grouping keys, the intermediate aggregate values of
    * all aggregate function and the aligned window start.
    *
    * The output is stored in Row by the following format:
    *
    * {{{
    *                      avg(x) aggOffsetInRow = 2      count(z) aggOffsetInRow = 5
    *                            |                          |
    *                            v                          v
    *        +---------+---------+--------+--------+--------+--------+-------------+
    *        |groupKey1|groupKey2|  sum1  | count1 |  sum2  | count2 | windowStart |
    *        +---------+---------+--------+--------+--------+--------+-------------+
    *                                              ^                 ^
    *                                              |                 |
    *                                 sum(y) aggOffsetInRow = 4      window start for pane mapping
    * }}}
    *
    * NOTE: this function is only used for time-based sliding windows on batch tables.
    */
  def createDataSetSlideWindowPrepareFlatMapFunction(
      window: LogicalWindow,
      namedAggregates: Seq[CalcitePair[AggregateCall, String]],
      groupings: Array[Int],
      inputType: TypeInformation[Row],
      isParserCaseSensitive: Boolean)
    : FlatMapFunction[Row, Row] = {

    window match {
      case EventTimeSlidingGroupWindow(_, _, size, slide) if isTimeInterval(size.resultType) =>
        new DataSetSlideTimeWindowAggFlatMapFunction(
          inputType.getArity - 1,
          asLong(size),
          asLong(slide),
          inputType)

      case _ =>
        throw new UnsupportedOperationException(
          s"$window is currently not supported in a batch environment.")
    }
  }

  /**
    * Create a [[org.apache.flink.api.common.functions.GroupReduceFunction]] to compute window
    * aggregates on batch tables. If all aggregates support partial aggregation and is a time
    * window, the [[org.apache.flink.api.common.functions.GroupReduceFunction]] implements
    * [[org.apache.flink.api.common.functions.CombineFunction]] as well.
    *
    * NOTE: this function is only used for window on batch tables.
    */
  def createDataSetWindowAggregationGroupReduceFunction(
      window: LogicalWindow,
      namedAggregates: Seq[CalcitePair[AggregateCall, String]],
      inputType: RelDataType,
      outputType: RelDataType,
      groupings: Array[Int],
      properties: Seq[NamedWindowProperty],
      isInputCombined: Boolean = false)
    : RichGroupReduceFunction[Row, Row] = {

    val (aggFieldIndexes, aggregates) = transformToAggregateFunctions(
      namedAggregates.map(_.getKey),
      inputType,
      needRetraction = false)

    // the mapping relation between field index of intermediate aggregate Row and output Row.
    val groupingOffsetMapping = getGroupKeysMapping(inputType, outputType, groupings)

    // the mapping relation between aggregate function index in list and its corresponding
    // field index in output Row.
    val aggOffsetMapping = getAggregateMapping(namedAggregates, outputType)

    if (groupingOffsetMapping.length != groupings.length ||
      aggOffsetMapping.length != namedAggregates.length) {
      throw new TableException(
        "Could not find output field in input data type " +
          "or aggregate functions.")
    }

    window match {
      case EventTimeTumblingGroupWindow(_, _, size) if isTimeInterval(size.resultType) =>
        // tumbling time window
        val (startPos, endPos) = computeWindowStartEndPropertyPos(properties)
        if (doAllSupportPartialMerge(aggregates)) {
          // for incremental aggregations
          new DataSetTumbleTimeWindowAggReduceCombineFunction(
            asLong(size),
            startPos,
            endPos,
            aggregates,
            groupingOffsetMapping,
            aggOffsetMapping,
            outputType.getFieldCount)
        }
        else {
          // for non-incremental aggregations
          new DataSetTumbleTimeWindowAggReduceGroupFunction(
            asLong(size),
            startPos,
            endPos,
            aggregates,
            groupingOffsetMapping,
            aggOffsetMapping,
            outputType.getFieldCount)
        }
      case EventTimeTumblingGroupWindow(_, _, size) =>
        // tumbling count window
        new DataSetTumbleCountWindowAggReduceGroupFunction(
          asLong(size),
          aggregates,
          groupingOffsetMapping,
          aggOffsetMapping,
          outputType.getFieldCount)

      case EventTimeSessionGroupWindow(_, _, gap) =>
        val (startPos, endPos) = computeWindowStartEndPropertyPos(properties)
        new DataSetSessionWindowAggReduceGroupFunction(
          aggregates,
          groupingOffsetMapping,
          aggOffsetMapping,
          outputType.getFieldCount,
          startPos,
          endPos,
          asLong(gap),
          isInputCombined)

      case EventTimeSlidingGroupWindow(_, _, size, _) if isTimeInterval(size.resultType) =>
        val (startPos, endPos) = computeWindowStartEndPropertyPos(properties)
        if (doAllSupportPartialMerge(aggregates)) {
          // for partial aggregations
          new DataSetSlideWindowAggReduceCombineFunction(
            aggregates,
            groupingOffsetMapping,
            aggOffsetMapping,
            outputType.getFieldCount,
            startPos,
            endPos,
            asLong(size))
        }
        else {
          // for non-partial aggregations
          new DataSetSlideWindowAggReduceGroupFunction(
            aggregates,
            groupingOffsetMapping,
            aggOffsetMapping,
            outputType.getFieldCount,
            startPos,
            endPos,
            asLong(size))
        }

      case EventTimeSlidingGroupWindow(_, _, size, _) =>
        new DataSetSlideWindowAggReduceGroupFunction(
            aggregates,
            groupingOffsetMapping,
            aggOffsetMapping,
            outputType.getFieldCount,
            None,
            None,
            asLong(size))

      case _ =>
        throw new UnsupportedOperationException(s"$window is currently not supported on batch")
    }
  }

  /**
    * Create a [[org.apache.flink.api.common.functions.MapPartitionFunction]] that aggregation
    * for aggregates.
    * The function returns aggregate values of all aggregate function which are
    * organized by the following format:
    *
    * {{{
    *       avg(x) aggOffsetInRow = 2  count(z) aggOffsetInRow = 5
    *           |                          |          windowEnd(max(rowtime)
    *           |                          |                   |
    *           v                          v                   v
    *        +--------+--------+--------+--------+-----------+---------+
    *        |  sum1  | count1 |  sum2  | count2 |windowStart|windowEnd|
    *        +--------+--------+--------+--------+-----------+---------+
    *                               ^                 ^
    *                               |                 |
    *             sum(y) aggOffsetInRow = 4    windowStart(min(rowtime))
    *
    * }}}
    *
    */
  def createDataSetWindowAggregationMapPartitionFunction(
    window: LogicalWindow,
    namedAggregates: Seq[CalcitePair[AggregateCall, String]],
    inputType: RelDataType,
    groupings: Array[Int]): MapPartitionFunction[Row, Row] = {

    val aggregates = transformToAggregateFunctions(
      namedAggregates.map(_.getKey),
      inputType,
      needRetraction = false)._2

    window match {
      case EventTimeSessionGroupWindow(_, _, gap) =>
        val combineReturnType: RowTypeInfo =
          createDataSetAggregateBufferDataType(
            groupings,
            aggregates,
            inputType,
            Option(Array(BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO)))

        new DataSetSessionWindowAggregatePreProcessor(
          aggregates,
          groupings,
          asLong(gap),
          combineReturnType)
      case _ =>
        throw new UnsupportedOperationException(s"$window is currently not supported on batch")
    }
  }

  /**
    * Create a [[org.apache.flink.api.common.functions.GroupCombineFunction]] that pre-aggregation
    * for aggregates.
    * The function returns intermediate aggregate values of all aggregate function which are
    * organized by the following format:
    * {{{
    *                      avg(x)                           windowEnd(max(rowtime)
    *                       |                                 |
    *                       v                                 v
    *   +---------+---------+----------------+----------------+-------------+-----------+
    *   |groupKey1|groupKey2| AvgAccumulator | SumAccumulator | windowStart | windowEnd |
    *   +---------+---------+----------------+----------------+-------------+-----------+
    *                                        ^                              ^
    *                                        |                              |
    *                                       sum(y)                       windowStart(min(rowtime))
    * }}}
    *
    */
  private[flink] def createDataSetWindowAggregationCombineFunction(
      window: LogicalWindow,
      namedAggregates: Seq[CalcitePair[AggregateCall, String]],
      inputType: RelDataType,
      groupings: Array[Int])
    : GroupCombineFunction[Row, Row] = {

    val aggregates = transformToAggregateFunctions(
      namedAggregates.map(_.getKey),
      inputType,
      needRetraction = false)._2

    window match {

      case EventTimeSessionGroupWindow(_, _, gap) =>
        val combineReturnType: RowTypeInfo =
          createDataSetAggregateBufferDataType(
            groupings,
            aggregates,
            inputType,
            Option(Array(BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO)))

        new DataSetSessionWindowAggregatePreProcessor(
          aggregates,
          groupings,
          asLong(gap),
          combineReturnType)

      case _ =>
        throw new UnsupportedOperationException(
          s" [ ${window.getClass.getCanonicalName.split("\\.").last} ] is currently not " +
            s"supported on batch")
    }
  }

  /**
    * Create functions to compute a [[org.apache.flink.table.plan.nodes.dataset.DataSetAggregate]].
    * If all aggregation functions support pre-aggregation, a pre-aggregation function and the
    * respective output type are generated as well.
    */
  private[flink] def createDataSetAggregateFunctions(
      namedAggregates: Seq[CalcitePair[AggregateCall, String]],
      inputType: RelDataType,
      outputType: RelDataType,
      groupings: Array[Int],
      inGroupingSet: Boolean): (Option[DataSetPreAggFunction],
        Option[TypeInformation[Row]],
        RichGroupReduceFunction[Row, Row]) = {

    val (aggInFields, aggregates) = transformToAggregateFunctions(
      namedAggregates.map(_.getKey),
      inputType,
      needRetraction = false)

    val (gkeyOutMapping, aggOutMapping) = getOutputMappings(
      namedAggregates,
      groupings,
      inputType,
      outputType
    )

    val groupingSetsMapping: Array[(Int, Int)] = if (inGroupingSet) {
      getGroupingSetsIndicatorMapping(inputType, outputType)
    } else {
      Array()
    }

    if (doAllSupportPartialMerge(aggregates)) {

      // compute grouping key and aggregation positions
      val gkeyInFields = gkeyOutMapping.map(_._2)
      val gkeyOutFields = gkeyOutMapping.map(_._1)
      val aggOutFields = aggOutMapping.map(_._1)

      // compute preaggregation type
      val preAggFieldTypes = gkeyInFields
        .map(inputType.getFieldList.get(_).getType)
        .map(FlinkTypeFactory.toTypeInfo) ++ createAccumulatorType(aggregates)
      val preAggRowType = new RowTypeInfo(preAggFieldTypes: _*)

      (
        Some(new DataSetPreAggFunction(
          aggregates,
          aggInFields,
          gkeyInFields
        )),
        Some(preAggRowType),
        new DataSetFinalAggFunction(
          aggregates,
          aggOutFields,
          gkeyOutFields,
          groupingSetsMapping,
          outputType.getFieldCount)
      )
    }
    else {
      (
        None,
        None,
        new DataSetAggFunction(
          aggregates,
          aggInFields,
          aggOutMapping,
          gkeyOutMapping,
          groupingSetsMapping,
          outputType.getFieldCount)
      )
    }

  }

  /**
    * Create an [[AllWindowFunction]] for non-partitioned window aggregates.
    */
  private[flink] def createAggregationAllWindowFunction(
      window: LogicalWindow,
      finalRowArity: Int,
      properties: Seq[NamedWindowProperty])
    : AllWindowFunction[Row, Row, DataStreamWindow] = {

    if (isTimeWindow(window)) {
      val (startPos, endPos) = computeWindowStartEndPropertyPos(properties)
      new IncrementalAggregateAllTimeWindowFunction(
        startPos,
        endPos,
        finalRowArity)
        .asInstanceOf[AllWindowFunction[Row, Row, DataStreamWindow]]
    } else {
      new IncrementalAggregateAllWindowFunction(
        finalRowArity)
    }
  }

  /**
    * Create a [[WindowFunction]] for group window aggregates.
    */
  private[flink] def createAggregationGroupWindowFunction(
      window: LogicalWindow,
      numGroupingKeys: Int,
      numAggregates: Int,
      finalRowArity: Int,
      properties: Seq[NamedWindowProperty])
    : WindowFunction[Row, Row, Tuple, DataStreamWindow] = {

    if (isTimeWindow(window)) {
      val (startPos, endPos) = computeWindowStartEndPropertyPos(properties)
      new IncrementalAggregateTimeWindowFunction(
        numGroupingKeys,
        numAggregates,
        startPos,
        endPos,
        finalRowArity)
        .asInstanceOf[WindowFunction[Row, Row, Tuple, DataStreamWindow]]
    } else {
      new IncrementalAggregateWindowFunction(
        numGroupingKeys,
        numAggregates,
        finalRowArity)
    }
  }

  private[flink] def createDataStreamAggregateFunction(
      namedAggregates: Seq[CalcitePair[AggregateCall, String]],
      inputType: RelDataType,
      outputType: RelDataType,
      groupKeysIndex: Array[Int])
    : (DataStreamAggFunction[Row, Row, Row], RowTypeInfo, RowTypeInfo) = {

    val (aggFields, aggregates) =
      transformToAggregateFunctions(
        namedAggregates.map(_.getKey),
        inputType,
        needRetraction = false)

    val aggregateMapping = getAggregateMapping(namedAggregates, outputType)

    if (aggregateMapping.length != namedAggregates.length) {
      throw new TableException(
        "Could not find output field in input data type or aggregate functions.")
    }

    val aggResultTypes = namedAggregates.map(a => FlinkTypeFactory.toTypeInfo(a.left.getType))

    val accumulatorRowType = createAccumulatorRowType(aggregates)
    val aggResultRowType = new RowTypeInfo(aggResultTypes: _*)
    val aggFunction = new AggregateAggFunction(aggregates, aggFields)

    (aggFunction, accumulatorRowType, aggResultRowType)
  }

  /**
    * Return true if all aggregates can be partially merged. False otherwise.
    */
  private[flink] def doAllSupportPartialMerge(
    aggregateCalls: Seq[AggregateCall],
    inputType: RelDataType,
    groupKeysCount: Int): Boolean = {

    val aggregateList = transformToAggregateFunctions(
      aggregateCalls,
      inputType,
      needRetraction = false)._2

    doAllSupportPartialMerge(aggregateList)
  }

  /**
    * Return true if all aggregates can be partially merged. False otherwise.
    */
  private[flink] def doAllSupportPartialMerge(
      aggregateList: Array[TableAggregateFunction[_ <: Any]]): Boolean = {
    aggregateList.forall(ifMethodExistInFunction("merge", _))
  }

  /**
    * @return A mappings of field positions from input type to output type for grouping keys and
    *         aggregates.
    */
  private def getOutputMappings(
      namedAggregates: Seq[CalcitePair[AggregateCall, String]],
      groupings: Array[Int],
      inputType: RelDataType,
      outputType: RelDataType) : (Array[(Int, Int)], Array[(Int, Int)]) = {

    val groupKeyNames: Seq[(String, Int)] =
      groupings.map(g => (inputType.getFieldList.get(g).getName, g))
    val aggNames: Seq[(String, Int)] =
      namedAggregates.zipWithIndex.map(a => (a._1.right, a._2))

    val groupOutMapping: Array[(Int, Int)] =
      groupKeyNames.map(g => (outputType.getField(g._1, false, false).getIndex, g._2)).toArray
    val aggOutMapping: Array[(Int, Int)] =
      aggNames.map(a => (outputType.getField(a._1, false, false).getIndex, a._2)).toArray

    (groupOutMapping, aggOutMapping)
  }

  /**
    * Determines the mapping of grouping keys to boolean indicators that describe the
    * current grouping set.
    *
    * E.g.: Given we group on f1 and f2 of the input type, the output type contains two
    * boolean indicator fields i$f1 and i$f2.
    */
  private def getGroupingSetsIndicatorMapping(
    inputType: RelDataType,
    outputType: RelDataType): Array[(Int, Int)] = {

    val inputFields = inputType.getFieldList.map(_.getName)

    // map from field -> i$field or field -> i$field_0
    val groupingFields = inputFields.map(inputFieldName => {
      val base = "i$" + inputFieldName
      var name = base
      var i = 0
      while (inputFields.contains(name)) {
          name = base + "_" + i // if i$XXX is already a field it will be suffixed by _NUMBER
          i = i + 1
        }
        inputFieldName -> name
      }).toMap

    val outputFields = outputType.getFieldList

    var mappingsBuffer = ArrayBuffer[(Int, Int)]()
    for (i <- outputFields.indices) {
      for (j <- outputFields.indices) {
        val possibleKey = outputFields(i).getName
        val possibleIndicator1 = outputFields(j).getName
        // get indicator for output field
        val possibleIndicator2 = groupingFields.getOrElse(possibleKey, null)

        // check if indicator names match
        if (possibleIndicator1 == possibleIndicator2) {
          mappingsBuffer += ((i, j))
        }
      }
    }
    mappingsBuffer.toArray
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

  private[flink] def computeWindowStartEndPropertyPos(
      properties: Seq[NamedWindowProperty]): (Option[Int], Option[Int]) = {

    val propPos = properties.foldRight((None: Option[Int], None: Option[Int], 0)) {
      (p, x) => p match {
        case NamedWindowProperty(_, prop) =>
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

  private def transformToAggregateFunctions(
      aggregateCalls: Seq[AggregateCall],
      inputType: RelDataType,
      needRetraction: Boolean)
  : (Array[Array[Int]], Array[TableAggregateFunction[_ <: Any]]) = {

    // store the aggregate fields of each aggregate function, by the same order of aggregates.
    val aggFieldIndexes = new Array[Array[Int]](aggregateCalls.size)
    val aggregates = new Array[TableAggregateFunction[_ <: Any]](aggregateCalls.size)

    // create aggregate function instances by function type and aggregate field data type.
    aggregateCalls.zipWithIndex.foreach { case (aggregateCall, index) =>
      val argList: util.List[Integer] = aggregateCall.getArgList
      if (argList.isEmpty) {
        if (aggregateCall.getAggregation.isInstanceOf[SqlCountAggFunction]) {
          aggFieldIndexes(index) = Array[Int](0)
        } else {
          throw new TableException("Aggregate fields should not be empty.")
        }
      } else {
        if (argList.size() > 1) {
          throw new TableException("Currently, do not support aggregate on multi fields.")
        }
        aggFieldIndexes(index) = argList.asScala.map(i => i.intValue).toArray
      }
      val sqlTypeName = inputType.getFieldList.get(aggFieldIndexes(index)(0)).getType
        .getSqlTypeName
      aggregateCall.getAggregation match {

        case _: SqlSumAggFunction | _: SqlSumEmptyIsZeroAggFunction =>
          if (needRetraction) {
            aggregates(index) = sqlTypeName match {
              case TINYINT =>
                new ByteSumWithRetractAggFunction
              case SMALLINT =>
                new ShortSumWithRetractAggFunction
              case INTEGER =>
                new IntSumWithRetractAggFunction
              case BIGINT =>
                new LongSumWithRetractAggFunction
              case FLOAT =>
                new FloatSumWithRetractAggFunction
              case DOUBLE =>
                new DoubleSumWithRetractAggFunction
              case DECIMAL =>
                new DecimalSumWithRetractAggFunction
              case sqlType: SqlTypeName =>
                throw new TableException("Sum aggregate does no support type:" + sqlType)
            }
          } else {
            aggregates(index) = sqlTypeName match {
              case TINYINT =>
                new ByteSumAggFunction
              case SMALLINT =>
                new ShortSumAggFunction
              case INTEGER =>
                new IntSumAggFunction
              case BIGINT =>
                new LongSumAggFunction
              case FLOAT =>
                new FloatSumAggFunction
              case DOUBLE =>
                new DoubleSumAggFunction
              case DECIMAL =>
                new DecimalSumAggFunction
              case sqlType: SqlTypeName =>
                throw new TableException("Sum aggregate does no support type:" + sqlType)
            }
          }

        case _: SqlAvgAggFunction =>
          aggregates(index) = sqlTypeName match {
            case TINYINT =>
              new ByteAvgAggFunction
            case SMALLINT =>
              new ShortAvgAggFunction
            case INTEGER =>
              new IntAvgAggFunction
            case BIGINT =>
              new LongAvgAggFunction
            case FLOAT =>
              new FloatAvgAggFunction
            case DOUBLE =>
              new DoubleAvgAggFunction
            case DECIMAL =>
              new DecimalAvgAggFunction
            case sqlType: SqlTypeName =>
              throw new TableException("Avg aggregate does no support type:" + sqlType)
          }

        case sqlMinMaxFunction: SqlMinMaxAggFunction =>
          aggregates(index) = if (sqlMinMaxFunction.getKind == SqlKind.MIN) {
            if (needRetraction) {
              sqlTypeName match {
                case TINYINT =>
                  new ByteMinWithRetractAggFunction
                case SMALLINT =>
                  new ShortMinWithRetractAggFunction
                case INTEGER =>
                  new IntMinWithRetractAggFunction
                case BIGINT =>
                  new LongMinWithRetractAggFunction
                case FLOAT =>
                  new FloatMinWithRetractAggFunction
                case DOUBLE =>
                  new DoubleMinWithRetractAggFunction
                case DECIMAL =>
                  new DecimalMinWithRetractAggFunction
                case BOOLEAN =>
                  new BooleanMinWithRetractAggFunction
                case VARCHAR | CHAR =>
                  new StringMinWithRetractAggFunction
                case sqlType: SqlTypeName =>
                  throw new TableException("Min with retract aggregate does no support type:" +
                                             sqlType)
              }
            } else {
              sqlTypeName match {
                case TINYINT =>
                  new ByteMinAggFunction
                case SMALLINT =>
                  new ShortMinAggFunction
                case INTEGER =>
                  new IntMinAggFunction
                case BIGINT =>
                  new LongMinAggFunction
                case FLOAT =>
                  new FloatMinAggFunction
                case DOUBLE =>
                  new DoubleMinAggFunction
                case DECIMAL =>
                  new DecimalMinAggFunction
                case BOOLEAN =>
                  new BooleanMinAggFunction
                case VARCHAR | CHAR =>
                  new StringMinAggFunction
                case sqlType: SqlTypeName =>
                  throw new TableException("Min aggregate does no support type:" + sqlType)
              }
            }
          } else {
            if (needRetraction) {
              sqlTypeName match {
                case TINYINT =>
                  new ByteMaxWithRetractAggFunction
                case SMALLINT =>
                  new ShortMaxWithRetractAggFunction
                case INTEGER =>
                  new IntMaxWithRetractAggFunction
                case BIGINT =>
                  new LongMaxWithRetractAggFunction
                case FLOAT =>
                  new FloatMaxWithRetractAggFunction
                case DOUBLE =>
                  new DoubleMaxWithRetractAggFunction
                case DECIMAL =>
                  new DecimalMaxWithRetractAggFunction
                case BOOLEAN =>
                  new BooleanMaxWithRetractAggFunction
                case VARCHAR | CHAR =>
                  new StringMaxWithRetractAggFunction
                case sqlType: SqlTypeName =>
                  throw new TableException("Max with retract aggregate does no support type:" +
                                             sqlType)
              }
            } else {
              sqlTypeName match {
                case TINYINT =>
                  new ByteMaxAggFunction
                case SMALLINT =>
                  new ShortMaxAggFunction
                case INTEGER =>
                  new IntMaxAggFunction
                case BIGINT =>
                  new LongMaxAggFunction
                case FLOAT =>
                  new FloatMaxAggFunction
                case DOUBLE =>
                  new DoubleMaxAggFunction
                case DECIMAL =>
                  new DecimalMaxAggFunction
                case BOOLEAN =>
                  new BooleanMaxAggFunction
                case VARCHAR | CHAR =>
                  new StringMaxAggFunction
                case sqlType: SqlTypeName =>
                  throw new TableException("Max aggregate does no support type:" + sqlType)
              }
            }
          }

        case _: SqlCountAggFunction =>
          aggregates(index) = new CountAggFunction

        case unSupported: SqlAggFunction =>
          throw new TableException("unsupported Function: " + unSupported.getName)
      }
    }

    (aggFieldIndexes, aggregates)
  }

  private def createAccumulatorType(
      aggregates: Array[TableAggregateFunction[_]]): Seq[TypeInformation[_]] = {

    val aggTypes: Seq[TypeInformation[_]] =
      aggregates.map {
        agg =>
          val accType = agg.getAccumulatorType
          if (accType != null) {
            accType
          } else {
            val accumulator = agg.createAccumulator()
            try {
              TypeInformation.of(accumulator.getClass)
            } catch {
              case ite: InvalidTypesException =>
                throw new TableException(
                  "Cannot infer type of accumulator. " +
                    "You can override AggregateFunction.getAccumulatorType() to specify the type.",
                  ite)
            }
          }
      }

    aggTypes
  }

  private def createDataSetAggregateBufferDataType(
      groupings: Array[Int],
      aggregates: Array[TableAggregateFunction[_]],
      inputType: RelDataType,
      windowKeyTypes: Option[Array[TypeInformation[_]]] = None): RowTypeInfo = {

    // get the field data types of group keys.
    val groupingTypes: Seq[TypeInformation[_]] =
      groupings
        .map(inputType.getFieldList.get(_).getType)
        .map(FlinkTypeFactory.toTypeInfo)

    // get all field data types of all intermediate aggregates
    val aggTypes: Seq[TypeInformation[_]] = createAccumulatorType(aggregates)

    // concat group key types, aggregation types, and window key types
    val allFieldTypes: Seq[TypeInformation[_]] = windowKeyTypes match {
      case None => groupingTypes ++: aggTypes
      case _ => groupingTypes ++: aggTypes ++: windowKeyTypes.get
    }
    new RowTypeInfo(allFieldTypes: _*)
  }

  private[flink] def createAccumulatorRowType(
      aggregates: Array[TableAggregateFunction[_]]): RowTypeInfo = {

    val aggTypes: Seq[TypeInformation[_]] = createAccumulatorType(aggregates)

    new RowTypeInfo(aggTypes: _*)
  }

  // Find the mapping between the index of aggregate list and aggregated value index in output Row.
  private def getAggregateMapping(
    namedAggregates: Seq[CalcitePair[AggregateCall, String]],
    outputType: RelDataType): Array[(Int, Int)] = {

    // the mapping relation between aggregate function index in list and its corresponding
    // field index in output Row.
    var aggOffsetMapping = ArrayBuffer[(Int, Int)]()

    outputType.getFieldList.zipWithIndex.foreach {
      case (outputFieldType, outputIndex) =>
        namedAggregates.zipWithIndex.foreach {
          case (namedAggCall, aggregateIndex) =>
            if (namedAggCall.getValue.equals(outputFieldType.getName) &&
              namedAggCall.getKey.getType.equals(outputFieldType.getType)) {
              aggOffsetMapping += ((outputIndex, aggregateIndex))
            }
        }
    }

    aggOffsetMapping.toArray
  }

  // Find the mapping between the index of group key in intermediate aggregate Row and its index
  // in output Row.
  private def getGroupKeysMapping(
    inputDatType: RelDataType,
    outputType: RelDataType,
    groupKeys: Array[Int]): Array[(Int, Int)] = {

    // the mapping relation between field index of intermediate aggregate Row and output Row.
    var groupingOffsetMapping = ArrayBuffer[(Int, Int)]()

    outputType.getFieldList.zipWithIndex.foreach {
      case (outputFieldType, outputIndex) =>
        inputDatType.getFieldList.zipWithIndex.foreach {
          // find the field index in input data type.
          case (inputFieldType, inputIndex) =>
            if (outputFieldType.getName.equals(inputFieldType.getName) &&
              outputFieldType.getType.equals(inputFieldType.getType)) {
              // as aggregated field in output data type would not have a matched field in
              // input data, so if inputIndex is not -1, it must be a group key. Then we can
              // find the field index in buffer data by the group keys index mapping between
              // input data and buffer data.
              for (i <- groupKeys.indices) {
                if (inputIndex == groupKeys(i)) {
                  groupingOffsetMapping += ((outputIndex, i))
                }
              }
            }
        }
    }

    groupingOffsetMapping.toArray
  }

  private def getTimeFieldPosition(
    timeField: Expression,
    inputType: RelDataType,
    isParserCaseSensitive: Boolean): Int = {

    timeField match {
      case ResolvedFieldReference(name, _) =>
        // get the RelDataType referenced by the time-field
        val relDataType = inputType.getFieldList.filter { r =>
          if (isParserCaseSensitive) {
            name.equals(r.getName)
          } else {
            name.equalsIgnoreCase(r.getName)
          }
        }
        // should only match one
        if (relDataType.length == 1) {
          relDataType.head.getIndex
        } else {
          throw TableException(
            s"Encountered more than one time attribute with the same name: $relDataType")
        }
      case e => throw TableException(
        "The time attribute of window in batch environment should be " +
          s"ResolvedFieldReference, but is $e")
    }
  }

  private[flink] def asLong(expr: Expression): Long = expr match {
    case Literal(value: Long, TimeIntervalTypeInfo.INTERVAL_MILLIS) => value
    case Literal(value: Long, RowIntervalTypeInfo.INTERVAL_ROWS) => value
    case _ => throw new IllegalArgumentException()
  }

  private[flink] def determineLargestTumblingSize(size: Long, slide: Long): Long = {
    if (slide > size) {
      gcd(slide, size)
    } else {
      gcd(size, slide)
    }
  }

  private def gcd(a: Long, b: Long): Long = {
    if (b == 0) a else gcd(b, a % b)
  }
}
