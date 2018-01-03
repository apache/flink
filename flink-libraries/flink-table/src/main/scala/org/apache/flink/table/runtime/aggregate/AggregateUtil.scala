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
import org.apache.flink.api.common.functions.{MapFunction, RichGroupReduceFunction, AggregateFunction => DataStreamAggFunction, _}
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.windowing.{AllWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.windows.{Window => DataStreamWindow}
import org.apache.flink.table.api.dataview.DataViewSpec
import org.apache.flink.table.api.{StreamQueryConfig, TableException}
import org.apache.flink.table.calcite.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.AggregationCodeGenerator
import org.apache.flink.table.expressions.ExpressionUtils.isTimeIntervalLiteral
import org.apache.flink.table.expressions._
import org.apache.flink.table.functions.aggfunctions._
import org.apache.flink.table.functions.utils.AggSqlFunction
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils._
import org.apache.flink.table.functions.{AggregateFunction => TableAggregateFunction}
import org.apache.flink.table.plan.logical._
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
import org.apache.flink.table.typeutils.TypeCheckUtils._
import org.apache.flink.table.typeutils.{RowIntervalTypeInfo, TimeIntervalTypeInfo}
import org.apache.flink.types.Row

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object AggregateUtil {

  type CalcitePair[T, R] = org.apache.calcite.util.Pair[T, R]
  type JavaList[T] = java.util.List[T]

  /**
    * Create an [[org.apache.flink.streaming.api.functions.ProcessFunction]] for unbounded OVER
    * window to evaluate final aggregate value.
    *
    * @param generator       code generator instance
    * @param namedAggregates Physical calls to aggregate functions and their output field names
    * @param aggregateInputType Physical type of the aggregate functions's input row.
    * @param inputType Physical type of the row.
    * @param inputTypeInfo Physical type information of the row.
    * @param inputFieldTypeInfo Physical type information of the row's fields.
    * @param rowTimeIdx The index of the rowtime field or None in case of processing time.
    * @param isPartitioned It is a tag that indicate whether the input is partitioned
    * @param isRowsClause It is a tag that indicates whether the OVER clause is ROWS clause
    */
  private[flink] def createUnboundedOverProcessFunction(
      generator: AggregationCodeGenerator,
      namedAggregates: Seq[CalcitePair[AggregateCall, String]],
      aggregateInputType: RelDataType,
      inputType: RelDataType,
      inputTypeInfo: TypeInformation[Row],
      inputFieldTypeInfo: Seq[TypeInformation[_]],
      queryConfig: StreamQueryConfig,
      rowTimeIdx: Option[Int],
      isPartitioned: Boolean,
      isRowsClause: Boolean)
    : ProcessFunction[CRow, CRow] = {

    val (aggFields, aggregates, accTypes, accSpecs) =
      transformToAggregateFunctions(
        namedAggregates.map(_.getKey),
        aggregateInputType,
        needRetraction = false,
        isStateBackedDataViews = true)

    val aggregationStateType: RowTypeInfo = new RowTypeInfo(accTypes: _*)

    val forwardMapping = (0 until inputType.getFieldCount).toArray
    val aggMapping = aggregates.indices.map(x => x + inputType.getFieldCount).toArray
    val outputArity = inputType.getFieldCount + aggregates.length

    val genFunction = generator.generateAggregations(
      "UnboundedProcessingOverAggregateHelper",
      inputFieldTypeInfo,
      aggregates,
      aggFields,
      aggMapping,
      partialResults = false,
      forwardMapping,
      None,
      outputArity,
      needRetract = false,
      needMerge = false,
      needReset = false,
      accConfig = Some(accSpecs)
    )

    if (rowTimeIdx.isDefined) {
      if (isRowsClause) {
        // ROWS unbounded over process function
        new RowTimeUnboundedRowsOver(
          genFunction,
          aggregationStateType,
          CRowTypeInfo(inputTypeInfo),
          rowTimeIdx.get,
          queryConfig)
      } else {
        // RANGE unbounded over process function
        new RowTimeUnboundedRangeOver(
          genFunction,
          aggregationStateType,
          CRowTypeInfo(inputTypeInfo),
          rowTimeIdx.get,
          queryConfig)
      }
    } else {
      new ProcTimeUnboundedOver(
        genFunction,
        aggregationStateType,
        queryConfig)
    }
  }

  /**
    * Create an [[org.apache.flink.streaming.api.functions.ProcessFunction]] for group (without
    * window) aggregate to evaluate final aggregate value.
    *
    * @param generator       code generator instance
    * @param namedAggregates List of calls to aggregate functions and their output field names
    * @param inputRowType    Input row type
    * @param inputFieldTypes Types of the physical input fields
    * @param groupings       the position (in the input Row) of the grouping keys
    * @param queryConfig     The configuration of the query to generate.
    * @param generateRetraction It is a tag that indicates whether generate retract record.
    * @param consumeRetraction It is a tag that indicates whether consume the retract record.
    * @return [[org.apache.flink.streaming.api.functions.ProcessFunction]]
    */
  private[flink] def createGroupAggregateFunction(
      generator: AggregationCodeGenerator,
      namedAggregates: Seq[CalcitePair[AggregateCall, String]],
      inputRowType: RelDataType,
      inputFieldTypes: Seq[TypeInformation[_]],
      groupings: Array[Int],
      queryConfig: StreamQueryConfig,
      generateRetraction: Boolean,
      consumeRetraction: Boolean): ProcessFunction[CRow, CRow] = {

    val (aggFields, aggregates, accTypes, accSpecs) =
      transformToAggregateFunctions(
        namedAggregates.map(_.getKey),
        inputRowType,
        consumeRetraction,
        isStateBackedDataViews = true)

    val aggMapping = aggregates.indices.map(_ + groupings.length).toArray

    val outputArity = groupings.length + aggregates.length

    val aggregationStateType: RowTypeInfo = new RowTypeInfo(accTypes: _*)

    val genFunction = generator.generateAggregations(
      "NonWindowedAggregationHelper",
      inputFieldTypes,
      aggregates,
      aggFields,
      aggMapping,
      partialResults = false,
      groupings,
      None,
      outputArity,
      consumeRetraction,
      needMerge = false,
      needReset = false,
      accConfig = Some(accSpecs)
    )

    new GroupAggProcessFunction(
      genFunction,
      aggregationStateType,
      generateRetraction,
      queryConfig)

  }

  /**
    * Create an [[org.apache.flink.streaming.api.functions.ProcessFunction]] for ROWS clause
    * bounded OVER window to evaluate final aggregate value.
    *
    * @param generator       code generator instance
    * @param namedAggregates Physical calls to aggregate functions and their output field names
    * @param aggregateInputType Physical type of the aggregate functions's input row.
    * @param inputType Physical type of the row.
    * @param inputTypeInfo Physical type information of the row.
    * @param inputFieldTypeInfo Physical type information of the row's fields.
    * @param precedingOffset the preceding offset
    * @param isRowsClause    It is a tag that indicates whether the OVER clause is ROWS clause
    * @param rowTimeIdx      The index of the rowtime field or None in case of processing time.
    * @return [[org.apache.flink.streaming.api.functions.ProcessFunction]]
    */
  private[flink] def createBoundedOverProcessFunction(
      generator: AggregationCodeGenerator,
      namedAggregates: Seq[CalcitePair[AggregateCall, String]],
      aggregateInputType: RelDataType,
      inputType: RelDataType,
      inputTypeInfo: TypeInformation[Row],
      inputFieldTypeInfo: Seq[TypeInformation[_]],
      precedingOffset: Long,
      queryConfig: StreamQueryConfig,
      isRowsClause: Boolean,
      rowTimeIdx: Option[Int])
    : ProcessFunction[CRow, CRow] = {

    val needRetract = true
    val (aggFields, aggregates, accTypes, accSpecs) =
      transformToAggregateFunctions(
        namedAggregates.map(_.getKey),
        aggregateInputType,
        needRetract,
        isStateBackedDataViews = true)

    val aggregationStateType: RowTypeInfo = new RowTypeInfo(accTypes: _*)
    val inputRowType = CRowTypeInfo(inputTypeInfo)

    val forwardMapping = (0 until inputType.getFieldCount).toArray
    val aggMapping = aggregates.indices.map(x => x + inputType.getFieldCount).toArray
    val outputArity = inputType.getFieldCount + aggregates.length

    val genFunction = generator.generateAggregations(
      "BoundedOverAggregateHelper",
      inputFieldTypeInfo,
      aggregates,
      aggFields,
      aggMapping,
      partialResults = false,
      forwardMapping,
      None,
      outputArity,
      needRetract,
      needMerge = false,
      needReset = false,
      accConfig = Some(accSpecs)
    )

    if (rowTimeIdx.isDefined) {
      if (isRowsClause) {
        new RowTimeBoundedRowsOver(
          genFunction,
          aggregationStateType,
          inputRowType,
          precedingOffset,
          rowTimeIdx.get,
          queryConfig)
      } else {
        new RowTimeBoundedRangeOver(
          genFunction,
          aggregationStateType,
          inputRowType,
          precedingOffset,
          rowTimeIdx.get,
          queryConfig)
      }
    } else {
      if (isRowsClause) {
        new ProcTimeBoundedRowsOver(
          genFunction,
          precedingOffset,
          aggregationStateType,
          inputRowType,
          queryConfig)
      } else {
        new ProcTimeBoundedRangeOver(
          genFunction,
          precedingOffset,
          aggregationStateType,
          inputRowType,
          queryConfig)
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
    generator: AggregationCodeGenerator,
    window: LogicalWindow,
    namedAggregates: Seq[CalcitePair[AggregateCall, String]],
    groupings: Array[Int],
    inputType: RelDataType,
    inputFieldTypeInfo: Seq[TypeInformation[_]],
    isParserCaseSensitive: Boolean)
  : MapFunction[Row, Row] = {

    val needRetract = false
    val (aggFieldIndexes, aggregates, accTypes, _) = transformToAggregateFunctions(
      namedAggregates.map(_.getKey),
      inputType,
      needRetract)

    val mapReturnType: RowTypeInfo =
      createRowTypeForKeysAndAggregates(
        groupings,
        aggregates,
        accTypes,
        inputType,
        Some(Array(BasicTypeInfo.LONG_TYPE_INFO)))

    val (timeFieldPos, tumbleTimeWindowSize) = window match {

      case TumblingGroupWindow(_, time, size) =>
        val timeFieldPos = getTimeFieldPosition(time, inputType, isParserCaseSensitive)
        size match {
          case Literal(value: Long, TimeIntervalTypeInfo.INTERVAL_MILLIS) =>
            (timeFieldPos, Some(value))
          case _ => (timeFieldPos, None)
        }

      case SessionGroupWindow(_, time, _) =>
        (getTimeFieldPosition(time, inputType, isParserCaseSensitive), None)

      case SlidingGroupWindow(_, time, size, slide) =>
        val timeFieldPos = getTimeFieldPosition(time, inputType, isParserCaseSensitive)
        size match {
          case Literal(_: Long, TimeIntervalTypeInfo.INTERVAL_MILLIS) =>
            // pre-tumble incremental aggregates on time-windows
            val timeFieldPos = getTimeFieldPosition(time, inputType, isParserCaseSensitive)
            val preTumblingSize = determineLargestTumblingSize(asLong(size), asLong(slide))
            (timeFieldPos, Some(preTumblingSize))
          case _ => (timeFieldPos, None)
        }

      case _ =>
        throw new UnsupportedOperationException(s"$window is currently not supported on batch")
    }

    val aggMapping = aggregates.indices.toArray.map(_ + groupings.length)
    val outputArity = aggregates.length + groupings.length + 1

    val genFunction = generator.generateAggregations(
      "DataSetAggregatePrepareMapHelper",
      inputFieldTypeInfo,
      aggregates,
      aggFieldIndexes,
      aggMapping,
      partialResults = true,
      groupings,
      None,
      outputArity,
      needRetract,
      needMerge = false,
      needReset = true,
      None
    )

    new DataSetWindowAggMapFunction(
      genFunction,
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
      generator: AggregationCodeGenerator,
      window: LogicalWindow,
      namedAggregates: Seq[CalcitePair[AggregateCall, String]],
      groupings: Array[Int],
      physicalInputRowType: RelDataType,
      physicalInputTypes: Seq[TypeInformation[_]],
      isParserCaseSensitive: Boolean)
    : RichGroupReduceFunction[Row, Row] = {

    val needRetract = false
    val (aggFieldIndexes, aggregates, accTypes, _) = transformToAggregateFunctions(
      namedAggregates.map(_.getKey),
      physicalInputRowType,
      needRetract)

    val returnType: RowTypeInfo = createRowTypeForKeysAndAggregates(
      groupings,
      aggregates,
      accTypes,
      physicalInputRowType,
      Some(Array(BasicTypeInfo.LONG_TYPE_INFO)))

    val keysAndAggregatesArity = groupings.length + namedAggregates.length

    window match {
      case SlidingGroupWindow(_, _, size, slide) if isTimeInterval(size.resultType) =>
        // sliding time-window for partial aggregations
        val genFunction = generator.generateAggregations(
          "DataSetAggregatePrepareMapHelper",
          physicalInputTypes,
          aggregates,
          aggFieldIndexes,
          aggregates.indices.map(_ + groupings.length).toArray,
          partialResults = true,
          groupings.indices.toArray,
          Some(aggregates.indices.map(_ + groupings.length).toArray),
          keysAndAggregatesArity + 1,
          needRetract,
          needMerge = true,
          needReset = true,
          None
        )
        new DataSetSlideTimeWindowAggReduceGroupFunction(
          genFunction,
          keysAndAggregatesArity,
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
      case SlidingGroupWindow(_, _, size, slide) if isTimeInterval(size.resultType) =>
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
      generator: AggregationCodeGenerator,
      window: LogicalWindow,
      namedAggregates: Seq[CalcitePair[AggregateCall, String]],
      physicalInputRowType: RelDataType,
      physicalInputTypes: Seq[TypeInformation[_]],
      outputType: RelDataType,
      groupings: Array[Int],
      properties: Seq[NamedWindowProperty],
      isInputCombined: Boolean = false)
    : RichGroupReduceFunction[Row, Row] = {

    val needRetract = false
    val (aggFieldIndexes, aggregates, _, _) = transformToAggregateFunctions(
      namedAggregates.map(_.getKey),
      physicalInputRowType,
      needRetract)

    val aggMapping = aggregates.indices.toArray.map(_ + groupings.length)

    val genPreAggFunction = generator.generateAggregations(
      "GroupingWindowAggregateHelper",
      physicalInputTypes,
      aggregates,
      aggFieldIndexes,
      aggMapping,
      partialResults = true,
      groupings,
      Some(aggregates.indices.map(_ + groupings.length).toArray),
      outputType.getFieldCount,
      needRetract,
      needMerge = true,
      needReset = true,
      None
    )

    val genFinalAggFunction = generator.generateAggregations(
      "GroupingWindowAggregateHelper",
      physicalInputTypes,
      aggregates,
      aggFieldIndexes,
      aggMapping,
      partialResults = false,
      groupings.indices.toArray,
      Some(aggregates.indices.map(_ + groupings.length).toArray),
      outputType.getFieldCount,
      needRetract,
      needMerge = true,
      needReset = true,
      None
    )

    val keysAndAggregatesArity = groupings.length + namedAggregates.length

    window match {
      case TumblingGroupWindow(_, _, size) if isTimeInterval(size.resultType) =>
        // tumbling time window
        val (startPos, endPos, timePos) = computeWindowPropertyPos(properties)
        if (doAllSupportPartialMerge(aggregates)) {
          // for incremental aggregations
          new DataSetTumbleTimeWindowAggReduceCombineFunction(
            genPreAggFunction,
            genFinalAggFunction,
            asLong(size),
            startPos,
            endPos,
            timePos,
            keysAndAggregatesArity)
        }
        else {
          // for non-incremental aggregations
          new DataSetTumbleTimeWindowAggReduceGroupFunction(
            genFinalAggFunction,
            asLong(size),
            startPos,
            endPos,
            timePos,
            outputType.getFieldCount)
        }
      case TumblingGroupWindow(_, _, size) =>
        // tumbling count window
        new DataSetTumbleCountWindowAggReduceGroupFunction(
          genFinalAggFunction,
          asLong(size))

      case SessionGroupWindow(_, _, gap) =>
        val (startPos, endPos, timePos) = computeWindowPropertyPos(properties)
        new DataSetSessionWindowAggReduceGroupFunction(
          genFinalAggFunction,
          keysAndAggregatesArity,
          startPos,
          endPos,
          timePos,
          asLong(gap),
          isInputCombined)

      case SlidingGroupWindow(_, _, size, _) if isTimeInterval(size.resultType) =>
        val (startPos, endPos, timePos) = computeWindowPropertyPos(properties)
        if (doAllSupportPartialMerge(aggregates)) {
          // for partial aggregations
          new DataSetSlideWindowAggReduceCombineFunction(
            genPreAggFunction,
            genFinalAggFunction,
            keysAndAggregatesArity,
            startPos,
            endPos,
            timePos,
            asLong(size))
        }
        else {
          // for non-partial aggregations
          new DataSetSlideWindowAggReduceGroupFunction(
            genFinalAggFunction,
            keysAndAggregatesArity,
            startPos,
            endPos,
            timePos,
            asLong(size))
        }

      case SlidingGroupWindow(_, _, size, _) =>
        new DataSetSlideWindowAggReduceGroupFunction(
            genFinalAggFunction,
            keysAndAggregatesArity,
            None,
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
    generator: AggregationCodeGenerator,
    window: LogicalWindow,
    namedAggregates: Seq[CalcitePair[AggregateCall, String]],
    physicalInputRowType: RelDataType,
    physicalInputTypes: Seq[TypeInformation[_]],
    groupings: Array[Int]): MapPartitionFunction[Row, Row] = {

    val needRetract = false
    val (aggFieldIndexes, aggregates, accTypes, _) = transformToAggregateFunctions(
      namedAggregates.map(_.getKey),
      physicalInputRowType,
      needRetract)

    val aggMapping = aggregates.indices.map(_ + groupings.length).toArray

    val keysAndAggregatesArity = groupings.length + namedAggregates.length

    window match {
      case SessionGroupWindow(_, _, gap) =>
        val combineReturnType: RowTypeInfo =
          createRowTypeForKeysAndAggregates(
            groupings,
            aggregates,
            accTypes,
            physicalInputRowType,
            Option(Array(BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO)))

        val genFunction = generator.generateAggregations(
          "GroupingWindowAggregateHelper",
          physicalInputTypes,
          aggregates,
          aggFieldIndexes,
          aggMapping,
          partialResults = true,
          groupings.indices.toArray,
          Some(aggregates.indices.map(_ + groupings.length).toArray),
          groupings.length + aggregates.length + 2,
          needRetract,
          needMerge = true,
          needReset = true,
          None
        )

        new DataSetSessionWindowAggregatePreProcessor(
          genFunction,
          keysAndAggregatesArity,
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
      generator: AggregationCodeGenerator,
      window: LogicalWindow,
      namedAggregates: Seq[CalcitePair[AggregateCall, String]],
      physicalInputRowType: RelDataType,
      physicalInputTypes: Seq[TypeInformation[_]],
      groupings: Array[Int])
    : GroupCombineFunction[Row, Row] = {

    val needRetract = false
    val (aggFieldIndexes, aggregates, accTypes, _) = transformToAggregateFunctions(
      namedAggregates.map(_.getKey),
      physicalInputRowType,
      needRetract)

    val aggMapping = aggregates.indices.map(_ + groupings.length).toArray

    val keysAndAggregatesArity = groupings.length + namedAggregates.length

    window match {

      case SessionGroupWindow(_, _, gap) =>
        val combineReturnType: RowTypeInfo =
          createRowTypeForKeysAndAggregates(
            groupings,
            aggregates,
            accTypes,
            physicalInputRowType,
            Option(Array(BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO)))

        val genFunction = generator.generateAggregations(
          "GroupingWindowAggregateHelper",
          physicalInputTypes,
          aggregates,
          aggFieldIndexes,
          aggMapping,
          partialResults = true,
          groupings.indices.toArray,
          Some(aggregates.indices.map(_ + groupings.length).toArray),
          groupings.length + aggregates.length + 2,
          needRetract,
          needMerge = true,
          needReset = true,
          None
        )

        new DataSetSessionWindowAggregatePreProcessor(
          genFunction,
          keysAndAggregatesArity,
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
      generator: AggregationCodeGenerator,
      namedAggregates: Seq[CalcitePair[AggregateCall, String]],
      inputType: RelDataType,
      inputFieldTypeInfo: Seq[TypeInformation[_]],
      outputType: RelDataType,
      groupings: Array[Int]): (
        Option[DataSetPreAggFunction],
        Option[TypeInformation[Row]],
        Either[DataSetAggFunction, DataSetFinalAggFunction]) = {

    val needRetract = false
    val (aggInFields, aggregates, accTypes, _) = transformToAggregateFunctions(
      namedAggregates.map(_.getKey),
      inputType,
      needRetract)

    val (gkeyOutMapping, aggOutMapping) = getOutputMappings(
      namedAggregates,
      groupings,
      inputType,
      outputType
    )

    val aggOutFields = aggOutMapping.map(_._1)

    if (doAllSupportPartialMerge(aggregates)) {

      // compute preaggregation type
      val preAggFieldTypes = gkeyOutMapping.map(_._2)
        .map(inputType.getFieldList.get(_).getType)
        .map(FlinkTypeFactory.toTypeInfo) ++ accTypes
      val preAggRowType = new RowTypeInfo(preAggFieldTypes: _*)

      val genPreAggFunction = generator.generateAggregations(
        "DataSetAggregatePrepareMapHelper",
        inputFieldTypeInfo,
        aggregates,
        aggInFields,
        aggregates.indices.map(_ + groupings.length).toArray,
        partialResults = true,
        groupings,
        None,
        groupings.length + aggregates.length,
        needRetract,
        needMerge = false,
        needReset = true,
        None
      )

      // compute mapping of forwarded grouping keys
      val gkeyMapping: Array[Int] = if (gkeyOutMapping.nonEmpty) {
        val gkeyOutFields = gkeyOutMapping.map(_._1)
        val mapping = Array.fill[Int](gkeyOutFields.max + 1)(-1)
        gkeyOutFields.zipWithIndex.foreach(m => mapping(m._1) = m._2)
        mapping
      } else {
        new Array[Int](0)
      }

      val genFinalAggFunction = generator.generateAggregations(
        "DataSetAggregateFinalHelper",
        inputFieldTypeInfo,
        aggregates,
        aggInFields,
        aggOutFields,
        partialResults = false,
        gkeyMapping,
        Some(aggregates.indices.map(_ + groupings.length).toArray),
        outputType.getFieldCount,
        needRetract,
        needMerge = true,
        needReset = true,
        None
      )

      (
        Some(new DataSetPreAggFunction(genPreAggFunction)),
        Some(preAggRowType),
        Right(new DataSetFinalAggFunction(genFinalAggFunction))
      )
    }
    else {
      val genFunction = generator.generateAggregations(
        "DataSetAggregateHelper",
        inputFieldTypeInfo,
        aggregates,
        aggInFields,
        aggOutFields,
        partialResults = false,
        groupings,
        None,
        outputType.getFieldCount,
        needRetract,
        needMerge = false,
        needReset = true,
        None
      )

      (
        None,
        None,
        Left(new DataSetAggFunction(genFunction))
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
    : AllWindowFunction[Row, CRow, DataStreamWindow] = {

    if (isTimeWindow(window)) {
      val (startPos, endPos, timePos) = computeWindowPropertyPos(properties)
      new IncrementalAggregateAllTimeWindowFunction(
        startPos,
        endPos,
        timePos,
        finalRowArity)
        .asInstanceOf[AllWindowFunction[Row, CRow, DataStreamWindow]]
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
      properties: Seq[NamedWindowProperty]):
    WindowFunction[Row, CRow, Row, DataStreamWindow] = {

    if (isTimeWindow(window)) {
      val (startPos, endPos, timePos) = computeWindowPropertyPos(properties)
      new IncrementalAggregateTimeWindowFunction(
        numGroupingKeys,
        numAggregates,
        startPos,
        endPos,
        timePos,
        finalRowArity)
        .asInstanceOf[WindowFunction[Row, CRow, Row, DataStreamWindow]]
    } else {
      new IncrementalAggregateWindowFunction(
        numGroupingKeys,
        numAggregates,
        finalRowArity)
    }
  }

  private[flink] def createDataStreamAggregateFunction(
      generator: AggregationCodeGenerator,
      namedAggregates: Seq[CalcitePair[AggregateCall, String]],
      inputType: RelDataType,
      inputFieldTypeInfo: Seq[TypeInformation[_]],
      outputType: RelDataType,
      groupingKeys: Array[Int],
      needMerge: Boolean)
    : (DataStreamAggFunction[CRow, Row, Row], RowTypeInfo, RowTypeInfo) = {

    val needRetract = false
    val (aggFields, aggregates, accTypes, _) =
      transformToAggregateFunctions(
        namedAggregates.map(_.getKey),
        inputType,
        needRetract)

    val aggMapping = aggregates.indices.toArray
    val outputArity = aggregates.length

    val genFunction = generator.generateAggregations(
      "GroupingWindowAggregateHelper",
      inputFieldTypeInfo,
      aggregates,
      aggFields,
      aggMapping,
      partialResults = false,
      groupingKeys,
      None,
      outputArity,
      needRetract,
      needMerge,
      needReset = false,
      None
    )

    val aggResultTypes = namedAggregates.map(a => FlinkTypeFactory.toTypeInfo(a.left.getType))

    val accumulatorRowType = new RowTypeInfo(accTypes: _*)
    val aggResultRowType = new RowTypeInfo(aggResultTypes: _*)
    val aggFunction = new AggregateAggFunction(genFunction)

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
      aggregateList: Array[TableAggregateFunction[_ <: Any, _ <: Any]]): Boolean = {
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

  private def isTimeWindow(window: LogicalWindow) = {
    window match {
      case TumblingGroupWindow(_, _, size) => isTimeIntervalLiteral(size)
      case SlidingGroupWindow(_, _, size, _) => isTimeIntervalLiteral(size)
      case SessionGroupWindow(_, _, _) => true
    }
  }

  /**
    * Computes the positions of (window start, window end, rowtime).
    */
  private[flink] def computeWindowPropertyPos(
      properties: Seq[NamedWindowProperty]): (Option[Int], Option[Int], Option[Int]) = {

    val propPos = properties.foldRight(
      (None: Option[Int], None: Option[Int], None: Option[Int], 0)) {
      case (p, (s, e, rt, i)) => p match {
        case NamedWindowProperty(_, prop) =>
          prop match {
            case WindowStart(_) if s.isDefined =>
              throw TableException("Duplicate window start property encountered. This is a bug.")
            case WindowStart(_) =>
              (Some(i), e, rt, i - 1)
            case WindowEnd(_) if e.isDefined =>
              throw TableException("Duplicate window end property encountered. This is a bug.")
            case WindowEnd(_) =>
              (s, Some(i), rt, i - 1)
            case RowtimeAttribute(_) if rt.isDefined =>
              throw TableException("Duplicate window rowtime property encountered. This is a bug.")
            case RowtimeAttribute(_) =>
              (s, e, Some(i), i - 1)
            case ProctimeAttribute(_) =>
              // ignore this property, it will be null at the position later
              (s, e, rt, i - 1)
          }
      }
    }
    (propPos._1, propPos._2, propPos._3)
  }

  private def transformToAggregateFunctions(
      aggregateCalls: Seq[AggregateCall],
      aggregateInputType: RelDataType,
      needRetraction: Boolean,
      isStateBackedDataViews: Boolean = false)
  : (Array[Array[Int]],
    Array[TableAggregateFunction[_, _]],
    Array[TypeInformation[_]],
    Array[Seq[DataViewSpec[_]]]) = {

    // store the aggregate fields of each aggregate function, by the same order of aggregates.
    val aggFieldIndexes = new Array[Array[Int]](aggregateCalls.size)
    val aggregates = new Array[TableAggregateFunction[_ <: Any, _ <: Any]](aggregateCalls.size)
    val accTypes = new Array[TypeInformation[_]](aggregateCalls.size)

    // create aggregate function instances by function type and aggregate field data type.
    aggregateCalls.zipWithIndex.foreach { case (aggregateCall, index) =>
      val argList: util.List[Integer] = aggregateCall.getArgList

      if (aggregateCall.getAggregation.isInstanceOf[SqlCountAggFunction]) {
        aggregates(index) = new CountAggFunction
        if (argList.isEmpty) {
          aggFieldIndexes(index) = Array[Int](-1)
        } else {
          aggFieldIndexes(index) = argList.asScala.map(i => i.intValue).toArray
        }
      } else {
        if (argList.isEmpty) {
          throw new TableException("Aggregate fields should not be empty.")
        } else {
          aggFieldIndexes(index) = argList.asScala.map(i => i.intValue).toArray
        }

        val relDataType = aggregateInputType.getFieldList.get(aggFieldIndexes(index)(0)).getType
        val sqlTypeName = relDataType.getSqlTypeName
        aggregateCall.getAggregation match {

          case _: SqlSumAggFunction =>
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
                  throw new TableException(s"Sum aggregate does no support type: '$sqlType'")
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
                  throw new TableException(s"Sum aggregate does no support type: '$sqlType'")
              }
            }

          case _: SqlSumEmptyIsZeroAggFunction =>
            if (needRetraction) {
              aggregates(index) = sqlTypeName match {
                case TINYINT =>
                  new ByteSum0WithRetractAggFunction
                case SMALLINT =>
                  new ShortSum0WithRetractAggFunction
                case INTEGER =>
                  new IntSum0WithRetractAggFunction
                case BIGINT =>
                  new LongSum0WithRetractAggFunction
                case FLOAT =>
                  new FloatSum0WithRetractAggFunction
                case DOUBLE =>
                  new DoubleSum0WithRetractAggFunction
                case DECIMAL =>
                  new DecimalSum0WithRetractAggFunction
                case sqlType: SqlTypeName =>
                  throw new TableException(s"Sum0 aggregate does no support type: '$sqlType'")
              }
            } else {
              aggregates(index) = sqlTypeName match {
                case TINYINT =>
                  new ByteSum0AggFunction
                case SMALLINT =>
                  new ShortSum0AggFunction
                case INTEGER =>
                  new IntSum0AggFunction
                case BIGINT =>
                  new LongSum0AggFunction
                case FLOAT =>
                  new FloatSum0AggFunction
                case DOUBLE =>
                  new DoubleSum0AggFunction
                case DECIMAL =>
                  new DecimalSum0AggFunction
                case sqlType: SqlTypeName =>
                  throw new TableException(s"Sum0 aggregate does no support type: '$sqlType'")
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
                throw new TableException(s"Avg aggregate does no support type: '$sqlType'")
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
                    throw new TableException(
                      s"Min with retract aggregate does no support type: '$sqlType'")
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
                    throw new TableException(s"Min aggregate does no support type: '$sqlType'")
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
                    throw new TableException(
                      s"Max with retract aggregate does no support type: '$sqlType'")
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
                    throw new TableException(s"Max aggregate does no support type: '$sqlType'")
                }
              }
            }

          case collect: SqlAggFunction if collect.getKind == SqlKind.COLLECT =>
            aggregates(index) = new CollectAggFunction(FlinkTypeFactory.toTypeInfo(relDataType))
            accTypes(index) = aggregates(index).getAccumulatorType

          case udagg: AggSqlFunction =>
            aggregates(index) = udagg.getFunction
            accTypes(index) = udagg.accType

          case unSupported: SqlAggFunction =>
            throw new TableException(s"unsupported Function: '${unSupported.getName}'")
        }
      }
    }

    val accSpecs = new Array[Seq[DataViewSpec[_]]](aggregateCalls.size)

    // create accumulator type information for every aggregate function
    aggregates.zipWithIndex.foreach { case (agg, index) =>
      if (accTypes(index) != null) {
        val (accType, specs) = removeStateViewFieldsFromAccTypeInfo(index,
          agg,
          accTypes(index),
          isStateBackedDataViews)
        if (specs.isDefined) {
          accSpecs(index) = specs.get
          accTypes(index) = accType
        } else {
          accSpecs(index) = Seq()
        }
      } else {
        accSpecs(index) = Seq()
        accTypes(index) = getAccumulatorTypeOfAggregateFunction(agg)
      }
    }

    (aggFieldIndexes, aggregates, accTypes, accSpecs)
  }

  private def createRowTypeForKeysAndAggregates(
      groupings: Array[Int],
      aggregates: Array[TableAggregateFunction[_, _]],
      aggTypes: Array[TypeInformation[_]],
      inputType: RelDataType,
      windowKeyTypes: Option[Array[TypeInformation[_]]] = None): RowTypeInfo = {

    // get the field data types of group keys.
    val groupingTypes: Seq[TypeInformation[_]] =
      groupings
        .map(inputType.getFieldList.get(_).getType)
        .map(FlinkTypeFactory.toTypeInfo)

    // concat group key types, aggregation types, and window key types
    val allFieldTypes: Seq[TypeInformation[_]] = windowKeyTypes match {
      case None => groupingTypes ++: aggTypes
      case _ => groupingTypes ++: aggTypes ++: windowKeyTypes.get
    }
    new RowTypeInfo(allFieldTypes: _*)
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
            s"Encountered more than one time attribute with the same name: '$relDataType'")
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

  private[flink] def determineLargestTumblingSize(size: Long, slide: Long) = gcd(size, slide)

  private def gcd(a: Long, b: Long): Long = {
    if (b == 0) a else gcd(b, a % b)
  }
}
