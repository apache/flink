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
import java.util.{ArrayList => JArrayList, List => JList}

import org.apache.calcite.rel.`type`._
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rex.RexLiteral
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.`type`.SqlTypeName._
import org.apache.calcite.sql.fun._
import org.apache.calcite.sql.{SqlAggFunction, SqlKind}
import org.apache.flink.api.common.functions.{MapFunction, RichGroupReduceFunction, AggregateFunction => DataStreamAggFunction, _}
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation, Types}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.windowing.{AllWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.windows.{Window => DataStreamWindow}
import org.apache.flink.table.api.dataview.DataViewSpec
import org.apache.flink.table.api.{StreamQueryConfig, TableConfig, TableException}
import org.apache.flink.table.calcite.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.{AggregationCodeGenerator, GeneratedTableAggregationsFunction}
import org.apache.flink.table.expressions.PlannerExpressionUtils.isTimeIntervalLiteral
import org.apache.flink.table.expressions._
import org.apache.flink.table.functions.aggfunctions._
import org.apache.flink.table.functions.utils.AggSqlFunction
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils._
import org.apache.flink.table.functions.{AggregateFunction, TableAggregateFunction, UserDefinedAggregateFunction, UserFunctionsTypeHelper}
import org.apache.flink.table.plan.logical._
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
import org.apache.flink.table.typeutils.TimeIntervalTypeInfo
import org.apache.flink.table.typeutils.TypeCheckUtils._
import org.apache.flink.types.Row

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable

object AggregateUtil {

  type CalcitePair[T, R] = org.apache.calcite.util.Pair[T, R]

  /**
    * Create an [[org.apache.flink.streaming.api.functions.ProcessFunction]] for unbounded OVER
    * window to evaluate final aggregate value.
    *
    * @param config             configuration that determines runtime behavior
    * @param nullableInput      input(s) can be null.
    * @param input              type information about the input of the Function
    * @param constants          constant expressions that act like a second input in the
    *                           parameter indices.
    * @param namedAggregates    Physical calls to aggregate functions and their output field names
    * @param aggregateInputType Physical type of the aggregate functions's input row.
    * @param inputType          Physical type of the row.
    * @param inputTypeInfo      Physical type information of the row.
    * @param inputFieldTypeInfo Physical type information of the row's fields.
    * @param rowTimeIdx         The index of the rowtime field or None in case of processing time.
    * @param isPartitioned      It is a tag that indicate whether the input is partitioned
    * @param isRowsClause       It is a tag that indicates whether the OVER clause is ROWS clause
    */
  private[flink] def createUnboundedOverProcessFunction[K](
      config: TableConfig,
      nullableInput: Boolean,
      input: TypeInformation[_ <: Any],
      constants: Option[Seq[RexLiteral]],
      namedAggregates: Seq[CalcitePair[AggregateCall, String]],
      aggregateInputType: RelDataType,
      inputType: RelDataType,
      inputTypeInfo: TypeInformation[Row],
      inputFieldTypeInfo: Seq[TypeInformation[_]],
      queryConfig: StreamQueryConfig,
      tableConfig: TableConfig,
      rowTimeIdx: Option[Int],
      isPartitioned: Boolean,
      isRowsClause: Boolean)
    : KeyedProcessFunction[K, CRow, CRow] = {

    val aggregateMetadata = extractAggregateMetadata(
        namedAggregates.map(_.getKey),
        aggregateInputType,
        inputFieldTypeInfo.length,
        needRetraction = false,
        tableConfig,
        isStateBackedDataViews = true)


    val forwardMapping = (0 until inputType.getFieldCount).toArray
    val aggMapping = aggregateMetadata.getAdjustedMapping(inputType.getFieldCount)

    val outputArity = inputType.getFieldCount + aggregateMetadata.getAggregateCallsCount

    val generator = new AggregationCodeGenerator(
      config,
      nullableInput,
      input,
      constants,
      "UnboundedProcessingOverAggregateHelper",
      inputFieldTypeInfo,
      aggregateMetadata.getAggregateFunctions,
      aggregateMetadata.getAggregateIndices,
      aggMapping,
      aggregateMetadata.getDistinctAccMapping,
      isStateBackedDataViews = true,
      partialResults = false,
      forwardMapping,
      None,
      outputArity,
      needRetract = false,
      needMerge = false,
      needReset = false,
      accConfig = Some(aggregateMetadata.getAggregatesAccumulatorSpecs)
    )

    val aggregationStateType: RowTypeInfo = new RowTypeInfo(aggregateMetadata
      .getAggregatesAccumulatorTypes: _*)
    val genFunction = generator.generateAggregations

    if (rowTimeIdx.isDefined) {
      if (isRowsClause) {
        // ROWS unbounded over process function
        new RowTimeUnboundedRowsOver[K](
          genFunction,
          aggregationStateType,
          CRowTypeInfo(inputTypeInfo),
          rowTimeIdx.get,
          queryConfig)
      } else {
        // RANGE unbounded over process function
        new RowTimeUnboundedRangeOver[K](
          genFunction,
          aggregationStateType,
          CRowTypeInfo(inputTypeInfo),
          rowTimeIdx.get,
          queryConfig)
      }
    } else {
      new ProcTimeUnboundedOver[K](
        genFunction,
        aggregationStateType,
        queryConfig)
    }
  }

  /**
    * Create an [[org.apache.flink.streaming.api.functions.ProcessFunction]] for group (without
    * window) aggregate to evaluate final aggregate value.
    *
    * @param config             configuration that determines runtime behavior
    * @param nullableInput      input(s) can be null.
    * @param input              type information about the input of the Function
    * @param constants          constant expressions that act like a second input in the
    *                           parameter indices.
    * @param namedAggregates    List of calls to aggregate functions and their output field names
    * @param inputRowType       Input row type
    * @param inputFieldTypes    Types of the physical input fields
    * @param outputType         Output type of the (table)aggregate node
    * @param groupings          the position (in the input Row) of the grouping keys
    * @param queryConfig        The configuration of the query to generate.
    * @param generateRetraction It is a tag that indicates whether generate retract record.
    * @param consumeRetraction  It is a tag that indicates whether consume the retract record.
    * @return [[org.apache.flink.streaming.api.functions.ProcessFunction]]
    */
  private[flink] def createDataStreamGroupAggregateFunction[K](
      config: TableConfig,
      nullableInput: Boolean,
      input: TypeInformation[_ <: Any],
      constants: Option[Seq[RexLiteral]],
      namedAggregates: Seq[CalcitePair[AggregateCall, String]],
      inputRowType: RelDataType,
      inputFieldTypes: Seq[TypeInformation[_]],
      outputType: RelDataType,
      groupings: Array[Int],
      queryConfig: StreamQueryConfig,
      generateRetraction: Boolean,
      consumeRetraction: Boolean): KeyedProcessFunction[K, CRow, CRow] = {

    val aggregateMetadata = extractAggregateMetadata(
        namedAggregates.map(_.getKey),
        inputRowType,
        inputFieldTypes.length,
        consumeRetraction,
        config,
        isStateBackedDataViews = true)

    val aggMapping = aggregateMetadata.getAdjustedMapping(groupings.length)

    val isTableAggregate = this.isTableAggregate(namedAggregates.map(_.left))
    val generator = new AggregationCodeGenerator(
      config: TableConfig,
      nullableInput: Boolean,
      input: TypeInformation[_ <: Any],
      constants: Option[Seq[RexLiteral]],
      s"NonWindowed${if (isTableAggregate) "Table" else ""}AggregationHelper",
      inputFieldTypes,
      aggregateMetadata.getAggregateFunctions,
      aggregateMetadata.getAggregateIndices,
      aggMapping,
      aggregateMetadata.getDistinctAccMapping,
      isStateBackedDataViews = true,
      partialResults = false,
      groupings,
      None,
      outputType.getFieldCount,
      consumeRetraction,
      needMerge = false,
      needReset = false,
      accConfig = Some(aggregateMetadata.getAggregatesAccumulatorSpecs)
    )

    val aggregationStateType: RowTypeInfo = new RowTypeInfo(aggregateMetadata
      .getAggregatesAccumulatorTypes: _*)

    if (isTableAggregate) {
      val genAggregations = generator
        .genAggregationsOrTableAggregations(outputType, groupings.length, namedAggregates, true)
      new GroupTableAggProcessFunction[K](
        genAggregations.asInstanceOf[GeneratedTableAggregationsFunction],
        aggregationStateType,
        generateRetraction,
        groupings.length,
        queryConfig)
    } else {
      val genAggregations = generator
        .genAggregationsOrTableAggregations(outputType, groupings.length, namedAggregates, false)
      new GroupAggProcessFunction[K](
        genAggregations,
        aggregationStateType,
        generateRetraction,
        queryConfig)
    }
  }

  /**
    * Create an [[org.apache.flink.streaming.api.functions.ProcessFunction]] for ROWS clause
    * bounded OVER window to evaluate final aggregate value.
    *
    * @param config             configuration that determines runtime behavior
    * @param nullableInput      input(s) can be null.
    * @param input              type information about the input of the Function
    * @param constants          constant expressions that act like a second input in the
    *                           parameter indices.
    * @param namedAggregates    Physical calls to aggregate functions and their output field names
    * @param aggregateInputType Physical type of the aggregate functions's input row.
    * @param inputType          Physical type of the row.
    * @param inputTypeInfo      Physical type information of the row.
    * @param inputFieldTypeInfo Physical type information of the row's fields.
    * @param precedingOffset    the preceding offset
    * @param isRowsClause       It is a tag that indicates whether the OVER clause is ROWS clause
    * @param rowTimeIdx         The index of the rowtime field or None in case of processing time.
    * @return [[org.apache.flink.streaming.api.functions.ProcessFunction]]
    */
  private[flink] def createBoundedOverProcessFunction[K](
      config: TableConfig,
      nullableInput: Boolean,
      input: TypeInformation[_ <: Any],
      constants: Option[Seq[RexLiteral]],
      namedAggregates: Seq[CalcitePair[AggregateCall, String]],
      aggregateInputType: RelDataType,
      inputType: RelDataType,
      inputTypeInfo: TypeInformation[Row],
      inputFieldTypeInfo: Seq[TypeInformation[_]],
      precedingOffset: Long,
      queryConfig: StreamQueryConfig,
      isRowsClause: Boolean,
      rowTimeIdx: Option[Int])
    : KeyedProcessFunction[K, CRow, CRow] = {

    val needRetract = true
    val aggregateMetadata = extractAggregateMetadata(
        namedAggregates.map(_.getKey),
        aggregateInputType,
        inputFieldTypeInfo.length,
        needRetract,
        config,
        isStateBackedDataViews = true)

    val inputRowType = CRowTypeInfo(inputTypeInfo)

    val forwardMapping = (0 until inputType.getFieldCount).toArray
    val aggMapping = aggregateMetadata.getAdjustedMapping(inputType.getFieldCount)

    val outputArity = inputType.getFieldCount + aggregateMetadata.getAggregateCallsCount

    val generator = new AggregationCodeGenerator(
      config,
      nullableInput,
      input,
      constants,
      "BoundedOverAggregateHelper",
      inputFieldTypeInfo,
      aggregateMetadata.getAggregateFunctions,
      aggregateMetadata.getAggregateIndices,
      aggMapping,
      aggregateMetadata.getDistinctAccMapping,
      isStateBackedDataViews = true,
      partialResults = false,
      forwardMapping,
      None,
      outputArity,
      needRetract,
      needMerge = false,
      needReset = false,
      accConfig = Some(aggregateMetadata.getAggregatesAccumulatorSpecs)
    )
    val genFunction = generator.generateAggregations

    val aggregationStateType: RowTypeInfo = new RowTypeInfo(aggregateMetadata
      .getAggregatesAccumulatorTypes: _*)
    if (rowTimeIdx.isDefined) {
      if (isRowsClause) {
        new RowTimeBoundedRowsOver[K](
          genFunction,
          aggregationStateType,
          inputRowType,
          precedingOffset,
          rowTimeIdx.get,
          queryConfig)
      } else {
        new RowTimeBoundedRangeOver[K](
          genFunction,
          aggregationStateType,
          inputRowType,
          precedingOffset,
          rowTimeIdx.get,
          queryConfig)
      }
    } else {
      if (isRowsClause) {
        new ProcTimeBoundedRowsOver[K](
          genFunction,
          precedingOffset,
          aggregationStateType,
          inputRowType,
          queryConfig)
      } else {
        new ProcTimeBoundedRangeOver[K](
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
    config: TableConfig,
    nullableInput: Boolean,
    inputTypeInfo: TypeInformation[_ <: Any],
    constants: Option[Seq[RexLiteral]],
    window: LogicalWindow,
    namedAggregates: Seq[CalcitePair[AggregateCall, String]],
    groupings: Array[Int],
    inputType: RelDataType,
    inputFieldTypeInfo: Seq[TypeInformation[_]],
    isParserCaseSensitive: Boolean,
    tableConfig: TableConfig)
  : MapFunction[Row, Row] = {

    val needRetract = false
    val aggregateMetadata = extractAggregateMetadata(
      namedAggregates.map(_.getKey),
      inputType,
      inputFieldTypeInfo.length,
      needRetract,
      tableConfig)

    val mapReturnType: RowTypeInfo =
      createRowTypeForKeysAndAggregates(
        groupings,
        aggregateMetadata.getAggregateFunctions,
        aggregateMetadata.getAggregatesAccumulatorTypes,
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

    val aggMapping = aggregateMetadata.getAdjustedMapping(groupings.length, partialResults = true)
    val outputArity = aggregateMetadata.getAggregateCallsCount + groupings.length +
      aggregateMetadata.getDistinctAccCount + 1

    val generator = new AggregationCodeGenerator(
      config,
      nullableInput,
      inputTypeInfo,
      constants,
      "DataSetAggregatePrepareMapHelper",
      inputFieldTypeInfo,
      aggregateMetadata.getAggregateFunctions,
      aggregateMetadata.getAggregateIndices,
      aggMapping,
      aggregateMetadata.getDistinctAccMapping,
      isStateBackedDataViews = false,
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
      generator.generateAggregations,
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
      config: TableConfig,
      nullableInput: Boolean,
      inputTypeInfo: TypeInformation[_ <: Any],
      constants: Option[Seq[RexLiteral]],
      window: LogicalWindow,
      namedAggregates: Seq[CalcitePair[AggregateCall, String]],
      groupings: Array[Int],
      physicalInputRowType: RelDataType,
      physicalInputTypes: Seq[TypeInformation[_]],
      isParserCaseSensitive: Boolean,
      tableConfig: TableConfig)
    : RichGroupReduceFunction[Row, Row] = {

    val needRetract = false
    val aggregateMetadata = extractAggregateMetadata(
      namedAggregates.map(_.getKey),
      physicalInputRowType,
      physicalInputTypes.length,
      needRetract,
      tableConfig)

    val returnType: RowTypeInfo = createRowTypeForKeysAndAggregates(
      groupings,
      aggregateMetadata.getAggregateFunctions,
      aggregateMetadata.getAggregatesAccumulatorTypes,
      physicalInputRowType,
      Some(Array(BasicTypeInfo.LONG_TYPE_INFO)))

    val aggMappings = aggregateMetadata.getAdjustedMapping(groupings.length, partialResults = true)
    val keysAndAggregatesArity =
      groupings.length + namedAggregates.length + aggregateMetadata.getDistinctAccCount

    window match {
      case SlidingGroupWindow(_, _, size, slide) if isTimeInterval(size.resultType) =>
        // sliding time-window for partial aggregations
        val generator = new AggregationCodeGenerator(
          config,
          nullableInput,
          inputTypeInfo,
          constants,
          "DataSetAggregatePrepareMapHelper",
          physicalInputTypes,
          aggregateMetadata.getAggregateFunctions,
          aggregateMetadata.getAggregateIndices,
          aggMappings,
          aggregateMetadata.getDistinctAccMapping,
          isStateBackedDataViews = false,
          partialResults = true,
          groupings.indices.toArray,
          Some(aggMappings),
          keysAndAggregatesArity + 1,
          needRetract,
          needMerge = true,
          needReset = true,
          None
        )
        new DataSetSlideTimeWindowAggReduceGroupFunction(
          generator.generateAggregations,
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
      config: TableConfig,
      nullableInput: Boolean,
      inputTypeInfo: TypeInformation[_ <: Any],
      constants: Option[Seq[RexLiteral]],
      window: LogicalWindow,
      namedAggregates: Seq[CalcitePair[AggregateCall, String]],
      physicalInputRowType: RelDataType,
      physicalInputTypes: Seq[TypeInformation[_]],
      outputType: RelDataType,
      groupings: Array[Int],
      properties: Seq[NamedWindowProperty],
      tableConfig: TableConfig,
      isInputCombined: Boolean = false)
    : RichGroupReduceFunction[Row, Row] = {

    val needRetract = false
    val aggregateMetadata = extractAggregateMetadata(
      namedAggregates.map(_.getKey),
      physicalInputRowType,
      physicalInputTypes.length,
      needRetract,
      tableConfig)

    val aggMapping = aggregateMetadata.getAdjustedMapping(groupings.length, partialResults = true)

    val generatorPre = new AggregationCodeGenerator(
      config,
      nullableInput,
      inputTypeInfo,
      constants,
      "GroupingWindowAggregateHelper",
      physicalInputTypes,
      aggregateMetadata.getAggregateFunctions,
      aggregateMetadata.getAggregateIndices,
      aggMapping,
      aggregateMetadata.getDistinctAccMapping,
      isStateBackedDataViews = false,
      partialResults = true,
      groupings.indices.toArray,
      Some(aggMapping),
      outputType.getFieldCount + aggregateMetadata.getDistinctAccCount,
      needRetract,
      needMerge = true,
      needReset = true,
      None
    )
    val genPreAggFunction = generatorPre.generateAggregations

    val generatorFinal = new AggregationCodeGenerator(
      config,
      nullableInput,
      inputTypeInfo,
      constants,
      "GroupingWindowAggregateHelper",
      physicalInputTypes,
      aggregateMetadata.getAggregateFunctions,
      aggregateMetadata.getAggregateIndices,
      aggMapping,
      aggregateMetadata.getDistinctAccMapping,
      isStateBackedDataViews = false,
      partialResults = false,
      groupings.indices.toArray,
      Some(aggMapping),
      outputType.getFieldCount,
      needRetract,
      needMerge = true,
      needReset = true,
      None
    )
    val genFinalAggFunction = generatorFinal.generateAggregations

    val keysAndAggregatesArity = groupings.length + namedAggregates.length

    window match {
      case TumblingGroupWindow(_, _, size) if isTimeInterval(size.resultType) =>
        // tumbling time window
        val (startPos, endPos, timePos) = computeWindowPropertyPos(properties)
        if (doAllSupportPartialMerge(aggregateMetadata.getAggregateFunctions)) {
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
        if (doAllSupportPartialMerge(aggregateMetadata.getAggregateFunctions)) {
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
    config: TableConfig,
    nullableInput: Boolean,
    inputTypeInfo: TypeInformation[_ <: Any],
    constants: Option[Seq[RexLiteral]],
    window: LogicalWindow,
    namedAggregates: Seq[CalcitePair[AggregateCall, String]],
    physicalInputRowType: RelDataType,
    physicalInputTypes: Seq[TypeInformation[_]],
    groupings: Array[Int],
    tableConfig: TableConfig): MapPartitionFunction[Row, Row] = {

    val needRetract = false
    val aggregateMetadata = extractAggregateMetadata(
      namedAggregates.map(_.getKey),
      physicalInputRowType,
      physicalInputTypes.length,
      needRetract,
      tableConfig)

    val aggMapping = aggregateMetadata.getAdjustedMapping(groupings.length, partialResults = true)
    val keysAndAggregatesArity = groupings.length + aggregateMetadata.getAggregateCallsCount +
      aggregateMetadata.getDistinctAccCount

    window match {
      case SessionGroupWindow(_, _, gap) =>
        val combineReturnType: RowTypeInfo =
          createRowTypeForKeysAndAggregates(
            groupings,
            aggregateMetadata.getAggregateFunctions,
            aggregateMetadata.getAggregatesAccumulatorTypes,
            physicalInputRowType,
            Option(Array(BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO)))

        val generator = new AggregationCodeGenerator(
          config,
          nullableInput,
          inputTypeInfo,
          constants,
          "GroupingWindowAggregateHelper",
          physicalInputTypes,
          aggregateMetadata.getAggregateFunctions,
          aggregateMetadata.getAggregateIndices,
          aggMapping,
          aggregateMetadata.getDistinctAccMapping,
          isStateBackedDataViews = false,
          partialResults = true,
          groupings.indices.toArray,
          Some(aggMapping),
          keysAndAggregatesArity + 2,
          needRetract,
          needMerge = true,
          needReset = true,
          None
        )

        new DataSetSessionWindowAggregatePreProcessor(
          generator.generateAggregations,
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
      config: TableConfig,
      nullableInput: Boolean,
      inputTypeInfo: TypeInformation[_ <: Any],
      constants: Option[Seq[RexLiteral]],
      window: LogicalWindow,
      namedAggregates: Seq[CalcitePair[AggregateCall, String]],
      physicalInputRowType: RelDataType,
      physicalInputTypes: Seq[TypeInformation[_]],
      groupings: Array[Int],
      tableConfig: TableConfig)
    : GroupCombineFunction[Row, Row] = {

    val needRetract = false
    val aggregateMetadata = extractAggregateMetadata(
      namedAggregates.map(_.getKey),
      physicalInputRowType,
      physicalInputTypes.length,
      needRetract,
      tableConfig)

    val aggMapping = aggregateMetadata.getAdjustedMapping(groupings.length, partialResults = true)
    val keysAndAggregatesArity =
      groupings.length + namedAggregates.length + aggregateMetadata.getDistinctAccCount

    window match {

      case SessionGroupWindow(_, _, gap) =>
        val combineReturnType: RowTypeInfo =
          createRowTypeForKeysAndAggregates(
            groupings,
            aggregateMetadata.getAggregateFunctions,
            aggregateMetadata.getAggregatesAccumulatorTypes,
            physicalInputRowType,
            Option(Array(BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO)))

        val generator = new AggregationCodeGenerator(
          config,
          nullableInput,
          inputTypeInfo,
          constants,
          "GroupingWindowAggregateHelper",
          physicalInputTypes,
          aggregateMetadata.getAggregateFunctions,
          aggregateMetadata.getAggregateIndices,
          aggMapping,
          aggregateMetadata.getDistinctAccMapping,
          isStateBackedDataViews = false,
          partialResults = true,
          groupings.indices.toArray,
          Some(aggMapping),
          keysAndAggregatesArity + 2,
          needRetract,
          needMerge = true,
          needReset = true,
          None
        )

        new DataSetSessionWindowAggregatePreProcessor(
          generator.generateAggregations,
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
      config: TableConfig,
      nullableInput: Boolean,
      inputTypeInfo: TypeInformation[_ <: Any],
      constants: Option[Seq[RexLiteral]],
      namedAggregates: Seq[CalcitePair[AggregateCall, String]],
      inputType: RelDataType,
      inputFieldTypeInfo: Seq[TypeInformation[_]],
      outputType: RelDataType,
      groupings: Array[Int],
      tableConfig: TableConfig): (
        Option[DataSetPreAggFunction],
        Option[TypeInformation[Row]],
        Either[DataSetAggFunction, DataSetFinalAggFunction]) = {

    val needRetract = false
    val aggregateMetadata = extractAggregateMetadata(
      namedAggregates.map(_.getKey),
      inputType,
      inputFieldTypeInfo.length,
      needRetract,
      tableConfig)

    val (gkeyOutMapping, aggOutMapping) = getOutputMappings(
      namedAggregates,
      groupings,
      inputType,
      outputType
    )

    val aggOutFields = aggOutMapping.map(_._1)

    if (doAllSupportPartialMerge(aggregateMetadata.getAggregateFunctions)) {

      val aggMapping = aggregateMetadata.getAdjustedMapping(groupings.length, partialResults = true)
      val keysAndAggregatesArity = groupings.length + aggregateMetadata.getAggregateCallsCount +
        aggregateMetadata.getDistinctAccCount

      // compute preaggregation type
      val preAggFieldTypes = gkeyOutMapping.map(_._2)
        .map(inputType.getFieldList.get(_).getType)
        .map(FlinkTypeFactory.toTypeInfo) ++ aggregateMetadata.getAggregatesAccumulatorTypes
      val preAggRowType = new RowTypeInfo(preAggFieldTypes: _*)

      val generatorPre = new AggregationCodeGenerator(
        config,
        nullableInput,
        inputTypeInfo,
        constants,
        "DataSetAggregatePrepareMapHelper",
        inputFieldTypeInfo,
        aggregateMetadata.getAggregateFunctions,
        aggregateMetadata.getAggregateIndices,
        aggMapping,
        aggregateMetadata.getDistinctAccMapping,
        isStateBackedDataViews = false,
        partialResults = true,
        groupings,
        None,
        keysAndAggregatesArity,
        needRetract,
        needMerge = false,
        needReset = true,
        None
      )
      val genPreAggFunction = generatorPre.generateAggregations

      // compute mapping of forwarded grouping keys
      val gkeyMapping: Array[Int] = if (gkeyOutMapping.nonEmpty) {
        val gkeyOutFields = gkeyOutMapping.map(_._1)
        val mapping = Array.fill[Int](gkeyOutFields.max + 1)(-1)
        gkeyOutFields.zipWithIndex.foreach(m => mapping(m._1) = m._2)
        mapping
      } else {
        new Array[Int](0)
      }

      val generatorFinal = new AggregationCodeGenerator(
        config,
        nullableInput,
        inputTypeInfo,
        constants,
        "DataSetAggregateFinalHelper",
        inputFieldTypeInfo,
        aggregateMetadata.getAggregateFunctions,
        aggregateMetadata.getAggregateIndices,
        aggOutFields,
        aggregateMetadata.getDistinctAccMapping,
        isStateBackedDataViews = false,
        partialResults = false,
        gkeyMapping,
        Some(aggMapping),
        outputType.getFieldCount,
        needRetract,
        needMerge = true,
        needReset = true,
        None
      )
      val genFinalAggFunction = generatorFinal.generateAggregations

      (
        Some(new DataSetPreAggFunction(genPreAggFunction)),
        Some(preAggRowType),
        Right(new DataSetFinalAggFunction(genFinalAggFunction))
      )
    }
    else {
      val generator = new AggregationCodeGenerator(
        config,
        nullableInput,
        inputTypeInfo,
        constants,
        "DataSetAggregateHelper",
        inputFieldTypeInfo,
        aggregateMetadata.getAggregateFunctions,
        aggregateMetadata.getAggregateIndices,
        aggOutFields,
        aggregateMetadata.getDistinctAccMapping,
        isStateBackedDataViews = false,
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
        Left(new DataSetAggFunction(generator.generateAggregations))
      )
    }

  }

  /**
    * Create an [[AllWindowFunction]] for non-partitioned window aggregates.
    */
  private[flink] def createAggregationAllWindowFunction(
    window: LogicalWindow,
    finalRowArity: Int,
    namedAggregates: Seq[CalcitePair[AggregateCall, String]],
    properties: Seq[NamedWindowProperty])
  : AllWindowFunction[Row, CRow, DataStreamWindow] = {

    val isTableAggregate = this.isTableAggregate(namedAggregates.map(_.getKey))
    if (isTimeWindow(window)) {
      val (startPos, endPos, timePos) = computeWindowPropertyPos(properties)
      new IncrementalAggregateAllTimeWindowFunction(
        startPos,
        endPos,
        timePos,
        finalRowArity,
        isTableAggregate)
        .asInstanceOf[AllWindowFunction[Row, CRow, DataStreamWindow]]
    } else {
      new IncrementalAggregateAllWindowFunction(
        finalRowArity,
        isTableAggregate)
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
    namedAggregates: Seq[CalcitePair[AggregateCall, String]],
    properties: Seq[NamedWindowProperty]):
  WindowFunction[Row, CRow, Row, DataStreamWindow] = {

    val isTableAggregate = this.isTableAggregate(namedAggregates.map(_.getKey))
    if (isTimeWindow(window)) {
      val (startPos, endPos, timePos) = computeWindowPropertyPos(properties)
      new IncrementalAggregateTimeWindowFunction(
        numGroupingKeys,
        numAggregates,
        startPos,
        endPos,
        timePos,
        finalRowArity,
        isTableAggregate)
        .asInstanceOf[WindowFunction[Row, CRow, Row, DataStreamWindow]]
    } else {
      new IncrementalAggregateWindowFunction(
        numGroupingKeys,
        numAggregates,
        finalRowArity,
        isTableAggregate)
    }
  }

  private[flink] def createDataStreamGroupWindowAggregateFunction(
    config: TableConfig,
    nullableInput: Boolean,
    inputTypeInfo: TypeInformation[_ <: Any],
    constants: Option[Seq[RexLiteral]],
    namedAggregates: Seq[CalcitePair[AggregateCall, String]],
    namedProperties: Seq[NamedWindowProperty],
    inputType: RelDataType,
    inputFieldTypeInfo: Seq[TypeInformation[_]],
    outputType: RelDataType,
    groupingKeys: Array[Int],
    needMerge: Boolean,
    tableConfig: TableConfig)
  : (DataStreamAggFunction[CRow, Row, Row], RowTypeInfo) = {

    val needRetract = false
    val aggregateMetadata =
      extractAggregateMetadata(
        namedAggregates.map(_.getKey),
        inputType,
        inputFieldTypeInfo.length,
        needRetract,
        tableConfig)

    val aggMapping = aggregateMetadata.getAdjustedMapping(0)
    val accumulatorRowType = new RowTypeInfo(aggregateMetadata.getAggregatesAccumulatorTypes: _*)

    val isTableAggregate = this.isTableAggregate(namedAggregates.map(e => e.left))
    val generator = new AggregationCodeGenerator(
      config,
      nullableInput,
      inputTypeInfo,
      constants,
      s"GroupingWindow${if (isTableAggregate) "Table" else ""}AggregateHelper",
      inputFieldTypeInfo,
      aggregateMetadata.getAggregateFunctions,
      aggregateMetadata.getAggregateIndices,
      aggMapping,
      aggregateMetadata.getDistinctAccMapping,
      isStateBackedDataViews = false,
      partialResults = false,
      groupingKeys,
      None,
      outputType.getFieldCount - groupingKeys.length - namedProperties.length,
      needRetract,
      needMerge,
      needReset = false,
      None
    )

    val genAggregations = generator.genAggregationsOrTableAggregations(
      outputType,
      groupingKeys.length,
      namedAggregates,
      false)
    val aggFunction = new AggregateAggFunction(genAggregations, isTableAggregate)

    (aggFunction, accumulatorRowType)
  }

  /**
    * Return true if all aggregates can be partially merged. False otherwise.
    */
  private[flink] def doAllSupportPartialMerge(
    aggregateCalls: Seq[AggregateCall],
    inputType: RelDataType,
    groupKeysCount: Int,
    tableConfig: TableConfig): Boolean = {

    val aggregateList = extractAggregateMetadata(
      aggregateCalls,
      inputType,
      inputType.getFieldCount,
      needRetraction = false,
      tableConfig).getAggregateFunctions

    doAllSupportPartialMerge(aggregateList)
  }

  /**
    * Return true if all aggregates can be partially merged. False otherwise.
    */
  private[flink] def doAllSupportPartialMerge(
      aggregateList: Array[UserDefinedAggregateFunction[_ <: Any, _ <: Any]]): Boolean = {
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
              throw new TableException(
                "Duplicate window start property encountered. This is a bug.")
            case WindowStart(_) =>
              (Some(i), e, rt, i - 1)
            case WindowEnd(_) if e.isDefined =>
              throw new TableException(
                "Duplicate window end property encountered. This is a bug.")
            case WindowEnd(_) =>
              (s, Some(i), rt, i - 1)
            case RowtimeAttribute(_) if rt.isDefined =>
              throw new TableException(
                "Duplicate window rowtime property encountered. This is a bug.")
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

  /**
    * Meta info of a multiple [[AggregateCall]] required to generate a single
    * [[GeneratedAggregations]] function.
    */
  private[flink] class AggregateMetadata(
    private val aggregates: Seq[(AggregateCallMetadata, Array[Int])],
    private val distinctAccTypesWithSpecs: Seq[(TypeInformation[_], Seq[DataViewSpec[_]])]) {

    def getAggregateFunctions: Array[UserDefinedAggregateFunction[_, _]] = {
      aggregates.map(_._1.aggregateFunction).toArray
    }

    def getAggregatesAccumulatorTypes: Array[TypeInformation[_]] = {
      aggregates.map(_._1.accumulatorType).toArray ++ distinctAccTypesWithSpecs.map(_._1)
    }

    def getAggregatesAccumulatorSpecs: Array[Seq[DataViewSpec[_]]] = {
      aggregates.map(_._1.accumulatorSpecs).toArray ++ distinctAccTypesWithSpecs.map(_._2)
    }

    def getDistinctAccMapping: Array[(Integer, JList[Integer])] = {
      val distinctAccMapping = mutable.Map[Integer, JList[Integer]]()
      aggregates.map(_._1.distinctAccIndex).zipWithIndex.foreach {
        case (distinctAccIndex, aggIndex) =>
          distinctAccMapping
            .getOrElseUpdate(distinctAccIndex, new JArrayList[Integer]())
            .add(aggIndex)
      }
      distinctAccMapping.toArray
    }

    def getAggregateCallsCount: Int = {
      aggregates.length
    }

    def getAggregateIndices: Array[Array[Int]] = {
      aggregates.map(_._2).toArray
    }

    def getAdjustedMapping(offset: Int, partialResults: Boolean = false): Array[Int] = {
      val accCount = getAggregateCallsCount + (if (partialResults) getDistinctAccCount else 0)
      (0 until accCount).map(_ + offset).toArray
    }

    def getDistinctAccCount: Int = {
      getDistinctAccMapping.count(_._1 >= 0)
    }
  }

  /**
    * Meta info of a single [[SqlAggFunction]] required to generate [[GeneratedAggregations]]
    * function.
    */
  private[flink] case class AggregateCallMetadata(
    aggregateFunction: UserDefinedAggregateFunction[_, _],
    accumulatorType: TypeInformation[_],
    accumulatorSpecs: Seq[DataViewSpec[_]],
    distinctAccIndex: Int
  )

  /**
    * Prepares metadata [[AggregateCallMetadata]] required to generate code for
    * [[GeneratedAggregations]] for a single [[SqlAggFunction]].
    *
    * @param aggregateFunction calcite's aggregate function
    * @param isDistinct true if should be distinct aggregation
    * @param distinctAccMap mapping of the distinct aggregate input fields index
    *                       to the corresponding acc index
    * @param argList indexes of the input fields of given aggregates
    * @param aggregateCount number of aggregates
    * @param inputFieldsCount number of input fields
    * @param aggregateInputTypes input types of given aggregate
    * @param needRetraction if the [[AggregateFunction]] should produce retractions
    * @param tableConfig tableConfig, required for decimal precision
    * @param isStateBackedDataViews if data should be backed by state backend
    * @param uniqueIdWithinAggregate index within an AggregateCallMetadata, used to create unique
    *                                state names for each aggregate function
    * @return the result contains required metadata:
    *   - flink's aggregate function
    *   - required accumulator information (type and specifications)
    *   - if the aggregate is distinct
    */
  private[flink] def extractAggregateCallMetadata(
      aggregateFunction: SqlAggFunction,
      isDistinct: Boolean,
      distinctAccMap: mutable.Map[util.Set[Integer], Integer],
      argList: util.List[Integer],
      aggregateCount: Int,
      inputFieldsCount: Int,
      aggregateInputTypes: Seq[RelDataType],
      needRetraction: Boolean,
      tableConfig: TableConfig,
      isStateBackedDataViews: Boolean,
      uniqueIdWithinAggregate: Int)
    : AggregateCallMetadata = {
    // store the aggregate fields of each aggregate function, by the same order of aggregates.
    // create aggregate function instances by function type and aggregate field data type.

    val aggregate: UserDefinedAggregateFunction[_, _] = createFlinkAggFunction(
      aggregateFunction,
      needRetraction,
      aggregateInputTypes,
      tableConfig)

    val (accumulatorType, accSpecs) = {
      val accType = aggregateFunction match {
        case udagg: AggSqlFunction => udagg.accType
        case _ => UserFunctionsTypeHelper.getAccumulatorTypeOfAggregateFunction(aggregate)
      }

      removeStateViewFieldsFromAccTypeInfo(
        uniqueIdWithinAggregate,
        aggregate.createAccumulator(),
        accType,
        isStateBackedDataViews)
    }

    // create distinct accumulator filter argument
    val distinctAccIndex = if (isDistinct) {
      getDistinctAccIndex(distinctAccMap, argList, aggregateCount, inputFieldsCount)
    } else {
      -1
    }

    AggregateCallMetadata(aggregate, accumulatorType, accSpecs.getOrElse(Seq()), distinctAccIndex)
  }

  private def getDistinctAccIndex(
      distinctAccMap: mutable.Map[util.Set[Integer], Integer],
      argList: util.List[Integer],
      aggregateCount: Int,
      inputFieldsCount: Int): Int = {

    val argListWithoutConstants = argList.toSet.filter(i => i > -1 && i < inputFieldsCount)
    distinctAccMap.get(argListWithoutConstants) match {
      case None =>
        val index: Int = aggregateCount + distinctAccMap.size
        distinctAccMap.put(argListWithoutConstants, index)
        index

      case Some(index) => index
    }
  }

  /**
    * Prepares metadata [[AggregateMetadata]] required to generate code for
    * [[AggregationsFunction]] for all [[AggregateCall]].
    *
    * @param aggregateCalls calcite's aggregate function
    * @param aggregateInputType input type of given aggregates
    * @param inputFieldsCount number of input fields
    * @param needRetraction if the [[AggregateFunction]] should produce retractions
    * @param tableConfig tableConfig, required for decimal precision
    * @param isStateBackedDataViews if data should be backed by state backend
    * @return the result contains required metadata:
    * - flink's aggregate function
    * - required accumulator information (type and specifications)
    * - indices important for each aggregate
    * - if the aggregate is distinct
    */
  private def extractAggregateMetadata(
      aggregateCalls: Seq[AggregateCall],
      aggregateInputType: RelDataType,
      inputFieldsCount: Int,
      needRetraction: Boolean,
      tableConfig: TableConfig,
      isStateBackedDataViews: Boolean = false)
    : AggregateMetadata = {

    val distinctAccMap = mutable.Map[util.Set[Integer], Integer]()

    val aggregatesWithIndices = aggregateCalls.zipWithIndex.map {
      case (aggregateCall, index) =>
        val argList: util.List[Integer] = aggregateCall.getArgList

        val aggFieldIndices = if (argList.isEmpty) {
          if (aggregateCall.getAggregation.isInstanceOf[SqlCountAggFunction]) {
            Array[Int](-1)
          } else {
            throw new TableException("Aggregate fields should not be empty.")
          }
        } else {
          argList.asScala.map(i => i.intValue).toArray
        }

        val inputTypes = argList.map(aggregateInputType.getFieldList.get(_).getType)
        val aggregateCallMetadata = extractAggregateCallMetadata(
          aggregateCall.getAggregation,
          aggregateCall.isDistinct,
          distinctAccMap,
          argList,
          aggregateCalls.length,
          inputFieldsCount,
          inputTypes,
          needRetraction,
          tableConfig,
          isStateBackedDataViews,
          index)

        (aggregateCallMetadata, aggFieldIndices)
    }

    val distinctAccType = Types.POJO(classOf[DistinctAccumulator])

    val distinctAccTypesWithSpecs = (0 until distinctAccMap.size).map { idx =>
      val (accType, accSpec) = removeStateViewFieldsFromAccTypeInfo(
        aggregateCalls.length + idx,
        new DistinctAccumulator(),
        distinctAccType,
        isStateBackedDataViews)
      (accType, accSpec.getOrElse(Seq()))
    }

    // store the aggregate fields of each aggregate function, by the same order of aggregates.
    new AggregateMetadata(aggregatesWithIndices, distinctAccTypesWithSpecs)
  }

  /**
    * Converts calcite's [[SqlAggFunction]] to a Flink's UDF [[AggregateFunction]].
    * create aggregate function instances by function type and aggregate field data type.
    */
  private def createFlinkAggFunction(
      aggFunc: SqlAggFunction,
      needRetraction: Boolean,
      inputDataType: Seq[RelDataType],
      tableConfig: TableConfig)
    : UserDefinedAggregateFunction[_ <: Any, _ <: Any] = {

    lazy val outputType = inputDataType.get(0)
    lazy val outputTypeName = if (inputDataType.isEmpty) {
      throw new TableException("Aggregate fields should not be empty.")
    } else {
      outputType.getSqlTypeName
    }

    aggFunc match {

      case collect: SqlAggFunction if collect.getKind == SqlKind.COLLECT =>
        new CollectAggFunction(FlinkTypeFactory.toTypeInfo(outputType))

      case udagg: AggSqlFunction =>
        udagg.getFunction

      case _: SqlCountAggFunction =>
        new CountAggFunction

      case _: SqlSumAggFunction =>
        if (needRetraction) {
          outputTypeName match {
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
          outputTypeName match {
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
          outputTypeName match {
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
          outputTypeName match {
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

      case a: SqlAvgAggFunction if a.kind == SqlKind.AVG =>
        outputTypeName match {
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
            new DecimalAvgAggFunction(tableConfig.getDecimalContext)
          case sqlType: SqlTypeName =>
            throw new TableException(s"Avg aggregate does no support type: '$sqlType'")
        }

      case sqlMinMaxFunction: SqlMinMaxAggFunction =>
        if (sqlMinMaxFunction.getKind == SqlKind.MIN) {
          if (needRetraction) {
            outputTypeName match {
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
              case TIMESTAMP =>
                new TimestampMinWithRetractAggFunction
              case DATE =>
                new DateMinWithRetractAggFunction
              case TIME =>
                new TimeMinWithRetractAggFunction
              case sqlType: SqlTypeName =>
                throw new TableException(
                  s"Min with retract aggregate does no support type: '$sqlType'")
            }
          } else {
            outputTypeName match {
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
              case TIMESTAMP =>
                new TimestampMinAggFunction
              case DATE =>
                new DateMinAggFunction
              case TIME =>
                new TimeMinAggFunction
              case sqlType: SqlTypeName =>
                throw new TableException(s"Min aggregate does no support type: '$sqlType'")
            }
          }
        } else {
          if (needRetraction) {
            outputTypeName match {
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
              case TIMESTAMP =>
                new TimestampMaxWithRetractAggFunction
              case DATE =>
                new DateMaxWithRetractAggFunction
              case TIME =>
                new TimeMaxWithRetractAggFunction
              case sqlType: SqlTypeName =>
                throw new TableException(
                  s"Max with retract aggregate does no support type: '$sqlType'")
            }
          } else {
            outputTypeName match {
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
              case TIMESTAMP =>
                new TimestampMaxAggFunction
              case DATE =>
                new DateMaxAggFunction
              case TIME =>
                new TimeMaxAggFunction
              case sqlType: SqlTypeName =>
                throw new TableException(s"Max aggregate does no support type: '$sqlType'")
            }
          }
        }

      case unSupported: SqlAggFunction =>
        throw new TableException(s"Unsupported Function: '${unSupported.getName}'")
    }
  }

  private def createRowTypeForKeysAndAggregates(
      groupings: Array[Int],
      aggregates: Array[UserDefinedAggregateFunction[_, _]],
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
    timeField: PlannerExpression,
    inputType: RelDataType,
    isParserCaseSensitive: Boolean): Int = {

    timeField match {
      case PlannerResolvedFieldReference(name: String, _) =>
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
          throw new TableException(
            s"Encountered more than one time attribute with the same name: '$relDataType'")
        }
      case e => throw new TableException(
        "The time attribute of window in batch environment should be " +
          s"ResolvedFieldReference, but is $e")
    }
  }

  private[flink] def asLong(expr: PlannerExpression): Long = expr match {
    case Literal(value: Long, TimeIntervalTypeInfo.INTERVAL_MILLIS) => value
    case Literal(value: Long, BasicTypeInfo.LONG_TYPE_INFO) => value
    case _ => throw new IllegalArgumentException()
  }

  private[flink] def determineLargestTumblingSize(size: Long, slide: Long) = gcd(size, slide)

  private def gcd(a: Long, b: Long): Long = {
    if (b == 0) a else gcd(b, a % b)
  }

  private[flink] def isTableAggregate(aggCalls: util.List[AggregateCall]): Boolean = {
    val aggregates = aggCalls
      .filter(e => e.getAggregation.isInstanceOf[AggSqlFunction])
      .map(e => e.getAggregation.asInstanceOf[AggSqlFunction].getFunction)
    containsTableAggregateFunction(aggregates)
  }

  private[flink] def containsTableAggregateFunction(
      aggregates: Seq[UserDefinedAggregateFunction[_, _]])
    : Boolean = {
    aggregates.exists(_.isInstanceOf[TableAggregateFunction[_, _]])
  }
}
