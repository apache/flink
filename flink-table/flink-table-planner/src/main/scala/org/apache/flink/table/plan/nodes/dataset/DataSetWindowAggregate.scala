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
package org.apache.flink.table.plan.nodes.dataset

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.rex.RexLiteral
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.java.typeutils.{ResultTypeQueryable, RowTypeInfo}
import org.apache.flink.table.api.internal.BatchTableEnvImpl
import org.apache.flink.table.api.{BatchQueryConfig, TableConfig}
import org.apache.flink.table.calcite.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.expressions.PlannerExpressionUtils._
import org.apache.flink.table.plan.logical._
import org.apache.flink.table.plan.nodes.CommonAggregate
import org.apache.flink.table.runtime.aggregate.AggregateUtil.{CalcitePair, _}
import org.apache.flink.table.typeutils.TypeCheckUtils.{isLong, isTimePoint}
import org.apache.flink.types.Row

/**
  * Flink RelNode which matches along with a LogicalWindowAggregate.
  */
class DataSetWindowAggregate(
    window: LogicalWindow,
    namedProperties: Seq[NamedWindowProperty],
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    namedAggregates: Seq[CalcitePair[AggregateCall, String]],
    rowRelDataType: RelDataType,
    inputType: RelDataType,
    grouping: Array[Int])
  extends SingleRel(cluster, traitSet, inputNode) with CommonAggregate with DataSetRel {

  override def deriveRowType() = rowRelDataType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataSetWindowAggregate(
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

  override def computeSelfCost(planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {
    val child = this.getInput
    val rowCnt = metadata.getRowCount(child)
    val rowSize = this.estimateRowSize(child.getRowType)
    val aggCnt = this.namedAggregates.size
    planner.getCostFactory.makeCost(rowCnt, rowCnt * aggCnt, rowCnt * rowSize)
  }

  override def translateToPlan(
      tableEnv: BatchTableEnvImpl,
      queryConfig: BatchQueryConfig): DataSet[Row] = {

    val inputDS = getInput.asInstanceOf[DataSetRel].translateToPlan(tableEnv, queryConfig)

    // whether identifiers are matched case-sensitively
    val caseSensitive = tableEnv.getParserConfig.caseSensitive()

    window match {
      case TumblingGroupWindow(_, timeField, size)
          if isTimePoint(timeField.resultType) || isLong(timeField.resultType) =>
        createEventTimeTumblingWindowDataSet(
          tableEnv.getConfig,
          false,
          inputDS.getType,
          None,
          inputDS,
          isTimeIntervalLiteral(size),
          caseSensitive,
          tableEnv.getConfig)

      case SessionGroupWindow(_, timeField, gap)
          if isTimePoint(timeField.resultType) || isLong(timeField.resultType) =>
        createEventTimeSessionWindowDataSet(
          tableEnv.getConfig,
          false,
          inputDS.getType,
          None,
          inputDS,
          caseSensitive,
          tableEnv.getConfig)

      case SlidingGroupWindow(_, timeField, size, slide)
          if isTimePoint(timeField.resultType) || isLong(timeField.resultType) =>
        createEventTimeSlidingWindowDataSet(
          tableEnv.getConfig,
          false,
          inputDS.getType,
          None,
          inputDS,
          isTimeIntervalLiteral(size),
          asLong(size),
          asLong(slide),
          caseSensitive,
          tableEnv.getConfig)

      case _ =>
        throw new UnsupportedOperationException(
          s"Window $window is not supported in a batch environment.")
    }
  }

  private def createEventTimeTumblingWindowDataSet(
      config: TableConfig,
      nullableInput: Boolean,
      inputType: TypeInformation[_ <: Any],
      constants: Option[Seq[RexLiteral]],
      inputDS: DataSet[Row],
      isTimeWindow: Boolean,
      isParserCaseSensitive: Boolean,
      tableConfig: TableConfig): DataSet[Row] = {

    val input = inputNode.asInstanceOf[DataSetRel]

    val mapFunction = createDataSetWindowPrepareMapFunction(
      config,
      nullableInput,
      inputType,
      constants,
      window,
      namedAggregates,
      grouping,
      input.getRowType,
      inputDS.getType.asInstanceOf[RowTypeInfo].getFieldTypes,
      isParserCaseSensitive,
      tableConfig)
    val groupReduceFunction = createDataSetWindowAggregationGroupReduceFunction(
      config,
      nullableInput,
      inputType,
      constants,
      window,
      namedAggregates,
      input.getRowType,
      inputDS.getType.asInstanceOf[RowTypeInfo].getFieldTypes,
      getRowType,
      grouping,
      namedProperties,
      tableConfig)

    val mappedInput = inputDS
      .map(mapFunction)
      .name(prepareOperatorName)

    val rowTypeInfo = FlinkTypeFactory.toInternalRowTypeInfo(getRowType)

    val mapReturnType = mapFunction.asInstanceOf[ResultTypeQueryable[Row]].getProducedType
    if (isTimeWindow) {
      // grouped time window aggregation
      // group by grouping keys and rowtime field (the last field in the row)
      val groupingKeys = grouping.indices ++ Seq(mapReturnType.getArity - 1)
      mappedInput.asInstanceOf[DataSet[Row]]
        .groupBy(groupingKeys: _*)
        .reduceGroup(groupReduceFunction)
        .returns(rowTypeInfo)
        .name(aggregateOperatorName)
    } else {
      // count window
      val groupingKeys = grouping.indices.toArray
      if (groupingKeys.length > 0) {
        // grouped aggregation
        mappedInput.asInstanceOf[DataSet[Row]]
          .groupBy(groupingKeys: _*)
          // sort on time field, it's the last element in the row
          .sortGroup(mapReturnType.getArity - 1, Order.ASCENDING)
          .reduceGroup(groupReduceFunction)
          .returns(rowTypeInfo)
          .name(aggregateOperatorName)
      } else {
        // TODO: count tumbling all window on event-time should sort all the data set
        // on event time before applying the windowing logic.
        throw new UnsupportedOperationException(
          "Count tumbling non-grouping windows on event-time are currently not supported.")
      }
    }
  }

  private[this] def createEventTimeSessionWindowDataSet(
      config: TableConfig,
      nullableInput: Boolean,
      inputTypeInfo: TypeInformation[_ <: Any],
      constants: Option[Seq[RexLiteral]],
      inputDS: DataSet[Row],
      isParserCaseSensitive: Boolean,
      tableConfig: TableConfig): DataSet[Row] = {

    val input = inputNode.asInstanceOf[DataSetRel]

    val groupingKeys = grouping.indices.toArray
    val rowTypeInfo = FlinkTypeFactory.toInternalRowTypeInfo(getRowType)

    // create mapFunction for initializing the aggregations
    val mapFunction = createDataSetWindowPrepareMapFunction(
      config,
      nullableInput,
      inputTypeInfo,
      constants,
      window,
      namedAggregates,
      grouping,
      input.getRowType,
      inputDS.getType.asInstanceOf[RowTypeInfo].getFieldTypes,
      isParserCaseSensitive,
      tableConfig)

    val mappedInput = inputDS.map(mapFunction).name(prepareOperatorName)

    val mapReturnType = mapFunction.asInstanceOf[ResultTypeQueryable[Row]].getProducedType

    // the position of the rowtime field in the intermediate result for map output
    val rowTimeFieldPos = mapReturnType.getArity - 1

    // do incremental aggregation
    if (doAllSupportPartialMerge(
      namedAggregates.map(_.getKey),
      inputType,
      grouping.length,
      tableConfig)) {

      // gets the window-start and window-end position  in the intermediate result.
      val windowStartPos = rowTimeFieldPos
      val windowEndPos = windowStartPos + 1
      // grouping window
      if (groupingKeys.length > 0) {
        // create groupCombineFunction for combine the aggregations
        val combineGroupFunction = createDataSetWindowAggregationCombineFunction(
          config,
          nullableInput,
          inputTypeInfo,
          constants,
          window,
          namedAggregates,
          input.getRowType,
          inputDS.getType.asInstanceOf[RowTypeInfo].getFieldTypes,
          grouping,
          tableConfig)

        // create groupReduceFunction for calculating the aggregations
        val groupReduceFunction = createDataSetWindowAggregationGroupReduceFunction(
          config,
          nullableInput,
          inputTypeInfo,
          constants,
          window,
          namedAggregates,
          input.getRowType,
          inputDS.getType.asInstanceOf[RowTypeInfo].getFieldTypes,
          rowRelDataType,
          grouping,
          namedProperties,
          tableConfig,
          isInputCombined = true)

        mappedInput
          .groupBy(groupingKeys: _*)
          .sortGroup(rowTimeFieldPos, Order.ASCENDING)
          .combineGroup(combineGroupFunction)
          .groupBy(groupingKeys: _*)
          .sortGroup(windowStartPos, Order.ASCENDING)
          .sortGroup(windowEndPos, Order.ASCENDING)
          .reduceGroup(groupReduceFunction)
          .returns(rowTypeInfo)
          .name(aggregateOperatorName)
      } else {
        // non-grouping window
        val mapPartitionFunction = createDataSetWindowAggregationMapPartitionFunction(
          config,
          nullableInput,
          inputTypeInfo,
          constants,
          window,
          namedAggregates,
          input.getRowType,
          inputDS.getType.asInstanceOf[RowTypeInfo].getFieldTypes,
          grouping,
          tableConfig)

        // create groupReduceFunction for calculating the aggregations
        val groupReduceFunction = createDataSetWindowAggregationGroupReduceFunction(
          config,
          nullableInput,
          inputTypeInfo,
          constants,
          window,
          namedAggregates,
          input.getRowType,
          inputDS.getType.asInstanceOf[RowTypeInfo].getFieldTypes,
          rowRelDataType,
          grouping,
          namedProperties,
          tableConfig,
          isInputCombined = true)

        mappedInput.sortPartition(rowTimeFieldPos, Order.ASCENDING)
          .mapPartition(mapPartitionFunction)
          .sortPartition(windowStartPos, Order.ASCENDING).setParallelism(1)
          .sortPartition(windowEndPos, Order.ASCENDING).setParallelism(1)
          .reduceGroup(groupReduceFunction)
          .returns(rowTypeInfo)
          .name(aggregateOperatorName)
          .asInstanceOf[DataSet[Row]]
      }
    // do non-incremental aggregation
    } else {
      // grouping window
      if (groupingKeys.length > 0) {

        // create groupReduceFunction for calculating the aggregations
        val groupReduceFunction = createDataSetWindowAggregationGroupReduceFunction(
          config,
          nullableInput,
          inputTypeInfo,
          constants,
          window,
          namedAggregates,
          input.getRowType,
          inputDS.getType.asInstanceOf[RowTypeInfo].getFieldTypes,
          rowRelDataType,
          grouping,
          namedProperties,
          tableConfig)

        mappedInput.groupBy(groupingKeys: _*)
          .sortGroup(rowTimeFieldPos, Order.ASCENDING)
          .reduceGroup(groupReduceFunction)
          .returns(rowTypeInfo)
          .name(aggregateOperatorName)
      } else {
        // non-grouping window
        val groupReduceFunction = createDataSetWindowAggregationGroupReduceFunction(
          config,
          nullableInput,
          inputTypeInfo,
          constants,
          window,
          namedAggregates,
          input.getRowType,
          inputDS.getType.asInstanceOf[RowTypeInfo].getFieldTypes,
          rowRelDataType,
          grouping,
          namedProperties,
          tableConfig)

        mappedInput.sortPartition(rowTimeFieldPos, Order.ASCENDING).setParallelism(1)
          .reduceGroup(groupReduceFunction)
          .returns(rowTypeInfo)
          .name(aggregateOperatorName)
          .asInstanceOf[DataSet[Row]]
      }
    }
  }

  private def createEventTimeSlidingWindowDataSet(
      config: TableConfig,
      nullableInput: Boolean,
      inputTypeInfo: TypeInformation[_ <: Any],
      constants: Option[Seq[RexLiteral]],
      inputDS: DataSet[Row],
      isTimeWindow: Boolean,
      size: Long,
      slide: Long,
      isParserCaseSensitive: Boolean,
      tableConfig: TableConfig)
    : DataSet[Row] = {

    val input = inputNode.asInstanceOf[DataSetRel]

    // create MapFunction for initializing the aggregations
    // it aligns the rowtime for pre-tumbling in case of a time-window for partial aggregates
    val mapFunction = createDataSetWindowPrepareMapFunction(
      config,
      nullableInput,
      inputTypeInfo,
      constants,
      window,
      namedAggregates,
      grouping,
      input.getRowType,
      inputDS.getType.asInstanceOf[RowTypeInfo].getFieldTypes,
      isParserCaseSensitive,
      tableConfig)

    val mappedDataSet = inputDS
      .map(mapFunction)
      .name(prepareOperatorName)

    val mapReturnType = mappedDataSet.getType

    val rowTypeInfo = FlinkTypeFactory.toInternalRowTypeInfo(getRowType)
    val groupingKeys = grouping.indices.toArray

    // do partial aggregation if possible
    val isPartial = doAllSupportPartialMerge(
      namedAggregates.map(_.getKey),
      inputType,
      grouping.length,
      tableConfig)

    // only pre-tumble if it is worth it
    val isLittleTumblingSize = determineLargestTumblingSize(size, slide) <= 1

    val preparedDataSet = if (isTimeWindow) {
      // time window

      if (isPartial && !isLittleTumblingSize) {
        // partial aggregates

        val groupingKeysAndAlignedRowtime = groupingKeys :+ mapReturnType.getArity - 1

        // create GroupReduceFunction
        // for pre-tumbling and replicating/omitting the content for each pane
        val prepareReduceFunction = createDataSetSlideWindowPrepareGroupReduceFunction(
          config,
          nullableInput,
          inputTypeInfo,
          constants,
          window,
          namedAggregates,
          grouping,
          input.getRowType,
          inputDS.getType.asInstanceOf[RowTypeInfo].getFieldTypes,
          isParserCaseSensitive,
          tableConfig)

        mappedDataSet.asInstanceOf[DataSet[Row]]
          .groupBy(groupingKeysAndAlignedRowtime: _*)
          .reduceGroup(prepareReduceFunction) // pre-tumbles and replicates/omits
          .name(prepareOperatorName)
      } else {
        // non-partial aggregates

        // create FlatMapFunction
        // for replicating/omitting the content for each pane
        val prepareFlatMapFunction = createDataSetSlideWindowPrepareFlatMapFunction(
          window,
          namedAggregates,
          grouping,
          mapReturnType,
          isParserCaseSensitive)

        mappedDataSet
          .flatMap(prepareFlatMapFunction) // replicates/omits
      }
    } else {
      // count window

      throw new UnsupportedOperationException(
          "Count sliding group windows on event-time are currently not supported.")
    }

    val prepareReduceReturnType = preparedDataSet.getType

    // create GroupReduceFunction for final aggregation and conversion to output row
    val aggregateReduceFunction = createDataSetWindowAggregationGroupReduceFunction(
      config,
      nullableInput,
      inputTypeInfo,
      constants,
      window,
      namedAggregates,
      input.getRowType,
      inputDS.getType.asInstanceOf[RowTypeInfo].getFieldTypes,
      rowRelDataType,
      grouping,
      namedProperties,
      tableConfig,
      isInputCombined = false)

    // gets the window-start position in the intermediate result.
    val windowStartPos = prepareReduceReturnType.getArity - 1

    val groupingKeysAndWindowStart = groupingKeys :+ windowStartPos

    preparedDataSet
      .groupBy(groupingKeysAndWindowStart: _*)
      .reduceGroup(aggregateReduceFunction)
      .returns(rowTypeInfo)
      .name(aggregateOperatorName)
  }

  private def prepareOperatorName: String = {
    val aggString = aggregationToString(
      inputType,
      grouping,
      getRowType,
      namedAggregates,
      namedProperties)
    s"prepare select: ($aggString)"
  }

  private def aggregateOperatorName: String = {
    val aggString = aggregationToString(
      inputType,
      grouping,
      getRowType,
      namedAggregates,
      namedProperties)
    if (grouping.length > 0) {
      s"groupBy: (${groupingToString(inputType, grouping)}), " +
        s"window: ($window), select: ($aggString)"
    } else {
      s"window: ($window), select: ($aggString)"
    }
  }
}
