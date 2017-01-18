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
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.java.typeutils.{ResultTypeQueryable, RowTypeInfo}
import org.apache.flink.table.api.BatchTableEnvironment
import org.apache.flink.table.calcite.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.logical._
import org.apache.flink.table.plan.nodes.FlinkAggregate
import org.apache.flink.table.runtime.aggregate.AggregateUtil.{CalcitePair, _}
import org.apache.flink.table.typeutils.TypeCheckUtils.isTimeInterval
import org.apache.flink.table.typeutils.TypeConverter
import org.apache.flink.types.Row

import scala.collection.JavaConversions._

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
  extends SingleRel(cluster, traitSet, inputNode)
  with FlinkAggregate
  with DataSetRel {

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

  override def computeSelfCost (planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {
    val child = this.getInput
    val rowCnt = metadata.getRowCount(child)
    val rowSize = this.estimateRowSize(child.getRowType)
    val aggCnt = this.namedAggregates.size
    planner.getCostFactory.makeCost(rowCnt, rowCnt * aggCnt, rowCnt * rowSize)
  }

  override def translateToPlan(
    tableEnv: BatchTableEnvironment,
    expectedType: Option[TypeInformation[Any]]): DataSet[Any] = {

    val config = tableEnv.getConfig

    val inputDS = getInput.asInstanceOf[DataSetRel].translateToPlan(
      tableEnv,
      // tell the input operator that this operator currently only supports Rows as input
      Some(TypeConverter.DEFAULT_ROW_TYPE))

    // whether identifiers are matched case-sensitively
    val caseSensitive = tableEnv.getFrameworkConfig.getParserConfig.caseSensitive()
    val result = window match {
      case EventTimeTumblingGroupWindow(_, _, size) =>
        createEventTimeTumblingWindowDataSet(
          inputDS,
          isTimeInterval(size.resultType),
          caseSensitive)
      case EventTimeSessionGroupWindow(_, _, gap) =>
        createEventTimeSessionWindowDataSet(inputDS,caseSensitive)
      case EventTimeSlidingGroupWindow(_, _, _, _) =>
        throw new UnsupportedOperationException(
          "Event-time sliding windows in a batch environment are currently not supported")
      case _: ProcessingTimeGroupWindow =>
        throw new UnsupportedOperationException(
          "Processing-time tumbling windows are not supported in a batch environment, " +
            "windows in a batch environment must declare a time attribute over which " +
            "the query is evaluated.")
    }

    // if the expected type is not a Row, inject a mapper to convert to the expected type
    expectedType match {
      case Some(typeInfo) if typeInfo.getTypeClass != classOf[Row] =>
        val mapName = s"convert: (${getRowType.getFieldNames.toList.mkString(", ")})"
        result.map(
          getConversionMapper(
            config = config,
            nullableInput = false,
            inputType = resultRowTypeInfo.asInstanceOf[TypeInformation[Any]],
            expectedType = expectedType.get,
            conversionOperatorName = "DataSetWindowAggregateConversion",
            fieldNames = getRowType.getFieldNames
          ))
          .name(mapName)
      case _ => result
    }
  }


  private def createEventTimeTumblingWindowDataSet(
      inputDS: DataSet[Any],
      isTimeWindow: Boolean,
      isParserCaseSensitive: Boolean)
    : DataSet[Any] = {
    val mapFunction = createDataSetWindowPrepareMapFunction(
      window,
      namedAggregates,
      grouping,
      inputType,
      isParserCaseSensitive)
    val groupReduceFunction = createDataSetWindowAggregationGroupReduceFunction(
      window,
      namedAggregates,
      inputType,
      getRowType,
      grouping,
      namedProperties)

    val mappedInput = inputDS
      .map(mapFunction)
      .name(prepareOperatorName)

    val mapReturnType = mapFunction.asInstanceOf[ResultTypeQueryable[Row]].getProducedType
    if (isTimeWindow) {
      // grouped time window aggregation
      // group by grouping keys and rowtime field (the last field in the row)
      val groupingKeys = grouping.indices ++ Seq(mapReturnType.getArity - 1)
      mappedInput.asInstanceOf[DataSet[Row]]
        .groupBy(groupingKeys: _*)
        .reduceGroup(groupReduceFunction)
        .returns(resultRowTypeInfo)
        .name(aggregateOperatorName)
        .asInstanceOf[DataSet[Any]]
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
          .returns(resultRowTypeInfo)
          .name(aggregateOperatorName)
          .asInstanceOf[DataSet[Any]]

      } else {
        // TODO: count tumbling all window on event-time should sort all the data set
        // on event time before applying the windowing logic.
        throw new UnsupportedOperationException(
          "Count tumbling non-grouping window on event-time are currently not supported.")
      }
    }
  }

  private[this] def createEventTimeSessionWindowDataSet(
    inputDS: DataSet[Any],
    isParserCaseSensitive: Boolean): DataSet[Any] = {

    val groupingKeys = grouping.indices.toArray
    val rowTypeInfo = resultRowTypeInfo

    // grouping window
    if (groupingKeys.length > 0) {
      //create mapFunction for initializing the aggregations
      val mapFunction = createDataSetWindowPrepareMapFunction(
        window,
        namedAggregates,
        grouping,
        inputType,isParserCaseSensitive)

      // create groupReduceFunction for calculating the aggregations
      val groupReduceFunction = createDataSetWindowAggregationGroupReduceFunction(
        window,
        namedAggregates,
        inputType,
        rowRelDataType,
        grouping,
        namedProperties)

      val mappedInput =
        inputDS
        .map(mapFunction)
        .name(prepareOperatorName)

      val mapReturnType = mapFunction.asInstanceOf[ResultTypeQueryable[Row]].getProducedType

      // the position of the rowtime field in the intermediate result for map output
      val rowTimeFilePos = mapReturnType.getArity - 1

      // do incremental aggregation
      if (doAllSupportPartialAggregation(
        namedAggregates.map(_.getKey),
        inputType,
        grouping.length)) {

        // gets the window-start and window-end position  in the intermediate result.
        val windowStartPos = rowTimeFilePos
        val windowEndPos = windowStartPos + 1

        val combineGroupFunction = createDataSetWindowAggregationCombineFunction(
          window,
          namedAggregates,
          inputType,
          grouping)

        mappedInput.groupBy(groupingKeys: _*)
        .sortGroup(rowTimeFilePos, Order.ASCENDING)
        .combineGroup(combineGroupFunction)
        .groupBy(groupingKeys: _*)
        .sortGroup(windowStartPos, Order.ASCENDING)
        .sortGroup(windowEndPos, Order.ASCENDING)
        .reduceGroup(groupReduceFunction)
        .returns(rowTypeInfo)
        .name(aggregateOperatorName)
        .asInstanceOf[DataSet[Any]]
      }
      // do non-incremental aggregation
      else {
        mappedInput.groupBy(groupingKeys: _*)
        .sortGroup(rowTimeFilePos, Order.ASCENDING)
        .reduceGroup(groupReduceFunction)
        .returns(rowTypeInfo)
        .name(aggregateOperatorName)
        .asInstanceOf[DataSet[Any]]
      }
    }
    // non-grouping window
    else {
      throw new UnsupportedOperationException(
        "Session non-grouping window on event-time are currently not supported.")
    }
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

  private def resultRowTypeInfo: RowTypeInfo = {
    // get the output types
    val fieldTypes: Array[TypeInformation[_]] = getRowType.getFieldList
      .map(field => FlinkTypeFactory.toTypeInfo(field.getType))
      .toArray
    new RowTypeInfo(fieldTypes: _*)
  }
}
