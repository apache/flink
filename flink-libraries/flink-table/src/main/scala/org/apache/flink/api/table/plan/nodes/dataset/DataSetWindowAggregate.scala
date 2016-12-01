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

package org.apache.flink.api.table.plan.nodes.dataset

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.flink.api.common.functions.RichGroupReduceFunction
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.java.operators.MapOperator
import org.apache.flink.api.table.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.api.table.plan.logical.{EventTimeSessionGroupWindow,LogicalWindow}
import org.apache.flink.api.table.plan.nodes.FlinkAggregate
import org.apache.flink.api.table.runtime.aggregate.AggregateUtil
import org.apache.flink.api.table.runtime.aggregate.AggregateUtil.CalcitePair
import org.apache.flink.api.table.typeutils.{RowTypeInfo, TypeConverter}
import org.apache.flink.api.table.{BatchTableEnvironment, FlinkTypeFactory, Row}

import scala.collection.JavaConverters._

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

  override def computeSelfCost(planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {
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
    val groupingKeys = grouping.indices.toArray

    // get the output types
    val fieldTypes: Array[TypeInformation[_]] =
    getRowType.getFieldList.asScala
    .map(field => FlinkTypeFactory.toTypeInfo(field.getType))
    .toArray

    val rowTypeInfo = new RowTypeInfo(fieldTypes)

    val inputDS = getInput.asInstanceOf[DataSetRel].translateToPlan(
      tableEnv,
      // tell the input operator that this operator currently only supports Rows as input
      Some(TypeConverter.DEFAULT_ROW_TYPE))

    val aggString = aggregationToString(
      inputType,
      grouping,
      getRowType,
      namedAggregates,
      namedProperties)

    val aggOpName =
      if (grouping.length > 0) {
        s"groupBy: (${groupingToString(inputType, grouping)}), " +
          s"window: ($window), select: ($aggString)"
      } else {
        s"window: ($window), select: ($aggString)"
      }

    //create mapFunction for initializing the aggregations
    val mapFunction = AggregateUtil.createDataSetWindowPrepareMapFunction(
      window,
      namedAggregates,
      grouping,
      inputType)

    // create groupReduceFunction for calculating the aggregations
    val groupReduceFunction =
    AggregateUtil.createDataSetWindowAggregateReduceGroupFunction(
      window,
      namedProperties,
      namedAggregates,
      inputType,
      rowRelDataType,
      grouping)


    val prepareOpName = s"prepare select: ($aggString)"

    val mappedInput =
      inputDS
      .map(mapFunction)
      .name(prepareOpName)

    // check whether all aggregates support partial aggregate
    val result: DataSet[Any] = {
      window match {
        case EventTimeSessionGroupWindow(_, _, _) =>
           createEventTimeSessionGroupWindowDataSet(
            aggOpName,
            groupReduceFunction,
            mappedInput)
        case _ =>
          throw new UnsupportedOperationException(
            s" [ ${window.getClass.getCanonicalName.split("\\.").last} ] is currently not " +
              s"supported on batch tables. ")
      }

    }
    // if the expected type is not a Row, inject a mapper to convert it to the expected type
    expectedType match {
      case Some(typeInfo) if typeInfo.getTypeClass != classOf[Row] =>
        val mapName = s"convert: (${getRowType.getFieldNames.asScala.toList.mkString(", ")})"
        result.map(
          getConversionMapper(
            config = config,
            nullableInput = false,
            inputType = rowTypeInfo.asInstanceOf[TypeInformation[Any]],
            expectedType = expectedType.get,
            conversionOperatorName = "DataSetWindowAggregateConversion",
            fieldNames = getRowType.getFieldNames.asScala
          ))
        .name(mapName)
      case _ => result
    }
  }
  private[this] def createEventTimeSessionGroupWindowDataSet(
    aggOpName: String,
    groupReduceFunction: RichGroupReduceFunction[Row, Row],
    mappedInput: MapOperator[Any, Row]): DataSet[Any] = {
    val groupingKeys = grouping.indices.toArray
    val rowTypeInfo = resultRowTypeInfo

    // gets the start and end position of the window in the intermediate result for
    // combine and reduce.
    val (windowStartPos, windowEndPos) =
    AggregateUtil.computeWindowStartEndPropertyIntermediatePos(
      namedAggregates,
      inputType,
      grouping)

    // the position of the rowtime field in the intermediate result for map output
    val rowTimeFilePos = windowStartPos
    // grouping window
    if (groupingKeys.length > 0) {
      // do incremental aggregation
      if (AggregateUtil.doAllSupportPartialAggregation(
        namedAggregates.map(_.getKey),
        inputType,
        grouping.length)) {
        val combineGroupFunction =
          AggregateUtil.createDataSetSessionWindowAggregateCombineFunction(
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
        .name(aggOpName)
        .asInstanceOf[DataSet[Any]]
      }
      // do incremental aggregation
      else {
        mappedInput.groupBy(groupingKeys: _*)
        .sortGroup(rowTimeFilePos, Order.ASCENDING)
        .reduceGroup(groupReduceFunction)
        .returns(rowTypeInfo)
        .name(aggOpName)
        .asInstanceOf[DataSet[Any]]
      }
    }
    // non-grouping window
    else {
      throw new UnsupportedOperationException(
        "Session non-grouping window on event-time are currently not supported.")
    }
  }

  private[this] def resultRowTypeInfo: RowTypeInfo = {
    // get the output types
    val fieldTypes: Array[TypeInformation[_]] =
    getRowType.getFieldList.asScala
    .map(field => FlinkTypeFactory.toTypeInfo(field.getType))
    .toArray
    new RowTypeInfo(fieldTypes)
  }

}
