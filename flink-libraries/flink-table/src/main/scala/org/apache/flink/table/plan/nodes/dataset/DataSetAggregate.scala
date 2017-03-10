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
import org.apache.flink.api.common.functions.GroupReduceFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.BatchTableEnvironment
import org.apache.flink.table.plan.nodes.dataset.forwarding.FieldForwardingUtils.getForwardedFields
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.nodes.CommonAggregate
import org.apache.flink.table.runtime.aggregate.{AggregateUtil, DataSetPreAggFunction}
import org.apache.flink.table.runtime.aggregate.AggregateUtil.CalcitePair
import org.apache.flink.types.Row

/**
  * Flink RelNode which matches along with a LogicalAggregate.
  */
class DataSetAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    namedAggregates: Seq[CalcitePair[AggregateCall, String]],
    rowRelDataType: RelDataType,
    inputType: RelDataType,
    grouping: Array[Int],
    inGroupingSet: Boolean)
  extends SingleRel(cluster, traitSet, inputNode) with CommonAggregate with DataSetRel {

  override def deriveRowType(): RelDataType = rowRelDataType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataSetAggregate(
      cluster,
      traitSet,
      inputs.get(0),
      namedAggregates,
      getRowType,
      inputType,
      grouping,
      inGroupingSet)
  }

  override def toString: String = {
    s"Aggregate(${
      if (!grouping.isEmpty) {
        s"groupBy: (${groupingToString(inputType, grouping)}), "
      } else {
        ""
      }
    }select: (${aggregationToString(inputType, grouping, getRowType, namedAggregates, Nil)}))"
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .itemIf("groupBy", groupingToString(inputType, grouping), !grouping.isEmpty)
      .item("select", aggregationToString(inputType, grouping, getRowType, namedAggregates, Nil))
  }

  override def computeSelfCost(planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {

    val child = this.getInput
    val rowCnt = metadata.getRowCount(child)
    val rowSize = this.estimateRowSize(child.getRowType)
    val aggCnt = this.namedAggregates.size
    planner.getCostFactory.makeCost(rowCnt, rowCnt * aggCnt, rowCnt * rowSize)
  }

  override def translateToPlan(tableEnv: BatchTableEnvironment): DataSet[Row] = {

    val (
      preAgg: Option[DataSetPreAggFunction],
      preAggType: Option[TypeInformation[Row]],
      finalAgg: GroupReduceFunction[Row, Row]
      ) = AggregateUtil.createDataSetAggregateFunctions(
        namedAggregates,
        inputType,
        rowRelDataType,
        grouping,
        inGroupingSet)

    val inputDS = getInput.asInstanceOf[DataSetRel].translateToPlan(tableEnv)

    val aggString = aggregationToString(inputType, grouping, getRowType, namedAggregates, Nil)

    val rowTypeInfo = FlinkTypeFactory.toInternalRowTypeInfo(getRowType).asInstanceOf[RowTypeInfo]

    if (grouping.length > 0) {
      // grouped aggregation
      val aggOpName = s"groupBy: (${groupingToString(inputType, grouping)}), " +
        s"select: ($aggString)"

      val inputTypeInfo = FlinkTypeFactory.toInternalRowTypeInfo(inputType)

      if (preAgg.isDefined) {

        val preAggFields = forwardFields(inputTypeInfo, preAggType.get, grouping)
        val fields = continueForwardFields(preAggType.get, rowTypeInfo, grouping)
        inputDS
          // pre-aggregation
          .groupBy(grouping: _*)
          .combineGroup(preAgg.get)
          .returns(preAggType.get)
          // forward fields at pre-aggregation
          .withForwardedFields(preAggFields)
          .name(aggOpName)
          // final aggregation
          .groupBy(grouping.indices: _*)
          .reduceGroup(finalAgg)
          .returns(rowTypeInfo)
          // forward fields at final conversion
          .withForwardedFields(fields)
          .name(aggOpName)
      } else {
        val fields = forwardFields(inputTypeInfo, rowTypeInfo, grouping)
        inputDS
          .groupBy(grouping: _*)
          .reduceGroup(finalAgg)
          .returns(rowTypeInfo)
          .withForwardedFields(fields)
          .name(aggOpName)
      }
    }
    else {
      // global aggregation
      val aggOpName = s"select:($aggString)"

      if (preAgg.isDefined) {
        inputDS
          // pre-aggregation
          .mapPartition(preAgg.get)
          .returns(preAggType.get)
          .name(aggOpName)
          // final aggregation
          .reduceGroup(finalAgg)
          .returns(rowTypeInfo)
          .name(aggOpName)
      } else {
        inputDS
          .reduceGroup(finalAgg)
          .returns(rowTypeInfo)
          .name(aggOpName)
      }
    }
  }

  private def continueForwardFields(
      inputRowType: TypeInformation[Row],
      resultRowType: TypeInformation[Row],
      aggIndices: Array[Int]) = {

    val names = inputType.getFieldNames
    val aggKeys = aggIndices.map(names.get)
    val outIndices = aggKeys.map(getRowType.getField(_, false, false).getIndex)

    getForwardedFields(
      inputRowType,
      resultRowType,
      aggIndices.indices.zip(outIndices))
  }

  private def forwardFields(
      inputRowType: TypeInformation[Row],
      resultRowType: TypeInformation[Row],
      aggIndices: Array[Int]) = {

    getForwardedFields(
      inputRowType,
      resultRowType,
      grouping.zipWithIndex)
  }
}
