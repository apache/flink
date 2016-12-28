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
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.nodes.FlinkAggregate
import org.apache.flink.table.runtime.aggregate.AggregateUtil
import org.apache.flink.table.runtime.aggregate.AggregateUtil.CalcitePair
import org.apache.flink.table.typeutils.TypeConverter
import org.apache.flink.table.api.BatchTableEnvironment
import org.apache.flink.types.Row

import scala.collection.JavaConverters._

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
    grouping: Array[Int])
  extends SingleRel(cluster, traitSet, inputNode)
  with FlinkAggregate
  with DataSetRel {

  override def deriveRowType() = rowRelDataType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataSetAggregate(
      cluster,
      traitSet,
      inputs.get(0),
      namedAggregates,
      getRowType,
      inputType,
      grouping)
  }

  override def toString: String = {
    s"Aggregate(${ if (!grouping.isEmpty) {
      s"groupBy: (${groupingToString(inputType, grouping)}), "
    } else {
      ""
    }}select: (${aggregationToString(inputType, grouping, getRowType, namedAggregates, Nil)}))"
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .itemIf("groupBy", groupingToString(inputType, grouping), !grouping.isEmpty)
      .item("select", aggregationToString(inputType, grouping, getRowType, namedAggregates, Nil))
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

    val groupingKeys = grouping.indices.toArray

    val mapFunction = AggregateUtil.createPrepareMapFunction(
      namedAggregates,
      grouping,
      inputType)

    val groupReduceFunction = AggregateUtil.createAggregateGroupReduceFunction(
      namedAggregates,
      inputType,
      rowRelDataType,
      grouping)

    val inputDS = getInput.asInstanceOf[DataSetRel].translateToPlan(
      tableEnv,
      // tell the input operator that this operator currently only supports Rows as input
      Some(TypeConverter.DEFAULT_ROW_TYPE))

    // get the output types
    val fieldTypes: Array[TypeInformation[_]] = getRowType.getFieldList.asScala
    .map(field => FlinkTypeFactory.toTypeInfo(field.getType))
    .toArray

    val aggString = aggregationToString(inputType, grouping, getRowType, namedAggregates, Nil)
    val prepareOpName = s"prepare select: ($aggString)"
    val mappedInput = inputDS
      .map(mapFunction)
      .name(prepareOpName)

    val rowTypeInfo = new RowTypeInfo(fieldTypes: _*)

    val result = {
      if (groupingKeys.length > 0) {
        // grouped aggregation
        val aggOpName = s"groupBy: (${groupingToString(inputType, grouping)}), " +
          s"select: ($aggString)"

        mappedInput.asInstanceOf[DataSet[Row]]
          .groupBy(groupingKeys: _*)
          .reduceGroup(groupReduceFunction)
          .returns(rowTypeInfo)
          .name(aggOpName)
          .asInstanceOf[DataSet[Any]]
      }
      else {
        // global aggregation
        val aggOpName = s"select:($aggString)"
        mappedInput.asInstanceOf[DataSet[Row]]
          .reduceGroup(groupReduceFunction)
          .returns(rowTypeInfo)
          .name(aggOpName)
          .asInstanceOf[DataSet[Any]]
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
          conversionOperatorName = "DataSetAggregateConversion",
          fieldNames = getRowType.getFieldNames.asScala
        ))
        .name(mapName)
      case _ => result
    }
  }
}
