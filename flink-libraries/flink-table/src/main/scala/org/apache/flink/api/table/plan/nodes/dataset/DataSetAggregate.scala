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

import com.google.common.collect.ImmutableList
import org.apache.calcite.rex.RexLiteral
import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.`type`.{RelDataTypeFactory, RelDataType}
import org.apache.calcite.rel.core.{RelFactories, AggregateCall}
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.java.operators.MapOperator
import org.apache.flink.api.table.plan.nodes.FlinkAggregate
import org.apache.flink.api.table.plan.rules.dataSet.DataSetValuesRule
import org.apache.flink.api.table.runtime.aggregate.AggregateUtil
import org.apache.flink.api.table.runtime.aggregate.AggregateUtil.CalcitePair
import org.apache.flink.api.table.typeutils.{RowTypeInfo, TypeConverter}
import org.apache.flink.api.table.{BatchTableEnvironment, FlinkTypeFactory, Row}

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
    // add grouping fields, position keys in the input, and input type
    val aggregateResult = AggregateUtil.createOperatorFunctionsForAggregates(
      namedAggregates,
      inputType,
      getRowType,
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
      .map(aggregateResult._1)
      .name(prepareOpName)

    val groupReduceFunction = aggregateResult._2
    val rowTypeInfo = new RowTypeInfo(fieldTypes)

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
        mappedInput.asInstanceOf[DataSet[Row]].union(dummyRow(mappedInput,tableEnv))
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

  /**
    * Dummy [[Row]] into a [[DataSet]] for result after map operations.
    * @param mapOperator after which insert dummy records
    * @param tableEnv [[BatchTableEnvironment]] for getting rel builder and type factory
    * @tparam IN mapOperator input type
    * @tparam OUT mapOperator output type
    * @return DataSet of type Row is contains null literals for columns
    */
  private def dummyRow[IN,OUT](
      mapOperator: MapOperator[IN,OUT],
      tableEnv: BatchTableEnvironment): DataSet[Row] = {

    val builder: RelDataTypeFactory.FieldInfoBuilder = getCluster.getTypeFactory.builder
    val rowInfo = mapOperator.getResultType.asInstanceOf[RowTypeInfo]

    val nullLiterals :ImmutableList[ImmutableList[RexLiteral]] =
      ImmutableList.of(ImmutableList.copyOf[RexLiteral](
        for (fieldName <- rowInfo.getFieldNames)
          yield {
            val columnType = tableEnv.getTypeFactory
              .createTypeFromTypeInfo(rowInfo.getTypeAt(fieldName))
            builder.add(fieldName, columnType)
            tableEnv.getRelBuilder.getRexBuilder
              .makeLiteral(null,columnType,false).asInstanceOf[RexLiteral]
          }))

    val dataType = builder.build()

    val relNode = RelFactories.DEFAULT_VALUES_FACTORY
      .createValues(getCluster, dataType, nullLiterals)

    DataSetValuesRule.INSTANCE.asInstanceOf[DataSetValuesRule]
      .convert(relNode).asInstanceOf[DataSetValues]
      .translateToPlan(tableEnv, Some(TypeConverter.DEFAULT_ROW_TYPE))
      .asInstanceOf[DataSet[Row]]
  }
}
