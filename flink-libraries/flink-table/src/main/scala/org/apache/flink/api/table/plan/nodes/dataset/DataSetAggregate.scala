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

import org.apache.calcite.plan.{RelOptCost, RelOptPlanner, RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.table.codegen.CodeGenerator
import org.apache.flink.api.table.runtime.MapRunner
import org.apache.flink.api.table.runtime.aggregate.AggregateUtil
import org.apache.flink.api.table.runtime.aggregate.AggregateUtil.CalcitePair
import org.apache.flink.api.table.typeutils.{TypeConverter, RowTypeInfo}
import org.apache.flink.api.table.{BatchTableEnvironment, Row, TableConfig}

import scala.collection.JavaConverters._

/**
  * Flink RelNode which matches along with a LogicalAggregate.
  */
class DataSetAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    namedAggregates: Seq[CalcitePair[AggregateCall, String]],
    rowType: RelDataType,
    inputType: RelDataType,
    grouping: Array[Int])
  extends SingleRel(cluster, traitSet, input)
  with DataSetRel {

  override def deriveRowType() = rowType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataSetAggregate(
      cluster,
      traitSet,
      inputs.get(0),
      namedAggregates,
      rowType,
      inputType,
      grouping)
  }

  override def toString: String = {
    s"Aggregate(${ if (!grouping.isEmpty) {
      s"groupBy: ($groupingToString), "
    } else {
      ""
    }}}select:($aggregationToString))"
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .itemIf("groupBy",groupingToString, !grouping.isEmpty)
      .item("select", aggregationToString)
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
    val aggregateResult = AggregateUtil.createOperatorFunctionsForAggregates(namedAggregates,
      inputType, rowType, grouping, config)

    val inputDS = input.asInstanceOf[DataSetRel].translateToPlan(
      tableEnv,
      // tell the input operator that this operator currently only supports Rows as input
      Some(TypeConverter.DEFAULT_ROW_TYPE))

    // get the output types
    val fieldTypes: Array[TypeInformation[_]] = rowType.getFieldList.asScala
    .map(f => f.getType.getSqlTypeName)
    .map(n => TypeConverter.sqlTypeToTypeInfo(n))
    .toArray

    val aggString = aggregationToString
    val prepareOpName = s"prepare select: ($aggString)"
    val mappedInput = inputDS
      .map(aggregateResult._1)
      .name(prepareOpName)

    val groupReduceFunction = aggregateResult._2
    val rowTypeInfo = new RowTypeInfo(fieldTypes, rowType.getFieldNames.asScala)

    val result = {
      if (groupingKeys.length > 0) {
        // grouped aggregation
        val aggOpName = s"groupBy: ($groupingToString), select:($aggString)"

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
        val mapName = s"convert: (${rowType.getFieldNames.asScala.toList.mkString(", ")})"
        result.map(typeConversion(config, rowTypeInfo, expectedType.get))
        .name(mapName)
      case _ => result
    }
  }

  private def groupingToString: String = {

    val inFields = inputType.getFieldNames.asScala
    grouping.map( inFields(_) ).mkString(", ")
  }

  private def aggregationToString: String = {

    val inFields = inputType.getFieldNames.asScala
    val outFields = rowType.getFieldNames.asScala

    val groupStrings = grouping.map( inFields(_) )

    val aggs = namedAggregates.map(_.getKey)
    val aggStrings = aggs.map( a => s"${a.getAggregation}(${
      if (a.getArgList.size() > 0) {
        inFields(a.getArgList.get(0))
      } else {
        "*"
      }
    })")

    (groupStrings ++ aggStrings).zip(outFields).map {
      case (f, o) => if (f == o) {
        f
      } else {
        s"$f AS $o"
      }
    }.mkString(", ")
  }

  private def typeConversion(
      config: TableConfig,
      rowTypeInfo: RowTypeInfo,
      expectedType: TypeInformation[Any]): MapFunction[Any, Any] = {

    val generator = new CodeGenerator(config, rowTypeInfo.asInstanceOf[TypeInformation[Any]])
    val conversion = generator.generateConverterResultExpression(
      expectedType, rowType.getFieldNames.asScala)

    val body =
      s"""
          |${conversion.code}
          |return ${conversion.resultTerm};
          |""".stripMargin

    val genFunction = generator.generateFunction(
      "AggregateOutputConversion",
      classOf[MapFunction[Any, Any]],
      body,
      expectedType)

    new MapRunner[Any, Any](
      genFunction.name,
      genFunction.code,
      genFunction.returnType)

  }

}
