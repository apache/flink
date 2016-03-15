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

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.table.plan.{PlanGenException, TypeConverter}
import org.apache.flink.api.table.runtime.aggregate.AggregateUtil
import org.apache.flink.api.table.runtime.aggregate.AggregateUtil.CalcitePair
import org.apache.flink.api.table.typeinfo.RowTypeInfo
import org.apache.flink.api.table.{Row, TableConfig}

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
    opName: String,
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
      opName,
      grouping)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw).item("name", opName)
  }

  override def translateToPlan(
      config: TableConfig,
      expectedType: Option[TypeInformation[Any]]): DataSet[Any] = {

    expectedType match {
      case Some(typeInfo) if typeInfo.getTypeClass != classOf[Row] =>
        throw new PlanGenException("Aggregate operations currently only support returning Rows.")
      case _ => // ok
    }

    val groupingKeys = grouping.indices.toArray
    // add grouping fields, position keys in the input, and input type
    val aggregateResult = AggregateUtil.createOperatorFunctionsForAggregates(namedAggregates,
      inputType, rowType, grouping, config)

    val inputDS = input.asInstanceOf[DataSetRel].translateToPlan(
      config,
      // tell the input operator that this operator currently only supports Rows as input
      Some(TypeConverter.DEFAULT_ROW_TYPE))

    // get the output types
    val fieldTypes: Array[TypeInformation[_]] = rowType.getFieldList.asScala
    .map(f => f.getType.getSqlTypeName)
    .map(n => TypeConverter.sqlTypeToTypeInfo(n))
    .toArray

    val rowTypeInfo = new RowTypeInfo(fieldTypes, rowType.getFieldNames.asScala)
    val aggString = aggregationToString
    val mappedInput = inputDS.map(aggregateResult._1).name(s"prepare $aggString")
    val groupReduceFunction = aggregateResult._2

    if (groupingKeys.length > 0) {

      val inFields = inputType.getFieldNames.asScala.toList
      val groupByString = s"groupBy: (${grouping.map( inFields(_) ).mkString(", ")})"

      mappedInput.asInstanceOf[DataSet[Row]]
        .groupBy(groupingKeys: _*)
        .reduceGroup(groupReduceFunction)
        .returns(rowTypeInfo)
          .name(groupByString + ", " + aggString)
        .asInstanceOf[DataSet[Any]]
    }
    else {
      // global aggregation
      mappedInput.asInstanceOf[DataSet[Row]]
        .reduceGroup(groupReduceFunction)
        .returns(rowTypeInfo)
        .asInstanceOf[DataSet[Any]]
    }
  }

  private def aggregationToString: String = {

    val inFields = inputType.getFieldNames.asScala.toList
    val outFields = rowType.getFieldNames.asScala.toList
    val aggs = namedAggregates.map(_.getKey)

    val groupFieldsString = grouping.map( inFields(_) )
    val aggsString = aggs.map( a => s"${a.getAggregation}(${inFields(a.getArgList.get(0))})")

    val outFieldsString = (groupFieldsString ++ aggsString).zip(outFields).map {
      case (f, o) => if (f == o) {
        f
      } else {
        s"$f AS $o"
      }
    }

    s"select: (${outFieldsString.mkString(", ")})"
  }

}
