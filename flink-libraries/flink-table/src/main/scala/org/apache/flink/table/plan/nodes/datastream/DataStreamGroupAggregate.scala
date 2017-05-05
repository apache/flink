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
package org.apache.flink.table.plan.nodes.datastream

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.flink.api.java.functions.NullByteKeySelector
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.StreamTableEnvironment
import org.apache.flink.table.codegen.CodeGenerator
import org.apache.flink.table.runtime.aggregate._
import org.apache.flink.table.plan.nodes.CommonAggregate
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.types.Row
import org.apache.flink.table.runtime.aggregate.AggregateUtil.CalcitePair

/**
  *
  * Flink RelNode for data stream unbounded group aggregate
  *
  * @param cluster         Cluster of the RelNode, represent for an environment of related
  *                        relational expressions during the optimization of a query.
  * @param traitSet        Trait set of the RelNode
  * @param inputNode       The input RelNode of aggregation
  * @param namedAggregates List of calls to aggregate functions and their output field names
  * @param rowRelDataType  The type of the rows of the RelNode
  * @param inputSchema     The type of the rows consumed by this RelNode
  * @param schema          The type of the rows emitted by this RelNode
  * @param groupings       The position (in the input Row) of the grouping keys
  */
class DataStreamGroupAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    namedAggregates: Seq[CalcitePair[AggregateCall, String]],
    rowRelDataType: RelDataType,
    schema: RowSchema,
    inputSchema: RowSchema,
    groupings: Array[Int])
  extends SingleRel(cluster, traitSet, inputNode)
    with CommonAggregate
    with DataStreamRel {

  override def deriveRowType() = schema.logicalType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataStreamGroupAggregate(
      cluster,
      traitSet,
      inputs.get(0),
      namedAggregates,
      getRowType,
      schema,
      inputSchema,
      groupings)
  }

  override def toString: String = {
    s"Aggregate(${
      if (!groupings.isEmpty) {
        s"groupBy: (${groupingToString(inputSchema.logicalType, groupings)}), "
      } else {
        ""
      }
    }select:(${aggregationToString(
      inputSchema.logicalType, groupings, getRowType, namedAggregates, Nil)}))"
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .itemIf("groupBy", groupingToString(
        inputSchema.logicalType, groupings), !groupings.isEmpty)
      .item("select", aggregationToString(
        inputSchema.logicalType, groupings, getRowType, namedAggregates, Nil))
  }

  override def translateToPlan(tableEnv: StreamTableEnvironment): DataStream[Row] = {

    val inputDS = input.asInstanceOf[DataStreamRel].translateToPlan(tableEnv)
    val physicalNamedAggregates = namedAggregates.map { namedAggregate =>
      new CalcitePair[AggregateCall, String](
        inputSchema.mapAggregateCall(namedAggregate.left),
        namedAggregate.right)
    }

    val generator = new CodeGenerator(
      tableEnv.getConfig,
      false,
      inputDS.getType)

    val aggString = aggregationToString(
      inputSchema.logicalType,
      groupings,
      getRowType,
      namedAggregates,
      Nil)

    val keyedAggOpName = s"groupBy: (${groupingToString(inputSchema.logicalType, groupings)}), " +
      s"select: ($aggString)"
    val nonKeyedAggOpName = s"select: ($aggString)"

    val physicalGrouping = groupings.map(inputSchema.mapIndex)

    val processFunction = AggregateUtil.createGroupAggregateFunction(
      generator,
      physicalNamedAggregates,
      inputSchema.logicalType,
      inputSchema.physicalFieldTypeInfo,
      groupings)

    val result: DataStream[Row] =
    // grouped / keyed aggregation
      if (physicalGrouping.nonEmpty) {
        inputDS
        .keyBy(groupings: _*)
        .process(processFunction)
        .returns(schema.physicalTypeInfo)
        .name(keyedAggOpName)
        .asInstanceOf[DataStream[Row]]
      }
      // global / non-keyed aggregation
      else {
        inputDS
        .keyBy(new NullByteKeySelector[Row])
        .process(processFunction)
        .setParallelism(1)
        .setMaxParallelism(1)
        .returns(schema.physicalTypeInfo)
        .name(nonKeyedAggOpName)
        .asInstanceOf[DataStream[Row]]
      }
    result
  }
}

