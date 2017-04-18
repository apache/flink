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
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.runtime.aggregate._
import org.apache.flink.table.plan.nodes.CommonAggregate
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
  * @param inputType       The type of the rows of aggregation input RelNode
  * @param groupings       The position (in the input Row) of the grouping keys
  */
class DataStreamGroupAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    namedAggregates: Seq[CalcitePair[AggregateCall, String]],
    rowRelDataType: RelDataType,
    inputType: RelDataType,
    groupings: Array[Int])
  extends SingleRel(cluster, traitSet, inputNode)
    with CommonAggregate
    with DataStreamRel {

  override def deriveRowType() = rowRelDataType

  override def needsUpdatesAsRetraction = true

  override def producesUpdates = true

  override def consumesRetractions = true

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataStreamGroupAggregate(
      cluster,
      traitSet,
      inputs.get(0),
      namedAggregates,
      getRowType,
      inputType,
      groupings)
  }

  override def toString: String = {
    s"Aggregate(${
      if (!groupings.isEmpty) {
        s"groupBy: (${groupingToString(inputType, groupings)}), "
      } else {
        ""
      }
    }select:(${aggregationToString(inputType, groupings, getRowType, namedAggregates, Nil)}))"
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .itemIf("groupBy", groupingToString(inputType, groupings), !groupings.isEmpty)
      .item("select", aggregationToString(inputType, groupings, getRowType, namedAggregates, Nil))
  }

  override def translateToPlan(tableEnv: StreamTableEnvironment): DataStream[Row] = {

    val inputDS = input.asInstanceOf[DataStreamRel].translateToPlan(tableEnv)

    val rowTypeInfo = FlinkTypeFactory.toInternalRowTypeInfo(getRowType)

    val aggString = aggregationToString(
      inputType,
      groupings,
      getRowType,
      namedAggregates,
      Nil)

    val keyedAggOpName = s"groupBy: (${groupingToString(inputType, groupings)}), " +
      s"select: ($aggString)"
    val nonKeyedAggOpName = s"select: ($aggString)"

    val processFunction = AggregateUtil.createGroupAggregateFunction(
      namedAggregates,
      inputType,
      groupings,
      DataStreamRetractionRules.isAccRetract(this),
      DataStreamRetractionRules.isAccRetract(getInput))

    val result: DataStream[Row] =
    // grouped / keyed aggregation
      if (groupings.nonEmpty) {
        inputDS
        .keyBy(groupings: _*)
        .process(processFunction)
        .returns(rowTypeInfo)
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
        .returns(rowTypeInfo)
        .name(nonKeyedAggOpName)
        .asInstanceOf[DataStream[Row]]
      }
    result
  }
}

