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
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.flink.api.java.functions.NullByteKeySelector
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.{StreamQueryConfig, StreamTableEnvironment}
import org.apache.flink.table.plan.nodes.CommonAggregate
import org.apache.flink.table.plan.rules.datastream.DataStreamRetractionRules
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.runtime.CRowKeySelector
import org.apache.flink.table.runtime.aggregate.AggregateUtil.CalcitePair
import org.apache.flink.table.runtime.aggregate._
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
import org.apache.flink.table.util.Logging

/**
  *
  * Flink RelNode for data stream unbounded table aggregate
  *
  * @param cluster         Cluster of the RelNode, represent for an environment of related
  *                        relational expressions during the optimization of a query.
  * @param traitSet        Trait set of the RelNode
  * @param inputNode       The input RelNode of aggregation
  * @param schema          The type of the rows emitted by this RelNode
  * @param inputSchema     The type of the rows consumed by this RelNode
  * @param namedAggregates List of calls to aggregate functions and their output field names
  * @param groupings       The position (in the input Row) of the grouping keys
  */
class DataStreamGroupTableAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    schema: RowSchema,
    inputSchema: RowSchema,
    val namedAggregates: Seq[CalcitePair[AggregateCall, String]],
    val groupings: Array[Int])
  extends SingleRel(cluster, traitSet, inputNode)
    with CommonAggregate
    with DataStreamRel
    with Logging {

  override def deriveRowType() = schema.relDataType

  override def needsUpdatesAsRetraction = true

  override def producesUpdates = true

  override def consumesRetractions = true

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataStreamGroupTableAggregate(
      cluster,
      traitSet,
      inputs.get(0),
      schema,
      inputSchema,
      namedAggregates,
      groupings)
  }

  override def toString: String = {
    s"TableAggregate(${
      if (!groupings.isEmpty) {
        s"groupBy: (${groupingToString(inputSchema.relDataType, groupings)}), "
      } else {
        ""
      }
    } flatAggregate:(${aggregationToString(
      inputSchema.relDataType, groupings, getRowType, namedAggregates, Nil)}))"
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .itemIf("groupBy", groupingToString(
        inputSchema.relDataType, groupings), !groupings.isEmpty)
      .item("flatAggregate", aggregationToString(
        inputSchema.relDataType, groupings, getRowType, namedAggregates, Nil))
  }

  override def translateToPlan(
      tableEnv: StreamTableEnvironment,
      queryConfig: StreamQueryConfig): DataStream[CRow] = {

    if (groupings.length > 0 && queryConfig.getMinIdleStateRetentionTime < 0) {
      LOG.warn(
        "No state retention interval configured for a query which accumulates state. " +
        "Please provide a query configuration with valid retention interval to prevent excessive " +
        "state size. You may specify a retention time of 0 to not clean up the state.")
    }

    val inputDS = getInput.asInstanceOf[DataStreamRel].translateToPlan(tableEnv, queryConfig)
    val outRowType = CRowTypeInfo(schema.typeInfo)

    val processFunction = AggregateUtil.createTableAggregateFunction(
      tableEnv.getConfig,
      false,
      inputSchema.typeInfo,
      None,
      namedAggregates,
      inputSchema.relDataType,
      inputSchema.fieldTypeInfos,
      schema,
      groupings,
      queryConfig,
      tableEnv.getConfig,
      DataStreamRetractionRules.isAccRetract(this),
      DataStreamRetractionRules.isAccRetract(getInput))

    val result: DataStream[CRow] =
      // grouped / keyed table aggregation
      if (groupings.nonEmpty) {
        inputDS
          .keyBy(new CRowKeySelector(groupings, inputSchema.projectedTypeInfo(groupings)))
          .process(processFunction)
          .returns(outRowType)
          .name(this.toString)
          .asInstanceOf[DataStream[CRow]]
      }
      // global / non-keyed table aggregation
      else {
        inputDS
          .keyBy(new NullByteKeySelector[CRow])
          .process(processFunction)
          .setParallelism(1)
          .setMaxParallelism(1)
          .returns(outRowType)
          .name(this.toString)
          .asInstanceOf[DataStream[CRow]]
      }
    result
  }
}

