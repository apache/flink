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
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.java.functions.NullByteKeySelector
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.{StreamQueryConfig, StreamTableEnvironment}
import org.apache.flink.table.codegen.CodeGenerator
import org.apache.flink.table.runtime.aggregate._
import org.apache.flink.table.plan.nodes.CommonAggregate
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.plan.rules.datastream.DataStreamRetractionRules
import org.apache.flink.table.runtime.aggregate.AggregateUtil.CalcitePair
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
import org.slf4j.LoggerFactory

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

  private val LOG = LoggerFactory.getLogger(this.getClass)

  override def deriveRowType() = schema.logicalType

  override def needsUpdatesAsRetraction = true

  override def producesUpdates = true

  override def consumesRetractions = true

  def getGroupings: Array[Int] = groupings

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

  override def translateToPlan(
      tableEnv: StreamTableEnvironment,
      queryConfig: StreamQueryConfig): DataStream[CRow] = {

    if (groupings.length > 0 && (queryConfig.getMinIdleStateRetentionTime < 0
      || queryConfig.getMaxIdleStateRetentionTime < 0)) {
      LOG.warn(
        "No state retention interval configured for a query which accumulates state. " +
        "Please provide a query configuration with valid retention interval to prevent excessive " +
        "state size. You may specify a retention time of 0 to not clean up the state.")
    }

    val inputDS = input.asInstanceOf[DataStreamRel].translateToPlan(tableEnv, queryConfig)

    val physicalNamedAggregates = namedAggregates.map { namedAggregate =>
      new CalcitePair[AggregateCall, String](
        inputSchema.mapAggregateCall(namedAggregate.left),
        namedAggregate.right)
    }

    val outRowType = CRowTypeInfo(schema.physicalTypeInfo)

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
      groupings,
      queryConfig,
      DataStreamRetractionRules.isAccRetract(this),
      DataStreamRetractionRules.isAccRetract(getInput))

    val result: DataStream[CRow] =
    // grouped / keyed aggregation
      if (physicalGrouping.nonEmpty) {
        inputDS
        .keyBy(groupings: _*)
        .process(processFunction)
        .returns(outRowType)
        .name(keyedAggOpName)
        .asInstanceOf[DataStream[CRow]]
      }
      // global / non-keyed aggregation
      else {
        inputDS
        .keyBy(new NullByteKeySelector[CRow])
        .process(processFunction)
        .setParallelism(1)
        .setMaxParallelism(1)
        .returns(outRowType)
        .name(nonKeyedAggOpName)
        .asInstanceOf[DataStream[CRow]]
      }
    result
  }
}

