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
import org.apache.calcite.rel.RelNode
import org.apache.flink.table.plan.nodes.CommonTableAggregate
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.runtime.aggregate.AggregateUtil.CalcitePair
import org.apache.flink.table.util.Logging

/**
  * Flink RelNode for data stream unbounded table aggregate.
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
  extends DataStreamGroupAggregateBase(
    cluster,
    traitSet,
    inputNode,
    namedAggregates,
    schema,
    inputSchema,
    groupings,
    "TableAggregate")
    with CommonTableAggregate
    with DataStreamRel
    with Logging {

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
}

