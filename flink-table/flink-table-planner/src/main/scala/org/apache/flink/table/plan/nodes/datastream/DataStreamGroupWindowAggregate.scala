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
import org.apache.flink.table.calcite.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.plan.logical._
import org.apache.flink.table.plan.nodes.CommonAggregate
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.runtime.aggregate.AggregateUtil._
import org.apache.flink.table.util.Logging

class DataStreamGroupWindowAggregate(
    window: LogicalWindow,
    namedProperties: Seq[NamedWindowProperty],
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    namedAggregates: Seq[CalcitePair[AggregateCall, String]],
    schema: RowSchema,
    inputSchema: RowSchema,
    grouping: Array[Int])
  extends DataStreamGroupWindowAggregateBase(
    window,
    namedProperties,
    cluster,
    traitSet,
    inputNode,
    namedAggregates,
    schema,
    inputSchema,
    grouping,
    "Aggregate")
    with CommonAggregate
    with DataStreamRel
    with Logging {

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataStreamGroupWindowAggregate(
      window,
      namedProperties,
      cluster,
      traitSet,
      inputs.get(0),
      namedAggregates,
      schema,
      inputSchema,
      grouping)
  }
}

