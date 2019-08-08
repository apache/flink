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

package org.apache.flink.table.planner.plan.nodes.calcite

import java.util

import org.apache.calcite.plan.{Convention, RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{Aggregate, AggregateCall}
import org.apache.calcite.util.ImmutableBitSet

/**
  * Sub-class of [[TableAggregate]] that is a relational expression which performs aggregations but
  * outputs 0 or more records for a group. This class corresponds to Calcite logical rel.
  */
class LogicalTableAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    groupSet: ImmutableBitSet,
    groupSets: util.List[ImmutableBitSet],
    aggCalls: util.List[AggregateCall])
  extends TableAggregate(cluster, traitSet, input, groupSet, groupSets, aggCalls) {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): TableAggregate = {
    new LogicalTableAggregate(
      cluster,
      traitSet,
      inputs.get(0),
      groupSet,
      groupSets,
      aggCalls
    )
  }
}

object LogicalTableAggregate {

  def create(aggregate: Aggregate): LogicalTableAggregate = {

    new LogicalTableAggregate(
      aggregate.getCluster,
      aggregate.getCluster.traitSetOf(Convention.NONE),
      aggregate.getInput,
      aggregate.getGroupSet,
      aggregate.getGroupSets,
      aggregate.getAggCallList)
  }
}
