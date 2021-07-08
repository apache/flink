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

import org.apache.flink.table.planner.expressions.PlannerNamedWindowProperty
import org.apache.flink.table.planner.plan.logical.LogicalWindow

import org.apache.calcite.plan.{Convention, RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.Aggregate.Group
import org.apache.calcite.rel.core.{Aggregate, AggregateCall}
import org.apache.calcite.util.ImmutableBitSet

import java.util

final class LogicalWindowAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    child: RelNode,
    groupSet: ImmutableBitSet,
    aggCalls: util.List[AggregateCall],
    window: LogicalWindow,
    namedProperties: Seq[PlannerNamedWindowProperty])
  extends WindowAggregate(cluster, traitSet, child, groupSet, aggCalls, window, namedProperties) {

  override def copy(
      traitSet: RelTraitSet,
      input: RelNode,
      groupSet: ImmutableBitSet,
      groupSets: util.List[ImmutableBitSet],
      aggCalls: util.List[AggregateCall]): Aggregate = {
    new LogicalWindowAggregate(
      cluster,
      traitSet,
      input,
      groupSet,
      aggCalls,
      window,
      namedProperties)
  }

  def copy(namedProperties: Seq[PlannerNamedWindowProperty]): LogicalWindowAggregate = {
    new LogicalWindowAggregate(
      cluster,
      traitSet,
      input,
      getGroupSet,
      aggCalls,
      window,
      namedProperties)
  }
}

object LogicalWindowAggregate {

  def create(
      window: LogicalWindow,
      namedProperties: Seq[PlannerNamedWindowProperty],
      agg: Aggregate): LogicalWindowAggregate = {
    require(agg.getGroupType == Group.SIMPLE)
    val cluster: RelOptCluster = agg.getCluster
    val traitSet: RelTraitSet = cluster.traitSetOf(Convention.NONE)

    new LogicalWindowAggregate(
      cluster,
      traitSet,
      agg.getInput,
      agg.getGroupSet,
      agg.getAggCallList,
      window,
      namedProperties)
  }
}
