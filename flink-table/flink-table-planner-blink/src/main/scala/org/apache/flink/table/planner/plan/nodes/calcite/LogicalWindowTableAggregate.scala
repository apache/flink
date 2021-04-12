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
import org.apache.calcite.rel.core.{Aggregate, AggregateCall}
import org.apache.calcite.util.ImmutableBitSet

import java.util

/**
  * Sub-class of [[WindowTableAggregate]] that is a relational expression which performs window
  * aggregations but outputs 0 or more records for a group. This class corresponds to Calcite
  * logical rel.
  */
class LogicalWindowTableAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    groupSet: ImmutableBitSet,
    groupSets: util.List[ImmutableBitSet],
    aggCalls: util.List[AggregateCall],
    window: LogicalWindow,
    namedProperties: Seq[PlannerNamedWindowProperty])
  extends WindowTableAggregate(
    cluster,
    traitSet,
    input,
    groupSet,
    groupSets,
    aggCalls,
    window,
    namedProperties) {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): WindowTableAggregate = {
    new LogicalWindowTableAggregate(
      cluster,
      traitSet,
      inputs.get(0),
      groupSet,
      groupSets,
      aggCalls,
      window,
      namedProperties)
  }
}

object LogicalWindowTableAggregate {

  def create(
    window: LogicalWindow,
    namedProperties: Seq[PlannerNamedWindowProperty],
    aggregate: Aggregate): LogicalWindowTableAggregate = {

    val cluster: RelOptCluster = aggregate.getCluster
    val traitSet: RelTraitSet = cluster.traitSetOf(Convention.NONE)
    new LogicalWindowTableAggregate(
      cluster,
      traitSet,
      aggregate.getInput,
      aggregate.getGroupSet,
      aggregate.getGroupSets,
      aggregate.getAggCallList,
      window,
      namedProperties)
  }

  def create(logicalWindowAggregate: LogicalWindowAggregate): LogicalWindowTableAggregate = {
    val cluster: RelOptCluster = logicalWindowAggregate.getCluster
    val traitSet: RelTraitSet = cluster.traitSetOf(Convention.NONE)
    new LogicalWindowTableAggregate(
      cluster,
      traitSet,
      logicalWindowAggregate.getInput,
      logicalWindowAggregate.getGroupSet,
      logicalWindowAggregate.getGroupSets,
      logicalWindowAggregate.getAggCallList,
      logicalWindowAggregate.getWindow,
      logicalWindowAggregate.getNamedProperties)
  }
}

