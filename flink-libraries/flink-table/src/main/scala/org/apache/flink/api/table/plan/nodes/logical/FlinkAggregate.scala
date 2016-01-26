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

package org.apache.flink.api.table.plan.nodes.logical

import java.util

import org.apache.calcite.plan.{RelOptCost, RelOptPlanner, RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{AggregateCall, Aggregate}
import org.apache.calcite.sql.fun.SqlAvgAggFunction
import org.apache.calcite.util.ImmutableBitSet

import scala.collection.JavaConversions._

class FlinkAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    indicator: Boolean,
    groupSet: ImmutableBitSet,
    groupSets: java.util.List[ImmutableBitSet],
    aggCalls: java.util.List[AggregateCall])
  extends Aggregate(cluster, traitSet, input, indicator, groupSet, groupSets, aggCalls)
  with FlinkRel {

  override def copy(
      traitSet: RelTraitSet,
      input: RelNode,
      indicator: Boolean,
      groupSet: ImmutableBitSet,
      groupSets: util.List[ImmutableBitSet],
      aggCalls: util.List[AggregateCall]): Aggregate = {

    new FlinkAggregate(
      cluster,
      traitSet,
      input,
      indicator,
      groupSet,
      groupSets,
      aggCalls
    )
  }

  override def computeSelfCost (planner: RelOptPlanner): RelOptCost = {

    val origCosts = super.computeSelfCost(planner)
    val deltaCost = planner.getCostFactory.makeHugeCost()

    // only prefer aggregations with transformed Avg
    aggCalls.toList.foldLeft[RelOptCost](origCosts){
      (c: RelOptCost, a: AggregateCall) =>
        if (a.getAggregation.isInstanceOf[SqlAvgAggFunction]) {
          c.plus(deltaCost)
        } else {
          c
        }
    }
  }
}
