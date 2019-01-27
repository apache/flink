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

package org.apache.flink.table.plan.rules.physical.batch

import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.physical.batch._

import org.apache.calcite.plan.{RelOptCluster, RelOptRuleCall}
import org.apache.calcite.plan.RelOptRule._
import org.apache.calcite.rel.{RelCollationTraitDef, RelNode}

/**
  * Rewrites CompleteSortAggregate if its input data is skew on group by keys and its input RelNode
  * is Exchange -> Sort, updates Exchange -> Sort -> CompleteSortAggregate to
  * Sort -> LocalSortAggregate -> Exchange -> Sort -> GlobalSortAggregate
  *
  */
class SplitCompleteSortAggRule
  extends BaseSplitCompleteAggRule(
    operand(classOf[BatchExecSortAggregate],
      operand(classOf[BatchExecSort],
        operand(classOf[BatchExecExchange],
          operand(classOf[RelNode], any)))),
    "SplitCompleteSortAggRule") {

  override def onMatch(call: RelOptRuleCall): Unit = {
    val agg = call.rels(0).asInstanceOf[BatchExecGroupAggregateBase]
    val grouping = agg.getGrouping
    val inputOfNewLocalSort = call.rels(3)
    // create local sort
    val localSort = createSort(agg.getCluster, inputOfNewLocalSort, grouping)
    val localAgg = createLocalAgg(agg, localSort, call.builder)
    val exchange = createExchange(agg, localAgg)
    // create global sort
    val globalSort = createSort(agg.getCluster, exchange, grouping.indices.toArray)
    val globalAgg = createGlobalAgg(agg, globalSort, call.builder())
    call.transformTo(globalAgg)
  }

  private def createSort(
    cluster: RelOptCluster,
    input: RelNode,
    sortKeys: Array[Int]): BatchExecSort = {
    val collation = createRelCollation(sortKeys)
    val emptyTraitSet = cluster.getPlanner.emptyTraitSet
    val traitSet = emptyTraitSet.replace(FlinkConventions.BATCH_PHYSICAL).replace(collation)
    new BatchExecSort(
      cluster,
      traitSet,
      input,
      RelCollationTraitDef.INSTANCE.canonize(collation)
    )
  }
}

object SplitCompleteSortAggRule {

  val INSTANCE = new SplitCompleteSortAggRule
}
