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

package org.apache.flink.table.planner.plan.rules.physical.batch

import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.physical.batch.{BatchExecExchange, BatchExecExpand, BatchExecSort, BatchExecSortAggregate}

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.rel.{RelCollationTraitDef, RelNode}

/**
  * An [[EnforceLocalAggRuleBase]] that matches [[BatchExecSortAggregate]]
  *
  * for example: select count(*) from t group by rollup (a, b)
  * The physical plan
  *
  *  {{{
  * SortAggregate(isMerge=[false], groupBy=[a, b, $e], select=[a, b, $e, COUNT(*)])
  * +- Sort(orderBy=[a ASC, c ASC, $e ASC])
  *    +- Exchange(distribution=[hash[a, b, $e]])
  *       +- Expand(projects=[{a=[$0], b=[$1], $e=[0]},
  *                           {a=[$0], b=[null], $e=[1]},
  *                           {a=[null], b=[null], $e=[3]}])
  * }}}
  *
  * will be rewritten to
  *
  * {{{
  * SortAggregate(isMerge=[true], groupBy=[a, b, $e], select=[a, b, $e, Final_COUNT(count1$0)])
  * +- Sort(orderBy=[a ASC, c ASC, $e ASC])
  *    +- Exchange(distribution=[hash[a, b, $e]])
  *       +- LocalSortAggregate(groupBy=[a, b, $e], select=[a, b, $e, Partial_COUNT(*) AS count1$0]
  *          +- Sort(orderBy=[a ASC, c ASC, $e ASC])
  *             +- Expand(projects=[{a=[$0], b=[$1], $e=[0]},
  *                                 {a=[$0], b=[null], $e=[1]},
  *                                 {a=[null], b=[null], $e=[3]}])
  * }}}
  */
class EnforceLocalSortAggRule extends EnforceLocalAggRuleBase(
  operand(classOf[BatchExecSortAggregate],
    operand(classOf[BatchExecSort],
      operand(classOf[BatchExecExchange],
        operand(classOf[BatchExecExpand], any)))),
  "EnforceLocalSortAggRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val agg: BatchExecSortAggregate = call.rel(0)
    val expand: BatchExecExpand = call.rel(3)

    val enableTwoPhaseAgg = isTwoPhaseAggEnabled(agg)

    val grouping = agg.getGrouping
    val constantShuffleKey = hasConstantShuffleKey(grouping, expand)

    grouping.nonEmpty && enableTwoPhaseAgg && constantShuffleKey
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val agg: BatchExecSortAggregate = call.rel(0)
    val expand: BatchExecExpand = call.rel(3)

    val localGrouping = agg.getGrouping
    // create local sort
    val localSort = createSort(expand, localGrouping)
    val localAgg = createLocalAgg(agg, localSort, call.builder)

    val exchange = createExchange(agg, localAgg)

    // create global sort
    val globalGrouping = localGrouping.indices.toArray
    val globalSort = createSort(exchange, globalGrouping)
    val globalAgg = createGlobalAgg(agg, globalSort, call.builder)
    call.transformTo(globalAgg)
  }

  private def createSort(
      input: RelNode,
      sortKeys: Array[Int]): BatchExecSort = {
    val cluster = input.getCluster
    val collation = createRelCollation(sortKeys)
    val traitSet = cluster.getPlanner.emptyTraitSet
      .replace(FlinkConventions.BATCH_PHYSICAL)
      .replace(collation)
    new BatchExecSort(
      cluster,
      traitSet,
      input,
      RelCollationTraitDef.INSTANCE.canonize(collation)
    )
  }
}

object EnforceLocalSortAggRule {
  val INSTANCE = new EnforceLocalSortAggRule
}
