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

import org.apache.flink.table.plan.nodes.physical.batch.{BatchExecExchange, BatchExecGroupAggregateBase, BatchExecHashAggregate}

import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.plan.RelOptRule._
import org.apache.calcite.rel.RelNode

/**
  * Rewrites CompleteHashAggregate if its input data is skew on group by keys and its input RelNode
  * is Exchange, updates Exchange -> CompleteHashAggregate to
  * LocalHashAggregate -> Exchange -> GlobalHashAggregate
  */
class SplitCompleteHashAggRule
  extends BaseSplitCompleteAggRule(
    operand(classOf[BatchExecHashAggregate],
      operand(classOf[BatchExecExchange],
        operand(classOf[RelNode], any))),
    "SplitCompleteHashAggRule") {

  override def onMatch(call: RelOptRuleCall): Unit = {
    val agg = call.rels(0).asInstanceOf[BatchExecGroupAggregateBase]
    val inputOfLocalAgg = call.rels(2)
    val localAgg = createLocalAgg(agg, inputOfLocalAgg, call.builder)
    val exchange = createExchange(agg, localAgg)
    val globalAgg = createGlobalAgg(agg, exchange, call.builder)
    call.transformTo(globalAgg)
  }
}

object SplitCompleteHashAggRule {

  val INSTANCE = new SplitCompleteHashAggRule
}
