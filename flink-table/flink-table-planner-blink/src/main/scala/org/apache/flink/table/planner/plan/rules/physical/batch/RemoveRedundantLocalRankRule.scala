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
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecRank

import org.apache.calcite.plan.RelOptRule._
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode

import scala.collection.JavaConversions._

/**
  * Planner rule that matches a global [[BatchExecRank]] on a local [[BatchExecRank]],
  * and merge them into a global [[BatchExecRank]].
  */
class RemoveRedundantLocalRankRule extends RelOptRule(
  operand(classOf[BatchExecRank],
    operand(classOf[BatchExecRank],
      operand(classOf[RelNode], FlinkConventions.BATCH_PHYSICAL, any))),
  "RemoveRedundantLocalRankRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val globalRank: BatchExecRank = call.rel(0)
    val localRank: BatchExecRank = call.rel(1)
    globalRank.isGlobal && !localRank.isGlobal &&
      globalRank.rankType == localRank.rankType &&
      globalRank.partitionKey == localRank.partitionKey &&
      globalRank.orderKey == globalRank.orderKey &&
      globalRank.rankEnd == localRank.rankEnd
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val globalRank: BatchExecRank = call.rel(0)
    val inputOfLocalRank: RelNode = call.rel(2)
    val newGlobalRank = globalRank.copy(globalRank.getTraitSet, List(inputOfLocalRank))
    call.transformTo(newGlobalRank)
  }
}

object RemoveRedundantLocalRankRule {
  val INSTANCE: RelOptRule = new RemoveRedundantLocalRankRule
}
