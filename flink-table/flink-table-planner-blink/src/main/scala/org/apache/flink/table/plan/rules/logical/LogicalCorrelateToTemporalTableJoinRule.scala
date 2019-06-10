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
package org.apache.flink.table.plan.rules.logical

import org.apache.calcite.plan.RelOptRule.{any, operand, some}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.logical.{LogicalCorrelate, LogicalFilter, LogicalSnapshot}

/**
  * The initial temporal table join is a Correlate, rewrite it into a Join to make the
  * join condition push-down into the Join
  */
class LogicalCorrelateToTemporalTableJoinRule
  extends RelOptRule(
    operand(classOf[LogicalFilter],
      operand(classOf[LogicalCorrelate], some(
        operand(classOf[RelNode], any()),
        operand(classOf[LogicalSnapshot], any())))),
    "LogicalCorrelateToTemporalTableJoinRule") {

  override def onMatch(call: RelOptRuleCall): Unit = {
    val filterOnCorrelate: LogicalFilter = call.rel(0)
    val correlate: LogicalCorrelate = call.rel(1)
    val leftNode: RelNode = call.rel(2)
    val snapshot: LogicalSnapshot = call.rel[LogicalSnapshot](3)

    val builder = call.builder()
    builder.push(leftNode)
    builder.push(snapshot)
    builder.join(
      correlate.getJoinType.toJoinType,
      filterOnCorrelate.getCondition)

    call.transformTo(builder.build())
  }

}

object LogicalCorrelateToTemporalTableJoinRule {
  val INSTANCE = new LogicalCorrelateToTemporalTableJoinRule
}
