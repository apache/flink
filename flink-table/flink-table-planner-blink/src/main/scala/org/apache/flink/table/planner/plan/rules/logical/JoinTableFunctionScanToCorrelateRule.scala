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

package org.apache.flink.table.planner.plan.rules.logical

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.plan.RelOptRule.{any, none, operand}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.logical.{LogicalJoin, LogicalTableFunctionScan}

/**
 * Rule that rewrites Join on TableFunctionScan to Correlate.
 */
class JoinTableFunctionScanToCorrelateRule extends RelOptRule(
  operand(
    classOf[LogicalJoin],
    operand(classOf[RelNode], any()),
    operand(classOf[LogicalTableFunctionScan], none())),
  "JoinTableFunctionScanToCorrelateRule") {

  override def onMatch(call: RelOptRuleCall): Unit = {
    val join: LogicalJoin = call.rel(0)
    val leftInput: RelNode = call.rel(1);
    val logicalTableFunctionScan: LogicalTableFunctionScan = call.rel(2);
    val correlate = call.builder()
      .push(leftInput)
      .push(logicalTableFunctionScan)
      .correlate(join.getJoinType, join.getCluster.createCorrel())
      .build()
    call.transformTo(correlate)
  }
}

object JoinTableFunctionScanToCorrelateRule {
  val INSTANCE = new JoinTableFunctionScanToCorrelateRule
}
