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

import org.apache.calcite.adapter.enumerable.EnumerableTableScan
import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptRuleOperand}
import org.apache.calcite.rel.logical.LogicalTableScan

/**
  * Rule that converts an EnumerableTableScan into a LogicalTableScan.
  * We need this rule because Calcite creates an EnumerableTableScan
  * when parsing a SQL query. We convert it into a LogicalTableScan
  * so we can merge the optimization process with any plan that might be created
  * by the Table API.
  */
class EnumerableToLogicalTableScan(
    operand: RelOptRuleOperand,
    description: String) extends RelOptRule(operand, description) {

  override def onMatch(call: RelOptRuleCall): Unit = {
    val oldRel = call.rel(0).asInstanceOf[EnumerableTableScan]
    val table = oldRel.getTable
    val newRel = LogicalTableScan.create(oldRel.getCluster, table)
    call.transformTo(newRel)
  }
}

object EnumerableToLogicalTableScan {
  val INSTANCE = new EnumerableToLogicalTableScan(
    operand(classOf[EnumerableTableScan], any),
    "EnumerableToLogicalTableScan")
}
