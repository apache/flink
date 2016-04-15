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

package org.apache.flink.api.table.plan.rules

import org.apache.calcite.plan.RelOptRule._
import org.apache.calcite.plan.{RelOptRuleCall, RelOptRule, RelOptRuleOperand}
import org.apache.calcite.prepare.RelOptTableImpl
import org.apache.calcite.rel.logical.LogicalTableScan
import org.apache.calcite.schema.StreamableTable
import org.apache.flink.api.table.plan.schema.TransStreamTable

/**
  * Custom rule that converts a LogicalScan into another LogicalScan
  * whose internal Table is [[StreamableTable]] and [[org.apache.calcite.schema.TranslatableTable]].
  */
class LogicalScanToStreamable(
    operand: RelOptRuleOperand,
    description: String) extends RelOptRule(operand, description) {

  override def onMatch(call: RelOptRuleCall): Unit = {
    val oldRel = call.rel(0).asInstanceOf[LogicalTableScan]
    val table = oldRel.getTable
    table.unwrap(classOf[StreamableTable]) match {
      case s: StreamableTable =>
        // already a StreamableTable => do nothing
      case _ => // convert to a StreamableTable
        val sTable = new TransStreamTable(oldRel, false)
        val newRel = LogicalTableScan.create(oldRel.getCluster,
          RelOptTableImpl.create(table.getRelOptSchema, table.getRowType, sTable))
        call.transformTo(newRel)
    }
  }
}

object LogicalScanToStreamable {
  val INSTANCE = new LogicalScanToStreamable(
    operand(classOf[LogicalTableScan], any),
    "LogicalScanToStreamable")
}

