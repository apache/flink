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

package org.apache.flink.table.plan.rules.dataSet

import org.apache.calcite.plan.RelOptRule._
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.flink.table.plan.nodes.dataset.{BatchTableSourceScan, DataSetCalc}
import org.apache.flink.table.plan.rules.common.PushFilterIntoTableSourceScanRuleBase
import org.apache.flink.table.plan.schema.TableSourceTable
import org.apache.flink.table.sources.FilterableTableSource

class PushFilterIntoBatchTableSourceScanRule extends RelOptRule(
  operand(classOf[DataSetCalc],
    operand(classOf[BatchTableSourceScan], none)),
  "PushFilterIntoBatchTableSourceScanRule")
  with PushFilterIntoTableSourceScanRuleBase {

  override def matches(call: RelOptRuleCall): Boolean = {
    val calc: DataSetCalc = call.rel(0).asInstanceOf[DataSetCalc]
    val scan: BatchTableSourceScan = call.rel(1).asInstanceOf[BatchTableSourceScan]
    scan.tableSource match {
      case source: FilterableTableSource[_] =>
        calc.getProgram.getCondition != null && !source.isFilterPushedDown
      case _ => false
    }
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val calc: DataSetCalc = call.rel(0).asInstanceOf[DataSetCalc]
    val scan: BatchTableSourceScan = call.rel(1).asInstanceOf[BatchTableSourceScan]
    val tableSourceTable = scan.getTable.unwrap(classOf[TableSourceTable[_]])
    val filterableSource = scan.tableSource.asInstanceOf[FilterableTableSource[_]]
    pushFilterIntoScan(call, calc, scan, tableSourceTable, filterableSource, description)
  }
}

object PushFilterIntoBatchTableSourceScanRule {
  val INSTANCE: RelOptRule = new PushFilterIntoBatchTableSourceScanRule
}
