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

import org.apache.calcite.plan.RelOptRule.{none, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.core.{Sort, TableScan}
import org.apache.calcite.rex.RexLiteral
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.table.plan.nodes.logical.{FlinkLogicalSort, FlinkLogicalTableSourceScan}
import org.apache.flink.table.plan.schema.{FlinkRelOptTable, TableSourceTable}
import org.apache.flink.table.plan.stats.{FlinkStatistic, TableStats}
import org.apache.flink.table.sources.LimitableTableSource

class PushLimitIntoTableSourceScanRule extends RelOptRule(
  operand(classOf[FlinkLogicalSort],
    operand(classOf[FlinkLogicalTableSourceScan], none)), "PushLimitIntoTableSourceScanRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val sort = call.rel(0).asInstanceOf[Sort]
    val fetch = sort.fetch
    val offset = sort.offset
    //Only push-down the limit whose offset equal zero. Because it is difficult to source based push
    //to handle the non-zero offset. And the non-zero offset usually appear together with sort.
    val onlyLimit =
      sort.getCollation.getFieldCollations.isEmpty &&
          (offset == null || RexLiteral.intValue(offset) == 0) &&
          fetch != null

    var supportPushDown = false
    if (onlyLimit) {
      val tableSource = call.rel(1).asInstanceOf[TableScan]
          .getTable.unwrap(classOf[TableSourceTable])
      supportPushDown = tableSource match {
        case table: TableSourceTable =>
          table.tableSource match {
            case source: LimitableTableSource => !source.isLimitPushedDown
            case _ => false
          }
        case _ => false
      }
    }
    supportPushDown
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val sort = call.rel(0).asInstanceOf[Sort]
    val scan = call.rel(1).asInstanceOf[FlinkLogicalTableSourceScan]
    val relOptTable = scan.getTable.asInstanceOf[FlinkRelOptTable]
    val limit = RexLiteral.intValue(sort.fetch)
    val relBuilder = call.builder()
    val newRelOptTable = applyLimit(limit, relOptTable, relBuilder)
    val newScan = scan.copy(scan.getTraitSet, newRelOptTable)
    call.transformTo(newScan)
  }

  private def applyLimit(
      limit: Long,
      relOptTable: FlinkRelOptTable,
      relBuilder: RelBuilder): FlinkRelOptTable = {
    val tableSourceTable = relOptTable.unwrap(classOf[TableSourceTable])
    val limitedSource = tableSourceTable.tableSource.asInstanceOf[LimitableTableSource]
    val newTableSource = limitedSource.applyLimit(limit)

    val oldStatistic = relOptTable.getFlinkStatistic
    val tableStats = oldStatistic.getTableStats
    val newStatistic = if (tableStats != null) {
      val newTableStats = TableStats(Math.min(limit, tableStats.rowCount), tableStats.colStats)
      FlinkStatistic.builder.statistic(oldStatistic).tableStats(newTableStats).build()
    } else {
      oldStatistic
    }
    val newTableSourceTable = tableSourceTable.replaceTableSource(newTableSource).copy(newStatistic)
    relOptTable.copy(newTableSourceTable, relOptTable.getRowType)
  }
}

object PushLimitIntoTableSourceScanRule {
  val INSTANCE: RelOptRule = new PushLimitIntoTableSourceScanRule
}




