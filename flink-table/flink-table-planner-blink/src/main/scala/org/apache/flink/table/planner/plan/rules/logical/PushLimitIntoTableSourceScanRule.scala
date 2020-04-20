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

import org.apache.flink.table.api.TableException
import org.apache.flink.table.plan.stats.TableStats
import org.apache.flink.table.planner.plan.nodes.logical.{FlinkLogicalSort, FlinkLogicalTableSourceScan}
import org.apache.flink.table.planner.plan.schema.{FlinkPreparingTableBase, TableSourceTable}
import org.apache.flink.table.planner.plan.stats.FlinkStatistic
import org.apache.flink.table.sources.LimitableTableSource

import org.apache.calcite.plan.RelOptRule.{none, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.core.{Sort, TableScan}
import org.apache.calcite.rex.RexLiteral
import org.apache.calcite.tools.RelBuilder

import java.util.Collections

/**
  * Planner rule that tries to push limit into a [[LimitableTableSource]].
  * The original limit will still be retained.
  *
  * The reasons why the limit still be retained:
  * 1.If the source is required to return the exact number of limit number, the implementation
  * of the source is highly required. The source is required to accurately control the record
  * number of split, and the parallelism setting also need to be adjusted accordingly.
  * 2.When remove the limit, maybe filter will be pushed down to the source after limit pushed
  * down. The source need know it should do limit first and do the filter later, it is hard to
  * implement.
  * 3.We can support limit with offset, we can push down offset + fetch to table source.
  */
class PushLimitIntoTableSourceScanRule extends RelOptRule(
  operand(classOf[FlinkLogicalSort],
    operand(classOf[FlinkLogicalTableSourceScan], none)), "PushLimitIntoTableSourceScanRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val sort = call.rel(0).asInstanceOf[Sort]
    val onlyLimit = sort.getCollation.getFieldCollations.isEmpty && sort.fetch != null
    if (onlyLimit) {
      call.rel(1).asInstanceOf[TableScan]
          .getTable.unwrap(classOf[TableSourceTable[_]]) match {
        case table: TableSourceTable[_] =>
          table.tableSource match {
            case source: LimitableTableSource[_] =>
              return !source.isLimitPushedDown
            case _ =>
          }
        case _ =>
      }
    }
    false
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val sort = call.rel(0).asInstanceOf[Sort]
    val scan = call.rel(1).asInstanceOf[FlinkLogicalTableSourceScan]
    val tableSourceTable = scan.getTable.unwrap(classOf[TableSourceTable[_]])
    val offset = if (sort.offset == null) 0 else RexLiteral.intValue(sort.offset)
    val limit = offset + RexLiteral.intValue(sort.fetch)
    val relBuilder = call.builder()
    val newRelOptTable = applyLimit(limit, tableSourceTable, relBuilder)
    val newScan = scan.copy(scan.getTraitSet, newRelOptTable)

    val newTableSource = newRelOptTable.unwrap(classOf[TableSourceTable[_]]).tableSource
    val oldTableSource = tableSourceTable.unwrap(classOf[TableSourceTable[_]]).tableSource

    if (newTableSource.asInstanceOf[LimitableTableSource[_]].isLimitPushedDown
        && newTableSource.explainSource().equals(oldTableSource.explainSource)) {
      throw new TableException("Failed to push limit into table source! "
          + "table source with pushdown capability must override and change "
          + "explainSource() API to explain the pushdown applied!")
    }

    call.transformTo(sort.copy(sort.getTraitSet, Collections.singletonList(newScan)))
  }

  private def applyLimit(
      limit: Long,
      relOptTable: FlinkPreparingTableBase,
      relBuilder: RelBuilder): TableSourceTable[_] = {
    val tableSourceTable = relOptTable.unwrap(classOf[TableSourceTable[Any]])
    val limitedSource = tableSourceTable.tableSource.asInstanceOf[LimitableTableSource[Any]]
    val newTableSource = limitedSource.applyLimit(limit)

    val statistic = relOptTable.getStatistic
    val newRowCount = if (statistic.getRowCount != null) {
      Math.min(limit, statistic.getRowCount.toLong)
    } else {
      limit
    }
    // Update TableStats after limit push down
    val newTableStats = new TableStats(newRowCount)
    val newStatistic = FlinkStatistic.builder()
        .statistic(statistic)
        .tableStats(newTableStats)
        .build()
    tableSourceTable.copy(newTableSource, newStatistic)
  }
}

object PushLimitIntoTableSourceScanRule {
  val INSTANCE: RelOptRule = new PushLimitIntoTableSourceScanRule
}
