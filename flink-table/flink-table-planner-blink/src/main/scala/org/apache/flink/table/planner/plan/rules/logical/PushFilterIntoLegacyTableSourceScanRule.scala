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

import org.apache.flink.table.api.config.OptimizerConfigOptions
import org.apache.flink.table.api.TableException
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.planner.calcite.FlinkContext
import org.apache.flink.table.planner.expressions.converter.ExpressionConverter
import org.apache.flink.table.planner.plan.schema.{FlinkPreparingTableBase, LegacyTableSourceTable}
import org.apache.flink.table.planner.plan.stats.FlinkStatistic
import org.apache.flink.table.planner.plan.utils.{FlinkRelOptUtil, RexNodeExtractor}
import org.apache.flink.table.sources.FilterableTableSource

import org.apache.calcite.plan.RelOptRule.{none, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.`type`.RelDataTypeFactory
import org.apache.calcite.rel.core.Filter
import org.apache.calcite.rel.logical.LogicalTableScan

import java.util
import java.util.TimeZone

import scala.collection.JavaConversions._

/**
  * Planner rule that tries to push a filter into a [[FilterableTableSource]].
  */
class PushFilterIntoLegacyTableSourceScanRule extends RelOptRule(
  operand(classOf[Filter],
    operand(classOf[LogicalTableScan], none)),
  "PushFilterIntoLegacyTableSourceScanRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val config = call.getPlanner.getContext.unwrap(classOf[FlinkContext]).getTableConfig
    if (!config.getConfiguration.getBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_SOURCE_PREDICATE_PUSHDOWN_ENABLED)) {
      return false
    }

    val filter: Filter = call.rel(0)
    if (filter.getCondition == null) {
      return false
    }

    val scan: LogicalTableScan = call.rel(1)
    scan.getTable.unwrap(classOf[LegacyTableSourceTable[_]]) match {
      case table: LegacyTableSourceTable[_] =>
        table.tableSource match {
          case source: FilterableTableSource[_] => !source.isFilterPushedDown
          case _ => false
        }
      case _ => false
    }
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val filter: Filter = call.rel(0)
    val scan: LogicalTableScan = call.rel(1)
    val table: LegacyTableSourceTable[_] = scan.getTable.asInstanceOf[LegacyTableSourceTable[_]]
    pushFilterIntoScan(call, filter, scan, table)
  }

  private def pushFilterIntoScan(
      call: RelOptRuleCall,
      filter: Filter,
      scan: LogicalTableScan,
      relOptTable: FlinkPreparingTableBase): Unit = {

    val relBuilder = call.builder()
    val context = call.getPlanner.getContext.unwrap(classOf[FlinkContext])
    val maxCnfNodeCount = FlinkRelOptUtil.getMaxCnfNodeCount(scan)
    val (predicates, unconvertedRexNodes) =
      RexNodeExtractor.extractConjunctiveConditions(
        filter.getCondition,
        maxCnfNodeCount,
        filter.getInput.getRowType.getFieldNames,
        relBuilder.getRexBuilder,
        context.getFunctionCatalog,
        context.getCatalogManager,
        TimeZone.getTimeZone(scan.getCluster.getPlanner.getContext
            .unwrap(classOf[FlinkContext]).getTableConfig.getLocalTimeZone))

    if (predicates.isEmpty) {
      // no condition can be translated to expression
      return
    }

    val remainingPredicates = new util.LinkedList[Expression]()
    predicates.foreach(e => remainingPredicates.add(e))

    val newRelOptTable: FlinkPreparingTableBase =
      applyPredicate(remainingPredicates, relOptTable, relBuilder.getTypeFactory)
    val newTableSource = newRelOptTable.unwrap(classOf[LegacyTableSourceTable[_]]).tableSource
    val oldTableSource = relOptTable.unwrap(classOf[LegacyTableSourceTable[_]]).tableSource

    if (newTableSource.asInstanceOf[FilterableTableSource[_]].isFilterPushedDown
      && newTableSource.explainSource().equals(oldTableSource.explainSource)) {
      throw new TableException("Failed to push filter into table source! "
        + "table source with pushdown capability must override and change "
        + "explainSource() API to explain the pushdown applied!")
    }

    val newScan = new LogicalTableScan(scan.getCluster, scan.getTraitSet, newRelOptTable)

    // check whether framework still need to do a filter
    if (remainingPredicates.isEmpty && unconvertedRexNodes.isEmpty) {
      call.transformTo(newScan)
    } else {
      relBuilder.push(scan)
      val converter = new ExpressionConverter(relBuilder)
      val remainingConditions = remainingPredicates.map(_.accept(converter)) ++ unconvertedRexNodes
      val remainingCondition = remainingConditions.reduce((l, r) => relBuilder.and(l, r))
      val newFilter = filter.copy(filter.getTraitSet, newScan, remainingCondition)
      call.transformTo(newFilter)
    }
  }

  private def applyPredicate(
      predicates: util.List[Expression],
      relOptTable: FlinkPreparingTableBase,
      typeFactory: RelDataTypeFactory): FlinkPreparingTableBase = {
    val originPredicatesSize = predicates.size()
    val tableSourceTable = relOptTable.unwrap(classOf[LegacyTableSourceTable[_]])
    val filterableSource = tableSourceTable.tableSource.asInstanceOf[FilterableTableSource[_]]
    val newTableSource = filterableSource.applyPredicate(predicates)
    val updatedPredicatesSize = predicates.size()
    val statistic = tableSourceTable.getStatistic
    val newStatistic = if (originPredicatesSize == updatedPredicatesSize) {
      // Keep all Statistics if no predicates can be pushed down
      statistic
    } else if (statistic == FlinkStatistic.UNKNOWN) {
      statistic
    } else {
      // Remove tableStats after predicates pushed down
      FlinkStatistic.builder().statistic(statistic).tableStats(null).build()
    }
    tableSourceTable.copy(newTableSource, newStatistic)
  }

}


object PushFilterIntoLegacyTableSourceScanRule {
  val INSTANCE: RelOptRule = new PushFilterIntoLegacyTableSourceScanRule
}
