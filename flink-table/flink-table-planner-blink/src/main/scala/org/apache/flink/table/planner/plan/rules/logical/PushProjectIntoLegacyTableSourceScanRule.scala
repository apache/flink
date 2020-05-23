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

import org.apache.flink.table.planner.plan.schema.LegacyTableSourceTable
import org.apache.flink.table.planner.plan.utils._
import org.apache.flink.table.sources._
import org.apache.flink.util.CollectionUtil

import org.apache.calcite.plan.RelOptRule.{none, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.logical.{LogicalProject, LogicalTableScan}
import org.apache.calcite.rel.rules.ProjectRemoveRule
import org.apache.flink.table.api.TableException

/**
  * Planner rule that pushes a [[LogicalProject]] into a [[LogicalTableScan]]
  * which wraps a [[ProjectableTableSource]] or a [[NestedFieldsProjectableTableSource]].
  */
class PushProjectIntoLegacyTableSourceScanRule extends RelOptRule(
  operand(classOf[LogicalProject],
    operand(classOf[LogicalTableScan], none)),
  "PushProjectIntoLegacyTableSourceScanRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val scan: LogicalTableScan = call.rel(1)
    scan.getTable.unwrap(classOf[LegacyTableSourceTable[_]]) match {
      case table: LegacyTableSourceTable[_] =>
        table.tableSource match {
          // projection pushdown is not supported for sources that provide time indicators
          case r: DefinedRowtimeAttributes if !CollectionUtil.isNullOrEmpty(
            r.getRowtimeAttributeDescriptors) => false
          case p: DefinedProctimeAttribute if p.getProctimeAttribute != null => false
          case _: ProjectableTableSource[_] => true
          case _: NestedFieldsProjectableTableSource[_] => true
          case _ => false
        }
      case _ => false
    }
  }

  override def onMatch(call: RelOptRuleCall) {
    val project: LogicalProject = call.rel(0)
    val scan: LogicalTableScan = call.rel(1)

    val usedFields = RexNodeExtractor.extractRefInputFields(project.getProjects)
    // if no fields can be projected, we keep the original plan.
    if (scan.getRowType.getFieldCount == usedFields.length) {
      return
    }

    val tableSourceTable = scan.getTable.unwrap(classOf[LegacyTableSourceTable[_]])
    val oldTableSource = tableSourceTable.tableSource
    val (newTableSource, isProjectSuccess) = oldTableSource match {
      case nested: NestedFieldsProjectableTableSource[_] =>
        val nestedFields = RexNodeExtractor.extractRefNestedInputFields(
          project.getProjects, usedFields)
        (nested.projectNestedFields(usedFields, nestedFields), true)
      case projecting: ProjectableTableSource[_] =>
        (projecting.projectFields(usedFields), true)
      case nonProjecting: TableSource[_] =>
        // projection cannot be pushed to TableSource
        (nonProjecting, false)
    }

    if (isProjectSuccess
      && newTableSource.explainSource().equals(oldTableSource.explainSource())) {
      throw new TableException("Failed to push project into table source! "
        + "table source with pushdown capability must override and change "
        + "explainSource() API to explain the pushdown applied!")
    }

    // check that table schema of the new table source is identical to original
    if (oldTableSource.getTableSchema != newTableSource.getTableSchema) {
      throw new TableException("TableSchema of ProjectableTableSource must not be modified " +
        "by projectFields() call. This is a bug in the implementation of the TableSource " +
        s"${oldTableSource.getClass.getCanonicalName}.")
    }

    // project push down does not change the statistic, we can reuse origin statistic
    val newTableSourceTable = tableSourceTable.copy(
      newTableSource,
      usedFields)
    // row type is changed after project push down
    val newScan = new LogicalTableScan(scan.getCluster, scan.getTraitSet, newTableSourceTable)

    // rewrite input field in projections
    val newProjects = RexNodeRewriter.rewriteWithNewFieldInput(project.getProjects, usedFields)
    val newProject = project.copy(
      project.getTraitSet,
      newScan,
      newProjects,
      project.getRowType)

    if (ProjectRemoveRule.isTrivial(newProject)) {
      // drop project if the transformed program merely returns its input
      call.transformTo(newScan)
    } else {
      call.transformTo(newProject)
    }
  }
}

object PushProjectIntoLegacyTableSourceScanRule {
  val INSTANCE: RelOptRule = new PushProjectIntoLegacyTableSourceScanRule
}
