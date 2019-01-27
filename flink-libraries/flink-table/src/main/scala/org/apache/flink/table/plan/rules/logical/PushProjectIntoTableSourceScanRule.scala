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
import org.apache.calcite.rel.core.Project
import org.apache.calcite.rel.logical.LogicalTableScan
import org.apache.calcite.rel.rules.ProjectRemoveRule
import org.apache.flink.table.plan.schema.{FlinkRelOptTable, TableSourceTable}
import org.apache.flink.table.plan.util._
import org.apache.flink.table.sources._

class PushProjectIntoTableSourceScanRule extends RelOptRule(
  operand(classOf[Project],
    operand(classOf[LogicalTableScan], none)),
  "PushProjectIntoTableSourceScanRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val scan: LogicalTableScan = call.rel(1).asInstanceOf[LogicalTableScan]
    scan.getTable.unwrap(classOf[TableSourceTable]) match {
      case table: TableSourceTable =>
        table.tableSource match {
          // projection pushdown is not supported for sources that provide time indicators
          case r: DefinedRowtimeAttributes if r.getRowtimeAttributeDescriptors != null => false
          case p: DefinedProctimeAttribute if p.getProctimeAttribute != null => false
          case _: ProjectableTableSource => true
          case _ => false
        }
      case _ => false
    }
  }

  override def onMatch(call: RelOptRuleCall) {
    val project: Project = call.rel(0).asInstanceOf[Project]
    val scan: LogicalTableScan = call.rel(1).asInstanceOf[LogicalTableScan]

    val usedFields = RexNodeExtractor.extractRefInputFields(project.getProjects)

    val table = scan.getTable.asInstanceOf[FlinkRelOptTable]
    val tableSourceTable = table.unwrap(classOf[TableSourceTable])
    val source = tableSourceTable.tableSource
    // if no fields can be projected, we keep the original plan.
    if (scan.getRowType.getFieldCount != usedFields.length) {
      val newTableSource = source match {
        case nested: NestedFieldsProjectableTableSource =>
          val nestedFields = RexNodeExtractor.extractRefNestedInputFields(
            project.getProjects, usedFields)
          nested.projectNestedFields(usedFields, nestedFields)
        case projecting: ProjectableTableSource =>
          projecting.projectFields(usedFields)
      }
      // project push down does not change the statistic, we can reuse origin statistic
      val newTableSoureTable = tableSourceTable.replaceTableSource(newTableSource)
      // row type is changed after project push down
      val newRowType = newTableSoureTable.getRowType(scan.getCluster.getTypeFactory)
      val newRelOptTable = table.copy(newTableSoureTable, newRowType)
      val newScan = new LogicalTableScan(scan.getCluster, scan.getTraitSet, newRelOptTable)
      if (scan.getRowType.getFieldCount == newScan.getRowType.getFieldCount) {
        // field count does not change, no need transform to newScan
        // e.g. select count(1) from table
        return
      }

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
}

object PushProjectIntoTableSourceScanRule {
  val INSTANCE: RelOptRule = new PushProjectIntoTableSourceScanRule
}
