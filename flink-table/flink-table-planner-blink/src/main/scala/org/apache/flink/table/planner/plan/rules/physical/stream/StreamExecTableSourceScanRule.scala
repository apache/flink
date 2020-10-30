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

package org.apache.flink.table.planner.plan.rules.physical.stream

import org.apache.flink.table.api.TableException
import org.apache.flink.table.connector.source.ScanTableSource
import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableSourceScan
import org.apache.flink.table.planner.plan.nodes.physical.stream.{StreamExecChangelogNormalize, StreamExecTableSourceScan}
import org.apache.flink.table.planner.plan.schema.TableSourceTable
import org.apache.flink.types.RowKind
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.TableScan
import org.apache.flink.table.planner.plan.utils.ScanUtil

/**
  * Rule that converts [[FlinkLogicalTableSourceScan]] to [[StreamExecTableSourceScan]].
 *
 * <p>Depends whether this is a scan source, this rule will also generate
 * [[StreamExecChangelogNormalize]] to materialize the upsert stream.
  */
class StreamExecTableSourceScanRule
  extends ConverterRule(
    classOf[FlinkLogicalTableSourceScan],
    FlinkConventions.LOGICAL,
    FlinkConventions.STREAM_PHYSICAL,
    "StreamExecTableSourceScanRule") {

  /** Rule must only match if TableScan targets a [[ScanTableSource]] */
  override def matches(call: RelOptRuleCall): Boolean = {
    val scan: TableScan = call.rel(0).asInstanceOf[TableScan]
    val tableSourceTable = scan.getTable.unwrap(classOf[TableSourceTable])
    tableSourceTable match {
      case tst: TableSourceTable =>
        tst.tableSource match {
          case _: ScanTableSource => true
          case _ => false
        }
      case _ => false
    }
  }

  def convert(rel: RelNode): RelNode = {
    val scan = rel.asInstanceOf[FlinkLogicalTableSourceScan]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)
    val newScan = new StreamExecTableSourceScan(
      rel.getCluster,
      traitSet,
      scan.getTable.asInstanceOf[TableSourceTable])

    val table = scan.getTable.asInstanceOf[TableSourceTable]
    val tableSource = table.tableSource.asInstanceOf[ScanTableSource]
    val changelogMode = tableSource.getChangelogMode
    if (changelogMode.contains(RowKind.UPDATE_AFTER) &&
        !changelogMode.contains(RowKind.UPDATE_BEFORE)) {
      // generate changelog normalize node for upsert source
      val primaryKey = table.catalogTable.getSchema.getPrimaryKey
      if (!primaryKey.isPresent) {
        throw new TableException(s"Table '${table.tableIdentifier.asSummaryString()}' produces" +
          " a changelog stream contains UPDATE_AFTER but no UPDATE_BEFORE," +
          " this requires to define primary key on the table.")
      }
      val keyFields = primaryKey.get().getColumns
      val inputFieldNames = newScan.getRowType.getFieldNames
      val primaryKeyIndices = ScanUtil.getPrimaryKeyIndices(inputFieldNames, keyFields)
      val requiredDistribution = FlinkRelDistribution.hash(primaryKeyIndices, requireStrict = true)
      val requiredTraitSet = rel.getCluster.getPlanner.emptyTraitSet()
        .replace(requiredDistribution)
        .replace(FlinkConventions.STREAM_PHYSICAL)
      val newInput: RelNode = RelOptRule.convert(newScan, requiredTraitSet)

      new StreamExecChangelogNormalize(
        scan.getCluster,
        traitSet,
        newInput,
        primaryKeyIndices)
    } else {
      newScan
    }
  }
}

object StreamExecTableSourceScanRule {
  val INSTANCE = new StreamExecTableSourceScanRule
}

