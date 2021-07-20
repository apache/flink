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

import org.apache.flink.table.connector.source.ScanTableSource
import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableSourceScan
import org.apache.flink.table.planner.plan.nodes.physical.stream.{StreamPhysicalChangelogNormalize, StreamPhysicalTableSourceScan}
import org.apache.flink.table.planner.plan.schema.TableSourceTable
import org.apache.flink.table.planner.plan.utils.ScanUtil
import org.apache.flink.table.planner.connectors.DynamicSourceUtils.{isSourceChangeEventsDuplicate, isUpsertSource}
import org.apache.flink.table.planner.utils.ShortcutUtils

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.TableScan

/**
  * Rule that converts [[FlinkLogicalTableSourceScan]] to [[StreamPhysicalTableSourceScan]].
 *
 * <p>Depends whether this is a scan source, this rule will also generate
 * [[StreamPhysicalChangelogNormalize]] to materialize the upsert stream.
  */
class StreamPhysicalTableSourceScanRule
  extends ConverterRule(
    classOf[FlinkLogicalTableSourceScan],
    FlinkConventions.LOGICAL,
    FlinkConventions.STREAM_PHYSICAL,
    "StreamPhysicalTableSourceScanRule") {

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
    val config = ShortcutUtils.unwrapContext(rel.getCluster).getTableConfig
    val table = scan.getTable.asInstanceOf[TableSourceTable]

    val newScan = new StreamPhysicalTableSourceScan(
      rel.getCluster,
      traitSet,
      scan.getHints,
      table)

    if (isUpsertSource(table.catalogTable, table.tableSource) ||
        isSourceChangeEventsDuplicate(table.catalogTable, table.tableSource, config)) {
      // generate changelog normalize node
      // primary key has been validated in CatalogSourceTable
      val primaryKey = table.catalogTable.getResolvedSchema.getPrimaryKey.get()
      val keyFields = primaryKey.getColumns
      val inputFieldNames = newScan.getRowType.getFieldNames
      val primaryKeyIndices = ScanUtil.getPrimaryKeyIndices(inputFieldNames, keyFields)
      val requiredDistribution = FlinkRelDistribution.hash(primaryKeyIndices, requireStrict = true)
      val requiredTraitSet = rel.getCluster.getPlanner.emptyTraitSet()
        .replace(requiredDistribution)
        .replace(FlinkConventions.STREAM_PHYSICAL)
      val newInput: RelNode = RelOptRule.convert(newScan, requiredTraitSet)

      new StreamPhysicalChangelogNormalize(
        scan.getCluster,
        traitSet,
        newInput,
        primaryKeyIndices)
    } else {
      newScan
    }
  }
}

object StreamPhysicalTableSourceScanRule {
  val INSTANCE = new StreamPhysicalTableSourceScanRule
}
