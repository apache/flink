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

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.TableScan
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.dataset.BatchTableSourceScan
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalTableSourceScan
import org.apache.flink.table.plan.schema.TableSourceSinkTable
import org.apache.flink.table.sources.BatchTableSource

class BatchTableSourceScanRule
  extends ConverterRule(
    classOf[FlinkLogicalTableSourceScan],
    FlinkConventions.LOGICAL,
    FlinkConventions.DATASET,
    "BatchTableSourceScanRule") {

  /** Rule must only match if TableScan targets a [[BatchTableSource]] */
  override def matches(call: RelOptRuleCall): Boolean = {
    val scan: TableScan = call.rel(0).asInstanceOf[TableScan]
    scan.getTable.unwrap(classOf[TableSourceSinkTable[_, _]]) match {
      case t: TableSourceSinkTable[_, _] if t.isBatchSourceTable => true
      // null
      case _ => false
    }
  }

  def convert(rel: RelNode): RelNode = {
    val scan: FlinkLogicalTableSourceScan = rel.asInstanceOf[FlinkLogicalTableSourceScan]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.DATASET)
    new BatchTableSourceScan(
      rel.getCluster,
      traitSet,
      scan.getTable,
      scan.tableSource.asInstanceOf[BatchTableSource[_]],
      scan.selectedFields
    )
  }
}

object BatchTableSourceScanRule {
  val INSTANCE: RelOptRule = new BatchTableSourceScanRule
}
