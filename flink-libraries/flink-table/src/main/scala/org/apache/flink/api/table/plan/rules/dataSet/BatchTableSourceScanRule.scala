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

package org.apache.flink.api.table.plan.rules.dataSet

import org.apache.calcite.plan.{Convention, RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.logical.LogicalTableScan
import org.apache.flink.api.table.plan.nodes.dataset.{BatchTableSourceScan, DataSetConvention}
import org.apache.flink.api.table.plan.schema.TableSourceTable
import org.apache.flink.api.table.sources.BatchTableSource

/** Rule to convert a [[LogicalTableScan]] into a [[BatchTableSourceScan]]. */
class BatchTableSourceScanRule
  extends ConverterRule(
      classOf[LogicalTableScan],
      Convention.NONE,
      DataSetConvention.INSTANCE,
      "BatchTableSourceScanRule")
  {

  /** Rule must only match if TableScan targets a [[BatchTableSource]] */
  override def matches(call: RelOptRuleCall): Boolean = {
    val scan: TableScan = call.rel(0).asInstanceOf[TableScan]
    val dataSetTable = scan.getTable.unwrap(classOf[TableSourceTable])
    dataSetTable match {
      case tst: TableSourceTable =>
        tst.tableSource match {
          case _: BatchTableSource[_] =>
            true
          case _ =>
            false
        }
      case _ =>
        false
    }
  }

  def convert(rel: RelNode): RelNode = {
    val scan: TableScan = rel.asInstanceOf[TableScan]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(DataSetConvention.INSTANCE)

    new BatchTableSourceScan(
      rel.getCluster,
      traitSet,
      scan.getTable,
      rel.getRowType
    )
  }
}

object BatchTableSourceScanRule {
  val INSTANCE: RelOptRule = new BatchTableSourceScanRule
}
