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
package org.apache.flink.table.planner.plan.nodes.logical

import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.common.CommonIntermediateTableScan
import org.apache.flink.table.planner.plan.schema.IntermediateRelTable

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan._
import org.apache.calcite.rel.{RelCollation, RelCollationTraitDef, RelNode}
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.convert.ConverterRule.Config
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.logical.LogicalTableScan

import java.util
import java.util.function.Supplier

/** A flink TableScan that wraps [[IntermediateRelTable]]. */
class FlinkLogicalIntermediateTableScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable)
  extends CommonIntermediateTableScan(cluster, traitSet, table)
  with FlinkLogicalRel {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new FlinkLogicalIntermediateTableScan(cluster, traitSet, getTable)
  }

}

class FlinkLogicalIntermediateTableScanConverter(config: Config) extends ConverterRule(config) {

  override def matches(call: RelOptRuleCall): Boolean = {
    val scan: TableScan = call.rel(0)
    val intermediateTable = scan.getTable.unwrap(classOf[IntermediateRelTable])
    intermediateTable != null
  }

  def convert(rel: RelNode): RelNode = {
    val scan = rel.asInstanceOf[TableScan]
    FlinkLogicalIntermediateTableScan.create(rel.getCluster, scan.getTable)
  }
}

object FlinkLogicalIntermediateTableScan {
  val CONVERTER: ConverterRule = new FlinkLogicalIntermediateTableScanConverter(
    Config.INSTANCE.withConversion(
      classOf[LogicalTableScan],
      Convention.NONE,
      FlinkConventions.LOGICAL,
      "FlinkLogicalIntermediateTableScanConverter"))

  def create(
      cluster: RelOptCluster,
      relOptTable: RelOptTable): FlinkLogicalIntermediateTableScan = {
    val table: IntermediateRelTable = relOptTable.unwrap(classOf[IntermediateRelTable])
    require(table != null)
    val traitSet = cluster
      .traitSetOf(FlinkConventions.LOGICAL)
      .replaceIfs(
        RelCollationTraitDef.INSTANCE,
        new Supplier[util.List[RelCollation]]() {
          def get: util.List[RelCollation] = {
            if (table != null) {
              table.getStatistic.getCollations
            } else {
              ImmutableList.of[RelCollation]
            }
          }
        }
      )
      .simplify()

    new FlinkLogicalIntermediateTableScan(cluster, traitSet, relOptTable)
  }
}
