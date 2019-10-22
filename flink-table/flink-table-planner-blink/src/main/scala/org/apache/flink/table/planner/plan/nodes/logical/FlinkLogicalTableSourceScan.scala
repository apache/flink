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

import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableSourceScan.isTableSourceScan
import org.apache.flink.table.planner.plan.schema.{FlinkRelOptTable, TableSourceTable}
import org.apache.flink.table.sources._

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.logical.LogicalTableScan
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelCollation, RelCollationTraitDef, RelNode, RelWriter}
import org.apache.calcite.schema.Table

import java.util
import java.util.function.Supplier

/**
  * Sub-class of [[TableScan]] that is a relational operator
  * which returns the contents of a [[TableSource]] in Flink.
  */
class FlinkLogicalTableSourceScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    relOptTable: FlinkRelOptTable)
  extends TableScan(cluster, traitSet, relOptTable)
  with FlinkLogicalRel {

  lazy val tableSource: TableSource[_] = tableSourceTable.tableSource

  private lazy val tableSourceTable = relOptTable.unwrap(classOf[TableSourceTable[_]])

  def copy(
      traitSet: RelTraitSet,
      relOptTable: FlinkRelOptTable): FlinkLogicalTableSourceScan = {
    new FlinkLogicalTableSourceScan(cluster, traitSet, relOptTable)
  }

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new FlinkLogicalTableSourceScan(cluster, traitSet, relOptTable)
  }

  override def deriveRowType(): RelDataType = {
    val flinkTypeFactory = cluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    tableSourceTable.getRowType(flinkTypeFactory)
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val rowCnt = mq.getRowCount(this)
    val rowSize = mq.getAverageRowSize(this)
    planner.getCostFactory.makeCost(rowCnt, rowCnt, rowCnt * rowSize)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("fields", tableSource.getTableSchema.getFieldNames.mkString(", "))
  }

}

class FlinkLogicalTableSourceScanConverter
  extends ConverterRule(
    classOf[LogicalTableScan],
    Convention.NONE,
    FlinkConventions.LOGICAL,
    "FlinkLogicalTableSourceScanConverter") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val scan: TableScan = call.rel(0)
    isTableSourceScan(scan)
  }

  def convert(rel: RelNode): RelNode = {
    val scan = rel.asInstanceOf[TableScan]
    val table = scan.getTable.asInstanceOf[FlinkRelOptTable]
    FlinkLogicalTableSourceScan.create(rel.getCluster, table)
  }
}

object FlinkLogicalTableSourceScan {
  val CONVERTER = new FlinkLogicalTableSourceScanConverter

  def isTableSourceScan(scan: TableScan): Boolean = {
    val tableSourceTable = scan.getTable.unwrap(classOf[TableSourceTable[_]])
    tableSourceTable != null
  }

  def create(cluster: RelOptCluster, relOptTable: FlinkRelOptTable): FlinkLogicalTableSourceScan = {
    val table = relOptTable.unwrap(classOf[Table])
    val traitSet = cluster.traitSetOf(FlinkConventions.LOGICAL).replaceIfs(
      RelCollationTraitDef.INSTANCE, new Supplier[util.List[RelCollation]]() {
        def get: util.List[RelCollation] = {
          if (table != null) {
            table.getStatistic.getCollations
          } else {
            ImmutableList.of[RelCollation]
          }
        }
      }).simplify()
    new FlinkLogicalTableSourceScan(cluster, traitSet, relOptTable)
  }
}
