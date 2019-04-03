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

package org.apache.flink.table.plan.nodes.logical

import org.apache.flink.table.api.TableException
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalTableSourceScan.isTableSourceScan
import org.apache.flink.table.plan.schema.{FlinkRelOptTable, TableSourceTable}
import org.apache.flink.table.sources._

import org.apache.calcite.plan._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.logical.LogicalTableScan
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, RelWriter}

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

  val tableSource: TableSource[_] = relOptTable.unwrap(classOf[TableSourceTable[_]]).tableSource

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

    tableSource match {
      case s: StreamTableSource[_] =>
        TableSourceUtil.getRelDataType(s, None, streaming = true, flinkTypeFactory)
      case _: BatchTableSource[_] =>
        flinkTypeFactory.buildLogicalRowType(
          tableSource.getTableSchema, isStreaming = Option.apply(false))
      case _ => throw new TableException("Unknown TableSource type.")
    }
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val rowCnt = mq.getRowCount(this)
    val rowSize = mq.getAverageRowSize(this)
    planner.getCostFactory.makeCost(rowCnt, rowCnt, rowCnt * rowSize)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("fields", tableSource.getTableSchema.getColumnNames.mkString(", "))
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
    val traitSet = cluster.traitSet().replace(FlinkConventions.LOGICAL).simplify()
    new FlinkLogicalTableSourceScan(cluster, traitSet, relOptTable)
  }
}
