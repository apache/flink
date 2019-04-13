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

import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.schema.DataStreamTable

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan._
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.logical.LogicalTableScan
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelCollation, RelCollationTraitDef, RelNode}
import org.apache.calcite.schema.Table

import java.util
import java.util.function.Supplier

/**
  * Sub-class of [[TableScan]] that is a relational operator
  * which returns the contents of a [[DataStreamTable]] in Flink.
  */
class FlinkLogicalDataStreamTableScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable)
  extends TableScan(cluster, traitSet, table)
  with FlinkLogicalRel {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new FlinkLogicalDataStreamTableScan(cluster, traitSet, getTable)
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val rowCnt = mq.getRowCount(this)
    val rowSize = mq.getAverageRowSize(this)
    planner.getCostFactory.makeCost(rowCnt, rowCnt, rowCnt * rowSize)
  }
}

class FlinkLogicalDataStreamTableScanConverter
  extends ConverterRule(
    classOf[LogicalTableScan],
    Convention.NONE,
    FlinkConventions.LOGICAL,
    "FlinkLogicalDataStreamTableScanConverter") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val scan: TableScan = call.rel(0)
    val dataStreamTable = scan.getTable.unwrap(classOf[DataStreamTable[_]])
    dataStreamTable != null
  }

  def convert(rel: RelNode): RelNode = {
    val scan = rel.asInstanceOf[TableScan]
    FlinkLogicalDataStreamTableScan.create(rel.getCluster, scan.getTable)
  }
}

object FlinkLogicalDataStreamTableScan {
  val CONVERTER = new FlinkLogicalDataStreamTableScanConverter

  def isDataStreamTableScan(scan: TableScan): Boolean = {
    val dataStreamTable = scan.getTable.unwrap(classOf[DataStreamTable[_]])
    dataStreamTable != null
  }

  def create(
      cluster: RelOptCluster,
      relOptTable: RelOptTable): FlinkLogicalDataStreamTableScan = {
    val table = relOptTable.unwrap(classOf[Table])
    val traitSet = cluster.traitSetOf(Convention.NONE).replaceIfs(
      RelCollationTraitDef.INSTANCE, new Supplier[util.List[RelCollation]]() {
        def get: util.List[RelCollation] = {
          if (table != null) {
            table.getStatistic.getCollations
          } else {
            ImmutableList.of[RelCollation]
          }
        }
      })
    val newTraitSet = traitSet.replace(FlinkConventions.LOGICAL).simplify()
    new FlinkLogicalDataStreamTableScan(cluster, newTraitSet, relOptTable)
  }
}
