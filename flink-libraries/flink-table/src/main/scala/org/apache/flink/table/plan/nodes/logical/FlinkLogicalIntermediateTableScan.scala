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

import org.apache.flink.table.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.schema.IntermediateRelNodeTable

import org.apache.calcite.plan.{Convention, RelOptRuleCall, _}
import org.apache.calcite.rel.{RelCollation, RelCollationTraitDef, RelNode}
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.logical.LogicalTableScan
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.schema.Table

import com.google.common.collect.ImmutableList

import java.util
import java.util.function.Supplier

class FlinkLogicalIntermediateTableScan (
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable)
  extends TableScan(cluster, traitSet, table)
  with FlinkLogicalRel {

  val intermediateTable:IntermediateRelNodeTable =
    getTable.unwrap(classOf[IntermediateRelNodeTable])

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new FlinkLogicalIntermediateTableScan(cluster, traitSet, getTable)
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val rowCnt = mq.getRowCount(this)
    val rowSize = mq.getAverageRowSize(this)
    planner.getCostFactory.makeCost(rowCnt, rowCnt, rowCnt * rowSize)
  }

  override def isDeterministic: Boolean = true
}

class FlinkLogicalIntermediateTableScanConverter
  extends ConverterRule(
    classOf[LogicalTableScan],
    Convention.NONE,
    FlinkConventions.LOGICAL,
    "FlinkLogicalIntermediateTableScanConverter") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val scan = call.rel[TableScan](0)
    FlinkLogicalIntermediateTableScan.isLogicalIntermediateTableScan(scan)
  }

  def convert(rel: RelNode): RelNode = {
    val scan = rel.asInstanceOf[TableScan]
    FlinkLogicalIntermediateTableScan.create(rel.getCluster, scan.getTable)
  }
}

object FlinkLogicalIntermediateTableScan {

  val CONVERTER = new FlinkLogicalIntermediateTableScanConverter

  def isLogicalIntermediateTableScan(scan: TableScan): Boolean = {
    val intermediateTable = scan.getTable.unwrap(classOf[IntermediateRelNodeTable])
    intermediateTable != null
  }

  def create(
    cluster: RelOptCluster, relOptTable: RelOptTable): FlinkLogicalIntermediateTableScan = {
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
    // FIXME: FlinkRelMdDistribution requires the current RelNode to compute
    // the distribution trait, so we have to create [[FlinkLogicalIntermediateTableScan]] to
    // calculate the distribution trait
    val scan = new FlinkLogicalIntermediateTableScan(cluster, traitSet, relOptTable)
    val newTraitSet = FlinkRelMetadataQuery.traitSet(scan)
                      .replace(FlinkConventions.LOGICAL).simplify()
    scan.copy(newTraitSet, scan.getInputs).asInstanceOf[FlinkLogicalIntermediateTableScan]
  }
}
