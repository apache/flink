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

package org.apache.flink.table.plan.nodes.dataset

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.flink.api.java.DataSet
import org.apache.flink.table.api.BatchTableEnvironment
import org.apache.flink.table.plan.nodes.TableSourceScan
import org.apache.flink.table.plan.schema.TableSourceTable
import org.apache.flink.table.sources.{BatchTableSource, TableSource}
import org.apache.flink.types.Row

/** Flink RelNode to read data from an external source defined by a [[BatchTableSource]]. */
class BatchTableSourceScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    tableSource: BatchTableSource[_])
  extends TableSourceScan(cluster, traitSet, table, tableSource)
  with BatchScan {

  override def computeSelfCost(planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {
    val rowCnt = metadata.getRowCount(this)
    planner.getCostFactory.makeCost(rowCnt, rowCnt, rowCnt * estimateRowSize(getRowType))
  }

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new BatchTableSourceScan(
      cluster,
      traitSet,
      getTable,
      tableSource
    )
  }

  override def copy(traitSet: RelTraitSet, newTableSource: TableSource[_]): TableSourceScan = {
    new BatchTableSourceScan(
      cluster,
      traitSet,
      getTable,
      newTableSource.asInstanceOf[BatchTableSource[_]]
    )
  }

  override def translateToPlan(tableEnv: BatchTableEnvironment): DataSet[Row] = {
    val config = tableEnv.getConfig
    val inputDataSet = tableSource.getDataSet(tableEnv.execEnv).asInstanceOf[DataSet[Any]]
    convertToInternalRow(inputDataSet, new TableSourceTable(tableSource), config)
  }
}
