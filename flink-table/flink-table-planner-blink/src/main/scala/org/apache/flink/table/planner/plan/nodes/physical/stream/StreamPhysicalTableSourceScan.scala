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

package org.apache.flink.table.planner.plan.nodes.physical.stream

import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecTableSourceScan
import org.apache.flink.table.planner.plan.nodes.physical.common.CommonPhysicalTableSourceScan
import org.apache.flink.table.planner.plan.schema.TableSourceTable

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.metadata.RelMetadataQuery

/**
 * Stream physical RelNode to read data from an external source defined by a
 * [[org.apache.flink.table.connector.source.ScanTableSource]].
 */
class StreamPhysicalTableSourceScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    tableSourceTable: TableSourceTable)
  extends CommonPhysicalTableSourceScan(cluster, traitSet, tableSourceTable)
  with StreamPhysicalRel {

  override def requireWatermark: Boolean = false

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new StreamPhysicalTableSourceScan(cluster, traitSet, tableSourceTable)
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val rowCnt = mq.getRowCount(this)
    val rowSize = mq.getAverageRowSize(this)
    planner.getCostFactory.makeCost(rowCnt, rowCnt, rowCnt * rowSize)
  }

  override def translateToExecNode(): ExecNode[_] = {
    new StreamExecTableSourceScan(
      tableSource,
      FlinkTypeFactory.toLogicalRowType(getRowType),
      getRelDetailedDescription)
  }
}
