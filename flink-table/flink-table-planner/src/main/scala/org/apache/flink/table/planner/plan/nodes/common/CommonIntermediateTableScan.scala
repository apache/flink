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

package org.apache.flink.table.planner.plan.nodes.common

import org.apache.flink.table.planner.plan.nodes.FlinkRelNode
import org.apache.flink.table.planner.plan.schema.IntermediateRelTable

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelOptTable, RelTraitSet}
import org.apache.calcite.rel.RelWriter
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.metadata.RelMetadataQuery

import scala.collection.JavaConversions._

/**
  * Base class that wraps [[IntermediateRelTable]].
  */
abstract class CommonIntermediateTableScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable)
  extends TableScan(cluster, traitSet, table)
  with FlinkRelNode {

  val intermediateTable: IntermediateRelTable = getTable.unwrap(classOf[IntermediateRelTable])

  override def getRelDetailedDescription: String = {
    intermediateTable.relNode.asInstanceOf[FlinkRelNode].getRelDetailedDescription
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val rowCnt = mq.getRowCount(this)
    val rowSize = mq.getAverageRowSize(this)
    planner.getCostFactory.makeCost(rowCnt, rowCnt, rowCnt * rowSize)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("fields", getRowType.getFieldNames.mkString(", "))
  }
}

