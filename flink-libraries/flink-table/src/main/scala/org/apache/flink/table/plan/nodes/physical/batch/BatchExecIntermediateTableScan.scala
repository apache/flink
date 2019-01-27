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

package org.apache.flink.table.plan.nodes.physical.batch

import org.apache.flink.table.plan.schema.IntermediateRelNodeTable

import org.apache.calcite.plan._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, RelWriter}

import scala.collection.JavaConversions._

class BatchExecIntermediateTableScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    rowRelDataType: RelDataType)
  extends TableScan(cluster, traitSet, table)
  with BatchPhysicalRel {

  val intermediateTable:IntermediateRelNodeTable =
    getTable.unwrap(classOf[IntermediateRelNodeTable])

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
    .item("fields", getRowType.getFieldNames.mkString(", "))
  }

  override def deriveRowType(): RelDataType = rowRelDataType

  override def computeSelfCost(planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {
    val rowCnt = metadata.getRowCount(this)
    planner.getCostFactory.makeCost(rowCnt, 0, 0)
  }

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new BatchExecIntermediateTableScan(cluster, traitSet, getTable, getRowType)
  }

  override def isDeterministic: Boolean = true
}
