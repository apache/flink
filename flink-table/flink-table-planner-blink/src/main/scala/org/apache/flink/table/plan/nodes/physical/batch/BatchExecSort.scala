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

import org.apache.flink.table.plan.cost.{FlinkCost, FlinkCostFactory}
import org.apache.flink.table.plan.util.{FlinkRelMdUtil, RelExplainUtil}

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.core.Sort
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelCollation, RelNode, RelWriter}
import org.apache.calcite.rex.RexNode

/**
  * Batch physical RelNode for [[Sort]].
  *
  * This node will output all data rather than `limit` records.
  */
class BatchExecSort(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    sortCollation: RelCollation)
  extends Sort(cluster, traitSet, inputRel, sortCollation)
  with BatchPhysicalRel {

  require(sortCollation.getFieldCollations.size() > 0)

  override def copy(
      traitSet: RelTraitSet,
      newInput: RelNode,
      newCollation: RelCollation,
      offset: RexNode,
      fetch: RexNode): Sort = {
    new BatchExecSort(cluster, traitSet, newInput, newCollation)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    pw.input("input", getInput)
      .item("orderBy", RelExplainUtil.collationToString(sortCollation, getRowType))
  }

  override def estimateRowCount(mq: RelMetadataQuery): Double = mq.getRowCount(getInput)

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val rowCount = mq.getRowCount(getInput)
    if (rowCount == null) {
      return null
    }
    val numOfSortKeys = sortCollation.getFieldCollations.size()
    val cpuCost = FlinkCost.COMPARE_CPU_COST * numOfSortKeys *
      rowCount * Math.max(Math.log(rowCount), 1.0)
    val memCost = FlinkRelMdUtil.computeSortMemory(mq, getInput)
    val costFactory = planner.getCostFactory.asInstanceOf[FlinkCostFactory]
    costFactory.makeCost(rowCount, cpuCost, 0, 0, memCost)
  }

}
