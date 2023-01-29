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
package org.apache.flink.table.planner.plan.nodes.physical.batch

import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.cost.{FlinkCost, FlinkCostFactory}
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, InputProperty}
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSortLimit
import org.apache.flink.table.planner.plan.utils.{RelExplainUtil, SortUtil}
import org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.{RelCollation, RelNode, RelWriter}
import org.apache.calcite.rel.core.Sort
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rex.{RexLiteral, RexNode}

import scala.collection.JavaConversions._

/**
 * Batch physical RelNode for [[Sort]].
 *
 * This RelNode take the `limit` elements beginning with the first `offset` elements.
 *
 * Firstly it take the first `offset + limit` elements of each child partition, secondly the child
 * partition will forward elements to a single partition, lastly it take the `limit` elements
 * beginning with the first `offset` elements from the single output partition.
 */
class BatchPhysicalSortLimit(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    sortCollation: RelCollation,
    offset: RexNode,
    fetch: RexNode,
    isGlobal: Boolean)
  extends Sort(cluster, traitSet, inputRel, sortCollation, offset, fetch)
  with BatchPhysicalRel {

  private val limitStart: Long = SortUtil.getLimitStart(offset)
  private val limitEnd: Long = SortUtil.getLimitEnd(offset, fetch)

  override def copy(
      traitSet: RelTraitSet,
      newInput: RelNode,
      newCollation: RelCollation,
      offset: RexNode,
      fetch: RexNode): Sort = {
    new BatchPhysicalSortLimit(cluster, traitSet, newInput, newCollation, offset, fetch, isGlobal)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    pw.input("input", getInput)
      .item("orderBy", RelExplainUtil.collationToString(sortCollation, getRowType))
      .item("offset", limitStart)
      .item("fetch", RelExplainUtil.fetchToString(fetch))
      .item("global", isGlobal)
  }

  override def estimateRowCount(mq: RelMetadataQuery): Double = {
    val inputRows = mq.getRowCount(this.getInput)
    if (inputRows == null) {
      inputRows
    } else {
      val rowCount = (inputRows - limitStart).max(1.0)
      if (fetch != null) {
        rowCount.min(RexLiteral.intValue(fetch))
      } else {
        rowCount
      }
    }
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val inputRowCnt = mq.getRowCount(getInput())
    val heapLen = Math.min(inputRowCnt, limitEnd)
    val numOfSort = sortCollation.getFieldCollations.size()
    val cpuCost = FlinkCost.COMPARE_CPU_COST * numOfSort * inputRowCnt * Math.log(heapLen)
    // assume memory is big enough to simplify the estimation.
    val memCost = heapLen * mq.getAverageRowSize(this)
    val rowCount = mq.getRowCount(this)
    val costFactory = planner.getCostFactory.asInstanceOf[FlinkCostFactory]
    costFactory.makeCost(rowCount, cpuCost, 0, 0, memCost)
  }

  override def translateToExecNode(): ExecNode[_] = {
    val requiredDistribution = if (isGlobal) {
      InputProperty.SINGLETON_DISTRIBUTION
    } else {
      InputProperty.UNKNOWN_DISTRIBUTION
    }

    new BatchExecSortLimit(
      unwrapTableConfig(this),
      SortUtil.getSortSpec(sortCollation.getFieldCollations),
      limitStart,
      limitEnd,
      isGlobal,
      InputProperty
        .builder()
        .requiredDistribution(requiredDistribution)
        .damBehavior(InputProperty.DamBehavior.END_INPUT)
        .build(),
      FlinkTypeFactory.toLogicalRowType(getRowType),
      getRelDetailedDescription)
  }
}
