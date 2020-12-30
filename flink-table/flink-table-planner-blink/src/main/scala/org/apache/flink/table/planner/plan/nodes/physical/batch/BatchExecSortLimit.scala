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

import org.apache.flink.api.dag.Transformation
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory
import org.apache.flink.table.api.TableException
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.codegen.sort.ComparatorCodeGenerator
import org.apache.flink.table.planner.delegation.BatchPlanner
import org.apache.flink.table.planner.plan.cost.{FlinkCost, FlinkCostFactory}
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil
import org.apache.flink.table.planner.plan.nodes.exec.{LegacyBatchExecNode, ExecEdge}
import org.apache.flink.table.planner.plan.utils.{RelExplainUtil, SortUtil}
import org.apache.flink.table.runtime.operators.sort.SortLimitOperator
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.core.Sort
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelCollation, RelNode, RelWriter}
import org.apache.calcite.rex.{RexLiteral, RexNode}

import java.util

import scala.collection.JavaConversions._

/**
  * Batch physical RelNode for [[Sort]].
  *
  * This RelNode take the `limit` elements beginning with the first `offset` elements.
  *
  * Firstly it take the first `offset + limit` elements of each child partition, secondly the child
  * partition will forward elements to a single partition, lastly it take the `limit` elements
  * beginning with the first `offset` elements from the single output partition.
  **/
class BatchExecSortLimit(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    sortCollation: RelCollation,
    offset: RexNode,
    fetch: RexNode,
    isGlobal: Boolean)
  extends Sort(cluster, traitSet, inputRel, sortCollation, offset, fetch)
  with BatchPhysicalRel
  with LegacyBatchExecNode[RowData] {

  private val limitStart: Long = SortUtil.getLimitStart(offset)
  private val limitEnd: Long = SortUtil.getLimitEnd(offset, fetch)

  private val (keys, orders, nullsIsLast) = SortUtil.getKeysAndOrders(
    sortCollation.getFieldCollations)

  override def copy(
      traitSet: RelTraitSet,
      newInput: RelNode,
      newCollation: RelCollation,
      offset: RexNode,
      fetch: RexNode): Sort = {
    new BatchExecSortLimit(cluster, traitSet, newInput, newCollation, offset, fetch, isGlobal)
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

  //~ ExecNode methods -----------------------------------------------------------

  override def getInputEdges: util.List[ExecEdge] = List(
    ExecEdge.builder()
      .damBehavior(ExecEdge.DamBehavior.END_INPUT)
      .build())

  override protected def translateToPlanInternal(
      planner: BatchPlanner): Transformation[RowData] = {
    if (limitEnd == Long.MaxValue) {
      throw new TableException("Not support limitEnd is max value now!")
    }

    val input = getInputNodes.get(0).translateToPlan(planner)
        .asInstanceOf[Transformation[RowData]]
    val inputType = input.getOutputType.asInstanceOf[InternalTypeInfo[RowData]]
    val types = inputType.toRowFieldTypes

    // generate comparator
    val genComparator = ComparatorCodeGenerator.gen(
      planner.getTableConfig, "SortLimitComparator", keys, keys.map(types(_)), orders, nullsIsLast)

    // TODO If input is ordered, there is no need to use the heap.
    val operator = new SortLimitOperator(isGlobal, limitStart, limitEnd, genComparator)

    ExecNodeUtil.createOneInputTransformation(
      input,
      getRelDetailedDescription,
      SimpleOperatorFactory.of(operator),
      inputType,
      input.getParallelism,
      0)
  }
}
