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

import org.apache.flink.runtime.operators.DamBehavior
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import org.apache.flink.table.api.BatchTableEnvironment
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.cost.FlinkCost._
import org.apache.flink.table.plan.cost.FlinkCostFactory
import org.apache.flink.table.plan.nodes.exec.{BatchExecNode, ExecNode}
import org.apache.flink.table.plan.util.RelExplainUtil.fetchToString
import org.apache.flink.table.plan.util.SortUtil
import org.apache.flink.table.runtime.sort.LimitOperator

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel._
import org.apache.calcite.rel.core.Sort
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rex.RexNode

import java.util

import scala.collection.JavaConversions._

/**
  * Batch physical RelNode for [[Sort]].
  *
  * This node will output `limit` records beginning with the first `offset` records without sort.
  */
class BatchExecLimit(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    offset: RexNode,
    fetch: RexNode,
    val isGlobal: Boolean)
  extends Sort(
    cluster,
    traitSet,
    inputRel,
    traitSet.getTrait(RelCollationTraitDef.INSTANCE),
    offset,
    fetch)
  with BatchPhysicalRel
  with BatchExecNode[BaseRow] {

  private lazy val limitStart: Long = SortUtil.getLimitStart(offset)
  private lazy val limitEnd: Long = SortUtil.getLimitEnd(offset, fetch)

  override def copy(
      traitSet: RelTraitSet,
      newInput: RelNode,
      newCollation: RelCollation,
      offset: RexNode,
      fetch: RexNode): Sort = {
    new BatchExecLimit(cluster, traitSet, newInput, offset, fetch, isGlobal)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    pw.input("input", getInput)
      .item("offset", limitStart)
      .item("fetch", fetchToString(fetch))
      .item("global", isGlobal)
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val rowCount = mq.getRowCount(this)
    val cpuCost = COMPARE_CPU_COST * rowCount
    val costFactory = planner.getCostFactory.asInstanceOf[FlinkCostFactory]
    costFactory.makeCost(rowCount, cpuCost, 0, 0, 0)
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getDamBehavior: DamBehavior = DamBehavior.PIPELINED

  override def getInputNodes: util.List[ExecNode[BatchTableEnvironment, _]] =
    List(getInput.asInstanceOf[ExecNode[BatchTableEnvironment, _]])

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[BatchTableEnvironment, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  override def translateToPlanInternal(
      tableEnv: BatchTableEnvironment): StreamTransformation[BaseRow] = {
    val input = getInputNodes.get(0).translateToPlan(tableEnv)
        .asInstanceOf[StreamTransformation[BaseRow]]
    val inputType = input.getOutputType
    val operator = new LimitOperator(isGlobal, limitStart, limitEnd)
    new OneInputTransformation(
      input,
      getOperatorName,
      operator,
      inputType,
      if (isGlobal) 1 else input.getParallelism)
  }

  private def getOperatorName = {
    val prefix = if (isGlobal) "Global" else "Local"
    s"${prefix}Limit(offset: $limitStart, fetch: ${fetchToString(fetch)})"
  }
}
