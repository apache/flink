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
import org.apache.flink.table.plan.cost.FlinkBatchCost._
import org.apache.flink.table.plan.cost.FlinkCostFactory
import org.apache.flink.table.plan.nodes.exec.RowBatchExecNode
import org.apache.flink.table.plan.nodes.exec.batch.BatchExecNodeVisitor
import org.apache.flink.table.plan.nodes.physical.FlinkPhysicalRel
import org.apache.flink.table.plan.util.SortUtil
import org.apache.flink.table.runtime.sort.LimitOperator

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel._
import org.apache.calcite.rel.core.Sort
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rex.{RexLiteral, RexNode}

class BatchExecLimit(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inp: RelNode,
    limitOffset: RexNode,
    limit: RexNode,
    val isGlobal: Boolean,
    description: String)
  extends Sort(
    cluster,
    traitSet,
    inp,
    traitSet.getTrait(RelCollationTraitDef.INSTANCE),
    limitOffset,
    limit)
  with BatchPhysicalRel
  with RowBatchExecNode {

  val limitStart: Long = if (offset != null) RexLiteral.intValue(offset) else 0L
  val limitEnd: Long = if (limit != null) {
    RexLiteral.intValue(limit) + limitStart
  } else {
    Long.MaxValue
  }

  override def copy(
      traitSet: RelTraitSet,
      newInput: RelNode,
      newCollation: RelCollation,
      offset: RexNode,
      fetch: RexNode): Sort = {
    new BatchExecLimit(
      cluster,
      traitSet,
      newInput,
      offset,
      fetch,
      isGlobal,
      description)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    pw.input("input", getInput)
      .item("offset", offsetToString)
      .item("limit", limitToString)
      .item("global", isGlobal)
  }

  private def offsetToString: String = {
    s"$limitStart"
  }

  private def limitToString: String = {
    if (limit != null) {
      s"${RexLiteral.intValue(limit)}"
    } else {
      "unlimited"
    }
  }

  override def isDeterministic: Boolean = SortUtil.isDeterministic(offset, fetch)

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val rowCount = mq.getRowCount(this)
    val cpuCost = COMPARE_CPU_COST * rowCount
    val costFactory = planner.getCostFactory.asInstanceOf[FlinkCostFactory]
    costFactory.makeCost(rowCount, cpuCost, 0, 0, 0)
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getDamBehavior: DamBehavior = DamBehavior.PIPELINED

  override def accept(visitor: BatchExecNodeVisitor): Unit = visitor.visit(this)

  override def getFlinkPhysicalRel: FlinkPhysicalRel = this

  /**
    * Internal method, translates the [[org.apache.flink.table.plan.nodes.exec.BatchExecNode]]
    * into a Batch operator.
    *
    * @param tableEnv The [[BatchTableEnvironment]] of the translated Table.
    */
  override def translateToPlanInternal(
      tableEnv: BatchTableEnvironment): StreamTransformation[BaseRow] = {
    val input = getInputNodes.get(0).translateToPlan(tableEnv)
      .asInstanceOf[StreamTransformation[BaseRow]]
    val inputType = input.getOutputType
    val operator = new LimitOperator(isGlobal, limitStart, limitEnd)
    val transformation = new OneInputTransformation(
      input,
      getOperatorName,
      operator,
      inputType,
      getResource.getParallelism)
    tableEnv.getRUKeeper.addTransformation(this, transformation)
    transformation.setResources(getResource.getReservedResourceSpec,
      getResource.getPreferResourceSpec)
    transformation
  }

  private def getOperatorName = {
    s"${if (isGlobal) "Global" else "Local"}Limit(offset: $offsetToString, limit: $limitToString)"
  }

}
