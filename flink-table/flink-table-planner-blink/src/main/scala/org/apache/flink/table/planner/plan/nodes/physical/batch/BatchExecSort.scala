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
import org.apache.flink.configuration.MemorySize
import org.apache.flink.runtime.operators.DamBehavior
import org.apache.flink.streaming.api.operators.{OneInputStreamOperator, SimpleOperatorFactory}
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.sort.SortCodeGenerator
import org.apache.flink.table.planner.delegation.BatchPlanner
import org.apache.flink.table.planner.plan.cost.{FlinkCost, FlinkCostFactory}
import org.apache.flink.table.planner.plan.nodes.exec.{BatchExecNode, ExecNode}
import org.apache.flink.table.planner.plan.utils.{FlinkRelMdUtil, RelExplainUtil, SortUtil}
import org.apache.flink.table.runtime.operators.sort.SortOperator
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.core.Sort
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelCollation, RelNode, RelWriter}
import org.apache.calcite.rex.RexNode

import java.util

import scala.collection.JavaConversions._

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
  with BatchPhysicalRel
  with BatchExecNode[RowData] {

  require(sortCollation.getFieldCollations.size() > 0)
  private val (keys, orders, nullsIsLast) = SortUtil.getKeysAndOrders(
    sortCollation.getFieldCollations)

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

  //~ ExecNode methods -----------------------------------------------------------

  override def getDamBehavior = DamBehavior.FULL_DAM

  override def getInputNodes: util.List[ExecNode[BatchPlanner, _]] =
    List(getInput.asInstanceOf[ExecNode[BatchPlanner, _]])

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[BatchPlanner, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  override protected def translateToPlanInternal(
      planner: BatchPlanner): Transformation[RowData] = {
    val input = getInputNodes.get(0).translateToPlan(planner)
        .asInstanceOf[Transformation[RowData]]

    val conf = planner.getTableConfig
    val inputType = FlinkTypeFactory.toLogicalRowType(getInput.getRowType)
    val outputType = FlinkTypeFactory.toLogicalRowType(getRowType)

    // sort code gen
    val keyTypes = keys.map(inputType.getTypeAt)
    val codeGen = new SortCodeGenerator(conf, keys, keyTypes, orders, nullsIsLast)

    val operator = new SortOperator(
      codeGen.generateNormalizedKeyComputer("BatchExecSortComputer"),
      codeGen.generateRecordComparator("BatchExecSortComparator"))

    val sortMemory = MemorySize.parse(conf.getConfiguration.getString(
      ExecutionConfigOptions.TABLE_EXEC_RESOURCE_SORT_MEMORY)).getBytes
    ExecNode.createOneInputTransformation(
      input,
      getRelDetailedDescription,
      SimpleOperatorFactory.of(operator.asInstanceOf[OneInputStreamOperator[RowData, RowData]]),
      InternalTypeInfo.of(outputType),
      input.getParallelism,
      sortMemory)
  }
}
