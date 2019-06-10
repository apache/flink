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
import org.apache.flink.streaming.api.operators.OneInputStreamOperator
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import org.apache.flink.table.api.{BatchTableEnvironment, TableConfigOptions}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.sort.SortCodeGenerator
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.cost.{FlinkCost, FlinkCostFactory}
import org.apache.flink.table.plan.nodes.exec.{BatchExecNode, ExecNode}
import org.apache.flink.table.plan.util.{FlinkRelMdUtil, RelExplainUtil, SortUtil}
import org.apache.flink.table.runtime.sort.SortOperator
import org.apache.flink.table.typeutils.BaseRowTypeInfo

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
  with BatchExecNode[BaseRow] {

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

    val conf = tableEnv.getConfig
    val inputType = FlinkTypeFactory.toLogicalRowType(getInput.getRowType)
    val outputType = FlinkTypeFactory.toLogicalRowType(getRowType)

    // sort code gen
    val keyTypes = keys.map(inputType.getTypeAt)
    val codeGen = new SortCodeGenerator(conf, keys, keyTypes, orders, nullsIsLast)

    val reservedMemorySize = conf.getConf.getInteger(
      TableConfigOptions.SQL_RESOURCE_SORT_BUFFER_MEM) * TableConfigOptions.SIZE_IN_MB

    val maxMemorySize = conf.getConf.getInteger(
      TableConfigOptions.SQL_RESOURCE_SORT_BUFFER_MAX_MEM) * TableConfigOptions.SIZE_IN_MB
    val perRequestSize = conf.getConf.getInteger(
      TableConfigOptions.SQL_EXEC_PER_REQUEST_MEM) * TableConfigOptions.SIZE_IN_MB

    val operator = new SortOperator(
      reservedMemorySize,
      maxMemorySize,
      perRequestSize.toLong,
      codeGen.generateNormalizedKeyComputer("BatchExecSortComputer"),
      codeGen.generateRecordComparator("BatchExecSortComparator"))

    new OneInputTransformation(
      input,
      s"Sort(${RelExplainUtil.collationToString(sortCollation, getRowType)})",
      operator.asInstanceOf[OneInputStreamOperator[BaseRow, BaseRow]],
      BaseRowTypeInfo.of(outputType),
      input.getParallelism)
  }
}
