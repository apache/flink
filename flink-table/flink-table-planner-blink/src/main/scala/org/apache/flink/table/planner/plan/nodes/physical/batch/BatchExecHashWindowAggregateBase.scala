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
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.data.RowData
import org.apache.flink.table.functions.UserDefinedFunction
import org.apache.flink.table.planner.calcite.FlinkRelBuilder.PlannerNamedWindowProperty
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.CodeGeneratorContext
import org.apache.flink.table.planner.codegen.agg.batch.{HashWindowCodeGenerator, WindowCodeGenerator}
import org.apache.flink.table.planner.delegation.BatchPlanner
import org.apache.flink.table.planner.plan.cost.{FlinkCost, FlinkCostFactory}
import org.apache.flink.table.planner.plan.logical.LogicalWindow
import org.apache.flink.table.planner.plan.nodes.exec.{BatchExecNode, ExecNode}
import org.apache.flink.table.planner.plan.utils.AggregateUtil.transformToBatchAggregateInfoList
import org.apache.flink.table.planner.plan.utils.FlinkRelMdUtil
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory
import org.apache.flink.table.runtime.operators.aggregate.BytesHashMap
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.configuration.MemorySize

import java.util

import scala.collection.JavaConversions._

abstract class BatchExecHashWindowAggregateBase(
    cluster: RelOptCluster,
    relBuilder: RelBuilder,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    outputRowType: RelDataType,
    inputRowType: RelDataType,
    aggInputRowType: RelDataType,
    grouping: Array[Int],
    auxGrouping: Array[Int],
    aggCallToAggFunction: Seq[(AggregateCall, UserDefinedFunction)],
    window: LogicalWindow,
    inputTimeFieldIndex: Int,
    inputTimeIsDate: Boolean,
    namedProperties: Seq[PlannerNamedWindowProperty],
    enableAssignPane: Boolean = false,
    isMerge: Boolean,
    isFinal: Boolean)
  extends BatchExecWindowAggregateBase(
    cluster,
    traitSet,
    inputRel,
    outputRowType,
    inputRowType,
    grouping,
    auxGrouping,
    aggCallToAggFunction,
    window,
    namedProperties,
    enableAssignPane,
    isMerge,
    isFinal)
  with BatchExecNode[RowData] {

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val numOfGroupKey = grouping.length
    val inputRowCnt = mq.getRowCount(getInput())
    if (inputRowCnt == null) {
      return null
    }
    // does not take pane optimization into consideration here
    // calculate hash code of groupKeys + timestamp
    val hashCpuCost = FlinkCost.HASH_CPU_COST * inputRowCnt * (numOfGroupKey + 1)
    val aggFunctionCpuCost = FlinkCost.FUNC_CPU_COST * inputRowCnt * aggCallToAggFunction.size
    val rowCnt = mq.getRowCount(this)
    // assume memory is enough to hold hashTable to simplify the estimation because spill will not
    // happen under the assumption
    //  We aim for a 200% utilization of the bucket table.
    val bucketSize = rowCnt * BytesHashMap.BUCKET_SIZE / FlinkCost.HASH_COLLISION_WEIGHT
    val rowAvgSize = FlinkRelMdUtil.binaryRowAverageSize(this)
    val recordSize = rowCnt * (rowAvgSize + BytesHashMap.RECORD_EXTRA_LENGTH)
    val memCost = bucketSize + recordSize
    val costFactory = planner.getCostFactory.asInstanceOf[FlinkCostFactory]
    costFactory.makeCost(rowCnt, hashCpuCost + aggFunctionCpuCost, 0, 0, memCost)
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getInputNodes: util.List[ExecNode[BatchPlanner, _]] =
    List(getInput.asInstanceOf[ExecNode[BatchPlanner, _]])

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[BatchPlanner, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  override protected def translateToPlanInternal(
      planner: BatchPlanner): Transformation[RowData] = {
    val config = planner.getTableConfig
    val input = getInputNodes.get(0).translateToPlan(planner)
        .asInstanceOf[Transformation[RowData]]
    val ctx = CodeGeneratorContext(config)
    val outputType = FlinkTypeFactory.toLogicalRowType(getRowType)
    val inputType = FlinkTypeFactory.toLogicalRowType(inputRowType)

    val aggInfos = transformToBatchAggregateInfoList(
      aggCallToAggFunction.map(_._1), aggInputRowType)

    val groupBufferLimitSize = config.getConfiguration.getInteger(
      ExecutionConfigOptions.TABLE_EXEC_WINDOW_AGG_BUFFER_SIZE_LIMIT)

    val (windowSize: Long, slideSize: Long) = WindowCodeGenerator.getWindowDef(window)

    val generatedOperator = new HashWindowCodeGenerator(
      ctx, relBuilder, window, inputTimeFieldIndex,
      inputTimeIsDate, namedProperties,
      aggInfos, inputRowType, grouping, auxGrouping, enableAssignPane, isMerge, isFinal).gen(
      inputType, outputType, groupBufferLimitSize, 0,
      windowSize, slideSize)
    val operator = new CodeGenOperatorFactory[RowData](generatedOperator)

    val managedMemory = MemorySize.parse(config.getConfiguration.getString(
      ExecutionConfigOptions.TABLE_EXEC_RESOURCE_HASH_AGG_MEMORY)).getBytes
    ExecNode.createOneInputTransformation(
      input,
      getRelDetailedDescription,
      operator,
      InternalTypeInfo.of(outputType),
      input.getParallelism,
      managedMemory)
  }
}
