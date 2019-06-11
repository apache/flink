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

import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import org.apache.flink.table.api.{BatchTableEnvironment, TableConfigOptions}
import org.apache.flink.table.calcite.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.CodeGeneratorContext
import org.apache.flink.table.codegen.agg.batch.{HashWindowCodeGenerator, WindowCodeGenerator}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.functions.UserDefinedFunction
import org.apache.flink.table.plan.cost.{FlinkCost, FlinkCostFactory}
import org.apache.flink.table.plan.logical.LogicalWindow
import org.apache.flink.table.plan.nodes.exec.{BatchExecNode, ExecNode}
import org.apache.flink.table.plan.nodes.resource.batch.parallelism.NodeResourceConfig
import org.apache.flink.table.plan.util.AggregateUtil.transformToBatchAggregateInfoList
import org.apache.flink.table.plan.util.FlinkRelMdUtil
import org.apache.flink.table.runtime.CodeGenOperatorFactory
import org.apache.flink.table.runtime.aggregate.BytesHashMap
import org.apache.flink.table.typeutils.BaseRowTypeInfo

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.tools.RelBuilder

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
    namedProperties: Seq[NamedWindowProperty],
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
  with BatchExecNode[BaseRow] {

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

  override def getInputNodes: util.List[ExecNode[BatchTableEnvironment, _]] =
    List(getInput.asInstanceOf[ExecNode[BatchTableEnvironment, _]])

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[BatchTableEnvironment, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  def getOperatorName: String

  override def translateToPlanInternal(
      tableEnv: BatchTableEnvironment): StreamTransformation[BaseRow] = {
    val input = getInputNodes.get(0).translateToPlan(tableEnv)
        .asInstanceOf[StreamTransformation[BaseRow]]
    val ctx = CodeGeneratorContext(tableEnv.getConfig)
    val outputType = FlinkTypeFactory.toLogicalRowType(getRowType)
    val inputType = FlinkTypeFactory.toLogicalRowType(inputRowType)

    val aggInfos = transformToBatchAggregateInfoList(
      aggCallToAggFunction.map(_._1), aggInputRowType)

    val groupBufferLimitSize = tableEnv.getConfig.getConf.getInteger(
      TableConfigOptions.SQL_EXEC_WINDOW_AGG_BUFFER_SIZE_LIMIT)

    val (windowSize: Long, slideSize: Long) = WindowCodeGenerator.getWindowDef(window)

    val reservedManagedMem = tableEnv.config.getConf.getInteger(
      TableConfigOptions.SQL_RESOURCE_HASH_AGG_TABLE_MEM) * NodeResourceConfig.SIZE_IN_MB
    val maxManagedMem = tableEnv.config.getConf.getInteger(
      TableConfigOptions.SQL_RESOURCE_HASH_AGG_TABLE_MAX_MEM) * NodeResourceConfig.SIZE_IN_MB

    val generatedOperator = new HashWindowCodeGenerator(
      ctx, relBuilder, window, inputTimeFieldIndex,
      inputTimeIsDate, namedProperties,
      aggInfos, inputRowType, grouping, auxGrouping, enableAssignPane, isMerge, isFinal).gen(
      inputType, outputType, groupBufferLimitSize, reservedManagedMem, maxManagedMem, 0,
      windowSize, slideSize)
    val operator = new CodeGenOperatorFactory[BaseRow](generatedOperator)
    new OneInputTransformation(
      input,
      getOperatorName,
      operator,
      BaseRowTypeInfo.of(outputType),
      getResource.getParallelism)
  }
}
