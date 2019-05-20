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
import org.apache.flink.table.api.{BatchTableEnvironment, TableConfig, TableConfigOptions}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.CodeGeneratorContext
import org.apache.flink.table.codegen.agg.batch.{AggWithoutKeysCodeGenerator, HashAggCodeGenerator}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.functions.UserDefinedFunction
import org.apache.flink.table.plan.cost.FlinkCost._
import org.apache.flink.table.plan.cost.FlinkCostFactory
import org.apache.flink.table.plan.nodes.exec.{BatchExecNode, ExecNode}
import org.apache.flink.table.plan.util.AggregateUtil.transformToBatchAggregateInfoList
import org.apache.flink.table.plan.util.FlinkRelMdUtil
import org.apache.flink.table.runtime.CodeGenOperatorFactory

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.util.Util

import java.util

import scala.collection.JavaConversions._

/**
  * Batch physical RelNode for hash-based aggregate operator.
  *
  * @see [[BatchExecGroupAggregateBase]] for more info.
  */
abstract class BatchExecHashAggregateBase(
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
    isMerge: Boolean,
    isFinal: Boolean)
  extends BatchExecGroupAggregateBase(
    cluster,
    relBuilder,
    traitSet,
    inputRel,
    outputRowType,
    inputRowType,
    grouping,
    auxGrouping,
    aggCallToAggFunction,
    isMerge,
    isFinal)
  with BatchExecNode[BaseRow] {

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val numOfGroupKey = grouping.length
    val inputRowCnt = mq.getRowCount(getInput())
    if (inputRowCnt == null) {
      return null
    }
    val hashCpuCost = HASH_CPU_COST * inputRowCnt * numOfGroupKey
    val aggFunctionCpuCost = FUNC_CPU_COST * inputRowCnt * aggCallToAggFunction.size
    val memCost: Double = if (numOfGroupKey == 0) {
      mq.getAverageRowSize(this)
    } else {
      // assume memory is enough to hold hashTable to simplify the estimation because spill will not
      // happen under the assumption
      val ndvOfGroupKey = Util.first(mq.getRowCount(this), 1d)
      //  We aim for a 200% utilization of the bucket table.
      // TODO use BytesHashMap.BUCKET_SIZE instead of 16
      val bucketSize = ndvOfGroupKey * 16 / HASH_COLLISION_WEIGHT
      // TODO use BytesHashMap.RECORD_EXTRA_LENGTH instead of 8
      val recordSize = ndvOfGroupKey * (FlinkRelMdUtil.binaryRowAverageSize(this) + 8)
      bucketSize + recordSize
    }
    val cpuCost = hashCpuCost + aggFunctionCpuCost
    val rowCount = mq.getRowCount(this)
    val costFactory = planner.getCostFactory.asInstanceOf[FlinkCostFactory]
    costFactory.makeCost(rowCount, cpuCost, 0, 0, memCost)
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

  def getParallelism(input: StreamTransformation[BaseRow], conf: TableConfig): Int

  override def translateToPlanInternal(
      tableEnv: BatchTableEnvironment): StreamTransformation[BaseRow] = {
    val input = getInputNodes.get(0).translateToPlan(tableEnv)
        .asInstanceOf[StreamTransformation[BaseRow]]
    val ctx = CodeGeneratorContext(tableEnv.getConfig)
    val outputType = FlinkTypeFactory.toInternalRowType(getRowType)
    val inputType = FlinkTypeFactory.toInternalRowType(inputRowType)

    val aggInfos = transformToBatchAggregateInfoList(
      aggCallToAggFunction.map(_._1), aggInputRowType)

    val generatedOperator = if (grouping.isEmpty) {
      AggWithoutKeysCodeGenerator.genWithoutKeys(
        ctx, relBuilder, aggInfos, inputType, outputType, isMerge, isFinal, "NoGrouping")
    } else {
      val reservedManagedMem = tableEnv.config.getConf.getInteger(
        TableConfigOptions.SQL_RESOURCE_HASH_AGG_TABLE_MEM) * TableConfigOptions.SIZE_IN_MB
      val maxManagedMem = tableEnv.config.getConf.getInteger(
        TableConfigOptions.SQL_RESOURCE_HASH_AGG_TABLE_MAX_MEM) * TableConfigOptions.SIZE_IN_MB
      new HashAggCodeGenerator(
        ctx, relBuilder, aggInfos, inputType, outputType, grouping, auxGrouping, isMerge, isFinal
      ).genWithKeys(reservedManagedMem, maxManagedMem)
    }
    val operator = new CodeGenOperatorFactory[BaseRow](generatedOperator)
    new OneInputTransformation(
      input,
      getOperatorName,
      operator,
      outputType.toTypeInfo,
      getParallelism(input, tableEnv.config))
  }
}
