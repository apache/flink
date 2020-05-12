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
import org.apache.flink.table.data.RowData
import org.apache.flink.table.functions.UserDefinedFunction
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.CodeGeneratorContext
import org.apache.flink.table.planner.codegen.agg.batch.{AggWithoutKeysCodeGenerator, SortAggCodeGenerator}
import org.apache.flink.table.planner.delegation.BatchPlanner
import org.apache.flink.table.planner.plan.cost.{FlinkCost, FlinkCostFactory}
import org.apache.flink.table.planner.plan.nodes.exec.{BatchExecNode, ExecNode}
import org.apache.flink.table.planner.plan.utils.AggregateUtil.transformToBatchAggregateInfoList
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.tools.RelBuilder

import java.util

import scala.collection.JavaConversions._

/**
  * Batch physical RelNode for sort-based aggregate operator.
  *
  * @see [[BatchExecGroupAggregateBase]] for more info.
  */
abstract class BatchExecSortAggregateBase(
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
  with BatchExecNode[RowData]{

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val inputRows = mq.getRowCount(getInput())
    if (inputRows == null) {
      return null
    }
    // sort is not done here
    val cpuCost = FlinkCost.FUNC_CPU_COST * inputRows * aggCallToAggFunction.size
    val avgRowSize: Double = mq.getAverageRowSize(this)
    val memCost = avgRowSize
    val rowCount = mq.getRowCount(this)
    val costFactory = planner.getCostFactory.asInstanceOf[FlinkCostFactory]
    costFactory.makeCost(rowCount, cpuCost, 0, 0, memCost)
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
    val input = getInputNodes.get(0).translateToPlan(planner)
        .asInstanceOf[Transformation[RowData]]
    val ctx = CodeGeneratorContext(planner.getTableConfig)
    val outputType = FlinkTypeFactory.toLogicalRowType(getRowType)
    val inputType = FlinkTypeFactory.toLogicalRowType(inputRowType)

    val aggInfos = transformToBatchAggregateInfoList(
      aggCallToAggFunction.map(_._1), aggInputRowType)

    val generatedOperator = if (grouping.isEmpty) {
      AggWithoutKeysCodeGenerator.genWithoutKeys(
        ctx, relBuilder, aggInfos, inputType, outputType, isMerge, isFinal, "NoGrouping")
    } else {
      SortAggCodeGenerator.genWithKeys(
        ctx, relBuilder, aggInfos, inputType, outputType, grouping, auxGrouping, isMerge, isFinal)
    }
    val operator = new CodeGenOperatorFactory[RowData](generatedOperator)
    ExecNode.createOneInputTransformation(
      input,
      getRelDetailedDescription,
      operator,
      RowDataTypeInfo.of(outputType),
      input.getParallelism)
  }
}
