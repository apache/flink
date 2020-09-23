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

import java.util

import org.apache.flink.api.dag.Transformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.operators.DamBehavior
import org.apache.flink.streaming.api.operators.OneInputStreamOperator
import org.apache.flink.streaming.api.transformations.OneInputTransformation
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.data.RowData
import org.apache.flink.table.functions.UserDefinedFunction
import org.apache.flink.table.functions.python.PythonFunctionInfo
import org.apache.flink.table.planner.calcite.FlinkRelBuilder.PlannerNamedWindowProperty
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.agg.batch.WindowCodeGenerator
import org.apache.flink.table.planner.delegation.BatchPlanner
import org.apache.flink.table.planner.expressions.{PlannerRowtimeAttribute, PlannerWindowEnd, PlannerWindowStart}
import org.apache.flink.table.planner.plan.cost.{FlinkCost, FlinkCostFactory}
import org.apache.flink.table.planner.plan.logical.LogicalWindow
import org.apache.flink.table.planner.plan.nodes.common.CommonPythonAggregate
import org.apache.flink.table.planner.plan.nodes.exec.{BatchExecNode, ExecNode}
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecPythonGroupWindowAggregate.ARROW_PYTHON_GROUP_WINDOW_AGGREGATE_FUNCTION_OPERATOR_NAME
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.logical.RowType

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.tools.RelBuilder

import scala.collection.JavaConversions._

/**
  * Batch physical RelNode for group widow aggregate (Python user defined aggregate function).
  */
class BatchExecPythonGroupWindowAggregate(
    cluster: RelOptCluster,
    relBuilder: RelBuilder,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    outputRowType: RelDataType,
    inputRowType: RelDataType,
    grouping: Array[Int],
    auxGrouping: Array[Int],
    aggCalls: Seq[AggregateCall],
    aggFunctions: Array[UserDefinedFunction],
    window: LogicalWindow,
    inputTimeFieldIndex: Int,
    inputTimeIsDate: Boolean,
    namedProperties: Seq[PlannerNamedWindowProperty])
  extends BatchExecWindowAggregateBase(
    cluster,
    traitSet,
    inputRel,
    outputRowType,
    inputRowType,
    grouping,
    auxGrouping,
    aggCalls.zip(aggFunctions),
    window,
    namedProperties,
    false,
    false,
    true)
  with BatchExecNode[RowData]
  with CommonPythonAggregate {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new BatchExecPythonGroupWindowAggregate(
      cluster,
      relBuilder,
      traitSet,
      inputs.get(0),
      outputRowType,
      inputRowType,
      grouping,
      auxGrouping,
      aggCalls,
      aggFunctions,
      window,
      inputTimeFieldIndex,
      inputTimeIsDate,
      namedProperties)
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val inputRowCnt = mq.getRowCount(getInput)
    if (inputRowCnt == null) {
      return null
    }
    // does not take pane optimization into consideration here
    // sort is not done here
    val aggCallToAggFunction = aggCalls.zip(aggFunctions)
    val cpu = FlinkCost.FUNC_CPU_COST * inputRowCnt * aggCallToAggFunction.size
    val averageRowSize: Double = mq.getAverageRowSize(this)
    val memCost = averageRowSize
    val costFactory = planner.getCostFactory.asInstanceOf[FlinkCostFactory]
    costFactory.makeCost(mq.getRowCount(this), cpu, 0, 0, memCost)
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getInputNodes: util.List[ExecNode[BatchPlanner, _]] =
    List(getInput.asInstanceOf[ExecNode[BatchPlanner, _]])

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[BatchPlanner, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  override def getDamBehavior: DamBehavior = DamBehavior.PIPELINED

  override protected def translateToPlanInternal(
      planner: BatchPlanner): Transformation[RowData] = {
    val input = getInputNodes.get(0).translateToPlan(planner)
      .asInstanceOf[Transformation[RowData]]
    val outputType = FlinkTypeFactory.toLogicalRowType(getRowType)
    val inputType = FlinkTypeFactory.toLogicalRowType(inputRowType)

    val (windowSize: Long, slideSize: Long) = WindowCodeGenerator.getWindowDef(window)

    val groupBufferLimitSize = planner.getTableConfig.getConfiguration.getInteger(
      ExecutionConfigOptions.TABLE_EXEC_WINDOW_AGG_BUFFER_SIZE_LIMIT)

    val ret = createPythonOneInputTransformation(
      input,
      inputType,
      outputType,
      inputTimeFieldIndex,
      groupBufferLimitSize,
      windowSize,
      slideSize,
      getConfig(planner.getExecEnv, planner.getTableConfig))

    if (isPythonWorkerUsingManagedMemory(planner.getTableConfig.getConfiguration)) {
      ExecNode.setManagedMemoryWeight(
        ret, getPythonWorkerMemory(planner.getTableConfig.getConfiguration).getBytes)
    }
    ret
  }

  private[this] def createPythonOneInputTransformation(
      inputTransform: Transformation[RowData],
      inputRowType: RowType,
      outputRowType: RowType,
      inputTimeFieldIndex: Int,
      maxLimitSize: Int,
      windowSize: Long,
      slideSize: Long,
      config: Configuration): OneInputTransformation[RowData, RowData] = {
    val namePropertyTypeArray = namedProperties.map {
      case PlannerNamedWindowProperty(_, p) => p match {
        case PlannerWindowStart(_) => 0
        case PlannerWindowEnd(_) => 1
        case PlannerRowtimeAttribute(_) => 2
      }
    }.toArray

    val (pythonUdafInputOffsets, pythonFunctionInfos) =
      extractPythonAggregateFunctionInfosFromAggregateCall(aggCalls)

    val pythonOperator = getPythonGroupWindowAggregateFunctionOperator(
      config,
      inputRowType,
      outputRowType,
      inputTimeFieldIndex,
      maxLimitSize,
      windowSize,
      slideSize,
      namePropertyTypeArray,
      pythonUdafInputOffsets,
      pythonFunctionInfos)

    new OneInputTransformation(
      inputTransform,
      "BatchExecPythonGroupWindowAggregate",
      pythonOperator,
      InternalTypeInfo.of(outputRowType),
      inputTransform.getParallelism)
  }

  private[this] def getPythonGroupWindowAggregateFunctionOperator(
      config: Configuration,
      inputRowType: RowType,
      outputRowType: RowType,
      inputTimeFieldIndex: Int,
      maxLimitSize: Int,
      windowSize: Long,
      slideSize: Long,
      namedProperties: Array[Int],
      udafInputOffsets: Array[Int],
      pythonFunctionInfos: Array[PythonFunctionInfo]): OneInputStreamOperator[RowData, RowData] = {
    val clazz = loadClass(ARROW_PYTHON_GROUP_WINDOW_AGGREGATE_FUNCTION_OPERATOR_NAME)

    val ctor = clazz.getConstructor(
      classOf[Configuration],
      classOf[Array[PythonFunctionInfo]],
      classOf[RowType],
      classOf[RowType],
      classOf[Int],
      classOf[Int],
      classOf[Long],
      classOf[Long],
      classOf[Array[Int]],
      classOf[Array[Int]],
      classOf[Array[Int]],
      classOf[Array[Int]])

    ctor.newInstance(
      config,
      pythonFunctionInfos,
      inputRowType,
      outputRowType,
      Integer.valueOf(inputTimeFieldIndex),
      Integer.valueOf(maxLimitSize),
      java.lang.Long.valueOf(windowSize),
      java.lang.Long.valueOf(slideSize),
      namedProperties,
      grouping,
      grouping ++ auxGrouping,
      udafInputOffsets)
      .asInstanceOf[OneInputStreamOperator[RowData, RowData]]
  }
}

object BatchExecPythonGroupWindowAggregate {
  val ARROW_PYTHON_GROUP_WINDOW_AGGREGATE_FUNCTION_OPERATOR_NAME: String =
    "org.apache.flink.table.runtime.operators.python.aggregate.arrow.batch." +
      "BatchArrowPythonGroupWindowAggregateFunctionOperator"
}
