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

package org.apache.flink.table.planner.plan.nodes.exec.batch

import org.apache.flink.api.dag.Transformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.memory.ManagedMemoryUseCase
import org.apache.flink.streaming.api.operators.OneInputStreamOperator
import org.apache.flink.streaming.api.transformations.OneInputTransformation
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.data.RowData
import org.apache.flink.table.functions.python.PythonFunctionInfo
import org.apache.flink.table.planner.calcite.FlinkRelBuilder.PlannerNamedWindowProperty
import org.apache.flink.table.planner.codegen.agg.batch.WindowCodeGenerator
import org.apache.flink.table.planner.delegation.PlannerBase
import org.apache.flink.table.planner.expressions.{PlannerRowtimeAttribute, PlannerWindowEnd, PlannerWindowStart}
import org.apache.flink.table.planner.plan.logical.LogicalWindow
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecPythonGroupWindowAggregate.ARROW_PYTHON_GROUP_WINDOW_AGGREGATE_FUNCTION_OPERATOR_NAME
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecPythonAggregate
import org.apache.flink.table.planner.plan.nodes.exec.{ExecEdge, ExecNode, ExecNodeBase}
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.logical.RowType

import org.apache.calcite.rel.core.AggregateCall

import java.util.Collections

/**
 * Batch [[ExecNode]] for group widow aggregate (Python user defined aggregate function).
 *
 * <p>Note: This class can't be ported to Java,
 * because java class can't extend scala interface with default implementation.
 * FLINK-20858 will port this class to Java.
 */
class BatchExecPythonGroupWindowAggregate(
    grouping: Array[Int],
    auxGrouping: Array[Int],
    aggCalls: Array[AggregateCall],
    window: LogicalWindow,
    inputTimeFieldIndex: Int,
    namedWindowProperties: Array[PlannerNamedWindowProperty],
    inputEdge: ExecEdge,
    outputType: RowType,
    description: String)
  extends ExecNodeBase[RowData](Collections.singletonList(inputEdge), outputType, description)
  with BatchExecNode[RowData]
  with CommonExecPythonAggregate {

  override protected def translateToPlanInternal(planner: PlannerBase): Transformation[RowData] = {
    val inputNode = getInputNodes.get(0).asInstanceOf[ExecNode[RowData]]
    val inputTransform = inputNode.translateToPlan(planner)

    val windowSizeAndSlideSize = WindowCodeGenerator.getWindowDef(window)

    val groupBufferLimitSize = planner.getTableConfig.getConfiguration.getInteger(
      ExecutionConfigOptions.TABLE_EXEC_WINDOW_AGG_BUFFER_SIZE_LIMIT)

    val transform = createPythonOneInputTransformation(
      inputTransform,
      inputNode.getOutputType.asInstanceOf[RowType],
      outputType,
      inputTimeFieldIndex,
      groupBufferLimitSize,
      windowSizeAndSlideSize.f0,
      windowSizeAndSlideSize.f1,
      getConfig(planner.getExecEnv, planner.getTableConfig))

    if (isPythonWorkerUsingManagedMemory(planner.getTableConfig.getConfiguration)) {
      transform.declareManagedMemoryUseCaseAtSlotScope(ManagedMemoryUseCase.PYTHON)
    }
    transform
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
    val namePropertyTypeArray = namedWindowProperties.map {
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
      namedWindowProperties: Array[Int],
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
      namedWindowProperties,
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
