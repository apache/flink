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
import org.apache.flink.table.data.RowData
import org.apache.flink.table.functions.python.PythonFunctionInfo
import org.apache.flink.table.planner.delegation.PlannerBase
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecPythonGroupAggregate.ARROW_PYTHON_AGGREGATE_FUNCTION_OPERATOR_NAME
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecPythonAggregate
import org.apache.flink.table.planner.plan.nodes.exec.{ExecEdge, ExecNode, ExecNodeBase}
import org.apache.flink.table.planner.utils.Logging
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.logical.RowType

import org.apache.calcite.rel.core.AggregateCall

import java.util.Collections

/**
 * Batch [[ExecNode]] for aggregate (Python user defined aggregate function).
 *
 * <p>Note: This class can't be ported to Java,
 * because java class can't extend scala interface with default implementation.
 * FLINK-20751 will port this class to Java.
 */
class BatchExecPythonGroupAggregate(
    grouping: Array[Int],
    auxGrouping: Array[Int],
    aggCalls: Seq[AggregateCall],
    inputEdge: ExecEdge,
    outputType: RowType,
    description: String)
  extends ExecNodeBase[RowData](Collections.singletonList(inputEdge), outputType, description)
  with BatchExecNode[RowData]
  with CommonExecPythonAggregate
  with Logging {

  override protected def translateToPlanInternal(
      planner: PlannerBase): Transformation[RowData] = {
    val inputNode = getInputNodes.get(0).asInstanceOf[ExecNode[RowData]]
    val inputTransform = inputNode.translateToPlan(planner)

    val ret = createPythonOneInputTransformation(
      inputTransform,
      inputNode.getOutputType.asInstanceOf[RowType],
      outputType,
      getConfig(planner.getExecEnv, planner.getTableConfig))
    if (isPythonWorkerUsingManagedMemory(planner.getTableConfig.getConfiguration)) {
      ret.declareManagedMemoryUseCaseAtSlotScope(ManagedMemoryUseCase.PYTHON)
    }
    ret
  }

  private[this] def createPythonOneInputTransformation(
      inputTransform: Transformation[RowData],
      inputRowType: RowType,
      outputRowType: RowType,
      config: Configuration): OneInputTransformation[RowData, RowData] = {

    val (pythonUdafInputOffsets, pythonFunctionInfos) =
      extractPythonAggregateFunctionInfosFromAggregateCall(aggCalls)

    val pythonOperator = getPythonAggregateFunctionOperator(
      config,
      inputRowType,
      outputRowType,
      pythonUdafInputOffsets,
      pythonFunctionInfos)

    new OneInputTransformation(
      inputTransform,
      "BatchExecPythonGroupAggregate",
      pythonOperator,
      InternalTypeInfo.of(outputRowType),
      inputTransform.getParallelism)
  }

  private[this] def getPythonAggregateFunctionOperator(
      config: Configuration,
      inputRowType: RowType,
      outputRowType: RowType,
      udafInputOffsets: Array[Int],
      pythonFunctionInfos: Array[PythonFunctionInfo]): OneInputStreamOperator[RowData, RowData] = {
    val clazz = loadClass(ARROW_PYTHON_AGGREGATE_FUNCTION_OPERATOR_NAME)

    val ctor = clazz.getConstructor(
      classOf[Configuration],
      classOf[Array[PythonFunctionInfo]],
      classOf[RowType],
      classOf[RowType],
      classOf[Array[Int]],
      classOf[Array[Int]],
      classOf[Array[Int]])
    ctor.newInstance(
      config,
      pythonFunctionInfos,
      inputRowType,
      outputRowType,
      grouping,
      grouping ++ auxGrouping,
      udafInputOffsets).asInstanceOf[OneInputStreamOperator[RowData, RowData]]
  }
}

object BatchExecPythonGroupAggregate {
  val ARROW_PYTHON_AGGREGATE_FUNCTION_OPERATOR_NAME: String =
    "org.apache.flink.table.runtime.operators.python.aggregate.arrow.batch." +
      "BatchArrowPythonGroupAggregateFunctionOperator"
}
