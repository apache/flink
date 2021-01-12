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
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecOverAggregateBase.OverWindowMode
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecPythonOverAggregate.ARROW_PYTHON_OVER_WINDOW_AGGREGATE_FUNCTION_OPERATOR_NAME
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonPythonAggregate
import org.apache.flink.table.planner.plan.nodes.exec.utils.OverSpec
import org.apache.flink.table.planner.plan.nodes.exec.{ExecEdge, ExecNode}
import org.apache.flink.table.planner.plan.utils.OverAggregateUtil.getLongBoundary
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.logical.RowType

import org.apache.calcite.rel.core.AggregateCall

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
 * Batch [[ExecNode]] for sort-based over window aggregate (Python user defined aggregate function).
 *
 * <p>Note: This class can't be ported to Java,
 * because java class can't extend scala interface with default implementation.
 * FLINK-20924 will port this class to Java.
 */
class BatchExecPythonOverAggregate(
    over: OverSpec,
    inputEdge: ExecEdge,
    outputType: RowType,
    description: String)
  extends BatchExecOverAggregateBase(over, inputEdge, outputType, description)
  with CommonPythonAggregate {

  override protected def translateToPlanInternal(planner: PlannerBase): Transformation[RowData] = {
    val inputNode = getInputNodes.get(0).asInstanceOf[ExecNode[RowData]]
    val input = inputNode.translateToPlan(planner)
    val windowBoundary = ArrayBuffer[(Long, Long, Boolean)]()
    val aggCallToWindowIndex = over.getGroups.zipWithIndex.flatMap {
      case (group, index) =>
        val mode = inferGroupMode(group)
        val boundary = mode match {
          case OverWindowMode.ROW if isUnboundedWindow(group) =>
            (Long.MinValue, Long.MaxValue, false)
          case OverWindowMode.ROW if isUnboundedPrecedingWindow(group) =>
            (Long.MinValue, getLongBoundary(over, group.getUpperBound), false)
          case OverWindowMode.ROW if isUnboundedFollowingWindow(group) =>
            (getLongBoundary(over, group.getLowerBound), Long.MaxValue, false)
          case OverWindowMode.ROW if isSlidingWindow(group) =>
            (getLongBoundary(over, group.getLowerBound),
              getLongBoundary(over, group.getUpperBound), false)
          case OverWindowMode.RANGE if isUnboundedWindow(group) =>
            (Long.MinValue, Long.MaxValue, true)
          case OverWindowMode.RANGE if isUnboundedPrecedingWindow(group) =>
            (Long.MinValue, getLongBoundary(over, group.getUpperBound), true)
          case OverWindowMode.RANGE if isUnboundedFollowingWindow(group) =>
            (getLongBoundary(over, group.getLowerBound), Long.MaxValue, true)
          case OverWindowMode.RANGE if isSlidingWindow(group) =>
            (getLongBoundary(over, group.getLowerBound),
              getLongBoundary(over, group.getUpperBound), true)
        }
        windowBoundary.append(boundary)
        group.getAggCalls.map((_, index))
    }
    val config = getConfig(planner.getExecEnv, planner.getTableConfig)
    val transform = createPythonOneInputTransformation(
      input,
      aggCallToWindowIndex,
      windowBoundary.toArray,
      inputNode.getOutputType.asInstanceOf[RowType],
      outputType,
      config)

    if (isPythonWorkerUsingManagedMemory(config)) {
      transform.declareManagedMemoryUseCaseAtSlotScope(ManagedMemoryUseCase.PYTHON)
    }
    transform
  }

  private def createPythonOneInputTransformation(
      inputTransform: Transformation[RowData],
      aggCallToWindowIndex: Seq[(AggregateCall, Int)],
      windowBoundary: Array[(Long, Long, Boolean)],
      inputRowType: RowType,
      outputRowType: RowType,
      config: Configuration): OneInputTransformation[RowData, RowData] = {
    val (pythonUdafInputOffsets, pythonFunctionInfos) =
      extractPythonAggregateFunctionInfosFromAggregateCall(aggCallToWindowIndex.map(_._1))
    val pythonOperator = getPythonOverWindowAggregateFunctionOperator(
      config,
      inputRowType,
      outputRowType,
      windowBoundary.map(_._1),
      windowBoundary.map(_._2),
      windowBoundary.map(_._3),
      aggCallToWindowIndex.map(_._2).toArray,
      pythonUdafInputOffsets,
      pythonFunctionInfos)

    new OneInputTransformation(
      inputTransform,
      "BatchExecPythonOverAggregate",
      pythonOperator,
      InternalTypeInfo.of(outputRowType),
      inputTransform.getParallelism)
  }

  private def getPythonOverWindowAggregateFunctionOperator(
      config: Configuration,
      inputRowType: RowType,
      outputRowType: RowType,
      lowerBinary: Array[Long],
      upperBinary: Array[Long],
      windowType: Array[Boolean],
      aggWindowIndex: Array[Int],
      udafInputOffsets: Array[Int],
      pythonFunctionInfos: Array[PythonFunctionInfo]): OneInputStreamOperator[RowData, RowData] = {
    val clazz = loadClass(ARROW_PYTHON_OVER_WINDOW_AGGREGATE_FUNCTION_OPERATOR_NAME)

    val ctor = clazz.getConstructor(
      classOf[Configuration],
      classOf[Array[PythonFunctionInfo]],
      classOf[RowType],
      classOf[RowType],
      classOf[Array[Long]],
      classOf[Array[Long]],
      classOf[Array[Boolean]],
      classOf[Array[Int]],
      classOf[Array[Int]],
      classOf[Array[Int]],
      classOf[Array[Int]],
      classOf[Int],
      classOf[Boolean])

    val partitionSpec = overSpec.getPartition
    val sortSpec = overSpec.getGroups.last.getSort
    ctor.newInstance(
      config,
      pythonFunctionInfos,
      inputRowType,
      outputRowType,
      lowerBinary,
      upperBinary,
      windowType,
      aggWindowIndex,
      partitionSpec.getFieldIndices,
      partitionSpec.getFieldIndices,
      udafInputOffsets,
      java.lang.Integer.valueOf(sortSpec.getFieldIndices()(0)),
      java.lang.Boolean.valueOf(sortSpec.getAscendingOrders()(0)))
      .asInstanceOf[OneInputStreamOperator[RowData, RowData]]
  }
}

object BatchExecPythonOverAggregate {
  val ARROW_PYTHON_OVER_WINDOW_AGGREGATE_FUNCTION_OPERATOR_NAME: String =
    "org.apache.flink.table.runtime.operators.python.aggregate.arrow.batch." +
      "BatchArrowPythonOverWindowAggregateFunctionOperator"
}
