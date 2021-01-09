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
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.memory.ManagedMemoryUseCase
import org.apache.flink.streaming.api.operators.OneInputStreamOperator
import org.apache.flink.streaming.api.transformations.OneInputTransformation
import org.apache.flink.table.data.RowData
import org.apache.flink.table.functions.python.PythonFunctionInfo
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.delegation.BatchPlanner
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecPythonAggregate
import org.apache.flink.table.planner.plan.nodes.exec.{ExecEdge, LegacyBatchExecNode}
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonPythonAggregate
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecPythonOverAggregate.ARROW_PYTHON_OVER_WINDOW_AGGREGATE_FUNCTION_OPERATOR_NAME
import org.apache.flink.table.planner.plan.nodes.physical.batch.OverWindowMode.OverWindowMode
import org.apache.flink.table.planner.plan.utils.OverAggregateUtil.getLongBoundary
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.logical.RowType

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelFieldCollation.Direction
import org.apache.calcite.rel._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{AggregateCall, Window}
import org.apache.calcite.sql.fun.SqlLeadLagAggFunction

import java.util

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
  * Batch physical RelNode for sort-based over [[Window]] aggregate (Python user defined aggregate
  * function).
  */
class BatchExecPythonOverAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    outputRowType: RelDataType,
    inputRowType: RelDataType,
    windowGroups: Seq[Window.Group],
    logicWindow: Window)
  extends BatchPhysicalOverAggregateBase(
    cluster,
    traitSet,
    inputRel,
    outputRowType,
    inputRowType,
    windowGroups,
    logicWindow)
  with LegacyBatchExecNode[RowData]
  with CommonPythonAggregate {

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new BatchExecPythonOverAggregate(
      cluster,
      traitSet,
      inputs.get(0),
      outputRowType,
      inputRowType,
      windowGroups,
      logicWindow)
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getInputEdges: util.List[ExecEdge] = List(ExecEdge.DEFAULT)

  override protected def translateToPlanInternal(
      planner: BatchPlanner): Transformation[RowData] = {
    val input = getInputNodes.get(0).translateToPlan(planner)
      .asInstanceOf[Transformation[RowData]]
    val outputType = FlinkTypeFactory.toLogicalRowType(getRowType)
    val inputType = FlinkTypeFactory.toLogicalRowType(inputRowType)
    val windowBoundary = ArrayBuffer[(Long, Long, Boolean)]()
    val aggFunctions = offsetAndInsensitiveSensitiveGroups.zipWithIndex.flatMap {
      case (windowGroup, index) =>
        val mode = inferGroupMode(windowGroup)
        val boundary = mode match {
          case OverWindowMode.Row if isUnboundedWindow(windowGroup) =>
            (Long.MinValue, Long.MaxValue, false)
          case OverWindowMode.Row if isUnboundedPrecedingWindow(windowGroup) =>
            (Long.MinValue, getLongBoundary(logicWindow, windowGroup.upperBound), false)
          case OverWindowMode.Row if isUnboundedFollowingWindow(windowGroup) =>
            (getLongBoundary(logicWindow, windowGroup.lowerBound), Long.MaxValue, false)
          case OverWindowMode.Row if isSlidingWindow(windowGroup) =>
            (getLongBoundary(logicWindow, windowGroup.lowerBound),
              getLongBoundary(logicWindow, windowGroup.upperBound), false)
          case OverWindowMode.Range if isUnboundedWindow(windowGroup) =>
            (Long.MinValue, Long.MaxValue, true)
          case OverWindowMode.Range if isUnboundedPrecedingWindow(windowGroup) =>
            (Long.MinValue, getLongBoundary(logicWindow, windowGroup.upperBound), true)
          case OverWindowMode.Range if isUnboundedFollowingWindow(windowGroup) =>
            (getLongBoundary(logicWindow, windowGroup.lowerBound), Long.MaxValue, true)
          case OverWindowMode.Range if isSlidingWindow(windowGroup) =>
            (getLongBoundary(logicWindow, windowGroup.lowerBound),
              getLongBoundary(logicWindow, windowGroup.upperBound), true)
        }
        windowBoundary.append(boundary)
        windowGroup.getAggregateCalls(logicWindow).map((_, index))
    }
    val ret = createPythonOneInputTransformation(
      input,
      aggFunctions,
      windowBoundary.toArray,
      inputType,
      outputType,
      partitionKeyIndices,
      getConfig(planner.getExecEnv, planner.getTableConfig))

    if (isPythonWorkerUsingManagedMemory(planner.getTableConfig.getConfiguration)) {
      ret.declareManagedMemoryUseCaseAtSlotScope(ManagedMemoryUseCase.PYTHON)
    }
    ret
  }

 private def inferGroupMode(group: Window.Group): OverWindowMode = {
    val aggCall = group.aggCalls(0)
    if (aggCall.getOperator.allowsFraming()) {
      if (group.isRows) OverWindowMode.Row else OverWindowMode.Range
    } else {
      if (aggCall.getOperator.isInstanceOf[SqlLeadLagAggFunction]) {
        OverWindowMode.Offset
      } else {
        OverWindowMode.Insensitive
      }
    }
  }

  private def isUnboundedWindow(group: Window.Group) =
    group.lowerBound.isUnbounded && group.upperBound.isUnbounded

  private def isUnboundedPrecedingWindow(group: Window.Group) =
    group.lowerBound.isUnbounded && !group.upperBound.isUnbounded

  private def isUnboundedFollowingWindow(group: Window.Group) =
    !group.lowerBound.isUnbounded && group.upperBound.isUnbounded

  private def isSlidingWindow(group: Window.Group) =
    !group.lowerBound.isUnbounded && !group.upperBound.isUnbounded

  private[this] def createPythonOneInputTransformation(
      inputTransform: Transformation[RowData],
      aggCallToWindowIndex: Seq[(AggregateCall, Int)],
      windowBoundary: Array[(Long, Long, Boolean)],
      inputRowType: RowType,
      outputRowType: RowType,
      groupingSet: Array[Int],
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

  private[this] def getPythonOverWindowAggregateFunctionOperator(
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

    val orderKey = offsetAndInsensitiveSensitiveGroups.last.orderKeys.getFieldCollations.get(0)
    ctor.newInstance(
      config,
      pythonFunctionInfos,
      inputRowType,
      outputRowType,
      lowerBinary,
      upperBinary,
      windowType,
      aggWindowIndex,
      partitionKeyIndices,
      partitionKeyIndices,
      udafInputOffsets,
      java.lang.Integer.valueOf(orderKey.getFieldIndex),
      java.lang.Boolean.valueOf(orderKey.direction == Direction.ASCENDING))
      .asInstanceOf[OneInputStreamOperator[RowData, RowData]]
  }
}

object BatchExecPythonOverAggregate {
  val ARROW_PYTHON_OVER_WINDOW_AGGREGATE_FUNCTION_OPERATOR_NAME: String =
    "org.apache.flink.table.runtime.operators.python.aggregate.arrow.batch." +
      "BatchArrowPythonOverWindowAggregateFunctionOperator"
}

object OverWindowMode extends Enumeration {
  type OverWindowMode = Value
  val Row: OverWindowMode = Value
  val Range: OverWindowMode = Value
  //Then it is a special kind of Window when the agg is LEAD&LAG.
  val Offset: OverWindowMode = Value
  val Insensitive: OverWindowMode = Value
}
