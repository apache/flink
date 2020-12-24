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
package org.apache.flink.table.planner.plan.nodes.physical.stream

import org.apache.flink.api.dag.Transformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.memory.ManagedMemoryUseCase
import org.apache.flink.streaming.api.operators.OneInputStreamOperator
import org.apache.flink.streaming.api.transformations.OneInputTransformation
import org.apache.flink.table.api.TableException
import org.apache.flink.table.data.RowData
import org.apache.flink.table.functions.python.PythonFunctionInfo
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecPythonAggregate
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecPythonOverAggregate
.{ARROW_PYTHON_OVER_WINDOW_RANGE_PROC_TIME_AGGREGATE_FUNCTION_OPERATOR_NAME,
  ARROW_PYTHON_OVER_WINDOW_RANGE_ROW_TIME_AGGREGATE_FUNCTION_OPERATOR_NAME,
  ARROW_PYTHON_OVER_WINDOW_ROWS_PROC_TIME_AGGREGATE_FUNCTION_OPERATOR_NAME,
  ARROW_PYTHON_OVER_WINDOW_ROWS_ROW_TIME_AGGREGATE_FUNCTION_OPERATOR_NAME}
import org.apache.flink.table.planner.plan.utils.{KeySelectorUtil, OverAggregateUtil}
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.logical.RowType

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelFieldCollation.Direction.ASCENDING
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.Window.Group
import org.apache.calcite.rel.core.{AggregateCall, Window}

import java.util

import scala.collection.JavaConverters._

/**
  * Stream physical RelNode for python time-based over [[Window]].
  */
class StreamExecPythonOverAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    outputRowType: RelDataType,
    inputRowType: RelDataType,
    logicWindow: Window)
  extends StreamExecOverAggregateBase(
    cluster,
    traitSet,
    inputRel,
    outputRowType,
    inputRowType,
    logicWindow)
  with CommonExecPythonAggregate {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new StreamExecPythonOverAggregate(
      cluster,
      traitSet,
      inputs.get(0),
      outputRowType,
      inputRowType,
      logicWindow
      )
  }

  override protected def translateToPlanInternal(
      planner: StreamPlanner): Transformation[RowData] = {
    val tableConfig = planner.getTableConfig

    val overWindow: Group = logicWindow.groups.get(0)

    val orderKeys = overWindow.orderKeys.getFieldCollations

    if (orderKeys.size() != 1) {
      throw new TableException(
        "The window can only be ordered by a single time column.")
    }
    val orderKey = orderKeys.get(0)

    if (!orderKey.direction.equals(ASCENDING)) {
      throw new TableException(
        "The window can only be ordered in ASCENDING mode.")
    }

    val inputDS = getInputNodes.get(0).translateToPlan(planner)
      .asInstanceOf[Transformation[RowData]]

    if (!logicWindow.groups.get(0).keys.isEmpty && tableConfig.getMinIdleStateRetentionTime < 0) {
      LOG.warn(
        "No state retention interval configured for a query which accumulates state. " +
          "Please provide a query configuration with valid retention interval to prevent " +
          "excessive state size. You may specify a retention time of 0 to not clean up the state.")
    }

    val timeType = outputRowType.getFieldList.get(orderKey.getFieldIndex).getType

    // check time field
    if (!FlinkTypeFactory.isRowtimeIndicatorType(timeType)
      && !FlinkTypeFactory.isProctimeIndicatorType(timeType)) {
      throw new TableException(
        "OVER windows' ordering in stream mode must be defined on a time attribute.")
    }

    // identify window rowtime attribute
    val rowTimeIdx: Option[Int] = if (FlinkTypeFactory.isRowtimeIndicatorType(timeType)) {
      Some(orderKey.getFieldIndex)
    } else if (FlinkTypeFactory.isProctimeIndicatorType(timeType)) {
      None
    } else {
      throw new TableException(
        "OVER windows can only be applied on time attributes.")
    }

    if (overWindow.lowerBound.isPreceding
      && overWindow.lowerBound.isUnbounded) {
      throw new TableException(
        "Python UDAF is not supported to be used in UNBOUNDED PRECEDING OVER windows."
      )
    } else if (!overWindow.upperBound.isCurrentRow) {
      throw new TableException(
        "Python UDAF is not supported to be used in UNBOUNDED FOLLOWING OVER windows."
      )
    }
    val aggregateCalls = logicWindow.groups.get(0).getAggregateCalls(logicWindow).asScala
    val inRowType = FlinkTypeFactory.toLogicalRowType(inputRel.getRowType)
    val outRowType = FlinkTypeFactory.toLogicalRowType(outputRowType)

    val partitionKeys: Array[Int] = overWindow.keys.toArray
    val inputTypeInfo = InternalTypeInfo.of(inRowType)

    val selector = KeySelectorUtil.getRowDataSelector(partitionKeys, inputTypeInfo)

    val boundValue = OverAggregateUtil.getBoundary(logicWindow, overWindow.lowerBound)

    val isRowsClause = overWindow.isRows

    if (boundValue.isInstanceOf[BigDecimal]) {
      throw new TableException(
        "the specific value is decimal which haven not supported yet.")
    }
    // bounded OVER window
    val precedingOffset = -1 * boundValue.asInstanceOf[Long]
    val ret = createPythonOneInputTransformation(
      inputDS,
      inRowType,
      outRowType,
      rowTimeIdx,
      aggregateCalls,
      precedingOffset,
      isRowsClause,
      partitionKeys,
      tableConfig.getMinIdleStateRetentionTime,
      tableConfig.getMaxIdleStateRetentionTime,
      getConfig(planner.getExecEnv, tableConfig))

    if (isPythonWorkerUsingManagedMemory(tableConfig.getConfiguration)) {
      ret.declareManagedMemoryUseCaseAtSlotScope(ManagedMemoryUseCase.PYTHON)
    }

    if (inputsContainSingleton()) {
      ret.setParallelism(1)
      ret.setMaxParallelism(1)
    }

    // set KeyType and Selector for state
    ret.setStateKeySelector(selector)
    ret.setStateKeyType(selector.getProducedType)
    ret
  }

  private[this] def createPythonOneInputTransformation(
      inputTransform: Transformation[RowData],
      inputRowType: RowType,
      outputRowType: RowType,
      rowTimeIdx: Option[Int],
      aggCalls: Seq[AggregateCall],
      lowerBoundary: Long,
      isRowsClause: Boolean,
      grouping: Array[Int],
      minIdleStateRetentionTime: Long,
      maxIdleStateRetentionTime: Long,
      config: Configuration): OneInputTransformation[RowData, RowData] = {
    val (pythonUdafInputOffsets, pythonFunctionInfos) =
      extractPythonAggregateFunctionInfosFromAggregateCall(aggCalls)
    val pythonOperator = getPythonOverWindowAggregateFunctionOperator(
      config,
      inputRowType,
      outputRowType,
      rowTimeIdx,
      lowerBoundary,
      isRowsClause,
      grouping,
      pythonUdafInputOffsets,
      pythonFunctionInfos,
      minIdleStateRetentionTime,
      maxIdleStateRetentionTime)

    new OneInputTransformation(
      inputTransform,
      "StreamExecPythonOverAggregate",
      pythonOperator,
      InternalTypeInfo.of(outputRowType),
      inputTransform.getParallelism)
  }

  private[this] def getPythonOverWindowAggregateFunctionOperator(
      config: Configuration,
      inputRowType: RowType,
      outputRowType: RowType,
      rowTimeIdx: Option[Int],
      lowerBinary: Long,
      isRowsClause: Boolean,
      grouping: Array[Int],
      udafInputOffsets: Array[Int],
      pythonFunctionInfos: Array[PythonFunctionInfo],
      minIdleStateRetentionTime: Long,
      maxIdleStateRetentionTime: Long): OneInputStreamOperator[RowData, RowData] = {
    val inputTimeFieldIndex = if (rowTimeIdx.isDefined) {
      rowTimeIdx.get
    } else {
      -1
    }
    if (isRowsClause) {
      val className = if (rowTimeIdx.isDefined) {
        ARROW_PYTHON_OVER_WINDOW_ROWS_ROW_TIME_AGGREGATE_FUNCTION_OPERATOR_NAME
      } else {
        ARROW_PYTHON_OVER_WINDOW_ROWS_PROC_TIME_AGGREGATE_FUNCTION_OPERATOR_NAME
      }
      val clazz = loadClass(className)
      val ctor = clazz.getConstructor(
        classOf[Configuration],
        classOf[Long],
        classOf[Long],
        classOf[Array[PythonFunctionInfo]],
        classOf[RowType],
        classOf[RowType],
        classOf[Int],
        classOf[Long],
        classOf[Array[Int]],
        classOf[Array[Int]])
      ctor.newInstance(
        config,
        minIdleStateRetentionTime.asInstanceOf[AnyRef],
        maxIdleStateRetentionTime.asInstanceOf[AnyRef],
        pythonFunctionInfos,
        inputRowType,
        outputRowType,
        inputTimeFieldIndex.asInstanceOf[AnyRef],
        lowerBinary.asInstanceOf[AnyRef],
        grouping,
        udafInputOffsets)
        .asInstanceOf[OneInputStreamOperator[RowData, RowData]]
    } else {
      val className = if (rowTimeIdx.isDefined) {
        ARROW_PYTHON_OVER_WINDOW_RANGE_ROW_TIME_AGGREGATE_FUNCTION_OPERATOR_NAME
      } else {
        ARROW_PYTHON_OVER_WINDOW_RANGE_PROC_TIME_AGGREGATE_FUNCTION_OPERATOR_NAME
      }
      val clazz = loadClass(className)
      val ctor = clazz.getConstructor(
        classOf[Configuration],
        classOf[Array[PythonFunctionInfo]],
        classOf[RowType],
        classOf[RowType],
        classOf[Int],
        classOf[Long],
        classOf[Array[Int]],
        classOf[Array[Int]])
      ctor.newInstance(
        config,
        pythonFunctionInfos,
        inputRowType,
        outputRowType,
        inputTimeFieldIndex.asInstanceOf[AnyRef],
        lowerBinary.asInstanceOf[AnyRef],
        grouping,
        udafInputOffsets)
        .asInstanceOf[OneInputStreamOperator[RowData, RowData]]
    }
  }
}

object StreamExecPythonOverAggregate {
  val ARROW_PYTHON_OVER_WINDOW_RANGE_ROW_TIME_AGGREGATE_FUNCTION_OPERATOR_NAME : String =
    "org.apache.flink.table.runtime.operators.python.aggregate.arrow.stream." +
      "StreamArrowPythonRowTimeBoundedRangeOperator"
  val ARROW_PYTHON_OVER_WINDOW_RANGE_PROC_TIME_AGGREGATE_FUNCTION_OPERATOR_NAME : String =
    "org.apache.flink.table.runtime.operators.python.aggregate.arrow.stream." +
      "StreamArrowPythonProcTimeBoundedRangeOperator"
  val ARROW_PYTHON_OVER_WINDOW_ROWS_ROW_TIME_AGGREGATE_FUNCTION_OPERATOR_NAME : String =
    "org.apache.flink.table.runtime.operators.python.aggregate.arrow.stream." +
      "StreamArrowPythonRowTimeBoundedRowsOperator"
  val ARROW_PYTHON_OVER_WINDOW_ROWS_PROC_TIME_AGGREGATE_FUNCTION_OPERATOR_NAME : String =
    "org.apache.flink.table.runtime.operators.python.aggregate.arrow.stream." +
      "StreamArrowPythonProcTimeBoundedRowsOperator"
}
