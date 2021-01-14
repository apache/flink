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

package org.apache.flink.table.planner.plan.nodes.exec.stream


import org.apache.flink.api.dag.Transformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.memory.ManagedMemoryUseCase
import org.apache.flink.streaming.api.operators.OneInputStreamOperator
import org.apache.flink.streaming.api.transformations.OneInputTransformation
import org.apache.flink.table.api.TableException
import org.apache.flink.table.data.RowData
import org.apache.flink.table.functions.python.PythonFunctionInfo
import org.apache.flink.table.planner.delegation.PlannerBase
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonPythonAggregate
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecPythonOverAggregate.{ARROW_PYTHON_OVER_WINDOW_RANGE_PROC_TIME_AGGREGATE_FUNCTION_OPERATOR_NAME, ARROW_PYTHON_OVER_WINDOW_RANGE_ROW_TIME_AGGREGATE_FUNCTION_OPERATOR_NAME, ARROW_PYTHON_OVER_WINDOW_ROWS_PROC_TIME_AGGREGATE_FUNCTION_OPERATOR_NAME, ARROW_PYTHON_OVER_WINDOW_ROWS_ROW_TIME_AGGREGATE_FUNCTION_OPERATOR_NAME}
import org.apache.flink.table.planner.plan.nodes.exec.utils.OverSpec
import org.apache.flink.table.planner.plan.nodes.exec.{ExecEdge, ExecNode, ExecNodeBase}
import org.apache.flink.table.planner.plan.utils.{KeySelectorUtil, OverAggregateUtil}
import org.apache.flink.table.planner.utils.Logging
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.logical.{RowType, TimestampKind, TimestampType}

import org.apache.calcite.rel.core.AggregateCall

import java.util.Collections

import scala.collection.JavaConversions._

/**
 * Stream [[ExecNode]] for python time-based over operator.
 *
 * <p>Note: This class can't be ported to Java,
 * because java class can't extend scala interface with default implementation.
 * FLINK-20924 will port this class to Java.
 */
class StreamExecPythonOverAggregate(
    overSpec: OverSpec, inputEdge: ExecEdge, outputType: RowType, description: String)
  extends ExecNodeBase[RowData](
    Collections.singletonList(inputEdge),
    outputType,
    description)
  with StreamExecNode[RowData]
  with CommonPythonAggregate
  with Logging {

  override protected def translateToPlanInternal(planner: PlannerBase): Transformation[RowData] = {
    val group = overSpec.getGroups.get(0)
    val orderKeys = group.getSort.getFieldIndices
    val isAscendingOrders = group.getSort.getAscendingOrders
    if (orderKeys.length != 1 || isAscendingOrders.length != 1) {
      throw new TableException("The window can only be ordered by a single time column.")
    }
    if (!isAscendingOrders(0)) {
      throw new TableException("The window can only be ordered in ASCENDING mode.")
    }

    val tableConfig = planner.getTableConfig
    val partitionKeys = overSpec.getPartition.getFieldIndices
    if (partitionKeys.nonEmpty && tableConfig.getMinIdleStateRetentionTime < 0) {
      LOG.warn(
        "No state retention interval configured for a query which accumulates state. " +
          "Please provide a query configuration with valid retention interval to prevent " +
          "excessive state size. You may specify a retention time of 0 to not clean up the state.")
    }

    val inputNode = getInputNodes.get(0).asInstanceOf[ExecNode[RowData]]
    val inputTransform = inputNode.translateToPlan(planner)
    val inputRowType = inputNode.getOutputType.asInstanceOf[RowType]

    val orderKey = orderKeys(0)
    val orderKeyType = inputRowType.getFields.get(orderKey).getType

    // check time field
    val rowTimeIdx: Option[Int] = orderKeyType match {
      case t: TimestampType if t.getKind == TimestampKind.ROWTIME => Some(orderKey)
      case t: TimestampType if t.getKind == TimestampKind.PROCTIME => None
      case _ =>
        throw new TableException(
          "OVER windows' ordering in stream mode must be defined on a time attribute.")
    }

    if (group.getLowerBound.isPreceding && group.getLowerBound.isUnbounded) {
      throw new TableException(
        "Python UDAF is not supported to be used in UNBOUNDED PRECEDING OVER windows.")
    } else if (!group.getUpperBound.isCurrentRow) {
      throw new TableException(
        "Python UDAF is not supported to be used in UNBOUNDED FOLLOWING OVER windows.")
    }

    val boundValue = OverAggregateUtil.getBoundary(overSpec, group.getLowerBound)
    if (boundValue.isInstanceOf[BigDecimal]) {
      throw new TableException(
        "the specific value is decimal which haven not supported yet.")
    }
    // bounded OVER window
    val precedingOffset = -1 * boundValue.asInstanceOf[Long]
    val config = getConfig(planner.getExecEnv, tableConfig)
    val transform = createPythonOneInputTransformation(
      inputTransform,
      inputRowType,
      getOutputType.asInstanceOf[RowType],
      rowTimeIdx,
      group.getAggCalls,
      precedingOffset,
      group.isRows,
      partitionKeys,
      tableConfig.getMinIdleStateRetentionTime,
      tableConfig.getMaxIdleStateRetentionTime,
      config)

    if (isPythonWorkerUsingManagedMemory(config)) {
      transform.declareManagedMemoryUseCaseAtSlotScope(ManagedMemoryUseCase.PYTHON)
    }

    if (inputsContainSingleton()) {
      transform.setParallelism(1)
      transform.setMaxParallelism(1)
    }

    // set KeyType and Selector for state
    val selector = KeySelectorUtil.getRowDataSelector(
      partitionKeys, InternalTypeInfo.of(inputRowType))
    transform.setStateKeySelector(selector)
    transform.setStateKeyType(selector.getProducedType)
    transform
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
  val ARROW_PYTHON_OVER_WINDOW_RANGE_ROW_TIME_AGGREGATE_FUNCTION_OPERATOR_NAME: String =
    "org.apache.flink.table.runtime.operators.python.aggregate.arrow.stream." +
      "StreamArrowPythonRowTimeBoundedRangeOperator"
  val ARROW_PYTHON_OVER_WINDOW_RANGE_PROC_TIME_AGGREGATE_FUNCTION_OPERATOR_NAME: String =
    "org.apache.flink.table.runtime.operators.python.aggregate.arrow.stream." +
      "StreamArrowPythonProcTimeBoundedRangeOperator"
  val ARROW_PYTHON_OVER_WINDOW_ROWS_ROW_TIME_AGGREGATE_FUNCTION_OPERATOR_NAME: String =
    "org.apache.flink.table.runtime.operators.python.aggregate.arrow.stream." +
      "StreamArrowPythonRowTimeBoundedRowsOperator"
  val ARROW_PYTHON_OVER_WINDOW_ROWS_PROC_TIME_AGGREGATE_FUNCTION_OPERATOR_NAME: String =
    "org.apache.flink.table.runtime.operators.python.aggregate.arrow.stream." +
      "StreamArrowPythonProcTimeBoundedRowsOperator"
}
