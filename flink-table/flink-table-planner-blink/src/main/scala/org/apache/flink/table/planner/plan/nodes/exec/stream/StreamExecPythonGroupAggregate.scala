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
import org.apache.flink.table.data.RowData
import org.apache.flink.table.functions.python.PythonAggregateFunctionInfo
import org.apache.flink.table.planner.delegation.PlannerBase
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecPythonAggregate
import org.apache.flink.table.planner.plan.nodes.exec.{ExecEdge, ExecNode, ExecNodeBase}
import org.apache.flink.table.planner.plan.utils.{AggregateUtil, KeySelectorUtil}
import org.apache.flink.table.planner.typeutils.DataViewUtils.DataViewSpec
import org.apache.flink.table.planner.utils.Logging
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.logical.RowType

import org.apache.calcite.rel.core.AggregateCall

import java.util.Collections

/**
 * Stream [[ExecNode]] for Python unbounded group aggregate.
 *
 * <p>Note: This class can't be ported to Java,
 * because java class can't extend scala interface with default implementation.
 * FLINK-20750 will port this class to Java.
 */
class StreamExecPythonGroupAggregate(
    grouping: Array[Int],
    aggCalls: Seq[AggregateCall],
    aggCallNeedRetractions: Array[Boolean],
    generateUpdateBefore: Boolean,
    needRetraction: Boolean,
    inputEdge: ExecEdge,
    outputType: RowType,
    description: String)
  extends ExecNodeBase[RowData](Collections.singletonList(inputEdge), outputType, description)
  with StreamExecNode[RowData]
  with CommonExecPythonAggregate
  with Logging {

  override protected def translateToPlanInternal(planner: PlannerBase): Transformation[RowData] = {
    val tableConfig = planner.getTableConfig

    if (grouping.length > 0 && tableConfig.getMinIdleStateRetentionTime < 0) {
      LOG.warn("No state retention interval configured for a query which accumulates state. " +
        "Please provide a query configuration with valid retention interval to prevent excessive " +
        "state size. You may specify a retention time of 0 to not clean up the state.")
    }

    val inputNode = getInputNodes.get(0).asInstanceOf[ExecNode[RowData]]
    val inputTransformation = inputNode.translateToPlan(planner)
    val inputRowType = inputNode.getOutputType.asInstanceOf[RowType]

    val aggInfoList = AggregateUtil.transformToStreamAggregateInfoList(
      inputRowType,
      aggCalls,
      aggCallNeedRetractions,
      needRetraction,
      isStateBackendDataViews = true)
    val inputCountIndex = aggInfoList.getIndexOfCountStar
    val countStarInserted = aggInfoList.countStarInserted

    var (pythonFunctionInfos, dataViewSpecs) =
      extractPythonAggregateFunctionInfos(aggInfoList, aggCalls)

    if (dataViewSpecs.forall(_.isEmpty)) {
      dataViewSpecs = Array()
    }

    val operator = getPythonAggregateFunctionOperator(
      getConfig(planner.getExecEnv, tableConfig),
      inputRowType,
      outputType,
      pythonFunctionInfos,
      dataViewSpecs,
      tableConfig.getMinIdleStateRetentionTime,
      tableConfig.getMaxIdleStateRetentionTime,
      grouping,
      generateUpdateBefore,
      inputCountIndex,
      countStarInserted)

    val selector = KeySelectorUtil.getRowDataSelector(grouping, InternalTypeInfo.of(inputRowType))

    // partitioned aggregation
    val ret = new OneInputTransformation(
      inputTransformation,
      getDesc,
      operator,
      InternalTypeInfo.of(outputType),
      inputTransformation.getParallelism)

    if (inputsContainSingleton()) {
      ret.setParallelism(1)
      ret.setMaxParallelism(1)
    }

    if (isPythonWorkerUsingManagedMemory(planner.getTableConfig.getConfiguration)) {
      ret.declareManagedMemoryUseCaseAtSlotScope(ManagedMemoryUseCase.PYTHON)
    }

    // set KeyType and Selector for state
    ret.setStateKeySelector(selector)
    ret.setStateKeyType(selector.getProducedType)
    ret
  }

  private def getPythonAggregateFunctionOperator(
      config: Configuration,
      inputType: RowType,
      outputType: RowType,
      aggregateFunctions: Array[PythonAggregateFunctionInfo],
      dataViewSpecs: Array[Array[DataViewSpec]],
      minIdleStateRetentionTime: Long,
      maxIdleStateRetentionTime: Long,
      grouping: Array[Int],
      generateUpdateBefore: Boolean,
      indexOfCountStar: Int,
      countStarInserted: Boolean): OneInputStreamOperator[RowData, RowData] = {

    val clazz = loadClass(StreamExecPythonGroupAggregate.PYTHON_STREAM_AGGREAGTE_OPERATOR_NAME)
    val ctor = clazz.getConstructor(
      classOf[Configuration],
      classOf[RowType],
      classOf[RowType],
      classOf[Array[PythonAggregateFunctionInfo]],
      classOf[Array[Array[DataViewSpec]]],
      classOf[Array[Int]],
      classOf[Int],
      classOf[Boolean],
      classOf[Boolean],
      classOf[Long],
      classOf[Long])
    ctor.newInstance(
      config.asInstanceOf[AnyRef],
      inputType.asInstanceOf[AnyRef],
      outputType.asInstanceOf[AnyRef],
      aggregateFunctions.asInstanceOf[AnyRef],
      dataViewSpecs.asInstanceOf[AnyRef],
      grouping.asInstanceOf[AnyRef],
      indexOfCountStar.asInstanceOf[AnyRef],
      countStarInserted.asInstanceOf[AnyRef],
      generateUpdateBefore.asInstanceOf[AnyRef],
      minIdleStateRetentionTime.asInstanceOf[AnyRef],
      maxIdleStateRetentionTime.asInstanceOf[AnyRef])
      .asInstanceOf[OneInputStreamOperator[RowData, RowData]]
  }
}

object StreamExecPythonGroupAggregate {
  val PYTHON_STREAM_AGGREAGTE_OPERATOR_NAME: String =
    "org.apache.flink.table.runtime.operators.python.aggregate." +
      "PythonStreamGroupAggregateOperator"
}
