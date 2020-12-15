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
import org.apache.flink.table.data.RowData
import org.apache.flink.table.functions.python.PythonAggregateFunctionInfo
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.planner.plan.nodes.common.CommonPythonAggregate
import org.apache.flink.table.planner.plan.utils.{ChangelogPlanUtils, KeySelectorUtil}
import org.apache.flink.table.planner.typeutils.DataViewUtils.DataViewSpec
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.logical.RowType

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall

import java.util

/**
  * Stream physical RelNode for unbounded python group table aggregate.
  */
class StreamExecPythonGroupTableAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    outputRowType: RelDataType,
    grouping: Array[Int],
    aggCalls: Seq[AggregateCall])
  extends StreamExecGroupTableAggregateBase(
    cluster,
    traitSet,
    inputRel,
    outputRowType,
    grouping,
    aggCalls)
  with CommonPythonAggregate {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new StreamExecPythonGroupTableAggregate(
      cluster,
      traitSet,
      inputs.get(0),
      outputRowType,
      grouping,
      aggCalls)
  }

  override protected def translateToPlanInternal(
      planner: StreamPlanner): Transformation[RowData] = {
    val tableConfig = planner.getTableConfig

    if (grouping.length > 0 && tableConfig.getMinIdleStateRetentionTime < 0) {
      LOG.warn("No state retention interval configured for a query which accumulates state. " +
        "Please provide a query configuration with valid retention interval to prevent excessive " +
        "state size. You may specify a retention time of 0 to not clean up the state.")
    }

    val inputTransformation = getInputNodes.get(0).translateToPlan(planner)
      .asInstanceOf[Transformation[RowData]]

    val outRowType = FlinkTypeFactory.toLogicalRowType(outputRowType)
    val inputRowType = FlinkTypeFactory.toLogicalRowType(getInput.getRowType)

    val generateUpdateBefore = ChangelogPlanUtils.generateUpdateBefore(this)

    val inputCountIndex = aggInfoList.getIndexOfCountStar

    var (pythonFunctionInfos, dataViewSpecs) =
      extractPythonAggregateFunctionInfos(aggInfoList, aggCalls)

    if (dataViewSpecs.forall(_.isEmpty)) {
      dataViewSpecs = Array(Array())
    }

    val operator = getPythonTableAggregateFunctionOperator(
      getConfig(planner.getExecEnv, tableConfig),
      inputRowType,
      outRowType,
      pythonFunctionInfos,
      dataViewSpecs,
      tableConfig.getMinIdleStateRetentionTime,
      tableConfig.getMaxIdleStateRetentionTime,
      grouping,
      generateUpdateBefore,
      inputCountIndex)

    val selector = KeySelectorUtil.getRowDataSelector(
      grouping,
      InternalTypeInfo.of(inputRowType))

    // partitioned aggregation
    val ret = new OneInputTransformation(
      inputTransformation,
      getRelDetailedDescription,
      operator,
      InternalTypeInfo.of(outRowType),
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

  private[this] def getPythonTableAggregateFunctionOperator(
      config: Configuration,
      inputType: RowType,
      outputType: RowType,
      aggregateFunctions: Array[PythonAggregateFunctionInfo],
      dataViewSpecs: Array[Array[DataViewSpec]],
      minIdleStateRetentionTime: Long,
      maxIdleStateRetentionTime: Long,
      grouping: Array[Int],
      generateUpdateBefore: Boolean,
      indexOfCountStar: Int): OneInputStreamOperator[RowData, RowData] = {

    val clazz = loadClass(
      StreamExecPythonGroupTableAggregate.PYTHON_STREAM_TABLE_AGGREGATE_OPERATOR_NAME)
    val ctor = clazz.getConstructor(
      classOf[Configuration],
      classOf[RowType],
      classOf[RowType],
      classOf[Array[PythonAggregateFunctionInfo]],
      classOf[Array[Array[DataViewSpec]]],
      classOf[Array[Int]],
      classOf[Int],
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
      generateUpdateBefore.asInstanceOf[AnyRef],
      minIdleStateRetentionTime.asInstanceOf[AnyRef],
      maxIdleStateRetentionTime.asInstanceOf[AnyRef])
      .asInstanceOf[OneInputStreamOperator[RowData, RowData]]
  }
}

object StreamExecPythonGroupTableAggregate {
  val PYTHON_STREAM_TABLE_AGGREGATE_OPERATOR_NAME: String =
    "org.apache.flink.table.runtime.operators.python.aggregate." +
      "PythonStreamGroupTableAggregateOperator"
}
