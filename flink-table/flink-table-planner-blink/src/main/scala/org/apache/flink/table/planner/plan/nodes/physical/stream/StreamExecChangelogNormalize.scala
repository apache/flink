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
import org.apache.flink.streaming.api.operators.KeyedProcessOperator
import org.apache.flink.streaming.api.transformations.OneInputTransformation
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.planner.plan.nodes.exec.LegacyStreamExecNode
import org.apache.flink.table.planner.plan.utils.{AggregateUtil, ChangelogPlanUtils, KeySelectorUtil}
import org.apache.flink.table.runtime.operators.bundle.KeyedMapBundleOperator
import org.apache.flink.table.runtime.operators.deduplicate.{ProcTimeDeduplicateKeepLastRowFunction, ProcTimeMiniBatchDeduplicateKeepLastRowFunction}
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}

import java.util

/**
 * Stream physical RelNode which normalizes a changelog stream which maybe an upsert stream or
 * a changelog stream containing duplicate events. This node normalize such stream into a regular
 * changelog stream that contains INSERT/UPDATE_BEFORE/UPDATE_AFTER/DELETE records without
 * duplication.
 */
class StreamExecChangelogNormalize(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    val uniqueKeys: Array[Int])
  extends SingleRel(cluster, traitSet, input)
  with StreamPhysicalRel
  with LegacyStreamExecNode[RowData] {

  override def requireWatermark: Boolean = false

  override def deriveRowType(): RelDataType = getInput.getRowType

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new StreamExecChangelogNormalize(
      cluster,
      traitSet,
      inputs.get(0),
      uniqueKeys)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val fieldNames = getRowType.getFieldNames
    super.explainTerms(pw)
      .item("key", uniqueKeys.map(fieldNames.get).mkString(", "))
  }

  //~ ExecNode methods -----------------------------------------------------------

  override protected def translateToPlanInternal(
      planner: StreamPlanner): Transformation[RowData] = {

    val inputTransform = getInputNodes.get(0).translateToPlan(planner)
      .asInstanceOf[Transformation[RowData]]

    val rowTypeInfo = inputTransform.getOutputType.asInstanceOf[InternalTypeInfo[RowData]]
    val generateUpdateBefore = ChangelogPlanUtils.generateUpdateBefore(this)
    val tableConfig = planner.getTableConfig
    val isMiniBatchEnabled = tableConfig.getConfiguration.getBoolean(
      ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED)
    val stateIdleTime = tableConfig.getIdleStateRetention.toMillis
    val operator = if (isMiniBatchEnabled) {
      val exeConfig = planner.getExecEnv.getConfig
      val rowSerializer = rowTypeInfo.createSerializer(exeConfig)
      val processFunction = new ProcTimeMiniBatchDeduplicateKeepLastRowFunction(
        rowTypeInfo,
        rowSerializer,
        stateIdleTime,
        generateUpdateBefore,
        true,   // generateInsert
        false)  // inputInsertOnly
      val trigger = AggregateUtil.createMiniBatchTrigger(tableConfig)
      new KeyedMapBundleOperator(
        processFunction,
        trigger)
    } else {
      val processFunction = new ProcTimeDeduplicateKeepLastRowFunction(
        rowTypeInfo,
        stateIdleTime,
        generateUpdateBefore,
        true,   // generateInsert
        false)  // inputInsertOnly
      new KeyedProcessOperator[RowData, RowData, RowData](processFunction)
    }

    val ret = new OneInputTransformation(
      inputTransform,
      getRelDetailedDescription,
      operator,
      rowTypeInfo,
      inputTransform.getParallelism)

    if (inputsContainSingleton()) {
      ret.setParallelism(1)
      ret.setMaxParallelism(1)
    }

    val selector = KeySelectorUtil.getRowDataSelector(uniqueKeys, rowTypeInfo)
    ret.setStateKeySelector(selector)
    ret.setStateKeyType(selector.getProducedType)
    ret
  }
}
