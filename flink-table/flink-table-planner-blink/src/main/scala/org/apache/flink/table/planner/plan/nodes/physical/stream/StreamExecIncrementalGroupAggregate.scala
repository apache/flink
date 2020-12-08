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
import org.apache.flink.streaming.api.transformations.OneInputTransformation
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.CodeGeneratorContext
import org.apache.flink.table.planner.codegen.agg.AggsHandlerCodeGenerator
import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.planner.plan.nodes.exec.LegacyStreamExecNode
import org.apache.flink.table.planner.plan.utils.{KeySelectorUtil, _}
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction
import org.apache.flink.table.runtime.operators.aggregate.MiniBatchIncrementalGroupAggFunction
import org.apache.flink.table.runtime.operators.bundle.KeyedMapBundleOperator
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.DataType

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.tools.RelBuilder

import java.util

import scala.collection.JavaConversions._

/**
  * Stream physical RelNode for unbounded incremental group aggregate.
  *
  * <p>Considering the following sub-plan:
  * {{{
  *   StreamExecGlobalGroupAggregate (final-global-aggregate)
  *   +- StreamExecExchange
  *      +- StreamExecLocalGroupAggregate (final-local-aggregate)
  *         +- StreamExecGlobalGroupAggregate (partial-global-aggregate)
  *            +- StreamExecExchange
  *               +- StreamExecLocalGroupAggregate (partial-local-aggregate)
  * }}}
  *
  * partial-global-aggregate and final-local-aggregate can be combined as
  * this node to share [[org.apache.flink.api.common.state.State]].
  * now the sub-plan is
  * {{{
  *   StreamExecGlobalGroupAggregate (final-global-aggregate)
  *   +- StreamExecExchange
  *      +- StreamExecIncrementalGroupAggregate
  *         +- StreamExecExchange
  *            +- StreamExecLocalGroupAggregate (partial-local-aggregate)
  * }}}
  *
  * @see [[StreamExecGroupAggregateBase]] for more info.
  */
class StreamExecIncrementalGroupAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    inputRowType: RelDataType,
    outputRowType: RelDataType,
    val partialAggInfoList: AggregateInfoList,
    finalAggInfoList: AggregateInfoList,
    val finalAggCalls: Seq[AggregateCall],
    val finalAggGrouping: Array[Int],
    val partialAggGrouping: Array[Int])
  extends StreamExecGroupAggregateBase(cluster, traitSet, inputRel)
  with LegacyStreamExecNode[RowData] {

  override def deriveRowType(): RelDataType = outputRowType

  override def requireWatermark: Boolean = false

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new StreamExecIncrementalGroupAggregate(
      cluster,
      traitSet,
      inputs.get(0),
      inputRowType,
      outputRowType,
      partialAggInfoList,
      finalAggInfoList,
      finalAggCalls,
      finalAggGrouping,
      partialAggGrouping)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("partialAggGrouping",
        RelExplainUtil.fieldToString(partialAggGrouping, inputRel.getRowType))
      .item("finalAggGrouping",
    RelExplainUtil.fieldToString(finalAggGrouping, inputRel.getRowType))
      .item("select", RelExplainUtil.streamGroupAggregationToString(
        inputRel.getRowType,
        getRowType,
        finalAggInfoList,
        finalAggGrouping,
        shuffleKey = Some(partialAggGrouping)))
  }

  //~ ExecNode methods -----------------------------------------------------------

  override protected def translateToPlanInternal(
      planner: StreamPlanner): Transformation[RowData] = {
    val config = planner.getTableConfig
    val inputTransformation = getInputNodes.get(0).translateToPlan(planner)
      .asInstanceOf[Transformation[RowData]]

    val inRowType = FlinkTypeFactory.toLogicalRowType(inputRel.getRowType)
    val outRowType = FlinkTypeFactory.toLogicalRowType(outputRowType)

    val partialAggsHandler = generateAggsHandler(
      "PartialGroupAggsHandler",
      partialAggInfoList,
      mergedAccOffset = partialAggGrouping.length,
      partialAggInfoList.getAccTypes,
      config,
      planner.getRelBuilder,
      // the partial aggregate accumulators will be buffered, so need copy
      inputFieldCopy = true)

    val finalAggsHandler = generateAggsHandler(
      "FinalGroupAggsHandler",
      finalAggInfoList,
      mergedAccOffset = 0,
      partialAggInfoList.getAccTypes,
      config,
      planner.getRelBuilder,
      // the final aggregate accumulators is not buffered
      inputFieldCopy = false)

    val partialKeySelector = KeySelectorUtil.getRowDataSelector(
      partialAggGrouping,
      InternalTypeInfo.of(inRowType))
    val finalKeySelector = KeySelectorUtil.getRowDataSelector(
      finalAggGrouping,
      partialKeySelector.getProducedType)

    val aggFunction = new MiniBatchIncrementalGroupAggFunction(
      partialAggsHandler,
      finalAggsHandler,
      finalKeySelector,
      config.getIdleStateRetention.toMillis)

    val operator = new KeyedMapBundleOperator(
      aggFunction,
      AggregateUtil.createMiniBatchTrigger(config))

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

    // set KeyType and Selector for state
    ret.setStateKeySelector(partialKeySelector)
    ret.setStateKeyType(partialKeySelector.getProducedType)
    ret
  }

  def generateAggsHandler(
    name: String,
    aggInfoList: AggregateInfoList,
    mergedAccOffset: Int,
    mergedAccExternalTypes: Array[DataType],
    config: TableConfig,
    relBuilder: RelBuilder,
    inputFieldCopy: Boolean): GeneratedAggsHandleFunction = {

    val generator = new AggsHandlerCodeGenerator(
      CodeGeneratorContext(config),
      relBuilder,
      FlinkTypeFactory.toLogicalRowType(inputRowType).getChildren,
      inputFieldCopy)

    generator
      .needAccumulate()
      .needMerge(mergedAccOffset, mergedAccOnHeap = true, mergedAccExternalTypes)
      .generateAggsHandler(name, aggInfoList)
  }
}
