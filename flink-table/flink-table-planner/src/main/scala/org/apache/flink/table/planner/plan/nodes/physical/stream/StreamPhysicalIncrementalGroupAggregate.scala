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

import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecIncrementalGroupAggregate
import org.apache.flink.table.planner.plan.nodes.exec.{InputProperty, ExecNode}
import org.apache.flink.table.planner.plan.utils._

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.{RelNode, RelWriter}

import java.util

/**
 * Stream physical RelNode for unbounded incremental group aggregate.
 *
 * <p>Considering the following sub-plan:
 * {{{
 *   StreamPhysicalGlobalGroupAggregate (final-global-aggregate)
 *   +- StreamPhysicalExchange
 *      +- StreamPhysicalLocalGroupAggregate (final-local-aggregate)
 *         +- StreamPhysicalGlobalGroupAggregate (partial-global-aggregate)
 *            +- StreamPhysicalExchange
 *               +- StreamPhysicalLocalGroupAggregate (partial-local-aggregate)
 * }}}
 *
 * partial-global-aggregate and final-local-aggregate can be combined as
 * this node to share [[org.apache.flink.api.common.state.State]].
 * now the sub-plan is
 * {{{
 *   StreamPhysicalGlobalGroupAggregate (final-global-aggregate)
 *   +- StreamPhysicalExchange
 *      +- StreamPhysicalIncrementalGroupAggregate
 *         +- StreamPhysicalExchange
 *            +- StreamPhysicalLocalGroupAggregate (partial-local-aggregate)
 * }}}
 *
 * @see [[StreamPhysicalGroupAggregateBase]] for more info.
 */
class StreamPhysicalIncrementalGroupAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    val partialAggGrouping: Array[Int],
    val partialAggCalls: Array[AggregateCall],
    val finalAggGrouping: Array[Int],
    val finalAggCalls: Array[AggregateCall],
    partialOriginalAggCalls: Array[AggregateCall],
    partialAggCallNeedRetractions: Array[Boolean],
    partialAggNeedRetraction: Boolean,
    partialLocalAggInputRowType: RelDataType,
    partialGlobalAggRowType: RelDataType)
  extends StreamPhysicalGroupAggregateBase(cluster, traitSet, inputRel) {

  private lazy val incrementalAggInfo = AggregateUtil.createIncrementalAggInfoList(
    FlinkTypeFactory.toLogicalRowType(partialLocalAggInputRowType),
    partialOriginalAggCalls,
    partialAggCallNeedRetractions,
    partialAggNeedRetraction)

  override def deriveRowType(): RelDataType = {
    AggregateUtil.inferLocalAggRowType(
      incrementalAggInfo,
      partialGlobalAggRowType,
      finalAggGrouping,
      getCluster.getTypeFactory.asInstanceOf[FlinkTypeFactory])
  }

  override def requireWatermark: Boolean = false

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new StreamPhysicalIncrementalGroupAggregate(
      cluster,
      traitSet,
      inputs.get(0),
      partialAggGrouping,
      partialAggCalls,
      finalAggGrouping,
      finalAggCalls,
      partialOriginalAggCalls,
      partialAggCallNeedRetractions,
      partialAggNeedRetraction,
      partialLocalAggInputRowType,
      partialGlobalAggRowType)
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
        incrementalAggInfo,
        finalAggGrouping,
        shuffleKey = Some(partialAggGrouping)))
  }

  override def translateToExecNode(): ExecNode[_] = {
    new StreamExecIncrementalGroupAggregate(
      partialAggGrouping,
      finalAggGrouping,
      partialOriginalAggCalls,
      partialAggCallNeedRetractions,
      FlinkTypeFactory.toLogicalRowType(partialLocalAggInputRowType),
      partialAggNeedRetraction,
      InputProperty.DEFAULT,
      FlinkTypeFactory.toLogicalRowType(getRowType),
      getRelDetailedDescription
    )
  }
}
