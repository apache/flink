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
package org.apache.flink.table.plan.nodes.physical.stream

import org.apache.flink.table.plan.util.{AggregateInfoList, RelExplainUtil}

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
  extends StreamExecGroupAggregateBase(cluster, traitSet, inputRel) {

  override def deriveRowType(): RelDataType = outputRowType

  override def producesUpdates = false

  override def needsUpdatesAsRetraction(input: RelNode) = true

  override def consumesRetractions = true

  override def producesRetractions: Boolean = false

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
    val inputRowType = getInput.getRowType
    super.explainTerms(pw)
      .item("finalAggGrouping", RelExplainUtil.fieldToString(finalAggGrouping, inputRowType))
      .item("partialAggGrouping",
        RelExplainUtil.fieldToString(partialAggGrouping, inputRel.getRowType))
      .item("select", RelExplainUtil.streamGroupAggregationToString(
        inputRel.getRowType,
        getRowType,
        finalAggInfoList,
        partialAggGrouping,
        shuffleKey = Some(finalAggGrouping)))
  }
}
