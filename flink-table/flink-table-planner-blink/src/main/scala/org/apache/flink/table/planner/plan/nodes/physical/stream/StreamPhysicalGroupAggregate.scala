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
import org.apache.flink.table.planner.plan.PartialFinalType
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGroupAggregate
import org.apache.flink.table.planner.plan.nodes.exec.{InputProperty, ExecNode}
import org.apache.flink.table.planner.plan.utils.{AggregateUtil, ChangelogPlanUtils, RelExplainUtil}

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.{RelNode, RelWriter}

import java.util

/**
 * Stream physical RelNode for unbounded group aggregate.
 *
 * This node does support un-splittable aggregate function (e.g. STDDEV_POP).
 *
 * @see [[StreamPhysicalGroupAggregateBase]] for more info.
 */
class StreamPhysicalGroupAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    outputRowType: RelDataType,
    val grouping: Array[Int],
    val aggCalls: Seq[AggregateCall],
    var partialFinalType: PartialFinalType = PartialFinalType.NONE)
  extends StreamPhysicalGroupAggregateBase(cluster, traitSet, inputRel) {

  private val aggInfoList =
    AggregateUtil.deriveAggregateInfoList(this, grouping.length, aggCalls)

  override def requireWatermark: Boolean = false

  override def deriveRowType(): RelDataType = outputRowType

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new StreamPhysicalGroupAggregate(
      cluster,
      traitSet,
      inputs.get(0),
      outputRowType,
      grouping,
      aggCalls,
      partialFinalType)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val inputRowType = getInput.getRowType
    super.explainTerms(pw)
      .itemIf("groupBy",
        RelExplainUtil.fieldToString(grouping, inputRowType), grouping.nonEmpty)
      .itemIf("partialFinalType", partialFinalType, partialFinalType != PartialFinalType.NONE)
      .item("select", RelExplainUtil.streamGroupAggregationToString(
        inputRowType,
        getRowType,
        aggInfoList,
        grouping))
  }

  override def translateToExecNode(): ExecNode[_] = {
    val aggCallNeedRetractions =
      AggregateUtil.deriveAggCallNeedRetractions(this, grouping.length, aggCalls)
    val generateUpdateBefore = ChangelogPlanUtils.generateUpdateBefore(this)
    val needRetraction = !ChangelogPlanUtils.inputInsertOnly(this)
    new StreamExecGroupAggregate(
      grouping,
      aggCalls.toArray,
      aggCallNeedRetractions,
      generateUpdateBefore,
      needRetraction,
      InputProperty.DEFAULT,
      FlinkTypeFactory.toLogicalRowType(getRowType),
      getRelDetailedDescription
    )
  }
}
