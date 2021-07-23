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
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGlobalGroupAggregate
import org.apache.flink.table.planner.plan.nodes.exec.{InputProperty, ExecNode}
import org.apache.flink.table.planner.plan.utils._

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.{RelNode, RelWriter}

/**
 * Stream physical RelNode for unbounded global group aggregate.
 *
 * @see [[StreamPhysicalGroupAggregateBase]] for more info.
 */
class StreamPhysicalGlobalGroupAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    outputRowType: RelDataType,
    val grouping: Array[Int],
    val aggCalls: Seq[AggregateCall],
    val aggCallNeedRetractions: Array[Boolean],
    val localAggInputRowType: RelDataType,
    val needRetraction: Boolean,
    val partialFinalType: PartialFinalType)
  extends StreamPhysicalGroupAggregateBase(cluster, traitSet, inputRel) {

  lazy val localAggInfoList: AggregateInfoList = AggregateUtil.transformToStreamAggregateInfoList(
    FlinkTypeFactory.toLogicalRowType(localAggInputRowType),
    aggCalls,
    aggCallNeedRetractions,
    needRetraction,
    isStateBackendDataViews = false)

  lazy val globalAggInfoList: AggregateInfoList = AggregateUtil.transformToStreamAggregateInfoList(
    FlinkTypeFactory.toLogicalRowType(localAggInputRowType),
    aggCalls,
    aggCallNeedRetractions,
    needRetraction,
    isStateBackendDataViews = true)

  override def requireWatermark: Boolean = false

  override def deriveRowType(): RelDataType = outputRowType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new StreamPhysicalGlobalGroupAggregate(
      cluster,
      traitSet,
      inputs.get(0),
      outputRowType,
      grouping,
      aggCalls,
      aggCallNeedRetractions,
      localAggInputRowType,
      needRetraction,
      partialFinalType)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .itemIf("groupBy",
        RelExplainUtil.fieldToString(grouping, inputRel.getRowType), grouping.nonEmpty)
      .itemIf("partialFinalType", partialFinalType, partialFinalType != PartialFinalType.NONE)
      .item("select", RelExplainUtil.streamGroupAggregationToString(
        inputRel.getRowType,
        getRowType,
        globalAggInfoList,
        grouping,
        isGlobal = true))
  }

  override def translateToExecNode(): ExecNode[_] = {
    val generateUpdateBefore = ChangelogPlanUtils.generateUpdateBefore(this)
    new StreamExecGlobalGroupAggregate(
      grouping,
      aggCalls.toArray,
      aggCallNeedRetractions,
      FlinkTypeFactory.toLogicalRowType(localAggInputRowType),
      generateUpdateBefore,
      needRetraction,
      InputProperty.DEFAULT,
      FlinkTypeFactory.toLogicalRowType(getRowType),
      getRelDetailedDescription
    )
  }
}
