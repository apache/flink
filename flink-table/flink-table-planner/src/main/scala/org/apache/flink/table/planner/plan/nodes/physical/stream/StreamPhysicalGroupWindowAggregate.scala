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
import org.apache.flink.table.planner.expressions.PlannerNamedWindowProperty
import org.apache.flink.table.planner.plan.logical._
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGroupWindowAggregate
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, InputProperty}
import org.apache.flink.table.planner.plan.utils.{ChangelogPlanUtils, WindowEmitStrategy}

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall

/**
 * Streaming group window aggregate physical node which will be translate to window operator.
 *
 * Note: The differences between [[StreamPhysicalWindowAggregate]] and
 * [[StreamPhysicalGroupWindowAggregate]] is that, [[StreamPhysicalWindowAggregate]] is translated
 * from window TVF syntax, but the other is from the legacy GROUP WINDOW FUNCTION syntax.
 * In the long future, [[StreamPhysicalGroupWindowAggregate]] will be dropped.
 */
class StreamPhysicalGroupWindowAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    outputRowType: RelDataType,
    grouping: Array[Int],
    aggCalls: Seq[AggregateCall],
    window: LogicalWindow,
    namedWindowProperties: Seq[PlannerNamedWindowProperty],
    emitStrategy: WindowEmitStrategy)
  extends StreamPhysicalGroupWindowAggregateBase(
    cluster,
    traitSet,
    inputRel,
    outputRowType,
    grouping,
    aggCalls,
    window,
    namedWindowProperties,
    emitStrategy) {

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new StreamPhysicalGroupWindowAggregate(
      cluster,
      traitSet,
      inputs.get(0),
      outputRowType,
      grouping,
      aggCalls,
      window,
      namedWindowProperties,
      emitStrategy)
  }

  override def translateToExecNode(): ExecNode[_] = {
    val needRetraction = !ChangelogPlanUtils.inputInsertOnly(this)
    new StreamExecGroupWindowAggregate(
      grouping,
      aggCalls.toArray,
      window,
      namedWindowProperties.toArray,
      needRetraction,
      InputProperty.DEFAULT,
      FlinkTypeFactory.toLogicalRowType(getRowType),
      getRelDetailedDescription
    )
  }
}
