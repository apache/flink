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
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecPythonGroupTableAggregate
import org.apache.flink.table.planner.plan.nodes.exec.{InputProperty, ExecNode}
import org.apache.flink.table.planner.plan.utils.{AggregateUtil, ChangelogPlanUtils}

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall

import java.util

/**
 * Stream physical RelNode for unbounded python group table aggregate.
 */
class StreamPhysicalPythonGroupTableAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    outputRowType: RelDataType,
    grouping: Array[Int],
    aggCalls: Seq[AggregateCall])
  extends StreamPhysicalGroupTableAggregateBase(
    cluster,
    traitSet,
    inputRel,
    outputRowType,
    grouping,
    aggCalls) {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new StreamPhysicalPythonGroupTableAggregate(
      cluster,
      traitSet,
      inputs.get(0),
      outputRowType,
      grouping,
      aggCalls)
  }

  override def translateToExecNode(): ExecNode[_] = {
    val aggCallNeedRetractions =
      AggregateUtil.deriveAggCallNeedRetractions(this, grouping.length, aggCalls)
    val generateUpdateBefore = ChangelogPlanUtils.generateUpdateBefore(this)
    val needRetraction = !ChangelogPlanUtils.inputInsertOnly(this)
    new StreamExecPythonGroupTableAggregate(
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
