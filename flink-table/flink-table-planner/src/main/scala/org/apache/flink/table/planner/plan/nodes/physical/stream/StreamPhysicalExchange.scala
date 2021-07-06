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
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecExchange
import org.apache.flink.table.planner.plan.nodes.exec.{InputProperty, ExecNode}
import org.apache.flink.table.planner.plan.nodes.physical.common.CommonPhysicalExchange

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.{RelDistribution, RelNode}

/**
 * Stream physical RelNode for [[org.apache.calcite.rel.core.Exchange]].
 */
class StreamPhysicalExchange(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    relNode: RelNode,
    relDistribution: RelDistribution)
  extends CommonPhysicalExchange(cluster, traitSet, relNode, relDistribution)
  with StreamPhysicalRel {

  override def requireWatermark: Boolean = false

  override def copy(
      traitSet: RelTraitSet,
      newInput: RelNode,
      newDistribution: RelDistribution): StreamPhysicalExchange = {
    new StreamPhysicalExchange(cluster, traitSet, newInput, newDistribution)
  }

  override def translateToExecNode(): ExecNode[_] = {
    new StreamExecExchange(
      InputProperty.builder.requiredDistribution(getRequiredDistribution).build,
      FlinkTypeFactory.toLogicalRowType(getRowType),
      getRelDetailedDescription)
  }
}
