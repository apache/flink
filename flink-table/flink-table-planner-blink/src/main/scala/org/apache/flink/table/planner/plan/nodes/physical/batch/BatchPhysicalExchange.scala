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

package org.apache.flink.table.planner.plan.nodes.physical.batch

import org.apache.flink.streaming.api.graph.GlobalDataExchangeMode
import org.apache.flink.streaming.api.transformations.ShuffleMode
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecExchange
import org.apache.flink.table.planner.plan.nodes.exec.{ExecEdge, ExecNode}
import org.apache.flink.table.planner.plan.nodes.physical.common.CommonPhysicalExchange
import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.{RelDistribution, RelNode}

/**
 * This RelNode represents a change of partitioning of the input elements for batch.
 */
class BatchPhysicalExchange(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    relDistribution: RelDistribution)
  extends CommonPhysicalExchange(cluster, traitSet, inputRel, relDistribution)
  with BatchPhysicalRel {

  override def copy(
      traitSet: RelTraitSet,
      newInput: RelNode,
      newDistribution: RelDistribution): BatchPhysicalExchange = {
    new BatchPhysicalExchange(cluster, traitSet, newInput, relDistribution)
  }

  override def translateToExecNode(): ExecNode[_] = {
    new BatchExecExchange(
      getExecEdge,
      FlinkTypeFactory.toLogicalRowType(getRowType),
      getRelDetailedDescription)
  }

  private def getExecEdge: ExecEdge = {
    if (distribution.getType == RelDistribution.Type.RANGE_DISTRIBUTED) {
      throw new UnsupportedOperationException("Range sort is not supported.")
    }

    val damBehavior = if (getShuffleMode eq ShuffleMode.BATCH) {
      ExecEdge.DamBehavior.BLOCKING
    } else {
      ExecEdge.DamBehavior.PIPELINED
    }

    ExecEdge.builder.
      requiredShuffle(getRequiredShuffle)
      .damBehavior(damBehavior)
      .build
  }

  private def getShuffleMode: ShuffleMode = {
    val tableConfig = FlinkRelOptUtil.getTableConfigFromContext(this)
    if (tableConfig.getConfiguration.getString(ExecutionConfigOptions.TABLE_EXEC_SHUFFLE_MODE)
      .equalsIgnoreCase(GlobalDataExchangeMode.ALL_EDGES_BLOCKING.toString)) {
      ShuffleMode.BATCH
    } else {
      ShuffleMode.UNDEFINED
    }
  }
}
