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

import org.apache.flink.runtime.state.KeyGroupRangeAssignment.DEFAULT_LOWER_BOUND_MAX_PARALLELISM
import org.apache.flink.table.plan.nodes.common.CommonPhysicalExchange
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.streaming.api.transformations.{PartitionTransformation, StreamTransformation}
import org.apache.flink.streaming.runtime.partitioner.{GlobalPartitioner, KeyGroupStreamPartitioner, StreamPartitioner}
import org.apache.flink.table.api.StreamTableEnvironment
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.util.KeySelectorUtil
import org.apache.flink.table.typeutils.BaseRowTypeInfo

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.{RelDistribution, RelNode}

import java.util

import scala.collection.JavaConversions._

/**
  * Stream physical RelNode for [[org.apache.calcite.rel.core.Exchange]].
  */
class StreamExecExchange(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    relNode: RelNode,
    relDistribution: RelDistribution)
  extends CommonPhysicalExchange(cluster, traitSet, relNode, relDistribution)
    with StreamPhysicalRel
    with StreamExecNode[BaseRow] {

  override def producesUpdates: Boolean = false

  override def needsUpdatesAsRetraction(input: RelNode): Boolean = false

  override def consumesRetractions: Boolean = false

  override def producesRetractions: Boolean = false

  override def requireWatermark: Boolean = false

  override def copy(
      traitSet: RelTraitSet,
      newInput: RelNode,
      newDistribution: RelDistribution): StreamExecExchange = {
    new StreamExecExchange(cluster, traitSet, newInput, newDistribution)
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getInputNodes: util.List[ExecNode[StreamTableEnvironment, _]] = {
    List(getInput.asInstanceOf[ExecNode[StreamTableEnvironment, _]])
  }

  override protected def translateToPlanInternal(
      tableEnv: StreamTableEnvironment): StreamTransformation[BaseRow] = {
    val inputTransform = getInputNodes.get(0).translateToPlan(tableEnv)
      .asInstanceOf[StreamTransformation[BaseRow]]
    val inputTypeInfo = inputTransform.getOutputType.asInstanceOf[BaseRowTypeInfo]
    val outputTypeInfo = FlinkTypeFactory.toInternalRowType(getRowType).toTypeInfo
    relDistribution.getType match {
      case RelDistribution.Type.SINGLETON =>
        val partitioner = new GlobalPartitioner[BaseRow]
        val transformation = new PartitionTransformation(
          inputTransform,
          partitioner.asInstanceOf[StreamPartitioner[BaseRow]])
        transformation.setOutputType(outputTypeInfo)
        transformation
      case RelDistribution.Type.HASH_DISTRIBUTED =>
        // TODO Eliminate duplicate keys

        val selector = KeySelectorUtil.getBaseRowSelector(
          relDistribution.getKeys.map(_.toInt).toArray, inputTypeInfo)
        val partitioner = new KeyGroupStreamPartitioner(selector,
          DEFAULT_LOWER_BOUND_MAX_PARALLELISM)
        val transformation = new PartitionTransformation(
          inputTransform,
          partitioner.asInstanceOf[StreamPartitioner[BaseRow]])
        transformation.setOutputType(outputTypeInfo)
        transformation
      case _ =>
        throw new UnsupportedOperationException(
          s"not support RelDistribution: ${relDistribution.getType} now!")
    }
  }

}
