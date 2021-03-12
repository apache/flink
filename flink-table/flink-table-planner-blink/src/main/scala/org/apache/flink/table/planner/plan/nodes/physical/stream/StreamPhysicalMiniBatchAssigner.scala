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
import org.apache.flink.table.planner.plan.`trait`.MiniBatchIntervalTraitDef
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecMiniBatchAssigner
import org.apache.flink.table.planner.plan.nodes.exec.{InputProperty, ExecNode}

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}

import java.util

/**
 * Stream physical RelNode for injecting a mini-batch event in the streaming data. The mini-batch
 * event will be recognized as a boundary between two mini-batches. The following operators will
 * keep track of the mini-batch events and trigger mini-batch once the mini-batch id is advanced.
 *
 * NOTE: currently, we leverage the runtime watermark mechanism to achieve the mini-batch, because
 * runtime doesn't support customized events and the watermark mechanism fully meets mini-batch
 * needs.
 */
class StreamPhysicalMiniBatchAssigner(
    cluster: RelOptCluster,
    traits: RelTraitSet,
    inputRel: RelNode)
  extends SingleRel(cluster, traits, inputRel)
  with StreamPhysicalRel {

  override def requireWatermark: Boolean = false

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new StreamPhysicalMiniBatchAssigner(
      cluster,
      traitSet,
      inputs.get(0))
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val miniBatchInterval = traits.getTrait(MiniBatchIntervalTraitDef.INSTANCE).getMiniBatchInterval
    super.explainTerms(pw)
      .item("interval", miniBatchInterval.getInterval + "ms")
      .item("mode", miniBatchInterval.getMode.toString)
  }

  override def translateToExecNode(): ExecNode[_] = {
    val miniBatchInterval = traits.getTrait(MiniBatchIntervalTraitDef.INSTANCE).getMiniBatchInterval
    new StreamExecMiniBatchAssigner(
      miniBatchInterval,
      InputProperty.DEFAULT,
      FlinkTypeFactory.toLogicalRowType(getRowType),
      getRelDetailedDescription
    )
  }
}
