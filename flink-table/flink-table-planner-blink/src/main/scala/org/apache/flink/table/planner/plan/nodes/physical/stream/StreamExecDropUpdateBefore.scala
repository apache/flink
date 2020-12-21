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

import org.apache.flink.api.dag.Transformation
import org.apache.flink.streaming.api.operators.StreamFilter
import org.apache.flink.streaming.api.transformations.OneInputTransformation
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.planner.plan.nodes.exec.LegacyStreamExecNode
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils
import org.apache.flink.table.runtime.operators.misc.DropUpdateBeforeFunction
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelNode, SingleRel}

import java.util

/**
 * Stream physical RelNode which will drop the UPDATE_BEFORE messages.
 * This is usually used as an optimization for the downstream operators that doesn't need
 * the UPDATE_BEFORE messages, but the upstream operator can't drop it by itself (e.g. the source).
 */
class StreamExecDropUpdateBefore(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode)
  extends SingleRel(cluster, traitSet, input)
  with StreamPhysicalRel
  with LegacyStreamExecNode[RowData] {

  override def requireWatermark: Boolean = false

  override def deriveRowType(): RelDataType = getInput.getRowType

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new StreamExecDropUpdateBefore(
      cluster,
      traitSet,
      inputs.get(0))
  }

  //~ ExecNode methods -----------------------------------------------------------

  override protected def translateToPlanInternal(
      planner: StreamPlanner): Transformation[RowData] = {

    // sanity check
    if (ChangelogPlanUtils.generateUpdateBefore(this)) {
      throw new IllegalStateException(s"${this.getClass.getSimpleName} is required to emit " +
        s"UPDATE_BEFORE messages. This should never happen." )
    }

    val inputTransform = getInputNodes.get(0).translateToPlan(planner)
      .asInstanceOf[Transformation[RowData]]
    val rowTypeInfo = inputTransform.getOutputType.asInstanceOf[InternalTypeInfo[RowData]]
    val operator = new StreamFilter[RowData](new DropUpdateBeforeFunction)

    new OneInputTransformation(
      inputTransform,
      getRelDetailedDescription,
      operator,
      rowTypeInfo,
      inputTransform.getParallelism)
  }
}
