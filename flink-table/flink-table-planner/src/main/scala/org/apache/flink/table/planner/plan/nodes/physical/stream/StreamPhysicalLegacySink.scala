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

import org.apache.flink.table.planner.plan.nodes.calcite.LegacySink
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, InputProperty}
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLegacySink
import org.apache.flink.table.planner.plan.utils.{ChangelogPlanUtils, UpdatingPlanChecker}
import org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType
import org.apache.flink.table.sinks._

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.hint.RelHint

import java.util

/**
 * Stream physical RelNode to write data into an external sink defined by a [[TableSink]].
 *
 * @tparam T
 *   The return type of the [[TableSink]].
 */
class StreamPhysicalLegacySink[T](
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    hints: util.List[RelHint],
    sink: TableSink[T],
    sinkName: String)
  extends LegacySink(cluster, traitSet, inputRel, hints, sink, sinkName)
  with StreamPhysicalRel {

  override def requireWatermark: Boolean = false

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new StreamPhysicalLegacySink(cluster, traitSet, inputs.get(0), hints, sink, sinkName)
  }

  override def translateToExecNode(): ExecNode[_] = {
    val upsertKeys = sink match {
      case upsertSink: UpsertStreamTableSink[T] =>
        UpdatingPlanChecker.getUniqueKeyForUpsertSink(this, upsertSink)
      case _ => Option.empty[Array[String]]
    }
    val needRetraction = !ChangelogPlanUtils.inputInsertOnly(this)

    new StreamExecLegacySink(
      unwrapTableConfig(this),
      sink,
      upsertKeys.orNull,
      needRetraction,
      InputProperty.DEFAULT,
      fromDataTypeToLogicalType(sink.getConsumedDataType),
      getRelDetailedDescription)
  }
}
