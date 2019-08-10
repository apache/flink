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

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.RelNode
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.planner.calcite.FlinkRelBuilder.PlannerNamedWindowProperty
import org.apache.flink.table.planner.codegen.agg.AggsHandlerCodeGenerator
import org.apache.flink.table.planner.plan.logical._
import org.apache.flink.table.planner.plan.utils.AggregateInfoList
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser
import org.apache.flink.table.runtime.operators.window.WindowOperator
import org.apache.flink.table.types.logical.LogicalType

/**
  * Streaming group window table aggregate physical node which will be translate to window operator.
  */
class StreamExecGroupWindowTableAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    outputRowType: RelDataType,
    inputRowType: RelDataType,
    grouping: Array[Int],
    aggCalls: Seq[AggregateCall],
    window: LogicalWindow,
    namedProperties: Seq[PlannerNamedWindowProperty],
    inputTimeFieldIndex: Int)
  extends StreamExecGroupWindowAggregateBase(
    cluster,
    traitSet,
    inputRel,
    outputRowType,
    inputRowType,
    grouping,
    aggCalls,
    window,
    namedProperties,
    inputTimeFieldIndex,
    None,
    "TableAggregate") {

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new StreamExecGroupWindowTableAggregate(
      cluster,
      traitSet,
      inputs.get(0),
      outputRowType,
      inputRowType,
      grouping,
      aggCalls,
      window,
      namedProperties,
      inputTimeFieldIndex)
  }

  override def createWindowOperator(
    config: TableConfig,
    aggCodeGenerator: AggsHandlerCodeGenerator,
    recordEqualiser: GeneratedRecordEqualiser,
    accTypes: Array[LogicalType],
    windowPropertyTypes: Array[LogicalType],
    aggValueTypes: Array[LogicalType],
    inputFields: Seq[LogicalType],
    timeIdx: Int,
    aggInfoList: AggregateInfoList): WindowOperator[_, _] = {

    // TODO: will be implement in the next commit, only for plan test.
    null
  }
}
