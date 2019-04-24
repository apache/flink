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

package org.apache.flink.table.plan.nodes.physical.batch

import org.apache.flink.runtime.operators.DamBehavior
import org.apache.flink.table.calcite.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.functions.UserDefinedFunction
import org.apache.flink.table.plan.logical.LogicalWindow

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.tools.RelBuilder

import java.util

class BatchExecHashWindowAggregate(
    window: LogicalWindow,
    inputTimeFieldIndex: Int,
    inputTimeIsDate: Boolean,
    namedProperties: Seq[NamedWindowProperty],
    cluster: RelOptCluster,
    relBuilder: RelBuilder,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    aggCallToAggFunction: Seq[(AggregateCall, UserDefinedFunction)],
    outputRowType: RelDataType,
    inputRowType: RelDataType,
    aggInputRowType: RelDataType,
    grouping: Array[Int],
    auxGrouping: Array[Int],
    enableAssignPane: Boolean = false,
    isMerge: Boolean)
  extends BatchExecHashWindowAggregateBase(
    window,
    inputTimeFieldIndex,
    inputTimeIsDate,
    namedProperties,
    cluster,
    relBuilder,
    traitSet,
    inputNode,
    aggCallToAggFunction,
    outputRowType,
    inputRowType,
    aggInputRowType,
    grouping,
    auxGrouping,
    enableAssignPane,
    isMerge,
    isFinal = true) {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new BatchExecHashWindowAggregate(
      window,
      inputTimeFieldIndex,
      inputTimeIsDate,
      namedProperties,
      cluster,
      relBuilder,
      traitSet,
      inputs.get(0),
      aggCallToAggFunction,
      getRowType,
      inputRowType,
      aggInputRowType,
      grouping,
      auxGrouping,
      enableAssignPane,
      isMerge)
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getDamBehavior: DamBehavior = DamBehavior.FULL_DAM

  override def getOperatorName: String = {
    val aggregateNamePrefix = if (isMerge) "Global" else "Complete"
    aggregateNamePrefix + "WindowHashAggregateBatchExec"
  }
}
