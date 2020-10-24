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

import org.apache.flink.table.functions.UserDefinedFunction
import org.apache.flink.table.planner.calcite.FlinkRelBuilder.PlannerNamedWindowProperty
import org.apache.flink.table.planner.plan.logical.LogicalWindow
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.tools.RelBuilder

import java.util

import scala.collection.JavaConversions._

class BatchExecLocalHashWindowAggregate(
    cluster: RelOptCluster,
    relBuilder: RelBuilder,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    outputRowType: RelDataType,
    inputRowType: RelDataType,
    grouping: Array[Int],
    auxGrouping: Array[Int],
    aggCallToAggFunction: Seq[(AggregateCall, UserDefinedFunction)],
    window: LogicalWindow,
    val inputTimeFieldIndex: Int,
    inputTimeIsDate: Boolean,
    namedProperties: Seq[PlannerNamedWindowProperty],
    enableAssignPane: Boolean = false)
  extends BatchExecHashWindowAggregateBase(
    cluster,
    relBuilder,
    traitSet,
    inputRel,
    outputRowType,
    inputRowType,
    inputRowType,
    grouping,
    auxGrouping,
    aggCallToAggFunction,
    window,
    inputTimeFieldIndex,
    inputTimeIsDate,
    namedProperties,
    enableAssignPane,
    isMerge = false,
    isFinal = false) {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new BatchExecLocalHashWindowAggregate(
      cluster,
      relBuilder,
      traitSet,
      inputs.get(0),
      getRowType,
      inputRowType,
      grouping,
      auxGrouping,
      aggCallToAggFunction,
      window,
      inputTimeFieldIndex,
      inputTimeIsDate,
      namedProperties,
      enableAssignPane)
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getInputEdges: util.List[ExecEdge] = List(ExecEdge.DEFAULT)
}
