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
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.expressions.PlannerNamedWindowProperty
import org.apache.flink.table.planner.plan.cost.{FlinkCost, FlinkCostFactory}
import org.apache.flink.table.planner.plan.logical.LogicalWindow
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecPythonGroupWindowAggregate
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, InputProperty}

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.metadata.RelMetadataQuery

import java.util

/**
 * Batch physical RelNode for group widow aggregate (Python user defined aggregate function).
 */
class BatchPhysicalPythonGroupWindowAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    outputRowType: RelDataType,
    inputRowType: RelDataType,
    grouping: Array[Int],
    auxGrouping: Array[Int],
    aggCalls: Seq[AggregateCall],
    aggFunctions: Array[UserDefinedFunction],
    window: LogicalWindow,
    inputTimeFieldIndex: Int,
    inputTimeIsDate: Boolean,
    namedWindowProperties: Seq[PlannerNamedWindowProperty])
  extends BatchPhysicalWindowAggregateBase(
    cluster,
    traitSet,
    inputRel,
    outputRowType,
    grouping,
    auxGrouping,
    aggCalls.zip(aggFunctions),
    window,
    namedWindowProperties,
    false,
    false,
    true) {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new BatchPhysicalPythonGroupWindowAggregate(
      cluster,
      traitSet,
      inputs.get(0),
      outputRowType,
      inputRowType,
      grouping,
      auxGrouping,
      aggCalls,
      aggFunctions,
      window,
      inputTimeFieldIndex,
      inputTimeIsDate,
      namedWindowProperties)
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val inputRowCnt = mq.getRowCount(getInput)
    if (inputRowCnt == null) {
      return null
    }
    // does not take pane optimization into consideration here
    // sort is not done here
    val aggCallToAggFunction = aggCalls.zip(aggFunctions)
    val cpu = FlinkCost.FUNC_CPU_COST * inputRowCnt * aggCallToAggFunction.size
    val averageRowSize: Double = mq.getAverageRowSize(this)
    val memCost = averageRowSize
    val costFactory = planner.getCostFactory.asInstanceOf[FlinkCostFactory]
    costFactory.makeCost(mq.getRowCount(this), cpu, 0, 0, memCost)
  }

  override def translateToExecNode(): ExecNode[_] = {
    new BatchExecPythonGroupWindowAggregate(
      grouping,
      grouping ++ auxGrouping,
      aggCalls.toArray,
      window,
      inputTimeFieldIndex,
      namedWindowProperties.toArray,
      InputProperty.DEFAULT,
      FlinkTypeFactory.toLogicalRowType(getRowType),
      getRelDetailedDescription
    )
  }
}
