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
import org.apache.flink.table.planner.expressions.PlannerNamedWindowProperty
import org.apache.flink.table.planner.plan.cost.{FlinkCost, FlinkCostFactory}
import org.apache.flink.table.planner.plan.logical.LogicalWindow
import org.apache.flink.table.planner.plan.utils.FlinkRelMdUtil
import org.apache.flink.table.runtime.util.collections.binary.BytesMap

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.metadata.RelMetadataQuery

/**
 * Batch physical RelNode for hash-based window aggregate.
 */
abstract class BatchPhysicalHashWindowAggregateBase(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    outputRowType: RelDataType,
    grouping: Array[Int],
    auxGrouping: Array[Int],
    aggCallToAggFunction: Seq[(AggregateCall, UserDefinedFunction)],
    window: LogicalWindow,
    namedWindowProperties: Seq[PlannerNamedWindowProperty],
    enableAssignPane: Boolean = false,
    isMerge: Boolean,
    isFinal: Boolean)
  extends BatchPhysicalWindowAggregateBase(
    cluster,
    traitSet,
    inputRel,
    outputRowType,
    grouping,
    auxGrouping,
    aggCallToAggFunction,
    window,
    namedWindowProperties,
    enableAssignPane,
    isMerge,
    isFinal) {

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val numOfGroupKey = grouping.length
    val inputRowCnt = mq.getRowCount(getInput())
    if (inputRowCnt == null) {
      return null
    }
    // does not take pane optimization into consideration here
    // calculate hash code of groupKeys + timestamp
    val hashCpuCost = FlinkCost.HASH_CPU_COST * inputRowCnt * (numOfGroupKey + 1)
    val aggFunctionCpuCost = FlinkCost.FUNC_CPU_COST * inputRowCnt * aggCallToAggFunction.size
    val rowCnt = mq.getRowCount(this)
    // assume memory is enough to hold hashTable to simplify the estimation because spill will not
    // happen under the assumption
    //  We aim for a 200% utilization of the bucket table.
    val bucketSize = rowCnt * BytesMap.BUCKET_SIZE / FlinkCost.HASH_COLLISION_WEIGHT
    val rowAvgSize = FlinkRelMdUtil.binaryRowAverageSize(this)
    val recordSize = rowCnt * (rowAvgSize + BytesMap.RECORD_EXTRA_LENGTH)
    val memCost = bucketSize + recordSize
    val costFactory = planner.getCostFactory.asInstanceOf[FlinkCostFactory]
    costFactory.makeCost(rowCnt, hashCpuCost + aggFunctionCpuCost, 0, 0, memCost)
  }
}
