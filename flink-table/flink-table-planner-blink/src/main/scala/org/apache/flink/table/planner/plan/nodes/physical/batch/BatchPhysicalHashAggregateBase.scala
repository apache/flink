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
import org.apache.flink.table.planner.plan.cost.FlinkCost._
import org.apache.flink.table.planner.plan.cost.FlinkCostFactory
import org.apache.flink.table.planner.plan.utils.FlinkRelMdUtil

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.util.Util

/**
  * Batch physical RelNode for hash-based aggregate operator.
  *
  * @see [[BatchPhysicalGroupAggregateBase]] for more info.
  */
abstract class BatchPhysicalHashAggregateBase(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    outputRowType: RelDataType,
    grouping: Array[Int],
    auxGrouping: Array[Int],
    aggCallToAggFunction: Seq[(AggregateCall, UserDefinedFunction)],
    isMerge: Boolean,
    isFinal: Boolean)
  extends BatchPhysicalGroupAggregateBase(
    cluster,
    traitSet,
    inputRel,
    outputRowType,
    grouping,
    auxGrouping,
    aggCallToAggFunction,
    isMerge,
    isFinal) {

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val numOfGroupKey = grouping.length
    val inputRowCnt = mq.getRowCount(getInput())
    if (inputRowCnt == null) {
      return null
    }
    val hashCpuCost = HASH_CPU_COST * inputRowCnt * numOfGroupKey
    val aggFunctionCpuCost = FUNC_CPU_COST * inputRowCnt * aggCallToAggFunction.size
    val memCost: Double = if (numOfGroupKey == 0) {
      mq.getAverageRowSize(this)
    } else {
      // assume memory is enough to hold hashTable to simplify the estimation because spill will not
      // happen under the assumption
      val ndvOfGroupKey = Util.first(mq.getRowCount(this), 1d)
      //  We aim for a 200% utilization of the bucket table.
      // TODO use BytesHashMap.BUCKET_SIZE instead of 16
      val bucketSize = ndvOfGroupKey * 16 / HASH_COLLISION_WEIGHT
      // TODO use BytesHashMap.RECORD_EXTRA_LENGTH instead of 8
      val recordSize = ndvOfGroupKey * (FlinkRelMdUtil.binaryRowAverageSize(this) + 8)
      bucketSize + recordSize
    }
    val cpuCost = hashCpuCost + aggFunctionCpuCost
    val rowCount = mq.getRowCount(this)
    val costFactory = planner.getCostFactory.asInstanceOf[FlinkCostFactory]
    costFactory.makeCost(rowCount, cpuCost, 0, 0, memCost)
  }
}
