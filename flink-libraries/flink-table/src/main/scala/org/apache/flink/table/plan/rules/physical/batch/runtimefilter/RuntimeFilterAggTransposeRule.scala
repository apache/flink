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

package org.apache.flink.table.plan.rules.physical.batch.runtimefilter

import org.apache.calcite.rex.RexNode
import org.apache.calcite.util.ImmutableBitSet
import org.apache.flink.table.functions.sql.internal.SqlRuntimeFilterFunction
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecGroupAggregateBase

/**
  * Planner rule that pushes a [[SqlRuntimeFilterFunction]]
  * past a [[BatchExecGroupAggregateBase]].
  */
class RuntimeFilterAggTransposeRule extends SingleRelRfPushDownRule(
  classOf[BatchExecGroupAggregateBase],
  "RuntimeFilterAggTransposeRule") {

  override def canPush(
      rel: BatchExecGroupAggregateBase,
      rCols: ImmutableBitSet,
      cond: RexNode): Boolean = {
    // If the filter references columns not in the group key, we cannot push
    val groupKeys = ImmutableBitSet.range(0, rel.getGrouping.length)
    groupKeys.contains(rCols)
  }

  override def getFieldAdjustments(rel: BatchExecGroupAggregateBase): Array[Int] = {
    val adjustments = new Array[Int](rel.getRowType.getFieldCount)
    var j = 0
    for (i <- rel.getGrouping) {
      adjustments(j) = i - j
      j += 1
    }
    adjustments
  }
}

object RuntimeFilterAggTransposeRule {
  val INSTANCE = new RuntimeFilterAggTransposeRule
}

