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

package org.apache.flink.table.plan.cost

import org.apache.calcite.plan.RelOptCost

/**
  * This class is based on Apache Calcite's `org.apache.calcite.plan.volcano.VolcanoCost#Factory`.
  */
class FlinkBatchCostFactory extends FlinkCostFactory {

  override def makeCost(
      rowCount: Double,
      cpu: Double,
      io: Double,
      network: Double,
      memory: Double): RelOptCost = {
    new FlinkBatchCost(rowCount, cpu, io, network, memory)
  }

  override def makeCost(dRows: Double, dCpu: Double, dIo: Double): RelOptCost = {
    new FlinkBatchCost(dRows, dCpu, dIo, 0.0, 0.0)
  }

  override def makeHugeCost: RelOptCost = FlinkBatchCost.Huge

  override def makeInfiniteCost: RelOptCost = FlinkBatchCost.Infinity

  override def makeTinyCost: RelOptCost = FlinkBatchCost.Tiny

  override def makeZeroCost: RelOptCost = FlinkBatchCost.Zero

}
