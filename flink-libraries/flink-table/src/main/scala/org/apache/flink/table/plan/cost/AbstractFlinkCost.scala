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
import org.apache.calcite.runtime.Utilities

/**
 * A [[RelOptCost]] that extends network cost and memory cost.
 */
abstract class AbstractFlinkCost(
    val rowCount: Double,
    val cpu: Double,
    val io: Double,
    val network: Double,
    val memory: Double)
  extends RelOptCost {

  /**
   * @return number of rows processed; this should not be confused with the
   *         row count produced by a relational expression
   *         ({ @link org.apache.calcite.rel.RelNode#estimateRowCount})
   */
  override def getRows: Double = rowCount

  /**
   * @return usage of CPU resources
   */
  override def getCpu: Double = cpu

  /**
   * @return usage of I/O resources
   */
  override def getIo: Double = io

  override def hashCode(): Int = {
    Utilities.hashCode(rowCount) +
      Utilities.hashCode(cpu) +
      Utilities.hashCode(io) +
      Utilities.hashCode(network) +
      Utilities.hashCode(memory)
  }

  override def toString: String = {
    s"{$rowCount rows, $cpu cpu, $io io, $network network, $memory memory}"
  }
}
