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

  // The ratio to convert memory cost into CPU cost.
  val MEMORY_TO_CPU_RATIO = 1.0
  // The ratio to convert io cost into CPU cost.
  val IO_TO_CPU_RATIO = 2.0
  // The ratio to convert network cost into CPU cost.
  val NETWORK_TO_CPU_RATIO = 4.0

  /**
    * @return number of rows processed; this should not be confused with the
    *         row count produced by a relational expression
    *         ([[org.apache.calcite.rel.RelNode#estimateRowCount]])
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

  /**
    * @return usage of network resources
    */
  def getNetwork: Double = network

  /**
    * @return usage of memory resources
    */
  def getMemory: Double = memory

  /**
    * Returns true if any factor is infinity, else false.
    */
  protected def hasInfinity: Boolean = {
    (this.rowCount == Double.PositiveInfinity) ||
      (this.cpu == Double.PositiveInfinity) ||
      (this.io == Double.PositiveInfinity) ||
      (this.network == Double.PositiveInfinity) ||
      (this.memory == Double.PositiveInfinity)
  }

  /**
    * Returns true iff each factor is exactly equal to
    * other cost's corresponding factor, else false.
    */
  protected def isEqualTo(other: AbstractFlinkCost): Boolean = {
    (this eq other) ||
      (this.rowCount == other.rowCount) &&
        (this.cpu == other.cpu) &&
        (this.io == other.io) &&
        (this.network == other.network) &&
        (this.memory == other.memory)
  }

  /**
    * Returns true if each factor is approximately equal to (allowing for slight roundoff errors)
    * other cost's corresponding factor, else false.
    */
  protected def isEqualToWithEpsilon(other: AbstractFlinkCost, epsilon: Double): Boolean = {
    (this eq other) ||
      ((Math.abs(this.rowCount - other.rowCount) < epsilon) &&
        (Math.abs(this.cpu - other.cpu) < epsilon) &&
        (Math.abs(this.io - other.io) < epsilon) &&
        (Math.abs(this.network - other.network) < epsilon) &&
        (Math.abs(this.memory - other.memory) < epsilon))
  }

  /**
    * Returns true iff this is less than or equal to other cost.
    *
    * <p>NOTES:
    * The optimization goal is to use minimal resources now, so the comparison order of factors is:
    * 1. first compare CPU. Each operator will use CPU, so we think it's the most important factor.
    * 2.then compare MEMORY, NETWORK and IO as a normalized value. Comparison order of them is
    * not easy to decide, so convert them to CPU cost by different ratio.
    * 3.finally compare ROWS. ROWS has been counted when calculating other factory.
    * e.g. CPU of Sort = nLogN(ROWS) * number of sort keys,
    * CPU of Filter = ROWS * condition cost on a row.
    */
  protected def isLessThanOrEqualTo(other: AbstractFlinkCost): Boolean = {
    if (this eq other) {
      return true
    }

    val thisNormCost = normalizeCost(this.memory, this.network, this.io)
    val otherNormCost = normalizeCost(other.memory, other.network, other.io)
    (this.cpu < other.cpu) ||
      (this.cpu == other.cpu && thisNormCost < otherNormCost) ||
      (this.cpu == other.cpu && thisNormCost == otherNormCost && this.rowCount <= other.rowCount)
  }

  private def normalizeCost(memory: Double, network: Double, io: Double): Double = {
    memory * MEMORY_TO_CPU_RATIO + network * NETWORK_TO_CPU_RATIO + io * IO_TO_CPU_RATIO
  }

  /**
    * Compares this to another cost.
    *
    * @param other another cost
    * @return true iff this is strictly less than other cost
    */
  override def isLt(other: RelOptCost): Boolean = isLe(other) && !this.equals(other)

  /**
    * Compute the geometric average of the ratios of all of the factors
    * which are non-zero and finite.
    */
  protected def computeRatio(other: AbstractFlinkCost): Double = {
    var d: Double = 1.0
    var n: Double = 0.0
    if ((this.rowCount != 0) && !this.rowCount.isInfinite &&
      (other.rowCount != 0) && !other.rowCount.isInfinite) {
      d *= this.rowCount / other.rowCount
      n += 1
    }
    if ((this.cpu != 0) && !this.cpu.isInfinite && (other.cpu != 0) && !other.cpu.isInfinite) {
      d *= this.cpu / other.cpu
      n += 1
    }
    if ((this.io != 0) && !this.io.isInfinite && (other.io != 0) && !other.io.isInfinite) {
      d *= this.io / other.io
      n += 1
    }
    if ((this.network != 0) && !this.network.isInfinite &&
      (other.network != 0) && !other.network.isInfinite) {
      d *= this.network / other.network
      n += 1
    }
    if ((this.memory != 0) && !this.memory.isInfinite &&
      (other.memory != 0) && !other.memory.isInfinite) {
      d *= this.memory / other.memory
      n += 1
    }
    if (n == 0) {
      return 1.0
    }
    Math.pow(d, 1 / n)
  }

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
