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

package org.apache.flink.table.planner.plan.cost

import org.apache.calcite.plan.{RelOptCost, RelOptUtil}
import org.apache.calcite.runtime.Utilities

/**
  * This class is based on Apache Calcite's [[org.apache.calcite.plan.volcano.VolcanoCost]]
  * and has an adapted cost comparison method `isLe(other: RelOptCost)`
  * that takes io, cpu, network and memory into account.
  */
class FlinkCost(
    val rowCount: Double,
    val cpu: Double,
    val io: Double,
    val network: Double,
    val memory: Double)
  extends FlinkCostBase {

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

  /**
    * @return usage of network resources
    */
  override def getNetwork: Double = network

  /**
    * @return usage of memory resources
    */
  override def getMemory: Double = memory

  /**
    * @return true iff this cost represents an expression that hasn't actually
    *         been implemented (e.g. a pure relational algebra expression) or can't
    *         actually be implemented, e.g. a transfer of data between two disconnected
    *         sites
    */
  override def isInfinite: Boolean = {
    (this eq FlinkCost.Infinity) ||
      (this.rowCount == Double.PositiveInfinity) ||
      (this.cpu == Double.PositiveInfinity) ||
      (this.io == Double.PositiveInfinity) ||
      (this.network == Double.PositiveInfinity) ||
      (this.memory == Double.PositiveInfinity)
  }

  /**
    * Compares this to another cost.
    *
    * @param other another cost
    * @return true iff this is exactly equal to other cost
    */
  override def equals(other: RelOptCost): Boolean = {
    (this eq other) ||
      other.isInstanceOf[FlinkCost] &&
        (this.rowCount == other.asInstanceOf[FlinkCost].rowCount) &&
        (this.cpu == other.asInstanceOf[FlinkCost].cpu) &&
        (this.io == other.asInstanceOf[FlinkCost].io) &&
        (this.network == other.asInstanceOf[FlinkCost].network) &&
        (this.memory == other.asInstanceOf[FlinkCost].memory)
  }

  /**
    * Compares this to another cost, allowing for slight roundoff errors.
    *
    * @param other another cost
    * @return true iff this is the same as the other cost within a roundoff
    *         margin of error
    */
  override def isEqWithEpsilon(other: RelOptCost): Boolean = {
    if (!other.isInstanceOf[FlinkCost]) {
      return false
    }
    val that: FlinkCost = other.asInstanceOf[FlinkCost]
    (this eq that) ||
      ((Math.abs(this.rowCount - that.rowCount) < RelOptUtil.EPSILON) &&
        (Math.abs(this.cpu - that.cpu) < RelOptUtil.EPSILON) &&
        (Math.abs(this.io - that.io) < RelOptUtil.EPSILON) &&
        (Math.abs(this.network - that.network) < RelOptUtil.EPSILON) &&
        (Math.abs(this.memory - that.memory) < RelOptUtil.EPSILON))
  }

  /**
    * Compares this to another cost.
    *
    * <p>NOTES:
    * * The optimization goal is to use minimal resources now, so the comparison order of factors
    * is:
    * * 1. first compare CPU. Each operator will use CPU, so we think it's the most important
    * factor.
    * * 2.then compare MEMORY, NETWORK and IO as a normalized value. Comparison order of them is
    * * not easy to decide, so convert them to CPU cost by different ratio.
    * * 3.finally compare ROWS. ROWS has been counted when calculating other factory.
    * * e.g. CPU of Sort = nLogN(ROWS) * number of sort keys,
    * * CPU of Filter = ROWS * condition cost on a row.
    *
    * @param other another cost
    * @return true iff this is less than or equal to other cost
    */
  override def isLe(other: RelOptCost): Boolean = {
    val that: FlinkCost = other.asInstanceOf[FlinkCost]
    if (this eq that) {
      return true
    }

    val cost1 = normalizeCost(this.memory, this.network, this.io)
    val cost2 = normalizeCost(that.memory, that.network, that.io)
    (this.cpu < that.cpu) ||
      (this.cpu == that.cpu && cost1 < cost2) ||
      (this.cpu == that.cpu && cost1 == cost2 && this.rowCount <= that.rowCount)
  }

  private def normalizeCost(memory: Double, network: Double, io: Double): Double = {
    memory * FlinkCost.MEMORY_TO_CPU_RATIO +
      network * FlinkCost.NETWORK_TO_CPU_RATIO +
      io * FlinkCost.IO_TO_CPU_RATIO
  }

  /**
    * Compares this to another cost.
    *
    * @param other another cost
    * @return true iff this is strictly less than other cost
    */
  override def isLt(other: RelOptCost): Boolean = isLe(other) && !this.equals(other)

  /**
    * Adds another cost to this.
    *
    * @param other another cost
    * @return sum of this and other cost
    */
  override def plus(other: RelOptCost): RelOptCost = {
    val that: FlinkCost = other.asInstanceOf[FlinkCost]
    if ((this eq FlinkCost.Infinity) || (that eq FlinkCost.Infinity)) {
      return FlinkCost.Infinity
    }
    new FlinkCost(
      this.rowCount + that.rowCount,
      this.cpu + that.cpu,
      this.io + that.io,
      this.network + that.network,
      this.memory + that.memory)
  }

  /**
    * Subtracts another cost from this.
    *
    * @param other another cost
    * @return difference between this and other cost
    */
  override def minus(other: RelOptCost): RelOptCost = {
    if (this eq FlinkCost.Infinity) {
      return this
    }
    val that: FlinkCost = other.asInstanceOf[FlinkCost]
    new FlinkCost(
      this.rowCount - that.rowCount,
      this.cpu - that.cpu,
      this.io - that.io,
      this.network - that.network,
      this.memory - that.memory)
  }

  /**
    * Multiplies this cost by a scalar factor.
    *
    * @param factor scalar factor
    * @return scalar product of this and factor
    */
  override def multiplyBy(factor: Double): RelOptCost = {
    if (this eq FlinkCost.Infinity) {
      return this
    }
    new FlinkCost(
      rowCount * factor,
      cpu * factor,
      io * factor,
      network * factor,
      memory * factor)
  }

  /**
    * Computes the ratio between this cost and another cost.
    *
    * <p>divideBy is the inverse of multiplyBy(double). For any
    * finite, non-zero cost and factor f, <code>
    * cost.divideBy(cost.multiplyBy(f))</code> yields <code>1 / f</code>.
    *
    * @param cost Other cost
    * @return Ratio between costs
    */
  override def divideBy(cost: RelOptCost): Double = {
    val that: FlinkCost = cost.asInstanceOf[FlinkCost]
    var d: Double = 1.0
    var n: Double = 0.0
    if ((this.rowCount != 0) && !this.rowCount.isInfinite &&
      (that.rowCount != 0) && !that.rowCount.isInfinite) {
      d *= this.rowCount / that.rowCount
      n += 1
    }
    if ((this.cpu != 0) && !this.cpu.isInfinite && (that.cpu != 0) && !that.cpu.isInfinite) {
      d *= this.cpu / that.cpu
      n += 1
    }
    if ((this.io != 0) && !this.io.isInfinite && (that.io != 0) && !that.io.isInfinite) {
      d *= this.io / that.io
      n += 1
    }
    if ((this.network != 0) && !this.network.isInfinite &&
      (that.network != 0) && !that.network.isInfinite) {
      d *= this.network / that.network
      n += 1
    }
    if ((this.memory != 0) && !this.memory.isInfinite &&
      (that.memory != 0) && !that.memory.isInfinite) {
      d *= this.memory / that.memory
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

object FlinkCost {

  private[flink] val Infinity = new FlinkCost(
    Double.PositiveInfinity,
    Double.PositiveInfinity,
    Double.PositiveInfinity,
    Double.PositiveInfinity,
    Double.PositiveInfinity) {
    override def toString: String = "{inf}"
  }

  private[flink] val Huge = new FlinkCost(
    Double.MaxValue, Double.MaxValue, Double.MaxValue, Double.MaxValue, Double.MaxValue) {
    override def toString: String = "{huge}"
  }

  private[flink] val Zero = new FlinkCost(0.0, 0.0, 0.0, 0.0, 0.0) {
    override def toString: String = "{0}"
  }

  private[flink] val Tiny = new FlinkCost(1.0, 1.0, 0.0, 0.0, 0.0) {
    override def toString = "{tiny}"
  }

  val FACTORY: FlinkCostFactory = new FlinkCostFactory

  // The ratio to convert memory cost into CPU cost.
  val MEMORY_TO_CPU_RATIO = 1.0
  // The ratio to convert io cost into CPU cost.
  val IO_TO_CPU_RATIO = 2.0
  // The ratio to convert network cost into CPU cost.
  val NETWORK_TO_CPU_RATIO = 4.0

  val BASE_CPU_COST: Int = 1

  /**
    * Hash cpu cost per field (for now we don't distinguish between fields of different types)
    * involves the cost of the following operations:
    * compute hash value, probe hash table, walk hash chain and compare with each element,
    * add to the end of hash chain if no match found
    */
  val HASH_CPU_COST: Int = 8 * BASE_CPU_COST

  /**
    * Serialize and deserialize cost, note it's a very expensive operation
    */
  val SERIALIZE_DESERIALIZE_CPU_COST: Int = 160 * BASE_CPU_COST

  /**
    * Cpu cost of random partition.
    */
  val RANDOM_CPU_COST: Int = 1 * BASE_CPU_COST

  /**
    * Cpu cost of singleton exchange
    */
  val SINGLETON_CPU_COST: Int = 1 * BASE_CPU_COST

  /**
    * Cpu cost of comparing one field with another (ignoring data types for now)
    */
  val COMPARE_CPU_COST: Int = 4 * BASE_CPU_COST

  /**
    * Cpu cost for a function evaluation
    */
  val FUNC_CPU_COST: Int = 12 * BASE_CPU_COST

  /**
    * Cpu cost of range partition, including cost of sample and cost of assign range index
    */
  val RANGE_PARTITION_CPU_COST: Int = 12 * BASE_CPU_COST

  /**
    * Default data size of a worker to process.
    * Note: only used in estimates cost of RelNode.
    * It is irrelevant to decides the parallelism of operators.
    */
  val SQL_DEFAULT_PARALLELISM_WORKER_PROCESS_SIZE: Int = 1024 * 1024 * 1024

  // we aim for a 200% utilization of the bucket table.
  val HASH_COLLISION_WEIGHT = 2
}
