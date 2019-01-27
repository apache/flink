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

import org.apache.calcite.plan.{RelOptCost, RelOptUtil}

/**
  * This class is based on Apache Calcite's `org.apache.calcite.plan.volcano.VolcanoCost` and has
  * an adapted cost comparison method `isLe(other: RelOptCost)` that takes io, cpu, network
  * and memory into account.
  */
class FlinkStreamCost(
    override val rowCount: Double,
    override val cpu: Double,
    override val io: Double,
    override val network: Double,
    override val memory: Double)
  extends AbstractFlinkCost(rowCount, cpu, io, network, memory) {

  /**
   * @return true iff this cost represents an expression that hasn't actually
   *         been implemented (e.g. a pure relational algebra expression) or can't
   *         actually be implemented, e.g. a transfer of data between two disconnected
   *         sites
   */
  override def isInfinite: Boolean = {
    (this eq FlinkStreamCost.Infinity) ||
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
  override def equals(other: RelOptCost): Boolean = (this eq other) ||
      other.isInstanceOf[FlinkStreamCost] &&
          (this.rowCount == other.asInstanceOf[FlinkStreamCost].rowCount) &&
          (this.cpu == other.asInstanceOf[FlinkStreamCost].cpu) &&
          (this.io == other.asInstanceOf[FlinkStreamCost].io) &&
          (this.network == other.asInstanceOf[FlinkStreamCost].network) &&
          (this.memory == other.asInstanceOf[FlinkStreamCost].memory)

  /**
   * Compares this to another cost, allowing for slight roundoff errors.
   *
   * @param other another cost
   * @return true iff this is the same as the other cost within a roundoff
   *         margin of error
   */
  override def isEqWithEpsilon(other: RelOptCost): Boolean = {
    if (!other.isInstanceOf[FlinkStreamCost]) return false
    val that = other.asInstanceOf[FlinkStreamCost]
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
   * @param other another cost
   * @return true iff this is less than or equal to other cost
   */
  override def isLe(other: RelOptCost): Boolean = {
    val that = other.asInstanceOf[FlinkStreamCost]
    (this eq that) ||
      (this.memory < that.memory) ||
      (this.memory == that.memory && this.network < that.network) ||
      (this.memory == that.memory && this.network == that.network && this.io < that.io) ||
      (this.memory == that.memory && this.network == that.network && this.io == that.io &&
        this.cpu < that.cpu) ||
      (this.memory == that.memory && this.network == that.network && this.io == that.io &&
        this.cpu == that.cpu && this.rowCount < that.rowCount)
  }

  /**
   * Compares this to another cost.
   *
   * @param other another cost
   * @return true iff this is strictly less than other cost
   */
  override def isLt(other: RelOptCost): Boolean = isLe(other) && !(this == other)

  /**
   * Adds another cost to this.
   *
   * @param other another cost
   * @return sum of this and other cost
   */
  override def plus(other: RelOptCost): RelOptCost = {
    val that = other.asInstanceOf[FlinkStreamCost]
    if ((this eq FlinkStreamCost.Infinity) || (that eq FlinkStreamCost.Infinity)) {
      FlinkStreamCost.Infinity
    } else {
      new FlinkStreamCost(
        this.rowCount + that.rowCount,
        this.cpu + that.cpu,
        this.io + that.io,
        this.network + that.network,
        this.memory + that.memory
      )
    }
  }

  /**
   * Subtracts another cost from this.
   *
   * @param other another cost
   * @return difference between this and other cost
   */
  override def minus(other: RelOptCost): RelOptCost = {
    if (this eq FlinkStreamCost.Infinity) return this
    val that = other.asInstanceOf[FlinkStreamCost]
    new FlinkStreamCost(
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
    if (this eq FlinkStreamCost.Infinity) return this
    new FlinkStreamCost(
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
    val that = cost.asInstanceOf[FlinkStreamCost]
    var d = 1.0
    var n = 0.0
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
    if (n == 0) return 1.0
    Math.pow(d, 1 / n)
  }

}

object FlinkStreamCost {

  private[flink] val Infinity = new FlinkStreamCost(
    Double.PositiveInfinity,
    Double.PositiveInfinity,
    Double.PositiveInfinity,
    Double.PositiveInfinity,
    Double.PositiveInfinity) {
    override def toString: String = "{inf}"
  }

  private[flink] val Huge = new FlinkStreamCost(
    Double.MaxValue, Double.MaxValue, Double.MaxValue, Double.MaxValue, Double.MaxValue) {
    override def toString: String = "{huge}"
  }

  private[flink] val Zero = new FlinkStreamCost(0.0, 0.0, 0.0, 0.0, 0.0) {
    override def toString: String = "{0}"
  }

  private[flink] val Tiny = new FlinkStreamCost(1.0, 1.0, 0.0, 0.0, 0.0) {
    override def toString = "{tiny}"
  }

  val FACTORY: FlinkStreamCostFactory = new FlinkStreamCostFactory
}
