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
  * This class is based on Apache Calcite's [[org.apache.calcite.plan.volcano.VolcanoCost]]
  * and has an adapted cost comparison method `isLe(other: RelOptCost)`
  * that takes io, cpu, network and memory into account.
  */
class FlinkStreamCost(
    rowCount: Double,
    cpu: Double,
    io: Double,
    network: Double,
    memory: Double)
  extends AbstractFlinkCost(rowCount, cpu, io, network, memory) {

  /**
    * @return true iff this cost represents an expression that hasn't actually
    *         been implemented (e.g. a pure relational algebra expression) or can't
    *         actually be implemented, e.g. a transfer of data between two disconnected
    *         sites
    */
  override def isInfinite: Boolean = (this eq FlinkStreamCost.Infinity) || hasInfinity

  /**
    * Compares this to another cost.
    *
    * @param other another cost
    * @return true iff this is exactly equal to other cost
    */
  override def equals(other: RelOptCost): Boolean = {
    (this eq other) ||
      (other.isInstanceOf[FlinkStreamCost] && isEqualTo(other.asInstanceOf[FlinkStreamCost]))
  }

  /**
    * Compares this to another cost, allowing for slight roundoff errors.
    *
    * @param other another cost
    * @return true iff this is the same as the other cost within a roundoff
    *         margin of error
    */
  override def isEqWithEpsilon(other: RelOptCost): Boolean = {
    other match {
      case that: FlinkStreamCost => isEqualToWithEpsilon(that, RelOptUtil.EPSILON)
      case _ => false
    }
  }

  /**
    * Compares this to another cost.
    *
    * @param other another cost
    * @return true iff this is less than or equal to other cost
    */
  override def isLe(other: RelOptCost): Boolean = {
    isLessThanOrEqualTo(other.asInstanceOf[FlinkStreamCost])
  }

  /**
    * Adds another cost to this.
    *
    * @param other another cost
    * @return sum of this and other cost
    */
  override def plus(other: RelOptCost): RelOptCost = {
    val that: FlinkStreamCost = other.asInstanceOf[FlinkStreamCost]
    if ((this eq FlinkStreamCost.Infinity) || (that eq FlinkStreamCost.Infinity)) {
      return FlinkStreamCost.Infinity
    }
    new FlinkStreamCost(
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
    if (this eq FlinkStreamCost.Infinity) {
      return this
    }
    val that: FlinkStreamCost = other.asInstanceOf[FlinkStreamCost]
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
    if (this eq FlinkStreamCost.Infinity) {
      return this
    }
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
  override def divideBy(cost: RelOptCost): Double = computeRatio(cost.asInstanceOf[FlinkStreamCost])

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
