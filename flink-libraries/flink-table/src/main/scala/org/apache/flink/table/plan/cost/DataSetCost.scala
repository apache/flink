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
class DataSetCost(
    override val rowCount: Double,
    override val cpu: Double,
    override val io: Double,
    override val network: Double,
    override val memory: Double)
  extends AbstractFlinkCost(rowCount, cpu, io, network, memory) {

  def isInfinite: Boolean = {
    (this eq DataSetCost.Infinity) ||
      (this.rowCount == Double.PositiveInfinity) ||
      (this.cpu == Double.PositiveInfinity) ||
      (this.io == Double.PositiveInfinity) ||
      (this.network == Double.PositiveInfinity) ||
      (this.memory == Double.PositiveInfinity)
  }

  def isLe(other: RelOptCost): Boolean = {
    val that: DataSetCost = other.asInstanceOf[DataSetCost]
    (this eq that) ||
      (this.memory < that.memory) ||
      (this.memory == that.memory && this.network < that.network) ||
      (this.memory == that.memory && this.network == that.network && this.io < that.io) ||
      (this.memory == that.memory && this.network == that.network && this.io == that.io &&
        this.cpu < that.cpu) ||
      (this.memory == that.memory && this.network == that.network && this.io == that.io &&
        this.cpu == that.cpu && this.rowCount < that.rowCount)
  }

  def equals(other: RelOptCost): Boolean = {
    (this eq other) ||
      other.isInstanceOf[DataSetCost] &&
        (this.rowCount == other.asInstanceOf[DataSetCost].rowCount) &&
        (this.cpu == other.asInstanceOf[DataSetCost].cpu) &&
        (this.io == other.asInstanceOf[DataSetCost].io) &&
        (this.network == other.asInstanceOf[DataSetCost].network) &&
        (this.memory == other.asInstanceOf[DataSetCost].memory)
  }

  def isEqWithEpsilon(other: RelOptCost): Boolean = {
    if (!other.isInstanceOf[DataSetCost]) {
      return false
    }
    val that: DataSetCost = other.asInstanceOf[DataSetCost]
    (this eq that) ||
      ((Math.abs(this.rowCount - that.rowCount) < RelOptUtil.EPSILON) &&
        (Math.abs(this.cpu - that.cpu) < RelOptUtil.EPSILON) &&
        (Math.abs(this.io - that.io) < RelOptUtil.EPSILON) &&
        (Math.abs(this.network - that.network) < RelOptUtil.EPSILON) &&
        (Math.abs(this.memory - that.memory) < RelOptUtil.EPSILON))
  }

  def minus(other: RelOptCost): RelOptCost = {
    if (this eq DataSetCost.Infinity) {
      return this
    }
    val that: DataSetCost = other.asInstanceOf[DataSetCost]
    new DataSetCost(
      this.rowCount - that.rowCount,
      this.cpu - that.cpu,
      this.io - that.io,
      this.network - that.network,
      this.memory - that.memory)
  }

  def multiplyBy(factor: Double): RelOptCost = {
    if (this eq DataSetCost.Infinity) {
      return this
    }
    new DataSetCost(rowCount * factor, cpu * factor, io * factor, network * factor, memory * factor)
  }

  def divideBy(cost: RelOptCost): Double = {
    val that: DataSetCost = cost.asInstanceOf[DataSetCost]
    var d: Double = 1
    var n: Double = 0
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

  def plus(other: RelOptCost): RelOptCost = {
    val that: DataSetCost = other.asInstanceOf[DataSetCost]
    if ((this eq DataSetCost.Infinity) || (that eq DataSetCost.Infinity)) {
      return DataSetCost.Infinity
    }
    new DataSetCost(
      this.rowCount + that.rowCount,
      this.cpu + that.cpu,
      this.io + that.io,
      this.network + that.network,
      this.memory + that.memory)
  }

}

object DataSetCost {

  private[flink] val Infinity = new DataSetCost(
    Double.PositiveInfinity,
    Double.PositiveInfinity,
    Double.PositiveInfinity,
    Double.PositiveInfinity,
    Double.PositiveInfinity) {
    override def toString: String = "{inf}"
  }

  private[flink] val Huge = new DataSetCost(
    Double.MaxValue, Double.MaxValue, Double.MaxValue, Double.MaxValue, Double.MaxValue) {
    override def toString: String = "{huge}"
  }

  private[flink] val Zero = new DataSetCost(0.0, 0.0, 0.0, 0.0, 0.0) {
    override def toString: String = "{0}"
  }

  private[flink] val Tiny = new DataSetCost(1.0, 1.0, 0.0, 0.0, 0.0) {
    override def toString = "{tiny}"
  }

  val FACTORY: DataSetCostFactory = new DataSetCostFactory
}
