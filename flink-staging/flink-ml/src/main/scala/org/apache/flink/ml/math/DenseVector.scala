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

package org.apache.flink.ml.math

/**
 * Dense vector implementation of [[Vector]]. The data is represented in a continuous array of
 * doubles.
 *
 * @param data Array of doubles to store the vector elements
 */
case class DenseVector(
    val data: Array[Double])
  extends Vector
  with Serializable {

  /**
   * Number of elements in a vector
   * @return
   */
  override def size: Int = {
    data.length
  }

  /**
   * Element wise access function
   *
   * @param index index of the accessed element
   * @return element at the given index
   */
  override def apply(index: Int): Double = {
    require(0 <= index && index < data.length, index + " not in [0, " + data.length + ")")
    data(index)
  }

  override def toString: String = {
    s"DenseVector(${data.mkString(", ")})"
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case dense: DenseVector => data.length == dense.data.length && data.sameElements(dense.data)
      case _ => false
    }
  }

  override def hashCode: Int = {
    java.util.Arrays.hashCode(data)
  }

  /**
   * Copies the vector instance
   *
   * @return Copy of the vector instance
   */
  override def copy: DenseVector = {
    DenseVector(data.clone())
  }

  /** Updates the element at the given index with the provided value
    *
    * @param index
    * @param value
    */
  override def update(index: Int, value: Double): Unit = {
    require(0 <= index && index < data.length, index + " not in [0, " + data.length + ")")

    data(index) = value
  }

  /** Returns the dot product of the recipient and the argument
    *
    * @param other a Vector
    * @return a scalar double of dot product
    */
  override def dot(other: Vector): Double = {
    require(size == other.size, "The size of vector must be equal.")

    other match {
      case SparseVector(_, otherIndices, otherData) =>
        otherIndices.zipWithIndex.map {
          case (idx, sparseIdx) => data(idx) * otherData(sparseIdx)
        }.sum
      case _ => (0 until size).map(i => data(i) * other(i)).sum
    }
  }

  /** Magnitude of a vector
    *
    * @return
    */
  override def magnitude: Double = math.sqrt(data.map(x => x * x).sum)

  def toSparseVector: SparseVector = {
    val nonZero = (0 until size).zip(data).filter(_._2 != 0)

    SparseVector.fromCOO(size, nonZero)
  }
}

object DenseVector {

  def apply(values: Double*): DenseVector = {
    new DenseVector(values.toArray)
  }

  def apply(values: Array[Int]): DenseVector = {
    new DenseVector(values.map(_.toDouble))
  }

  def zeros(size: Int): DenseVector = {
    init(size, 0.0)
  }

  def eye(size: Int): DenseVector = {
    init(size, 1.0)
  }

  def init(size: Int, value: Double): DenseVector = {
    new DenseVector(Array.fill(size)(value))
  }
}
