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
 * @param values Array of doubles to store the vector elements
 */
case class DenseVector(val values: Array[Double]) extends Vector {

  /**
   * Number of elements in a vector
   * @return
   */
  override def size: Int = {
    values.length
  }

  /**
   * Element wise access function
   *
   * @param index index of the accessed element
   * @return element at the given index
   */
  override def apply(index: Int): Double = {
    require(0 <= index && index < values.length, s"Index $index is out of bounds " +
      s"[0, ${values.length})")
    values(index)
  }

  override def toString: String = {
    s"DenseVector(${values.mkString(", ")})"
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
