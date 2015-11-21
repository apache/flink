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

/** Type class to allow the vector construction from different data types
  *
  * @tparam T Subtype of [[Vector]]
  */
trait VectorBuilder[T <: Vector] extends Serializable {
  /** Builds a [[Vector]] of type T from a List[Double]
    *
    * @param data Input data where the index denotes the resulting index of the vector
    * @return A vector of type T
    */
  def build(data: List[Double]): T
}

object VectorBuilder{

  /** Type class implementation for [[org.apache.flink.ml.math.DenseVector]] */
  implicit val denseVectorBuilder = new VectorBuilder[DenseVector] {
    override def build(data: List[Double]): DenseVector = {
      new DenseVector(data.toArray)
    }
  }

  /** Type class implementation for [[org.apache.flink.ml.math.SparseVector]] */
  implicit val sparseVectorBuilder = new VectorBuilder[SparseVector] {
    override def build(data: List[Double]): SparseVector = {
      // Enrich elements with explicit indices and filter out zero entries
      SparseVector.fromCOO(data.length, data.indices.zip(data).filter(_._2 != 0.0))
    }
  }

  /** Type class implementation for [[Vector]] */
  implicit val vectorBuilder = new VectorBuilder[Vector] {
    override def build(data: List[Double]): Vector = {
      new DenseVector(data.toArray)
    }
  }
}
