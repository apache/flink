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

import org.scalatest.{Matchers, FlatSpec}

class DenseVectorSuite extends FlatSpec with Matchers {

  behavior of "Flink's DenseVector"

  it should "contain the initialization data" in {
    val data = Array.range(1,10)

    val vector = DenseVector(data)

    assertResult(data.length)(vector.size)

    data.zip(vector.map(_._2)).foreach{case (expected, actual) => assertResult(expected)(actual)}
  }

  it should "fail in case of an illegal element access" in {
    val size = 10

    val vector = DenseVector.zeros(size)

    intercept[IllegalArgumentException] {
      vector(-1)
    }

    intercept[IllegalArgumentException] {
      vector(size)
    }
  }
  
  it should "calculate dot product with DenseVector" in {
    val vec1 = DenseVector(Array(1, 0, 1))
    val vec2 = DenseVector(Array(0, 1, 0))

    vec1.dot(vec2) should be(0)
  }

  it should "calculate dot product with SparseVector" in {
    val vec1 = DenseVector(Array(1, 0, 1))
    val vec2 = SparseVector.fromCOO(3, (0, 1), (1, 1))

    vec1.dot(vec2) should be(1)
  }

  it should "calculate dot product with SparseVector 2" in {
    val vec1 = DenseVector(Array(1, 0, 1, 0, 0))
    val vec2 = SparseVector.fromCOO(5, (2, 1), (4, 1))

    vec1.dot(vec2) should be(1)
  }

  it should "fail in case of calculation dot product with different size vector" in {
    val vec1 = DenseVector(Array(1, 0))
    val vec2 = DenseVector(Array(0))

    intercept[IllegalArgumentException] {
      vec1.dot(vec2)
    }
  }

  it should "calculate magnitude of vector" in {
    val vec = DenseVector(Array(1, 4, 8))

    vec.magnitude should be(9)
  }
}
