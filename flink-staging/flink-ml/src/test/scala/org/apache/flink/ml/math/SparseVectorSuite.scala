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

class SparseVectorSuite extends FlatSpec with Matchers {

  behavior of "Flink's SparseVector"

  it should "contain a single element provided as coordinate list (COO)" in {
    val sparseVector = SparseVector.fromCOO(3, (0, 1))

    sparseVector(0) should equal(1)

    for(index <- 1 until 3) {
      sparseVector(index) should equal(0)
    }
  }

  it should "contain the initialization data provided as coordinate list (COO)" in {
    val data = List[(Int, Double)]((0, 1), (2, 0), (4, 42), (0, 3))
    val size = 5
    val sparseVector = SparseVector.fromCOO(size, data)

    val expectedSparseVector = SparseVector.fromCOO(5, (0, 4), (4, 42), (2, 0))
    val expectedDenseVector = DenseVector.zeros(5)

    expectedDenseVector(0) = 4
    expectedDenseVector(4) = 42

    sparseVector should equal(expectedSparseVector)
    sparseVector.equalsVector(expectedDenseVector) should be(true)

    val denseVector = sparseVector.toDenseVector

    denseVector should equal(expectedDenseVector)

    val dataMap = data.
      groupBy{_._1}.
      mapValues{
      entries =>
        entries.map(_._2).reduce(_ + _)
    }

    for(index <- 0 until size) {
      sparseVector(index) should be(dataMap.getOrElse(index, 0))
    }
  }

  it should "fail when accessing elements using an invalid index" in {
    val sparseVector = SparseVector.fromCOO(5, (1, 1), (3, 3), (4, 4))

    intercept[IllegalArgumentException] {
      sparseVector(-1)
    }

    intercept[IllegalArgumentException] {
      sparseVector(5)
    }
  }

  it should "fail when the COO list contains elements with invalid indices" in {
    intercept[IllegalArgumentException] {
      val sparseVector = SparseVector.fromCOO(5, (0, 1), (-1, 34), (3, 2))
    }

    intercept[IllegalArgumentException] {
      val sparseVector = SparseVector.fromCOO(5, (0, 1), (4,3), (5, 1))
    }
  }

  it should "be copyable" in {
    val sparseVector = SparseVector.fromCOO(5, (0, 1), (4, 3), (3, 2))

    val copy = sparseVector.copy

    sparseVector should equal(copy)

    copy(3) = 3

    sparseVector should not equal(copy)
  }

  it should "calculate dot product with SparseVector" in {
    val vec1 = SparseVector.fromCOO(4, (0, 1), (2, 1))
    val vec2 = SparseVector.fromCOO(4, (1, 1), (3, 1))

    vec1.dot(vec2) should be(0)
  }

  it should "calculate dot product with SparseVector 2" in {
    val vec1 = SparseVector.fromCOO(5, (2, 3), (4, 1))
    val vec2 = SparseVector.fromCOO(5, (4, 2), (2, 1))

    vec1.dot(vec2) should be(5)
  }

  it should "calculate dot product with DenseVector" in {
    val vec1 = SparseVector.fromCOO(4, (0, 1), (2, 1))
    val vec2 = DenseVector(Array(0, 1, 0, 1))

    vec1.dot(vec2) should be(0)
  }

  it should "fail in case of calculation dot product with different size vector" in {
    val vec1 = SparseVector.fromCOO(4, (0, 1), (2, 1))
    val vec2 = DenseVector(Array(0, 1, 0))

    intercept[IllegalArgumentException] {
      vec1.dot(vec2)
    }
  }

  it should "calculate magnitude of vector" in {
    val vec = SparseVector.fromCOO(3, (0, 1), (1, 4), (2, 8))

    vec.magnitude should be(9)
  }
}
