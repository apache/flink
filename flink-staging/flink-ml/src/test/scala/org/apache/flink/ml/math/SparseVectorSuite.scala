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

  it should "contain the initialization data provided as coordinate list (COO)" in {
    val data = List[(Int, Double)]((0, 1), (2, 0), (4, 42), (0, 3))
    val size = 5
    val sparseVector = SparseVector.fromCOO(size, data)

    val expectedSparseVector = SparseVector.fromCOO(5, (0, 4), (4, 42))
    val expectedDenseVector = DenseVector.zeros(5)

    expectedDenseVector(0) = 4
    expectedDenseVector(4) = 42

    sparseVector should equal(expectedSparseVector)
    sparseVector should equal(expectedDenseVector)

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
}
