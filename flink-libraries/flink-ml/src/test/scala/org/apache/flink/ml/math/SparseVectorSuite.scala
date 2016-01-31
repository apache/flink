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

    for (index <- 1 until 3) {
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
      groupBy {
      _._1
    }.
      mapValues {
      entries =>
        entries.map(_._2).sum
    }

    for (index <- 0 until size) {
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
      val sparseVector = SparseVector.fromCOO(5, (0, 1), (4, 3), (5, 1))
    }
  }

  it should "be copyable" in {
    val sparseVector = SparseVector.fromCOO(5, (0, 1), (4, 3), (3, 2))

    val copy = sparseVector.copy

    sparseVector should equal(copy)

    copy(3) = 3

    sparseVector should not equal (copy)
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

  it should "calculate outer product with SparseVector correctly as SparseMatrix" in {
    val vec1 = SparseVector(3, Array(0, 2), Array(1, 1))
    val vec2 = SparseVector(3, Array(1), Array(1))

    vec1.outer(vec2) should be(an[SparseMatrix])
    vec1.outer(vec2) should be(SparseMatrix.fromCOO(3, 3, (0, 1, 1), (2, 1, 1)))
  }

  it should "calculate outer product with DenseVector correctly as SparseMatrix" in {
    val vec1 = SparseVector(3, Array(0, 2), Array(1, 1))
    val vec2 = DenseVector(Array(0, 1, 0))

    vec1.outer(vec2) should be(an[SparseMatrix])
    vec1.outer(vec2) should be(SparseMatrix.fromCOO(3, 3, (0, 1, 1), (2, 1, 1)))
  }

  it should "calculate outer product with a DenseVector correctly as SparseMatrix 2" in {
    val vec1 = SparseVector(5, Array(0, 2), Array(1, 1))
    val vec2 = DenseVector(Array(0, 0, 1, 0, 1))

    val entries = Iterable((0, 2, 1.0), (0, 4, 1.0), (2, 2, 1.0), (2, 4, 1.0))
    vec1.outer(vec2) should be(SparseMatrix.fromCOO(5, 5, entries))
  }

  it should "calculate outer product with a SparseVector correctly as SparseMatrix 2" in {
    val vec1 = SparseVector(5, Array(0, 2), Array(1, 1))
    val vec2 = SparseVector.fromCOO(5, (2, 1), (4, 1))

    val entries = Iterable((0, 2, 1.0), (0, 4, 1.0), (2, 2, 1.0), (2, 4, 1.0))
    vec1.outer(vec2) should be(SparseMatrix.fromCOO(5, 5, entries))
  }


  it should s"""calculate right outer product with DenseVector
               |with one-dimensional unit DenseVector as identity""".stripMargin in {
    val vec = SparseVector(5, Array(0, 2), Array(1, 1))
    val unit = DenseVector(1)

    vec.outer(unit) should equal(SparseMatrix.fromCOO(vec.size, 1, (0, 0, 1), (2, 0, 1)))
  }

  it should s"""calculate right outer product with DenseVector
               |with one-dimensional unit SparseVector as identity""".stripMargin in {
    val vec = SparseVector(5, Array(0, 2), Array(1, 1))
    val unit = SparseVector(1, Array(0), Array(1))

    vec.outer(unit) should equal(SparseMatrix.fromCOO(vec.size, 1, (0, 0, 1), (2, 0, 1)))
  }

  it should s"""calculate left outer product for SparseVector
               |with one-dimensional unit DenseVector as identity""".stripMargin in {
    val vec = SparseVector(5, Array(0, 1, 2, 3, 4), Array(1, 2, 3, 4, 5))
    val unit = DenseVector(1)

    val entries = Iterable((0, 0, 1.0), (0, 1, 2.0), (0, 2, 3.0), (0, 3, 4.0), (0, 4, 5.0))
    unit.outer(vec) should equal(SparseMatrix.fromCOO(1, vec.size, entries))
  }

  it should s"""calculate outer product with SparseVector
               |via multiplication if both vectors are one-dimensional""".stripMargin in {
    val vec1 = SparseVector.fromCOO(1, (0, 2))
    val vec2 = SparseVector.fromCOO(1, (0, 3))

    vec1.outer(vec2) should be(SparseMatrix.fromCOO(1, 1, (0, 0, 2 * 3)))
  }

  it should s"""calculate outer product with DenseVector
               |via multiplication if both vectors are one-dimensional""".stripMargin in {
    val vec1 = SparseVector(1, Array(0), Array(2))
    val vec2 = DenseVector(Array(3))

    vec1.outer(vec2) should be(SparseMatrix.fromCOO(1, 1, (0, 0, 2 * 3)))
  }

  it should "calculate magnitude of vector" in {
    val vec = SparseVector.fromCOO(3, (0, 1), (1, 4), (2, 8))

    vec.magnitude should be(9)
  }

  it should "convert from and to Breeze vectors" in {
    import Breeze._

    val flinkVector = SparseVector.fromCOO(3, (1, 1.0), (2, 2.0))
    val breezeVector = breeze.linalg.SparseVector(3)(1 -> 1.0, 2 -> 2.0)

    // use the vector BreezeVectorConverter
    flinkVector should equal(breezeVector.fromBreeze)

    // use the sparse vector BreezeVectorConverter
    flinkVector should equal(breezeVector.fromBreeze(SparseVector.sparseVectorConverter))

    flinkVector.asBreeze should be(breezeVector)
  }
}
