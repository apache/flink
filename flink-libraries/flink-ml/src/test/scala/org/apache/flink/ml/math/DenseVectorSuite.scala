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
    val data = Array.range(1, 10)

    val vector = DenseVector(data)

    assertResult(data.length)(vector.size)

    data.zip(vector.map(_._2)).foreach { case (expected, actual) => assertResult(expected)(actual) }
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

  it should "calculate outer product with DenseVector correctly as DenseMatrix" in {
    val vec1 = DenseVector(Array(1, 0, 1))
    val vec2 = DenseVector(Array(0, 1, 0))

    vec1.outer(vec2) should be(an[DenseMatrix])
    vec1.outer(vec2) should be(DenseMatrix(3, 3, Array(0, 1, 0, 0, 0, 0, 0, 1, 0)))
  }

  it should "calculate outer product with SparseVector correctly as SparseMatrix" in {
    val vec1 = DenseVector(Array(1, 0, 1))
    val vec2 = SparseVector(3, Array(1), Array(1))

    vec1.outer(vec2) should be(an[SparseMatrix])
    vec1.outer(vec2) should be(SparseMatrix.fromCOO(3, 3, (0, 1, 1), (2, 1, 1)))
  }

  it should "calculate outer product with a DenseVector correctly as DenseMatrix 2" in {
    val vec1 = DenseVector(Array(1, 0, 1, 0, 0))
    val vec2 = DenseVector(Array(0, 0, 1, 0, 1))

    val values = Array(0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    vec1.outer(vec2) should be(DenseMatrix(5, 5, values))
  }

  it should "calculate outer product with a SparseVector correctly as SparseMatrix 2" in {
    val vec1 = DenseVector(Array(1, 0, 1, 0, 0))
    val vec2 = SparseVector.fromCOO(5, (2, 1), (4, 1))

    val entries = Iterable((0, 2, 1.0), (0, 4, 1.0), (2, 2, 1.0), (2, 4, 1.0))
    vec1.outer(vec2) should be(SparseMatrix.fromCOO(5, 5, entries))
  }

  it should "DenseVector right outer product with one-dimensional DenseVector as identity" in {
    val vec = DenseVector(Array(1, 0, 1, 0, 0))
    val unit = DenseVector(1)

    vec.outer(unit) should equal(DenseMatrix(vec.size, 1, vec.data))
  }

  it should "DenseVector right outer product with one-dimensional SparseVector as identity" in {
    val vec = DenseVector(Array(1, 0, 1, 0, 0))
    val unit = SparseVector(1, Array(0), Array(1))

    vec.outer(unit) should equal(SparseMatrix.fromCOO(vec.size, 1, (0, 0, 1), (2, 0, 1)))
  }

  it should "DenseVector left outer product with one-dimensional unit DenseVector as identity" in {
    val vec = DenseVector(Array(1, 2, 3, 4, 5))
    val unit = DenseVector(1)

    unit.outer(vec) should equal(DenseMatrix(1, vec.size, vec.data))
  }

  it should "SparseVector left outer product with one-dimensional unit DenseVector as identity" in {
    val vec = SparseVector(5, Array(0, 1, 2, 3, 4), Array(1, 2, 3, 4, 5))
    val unit = DenseVector(1)

    val entries = Iterable((0, 0, 1.0), (0, 1, 2.0), (0, 2, 3.0), (0, 3, 4.0), (0, 4, 5.0))
    unit.outer(vec) should equal(SparseMatrix.fromCOO(1, vec.size, entries))
  }

  it should "DenseVector outer product via multiplication if both vectors are one-dimensional" in {
    val vec1 = DenseVector(Array(2))
    val vec2 = DenseVector(Array(3))

    vec1.outer(vec2) should be(DenseMatrix(1, 1, 2 * 3))
  }

  it should "SparseVector outer product via multiplication if both vectors are one-dimensional" in {
    val vec1 = DenseVector(Array(2))
    val vec2 = SparseVector(1, Array(0), Array(3))

    vec1.outer(vec2) should be(SparseMatrix.fromCOO(1, 1, (0, 0, 2 * 3)))
  }

  it should "calculate magnitude of vector" in {
    val vec = DenseVector(Array(1, 4, 8))

    vec.magnitude should be(Math.sqrt((1 * 1) + (4 * 4) + (8 * 8)))
  }

  it should "convert from and to Breeze vector" in {
    import Breeze._

    val flinkVector = DenseVector(1, 2, 3)
    val breezeVector = breeze.linalg.DenseVector.apply(1.0, 2.0, 3.0)

    // use the vector BreezeVectorConverter
    flinkVector should equal(breezeVector.fromBreeze)

    // use the sparse vector BreezeVectorConverter
    flinkVector should equal(breezeVector.fromBreeze(DenseVector.denseVectorConverter))

    flinkVector.asBreeze should be(breezeVector)
  }
}
