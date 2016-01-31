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

class SparseMatrixSuite extends FlatSpec with Matchers {

  behavior of "Flink's SparseMatrix"

  it should "contain a single element provided as a coordinate list (COO)" in {
    val sparseMatrix = SparseMatrix.fromCOO(4, 4, (0, 0, 1))

    sparseMatrix(0, 0) should equal(1)

    for(i <- 1 until sparseMatrix.size) {
      val row = i / sparseMatrix.numCols
      val col = i % sparseMatrix.numCols

      sparseMatrix(row, col) should equal(0)
    }
  }

  it should "be initialized from a coordinate list representation (COO)" in {
    val data = List[(Int, Int, Double)]((0, 0, 0), (0, 1, 0), (3, 4, 43), (2, 1, 17),
      (3, 3, 88), (4 , 2, 99), (1, 4, 91), (3, 4, -1))

    val numRows = 5
    val numCols = 5

    val sparseMatrix = SparseMatrix.fromCOO(numRows, numCols, data)

    val expectedSparseMatrix = SparseMatrix.fromCOO(5, 5, (3, 4, 42), (2, 1, 17), (3, 3, 88),
      (4, 2, 99), (1, 4, 91), (0, 0, 0), (0, 1, 0))

    val expectedDenseMatrix = DenseMatrix.zeros(5, 5)
    expectedDenseMatrix(3, 4) = 42
    expectedDenseMatrix(2, 1) = 17
    expectedDenseMatrix(3, 3) = 88
    expectedDenseMatrix(4, 2) = 99
    expectedDenseMatrix(1, 4) = 91

    sparseMatrix should equal(expectedSparseMatrix)
    sparseMatrix.equalsMatrix(expectedDenseMatrix) should be(true)

    sparseMatrix.toDenseMatrix.data.sameElements(expectedDenseMatrix.data) should be(true)

    val dataMap = data.
      map{ case (row, col, value) => (row, col) -> value }.
      groupBy{_._1}.
      mapValues{
      entries =>
        entries.map(_._2).sum
    }

    for(row <- 0 until numRows; col <- 0 until numCols) {
      sparseMatrix(row, col) should be(dataMap.getOrElse((row, col), 0))
    }

    // test access to defined field even though it was set to 0
    sparseMatrix(0, 1) = 10

    // test that a non-defined field is not accessible
    intercept[IllegalArgumentException]{
      sparseMatrix(1, 1) = 1
    }
  }

  it should "fail when accessing zero elements or using invalid indices" in {
    val data = List[(Int, Int, Double)]((0, 0, 0), (0, 1, 0), (3, 4, 43), (2, 1, 17),
      (3, 3, 88), (4 , 2, 99), (1, 4, 91), (3, 4, -1))

    val numRows = 5
    val numCols = 5

    val sparseMatrix = SparseMatrix.fromCOO(numRows, numCols, data)

    intercept[IllegalArgumentException] {
      sparseMatrix(-1, 4)
    }

    intercept[IllegalArgumentException] {
      sparseMatrix(numRows, 0)
    }

    intercept[IllegalArgumentException] {
      sparseMatrix(0, numCols)
    }

    intercept[IllegalArgumentException] {
      sparseMatrix(3, -1)
    }
  }

  it should "fail when elements of the COO list have invalid indices" in {
    intercept[IllegalArgumentException]{
      val sparseMatrix = SparseMatrix.fromCOO(5 ,5, (5, 0, 10),  (0, 0, 0), (0, 1, 0), (3, 4, 43),
        (2, 1, 17))
    }

    intercept[IllegalArgumentException]{
      val sparseMatrix = SparseMatrix.fromCOO(5, 5,  (0, 0, 0), (0, 1, 0), (3, 4, 43), (2, 1, 17),
        (-1, 4, 20))
    }
  }

  it should "be copyable" in {
    val sparseMatrix = SparseMatrix.fromCOO(4, 4, (0, 1, 2), (2, 3, 1), (2, 0, 42), (1, 3, 3))

    val copy = sparseMatrix.copy

    sparseMatrix should equal(copy)

    copy(2, 3) = 2

    sparseMatrix should not equal copy
  }
}
