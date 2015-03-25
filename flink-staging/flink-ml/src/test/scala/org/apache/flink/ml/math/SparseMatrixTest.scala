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

import org.junit.Test
import org.scalatest.ShouldMatchers

class SparseMatrixTest extends ShouldMatchers {

  @Test
  def testSparseMatrixFromCOO: Unit = {
    val sparseMatrix = SparseMatrix.fromCOO(5, 5, (0, 0, 0), (0, 1, 0), (3, 4, 43), (2, 1, 17),
      (3, 3, 88), (4 , 2, 99), (1, 4, 91), (3, 4, -1))

    val expectedSparseMatrix = SparseMatrix.fromCOO(5, 5, (3, 4, 42), (2, 1, 17), (3, 3, 88),
      (4, 2, 99), (1, 4, 91))

    val expectedDenseMatrix = DenseMatrix.zeros(5, 5)
    expectedDenseMatrix(3, 4) = 42
    expectedDenseMatrix(2, 1) = 17
    expectedDenseMatrix(3, 3) = 88
    expectedDenseMatrix(4, 2) = 99
    expectedDenseMatrix(1, 4) = 91

    sparseMatrix should equal(expectedSparseMatrix)
    sparseMatrix should equal(expectedDenseMatrix)

    sparseMatrix.toDenseMatrix.data.sameElements(expectedDenseMatrix.data) should be(true)

    sparseMatrix(0, 1) = 10

    intercept[IllegalArgumentException]{
      sparseMatrix(1, 1) = 1
    }
  }

  @Test
  def testInvalidIndexAccess: Unit = {
    val sparseVector = SparseVector.fromCOO(5, (1, 1), (3, 3), (4, 4))

    intercept[IllegalArgumentException] {
      sparseVector(-1)
    }

    intercept[IllegalArgumentException] {
      sparseVector(5)
    }

    sparseVector(0) should equal(0)
    sparseVector(3) should equal(3)
  }

  @Test
  def testSparseMatrixFromCOOWithInvalidIndices: Unit = {
    intercept[IllegalArgumentException]{
      val sparseMatrix = SparseMatrix.fromCOO(5 ,5, (5, 0, 10),  (0, 0, 0), (0, 1, 0), (3, 4, 43),
        (2, 1, 17))
    }

    intercept[IllegalArgumentException]{
      val sparseMatrix = SparseMatrix.fromCOO(5, 5,  (0, 0, 0), (0, 1, 0), (3, 4, 43), (2, 1, 17),
        (-1, 4, 20))
    }
  }

  @Test
  def testSparseMatrixCopy: Unit = {
    val sparseMatrix = SparseMatrix.fromCOO(4, 4, (0, 1, 2), (2, 3, 1), (2, 0, 42), (1, 3, 3))

    val copy = sparseMatrix.copy

    sparseMatrix should equal(copy)

    copy(2, 3) = 2

    sparseMatrix should not equal(copy)
  }
}
