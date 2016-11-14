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

import Breeze._
import breeze.linalg

import org.scalatest.{Matchers, FlatSpec}

class BreezeMathSuite extends FlatSpec with Matchers {

  behavior of "Breeze vector conversion"

  it should "convert a DenseMatrix into breeze.linalg.DenseMatrix and vice versa" in {
    val numRows = 5
    val numCols = 4

    val data = Array.range(0, numRows * numCols)
    val expectedData = Array.range(0, numRows * numCols).map(_ * 2)

    val denseMatrix = DenseMatrix(numRows, numCols, data)
    val expectedMatrix = DenseMatrix(numRows, numCols, expectedData)

    val m = denseMatrix.asBreeze

    val result = (m * 2.0).fromBreeze

    result should equal(expectedMatrix)
  }

  it should "convert a SparseMatrix into breeze.linalg.CSCMatrix" in {
    val numRows = 5
    val numCols = 4

    val sparseMatrix = SparseMatrix.fromCOO(numRows, numCols,
      (0, 1, 1),
      (4, 3, 13),
      (3, 2, 45),
      (4, 0, 12))

    val expectedMatrix = SparseMatrix.fromCOO(numRows, numCols,
      (0, 1, 2),
      (4, 3, 26),
      (3, 2, 90),
      (4, 0, 24))

    val sm = sparseMatrix.asBreeze

    val result = (sm * 2.0).fromBreeze

    result should equal(expectedMatrix)
  }

  it should "convert a dense Flink vector into a dense Breeze vector and vice versa" in {
    val vector = DenseVector(1, 2, 3)

    val breezeVector = vector.asBreeze

    val flinkVector = breezeVector.fromBreeze

    breezeVector.getClass should be(new linalg.DenseVector[Double](0).getClass())
    flinkVector.getClass should be (classOf[DenseVector])

    flinkVector should equal(vector)
  }

  it should "convert a sparse Flink vector into a sparse Breeze vector and given the right " +
    "converter back into a dense Flink vector" in {
    implicit val converter = implicitly[BreezeVectorConverter[DenseVector]]

    val vector = SparseVector.fromCOO(3, (1, 1.0), (2, 2.0))

    val breezeVector = vector.asBreeze

    val flinkVector = breezeVector.fromBreeze

    breezeVector.getClass should be(new linalg.SparseVector[Double](null).getClass())
    flinkVector.getClass should be (classOf[DenseVector])

    flinkVector.equalsVector(vector) should be(true)
  }
}
