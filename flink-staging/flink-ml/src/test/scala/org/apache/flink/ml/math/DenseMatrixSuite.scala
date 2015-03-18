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

import org.scalatest.FlatSpec

class DenseMatrixSuite extends FlatSpec {

  behavior of "A DenseMatrix"

  it should "contain the initialization data after intialization" in {
    val numRows = 10
    val numCols = 13

    val data = Array.range(0, numRows*numCols)

    val matrix = DenseMatrix(numRows, numCols, data)

    assertResult(numRows)(matrix.numRows)
    assertResult(numCols)(matrix.numCols)

    for(row <- 0 until numRows; col <- 0 until numCols) {
      assertResult(data(col*numRows + row))(matrix(row, col))
    }
  }

  it should "throw an IllegalArgumentException in case of an invalid index access" in {
    val numRows = 10
    val numCols = 13

    val matrix = DenseMatrix.zeros(numRows, numCols)

    intercept[IllegalArgumentException] {
      matrix(-1, 2)
    }

    intercept[IllegalArgumentException] {
      matrix(0, -1)
    }

    intercept[IllegalArgumentException] {
      matrix(numRows, 0)
    }

    intercept[IllegalArgumentException] {
      matrix(0, numCols)
    }

    intercept[IllegalArgumentException] {
      matrix(numRows, numCols)
    }
  }
}
