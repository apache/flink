/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.common.matrix

import breeze.linalg.cholesky
import com.github.fommil.netlib.LAPACK.{getInstance => lapack}
import org.netlib.util.intW

/** Cholesky Decomposition.
  * For a symmetric, positive definite matrix A, the Cholesky decomposition
  * is an lower triangular matrix L so that A = L*L'.
  * Exception would be raised if the matrix is not symmetric positive definite
  * matrix.
  */
class CholeskyDecomposition(val A: DenseMatrix) {

  val c = cholesky(MatVecOp.toBreezeMatrix(A))

  /**
    * Solve A*X = B. The result is returned in B.
    *
    * @param B A Matrix with as many rows as A and any number of columns.
    */
  def solve(B: DenseMatrix): Unit = {
    val n = A.numRows()
    val info = new intW(0)
    lapack.dpotrs("L", n, B.numCols(), c.data, n, B.data, n, info)
    assert(info.`val` == 0, "A is not positive definite.")
  }

  /**
    * Get matrix L.
    */
  def getL(): DenseMatrix = {
    MatVecOp.fromBreezeMatrix(c)
  }
}
