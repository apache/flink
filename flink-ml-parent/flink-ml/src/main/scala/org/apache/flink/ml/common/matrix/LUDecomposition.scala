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

import breeze.linalg.LU
import com.github.fommil.netlib.LAPACK.{getInstance => lapack}
import org.netlib.util.intW

/** Computes the LU factorization of the given real M-by-N matrix A such that
  * A = P * L * U where P is a permutation matrix (row exchanges).
  * Upon completion, a tuple consisting of a matrix 'lu' and an integer array 'p'.
  * The upper triangular portion of 'lu' resembles U whereas the lower triangular portion of
  * 'lu' resembles L up to but not including the diagonal elements of L which are
  * all equal to 1.
  * The primary use of the LU decomposition is in the solution of square systems
  * of simultaneous linear equations. This will fail if isNonsingular() returns false.
  */
class LUDecomposition(val A: DenseMatrix) {

  val (lu, p) = LU(MatVecOp.toBreezeMatrix(A))

  /**
    * Check whether matrix A is singular
    *
    * @return
    */
  def isNonsingular: Boolean = {
    var j = 0
    val n = lu.cols
    while (j < n) {
      if (lu(j, j) == 0.0)
        return false
      j += 1
    }
    true
  }

  /**
    * Solve A*X = B. The result is returned in B.
    *
    * @param B A Matrix with as many rows as A and any number of columns.
    */
  def solve(B: DenseMatrix): Unit = {
    val n = A.numRows()
    val info = new intW(0)
    lapack.dgetrs("N", n, B.numCols(), lu.data, n, p, B.data, n, info)
    assert(info.`val` == 0, "A is singular")
  }

  /**
    * Get L and U, packed in a single matrix.
    * The upper triangular portion of LU resembles U whereas the lower triangular portion of
    * LU resembles L up to but not including the diagonal elements of L which are
    * all equal to 1.
    */
  def getLU: DenseMatrix = {
    MatVecOp.fromBreezeMatrix(lu)
  }

  /**
    * Get permutation array P
    */
  def getP: Array[Int] = {
    p
  }
}
