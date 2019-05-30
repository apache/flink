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

import breeze.linalg.svd

/** Singular Value Decomposition.
  * <P>
  * For an m-by-n matrix A with m >= n, the singular value decomposition is
  * an m-by-n orthogonal matrix U, an n-by-n diagonal matrix S, and
  * an n-by-n orthogonal matrix V so that A = U*S*V'.
  * <P>
  * The singular values, sigma[k] = S[k][k], are ordered so that
  * sigma[0] >= sigma[1] >= ... >= sigma[n-1].
  * <P>
  * The singular value decompostion always exists, so the constructor will
  * never fail.  The matrix condition number and the effective numerical
  * rank can be computed from this decomposition.
  */
class SingularValueDecomposition(val A: DenseMatrix) {

  val svd.SVD(u, s, vt) = svd(MatVecOp.toBreezeMatrix(A))

  /**
    * Return the left singular vectors.
    */
  def getU(): DenseMatrix = {
    MatVecOp.fromBreezeMatrix(u)
  }

  /**
    * Return the right singular vectors.
    */
  def getV(): DenseMatrix = {
    MatVecOp.fromBreezeMatrix(vt).transpose()
  }

  /**
    * Return all sigular values.
    */
  def getSingularValues(): DenseVector = {
    MatVecOp.fromBreezeVector(s)
  }

  /** Two norm condition number
    *
    * @return max(S)/min(S)
    */
  def cond(): Double = {
    val m = A.numRows()
    val n = A.numCols()
    s(0) / s(Math.min(m, n) - 1)
  }

  /** Effective numerical matrix rank
    *
    * @return Number of nonnegligible singular values.
    */
  def rank(): Int = {
    val m = A.numRows()
    val n = A.numCols()
    val eps = Math.pow(2.0, -52.0)
    val tol = Math.max(m, n) * s(0) * eps
    var r: Int = 0
    for (i <- 0 until s.length) {
      if (s(i) > tol)
        r = r + 1
    }
    r
  }

  /**
    * Two norm of A.
    *
    * @return
    */
  def norm2(): Double = {
    s(0)
  }
}
