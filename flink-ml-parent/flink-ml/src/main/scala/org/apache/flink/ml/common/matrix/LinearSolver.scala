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

import com.github.fommil.netlib.LAPACK.{getInstance => lapack}
import org.netlib.util.intW

object LinearSolver {
  /**
    * Solve Ax = B, where A is a symmetric positive-definite matrix.
    *
    * At return, the solution is returned in B, and the Cholesky
    * decomposition is returned in A.
    *
    * @param A A symmetric positive-definite matrix.
    * @param B A Matrix with as many rows as A and any number of columns.
    */
  def symmetricPositiveDefiniteSolve(A: DenseMatrix, B: DenseMatrix): Unit = {
    val n = A.numCols()
    val nrhs = B.numCols()
    val info = new intW(0)
    require(A.isSymmetric, "A is not symmetric")
    lapack.dposv("U", n, nrhs, A.data, n, B.data, n, info)
    // check solution
    if (info.`val` > 0) {
      throw new IllegalArgumentException("A is not positive definite.")
    } else if (info.`val` < 0) {
      throw new RuntimeException("Invalid input to lapack routine.")
    }
  }

  /**
    * Solve Ax = B, where A is a symmetric indefinite matrix.
    *
    * At return, the solution is returned in B, and the LDL
    * decomposition is returned in A.
    *
    * @param A A symmetric indefinite non-singular matrix.
    * @param B A Matrix with as many rows as A and any number of columns.
    */
  def symmetricIndefiniteSolve(A: DenseMatrix, B: DenseMatrix): Unit = {
    val n = A.numCols()
    val nrhs = B.numCols()
    val info = new intW(0)
    require(A.isSymmetric, "A is not symmetric")
    val ipiv = new Array[Int](n)

    // Calculate optimal size of work data 'work'
    val work = new Array[Double](1)
    lapack.dsysv("U", n, nrhs, A.data, n, ipiv, B.data, n, work, -1, info)

    // do Solve
    val lwork = if (info.`val` != 0) n else work(0).toInt
    var workspace = new Array[Double](lwork)
    lapack.dsysv("U", n, nrhs, A.data, n, ipiv, B.data, n, work, lwork, info)

    // check solution
    if (info.`val` > 0) {
      throw new IllegalArgumentException("A is singular.")
    } else if (info.`val` < 0) {
      throw new RuntimeException("Invalid input to lapack routine.")
    }
  }

  /**
    * Solve Ax = B, where A is a squared non-singular matrix.
    *
    * At return, the solution is returned in B, and the LU
    * decomposition is returned in A.
    *
    * @param A A squared non-singular matrix.
    * @param B A Matrix with as many rows as A and any number of columns.
    */
  def nonSymmetricSolve(A: DenseMatrix, B: DenseMatrix): Unit = {
    val n = A.numCols()
    val nrhs = B.numCols()
    val info = new intW(0)
    val ipiv = new Array[Int](n)
    lapack.dgesv(n, nrhs, A.data, n, ipiv, B.data, n, info)
    // check solution
    if (info.`val` > 0) {
      throw new IllegalArgumentException("A is singular.")
    } else if (info.`val` < 0) {
      throw new RuntimeException("Invalid input to lapack routine.")
    }
  }

  /**
    * Solve Ax = B, whre A is m x n matrix and m <= n.
    * The solution is returned in B, while LQ decomposition is returned in A.
    *
    * The system is under determined, we find x that satisfies the equations
    * as well as having minimized L2 norm.
    *
    * @param A
    * @param B
    */
  def underDeterminedSolve(A: DenseMatrix, B: DenseMatrix): Unit = {
    val m = A.numRows()
    val n = A.numCols()
    val nrhs = B.numCols()
    val info = new intW(0)
    require(m <= n, "A should have no more number of rows than number of columns")
    require(B.numRows() == A.numCols())

    // Calculate optimal size of work data 'work'
    val work = new Array[Double](1)
    lapack.dgels("N", m, n, nrhs, A.data, m, B.data, n, work, -1, info)

    // do Solve
    val defaultlwork = n + Math.max(nrhs, n)
    val lwork = if (info.`val` != 0) defaultlwork else work(0).toInt
    val workspace = new Array[Double](lwork)
    lapack.dgels("N", m, n, nrhs, A.data, m, B.data, n, workspace, lwork, info)

    // check solution
    if (info.`val` > 0) {
      throw new IllegalArgumentException("A is rank deficient.")
    } else if (info.`val` < 0) {
      throw new RuntimeException("Invalid input to lapack routine.")
    }
  }

  /**
    * Solve Ax=B, where A is m x n matrix and m >= n.
    * A solution will be returned even when A is rank deficient.
    *
    * @param A
    * @param B
    */
  def roubustSolve(A: DenseMatrix, B: DenseMatrix): DenseMatrix = {
    val m = A.numRows()
    val n = A.numCols()
    val nrhs = B.numCols()
    require(m >= n)

    val l1norm = new DenseVector(B.data).normL1()
    if (l1norm == 0)
      return DenseMatrix.eye(n, nrhs)

    val svd = new SingularValueDecomposition(A)
    val s = svd.getSingularValues()
    val invTranS = DenseMatrix.zeros(n, n)
    val eps = Math.pow(2.0, -52.0)
    val tol = Math.max(m, n) * s.get(0) * eps
    for (i <- 0 until n) {
      if (s.get(i) > tol)
        invTranS.set(i, i, 1.0 / s.get(i))
    }

    val V = svd.getV()
    val U = svd.getU().getSubMatrix(0, m, 0, n)
    val VST = V.times(invTranS) // n x n
    val UTB = DenseMatrix.zeros(n, nrhs)
    MatVecOp.gemm(1.0, U, true, B, false, 0.0, UTB)
    VST.times(UTB)
  }
}
