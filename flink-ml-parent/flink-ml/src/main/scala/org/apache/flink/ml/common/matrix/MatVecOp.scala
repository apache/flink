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

import breeze.linalg.{inv, DenseMatrix => BDM, DenseVector => BDV}
import com.github.fommil.netlib.BLAS.{getInstance => blas}

object MatVecOp extends Serializable {

  private[matrix] def toBreezeMatrix(A: DenseMatrix): BDM[Double] = {
    new BDM[Double](A.numRows, A.numCols, A.data, 0, A.numRows, false)
  }

  private[matrix] def toBreezeVector(v: DenseVector): BDV[Double] = {
    new BDV[Double](v.getData())
  }

  private[matrix] def fromBreezeMatrix(A: BDM[Double]): DenseMatrix = {
    assert(A.offset == 0)
    assert(!A.isTranspose)
    assert(!A.isTranspose || A.majorStride == A.cols)
    assert(A.isTranspose || A.majorStride == A.rows)
    DenseMatrix.fromDataBuffer(A.rows, A.cols, A.data)
  }

  private[matrix] def fromBreezeVector(v: BDV[Double]): DenseVector = {
    assert(v.stride == 1)
    assert(v.offset == 0)
    val vector = new DenseVector()
    vector.setData(v.data)
    vector
  }

  /**
    * C := alpha * A * B + beta * C
    */
  def gemm(alpha: Double,
           A: DenseMatrix, transA: Boolean,
           B: DenseMatrix, transB: Boolean,
           beta: Double,
           C: DenseMatrix): Unit = {
    if (transA) require(A.numCols() == C.numRows())
    else require(A.numRows() == C.numRows())
    if (transB) require(B.numRows() == C.numCols())
    else require(B.numCols() == C.numCols())

    val m = C.numRows()
    val n = C.numCols()
    val k = if (transA) A.numRows() else A.numCols()
    val lda = A.numRows()
    val ldb = B.numRows()
    val ldc = C.numRows()
    val ta = if (transA) "T" else "N"
    val tb = if (transB) "T" else "N"
    blas.dgemm(ta, tb, m, n, k, alpha, A.data, lda, B.data, ldb, beta, C.data, ldc)
  }

  /**
    * y := alpha * A * x + beta * y
    */
  def gemv(alpha: Double,
           A: DenseMatrix, transA: Boolean,
           x: DenseVector,
           beta: Double,
           y: DenseVector): Unit = {
    if (transA) require(A.numCols() == y.size()) else require(A.numRows() == y.size())
    if (transA) require(A.numRows() == x.size()) else require(A.numCols() == x.size())
    val m = A.numRows()
    val n = A.numCols()
    val lda = A.numRows()
    val ta = if (transA) "T" else "N"
    blas.dgemv(ta, m, n, alpha, A.data, lda, x.getData, 1, beta, y.getData, 1)
  }

  /**
    * Inverse matrix A.
    */
  def inverse(A: DenseMatrix): DenseMatrix = {
    val invA = inv(toBreezeMatrix(A))
    fromBreezeMatrix(invA)
  }

  /**
    * Return the determinant of A
    *
    * @param A
    * @return
    */
  def det(A: DenseMatrix): Double = {
    breeze.linalg.det(toBreezeMatrix(A))
  }

  /**
    * Return the rank of A
    *
    * @param A
    * @return
    */
  def rank(A: DenseMatrix): Int = {
    breeze.linalg.rank(toBreezeMatrix(A))
  }
}
