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

object LeastSquareSolver {
  /**
    * Find x that minimize ||Ax-B||^2^, where A is a full rank rectangular matrix.
    *
    * At return, the solution is returned in B, and the QR
    * decomposition is returned in A.
    *
    * @param A A full rank rectangular matrix.
    * @param B A Matrix with as many rows as A and any number of columns.
    */
  def solve(A: DenseMatrix, B: DenseMatrix): Unit = {
    val m = A.numRows()
    val n = A.numCols()
    val nrhs = B.numCols()
    val info = new intW(0)
    require(m >= n, "A should have no less number of rows than number of columns")

    // Calculate optimal size of work data 'work'
    val work = new Array[Double](1)
    lapack.dgels("N", m, n, nrhs, A.data, m, B.data, m, work, -1, info)

    // do Solve
    val lwork = if (info.`val` != 0) n else work(0).toInt
    var workspace = new Array[Double](lwork)
    lapack.dgels("N", m, n, nrhs, A.data, m, B.data, m, workspace, lwork, info)

    // check solution
    if (info.`val` > 0) {
      throw new IllegalArgumentException("A is rank deficient.")
    } else if (info.`val` < 0) {
      throw new RuntimeException("Invalid input to lapack routine.")
    }
  }
}
