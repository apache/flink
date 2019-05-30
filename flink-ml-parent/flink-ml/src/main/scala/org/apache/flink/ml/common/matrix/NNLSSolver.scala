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

import breeze.optimize.linear.NNLS
import org.apache.flink.ml.common.matrix.MatVecOp.{fromBreezeVector, toBreezeMatrix, toBreezeVector}

object NNLSSolver {
  /**
    * Solve a least squares problem, possibly with nonnegativity constraints, by a modified
    * projected gradient method.  That is, find x minimising ||Ax - b||_2 given A^T A and A^T b,
    * subject to x >= 0.
    */
  def solve(ata: DenseMatrix, atb: DenseVector): DenseVector = {
    val m = ata.numRows()
    val n = ata.numCols()
    require(m == n, "m != n")

    val nnls = new NNLS()
    val ret = nnls.minimize(toBreezeMatrix(ata), toBreezeVector(atb))
    fromBreezeVector(ret)
  }
}
