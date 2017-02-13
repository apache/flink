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

import org.scalatest.{Matchers, FlatSpec}

class NormSuite extends FlatSpec with Matchers {

  it should "calculate L^1 norm correctly for a sparse/dense vector" in {
    val data = Array.fill(16)(1)
    val vectorD = DenseVector(data)
    val vectorS = SparseVector.fromCOO(5, (0,1))
    assertResult(Norm.norm(vectorD, 1))(16)
    assertResult(Norm.norm(vectorS, 1))(1)
  }

  it should "calculate L^1 norm correctly with increasing numbers for a dense vector" in {
    val N = 100
    val data = Array.range(1, N + 1)
    val vector = DenseVector(data)
    assertResult(Norm.norm(vector, 1))(N*(N + 1)/2)
  }

  it should "calculate L^1 norm correctly for a dense vector with negative elements" in {
    val data = Array(-1, 1, -1, 1)
    val vectorD = DenseVector(data)
    val vectorS = SparseVector.fromCOO(4, (0,1), (1,-1))
    assertResult(Norm.norm(vectorD,1))(data.length)
    assertResult(Norm.norm(vectorS,1))(2)
  }

  it should "calculate L^2 norm correctly for a sparse/dense vector" in {
    val dataD = Array.fill(16)(1)
    val vectorD = DenseVector(dataD)
    val vectorS = SparseVector.fromCOO(16, (0,4), (1,1), (10,2), (5,2))
    assertResult(Norm.norm(vectorD))(4)
    assertResult(Norm.norm(vectorS))(5)
  }
}

