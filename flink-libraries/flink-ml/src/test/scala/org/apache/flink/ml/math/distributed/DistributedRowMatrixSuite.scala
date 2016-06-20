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

package org.apache.flink.ml.math.distributed

import org.apache.flink.api.scala._
import org.apache.flink.ml.util.FlinkTestBase
import org.scalatest.{FlatSpec, Matchers}

class DistributedRowMatrixSuite extends FlatSpec with Matchers with FlinkTestBase {
  behavior of "DistributedRowMatrix"

  val rawSampleData = List(
    (0, 0, 3.0),
    (0, 1, 3.0),
    (0, 3, 4.0),
    (2, 3, 4.0),
    (1, 4, 3.0),
    (1, 1, 3.0),
    (2, 1, 3.0),
    (2, 2, 3.0)
  )

  it should "contain the initialization data" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val rowDataset = env.fromCollection(rawSampleData)
    val dmatrix = DistributedRowMatrix.fromCOO(rowDataset, 3, 5)

    dmatrix.toCOO.toSet.filter(_._3 != 0) shouldBe rawSampleData.toSet
  }

  it should "return the correct dimensions when provided by the user" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val rowDataset = env.fromCollection(rawSampleData)
    val dmatrix = DistributedRowMatrix.fromCOO(rowDataset, 3, 5)

    dmatrix.numCols shouldBe 5
    dmatrix.numRows shouldBe 3
  }


  it should "return a sparse local matrix containing the initialization data" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val rowDataset = env.fromCollection(rawSampleData)
    val dmatrix = DistributedRowMatrix.fromCOO(rowDataset, 3, 5)

    dmatrix.toLocalSparseMatrix.iterator.filter(_._3 != 0).toSet shouldBe rawSampleData.toSet
  }

  it should "return a dense local matrix containing the initialization data" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val rowDataset = env.fromCollection(rawSampleData)
    val dmatrix = DistributedRowMatrix.fromCOO(rowDataset, 3, 5)

    dmatrix.toLocalDenseMatrix.iterator.filter(_._3 != 0).toSet shouldBe rawSampleData.toSet
  }

  "add" should "correctly add two distributed row matrices" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val rawSampleSum1 = List(
      (0, 0, 1.0),
      (7, 4, 3.0),
      (0, 1, 8.0),
      (2, 8, 12.0)
    )

    val rawSampleSum2 = List(
      (0, 0, 2.0),
      (3, 4, 4.0),
      (2, 8, 8.0)
    )

    val addBlockMatrix1 = DistributedRowMatrix.fromCOO(env.fromCollection(rawSampleSum1), 10, 10)
    val addBlockMatrix2 = DistributedRowMatrix.fromCOO(env.fromCollection(rawSampleSum2), 10, 10)

    val expected = List(
      (0, 0, 3.0),
      (0, 1, 8.0),
      (3, 4, 4.0),
      (2, 8, 20.0),
      (7, 4, 3.0)
    )
    val result = addBlockMatrix1
      .add(addBlockMatrix2)
      .toLocalSparseMatrix
      .filter(_._3 != 0.0)
    result.toSet shouldEqual expected.toSet
  }
}
