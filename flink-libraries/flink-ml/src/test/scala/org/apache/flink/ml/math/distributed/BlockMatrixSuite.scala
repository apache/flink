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
import org.apache.flink.ml.math.SparseMatrix
import org.apache.flink.test.util.FlinkTestBase
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

class BlockMatrixSuite
    extends FlatSpec
    with Matchers
    with GivenWhenThen
    with FlinkTestBase {

  val env = ExecutionEnvironment.getExecutionEnvironment

  val rawSampleData = List(
      (0, 0, 3.0),
      (1, 0, 1.0),
      (3, 1, 4.0),
      (3, 3, 12.0)
  )

  val bm1 = DistributedRowMatrix
    .fromCOO(env.fromCollection(rawSampleData), 4, 4)
    .toBlockMatrix(3, 3)

  val rawSampleData2 = List(
      (0, 0, 2.0),
      (1, 1, 1.0),
      (1, 2, 12.0),
      (3, 2, 35.0)
  )

  val bm2 = DistributedRowMatrix
    .fromCOO(env.fromCollection(rawSampleData2), 4, 4)
    .toBlockMatrix(3, 3)

  "multiply" should "correctly multiply two matrices" in {
    val result = bm1.multiply(bm2)
    result.toRowMatrix.toCOO.toSet.filter(_._3 != 0) shouldBe Set(
        (0, 0, 6.0),
        (1, 0, 2.0),
        (3, 1, 4.0),
        (3, 2, 468.0)
    )
    result.numRows shouldBe 4
    result.numCols shouldBe 4
  }

  "sum" should "correctly sum two matrices" in {

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

    val sumBlockMatrix1 = DistributedRowMatrix
      .fromCOO(env.fromCollection(rawSampleSum1), 10, 10)
      .toBlockMatrix(5, 5)
    val sumBlockMatrix2 = DistributedRowMatrix
      .fromCOO(env.fromCollection(rawSampleSum2), 10, 10)
      .toBlockMatrix(5, 5)

    val expected = SparseMatrix.fromCOO(10,
                                        10,
                                        List(
                                            (0, 0, 3.0),
                                            (0, 1, 8.0),
                                            (3, 4, 4.0),
                                            (2, 8, 20.0),
                                            (7, 4, 3.0)
                                        ))
    val result =
      sumBlockMatrix1.sum(sumBlockMatrix2).toRowMatrix.toLocalDenseMatrix

    result shouldBe expected.toDenseMatrix
    result.numRows shouldBe sumBlockMatrix1.numRows
    result.numCols shouldBe sumBlockMatrix1.numCols
  }
}
