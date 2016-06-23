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
import org.apache.flink.ml.math.{SparseMatrix, SparseVector}
import org.apache.flink.test.util.FlinkTestBase
import org.scalatest.{FlatSpec, Matchers}

class MatrixConversionsSuite
    extends FlatSpec
    with Matchers
    with FlinkTestBase {


  "DistributedRowMatrix.toBlockMatrix" should "preserve the matrix structure after conversion" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val rawSampleData = List(
        (0, 0, 3.0),
        (0, 1, 1.0),
        (0, 3, 4.0),
        (2, 3, 60.0),
        (1, 2, 50.0),
        (1, 1, 12.0),
        (2, 1, 14.0),
        (3, 2, 18.0)
    )

    val d1 =
      DistributedRowMatrix.fromCOO(env.fromCollection(rawSampleData), 4, 4)
    //val blockMatrix = d1.toBlockMatrix(2, 2).getDataset.collect

    val block0 = Block.apply(
        SparseMatrix.fromCOO(2,
                             2,
                             List(
                                 (0, 0, 3.0),
                                 (1, 0, 0.0),
                                 (0, 1, 1.0),
                                 (1, 1, 12.0)
                             )))

    val block1 = Block.apply(
        SparseMatrix.fromCOO(2,
                             2,
                             List(
                                 (0, 0, 0.0),
                                 (1, 0, 50.0),
                                 (0, 1, 4.0),
                                 (1, 1, 0.0)
                             )))

    val block2 = Block.apply(
        SparseMatrix.fromCOO(2,
                             2,
                             List(
                                 (0, 0, 0.0),
                                 (1, 0, 0.0),
                                 (0, 1, 14.0),
                                 (1, 1, 0.0)
                             )))

    val block3 = Block.apply(
        SparseMatrix.fromCOO(2,
                             2,
                             List(
                                 (0, 0, 0.0),
                                 (1, 0, 18.0),
                                 (0, 1, 60.0),
                                 (1, 1, 0.0)
                             )))

    //blockMatrix.toSet shouldBe Set(
    //    (0, block0), (1, block1), (2, block2), (3, block3))

    val data2 = (39, 39, 123.0) :: (5, 30, 42.0) :: rawSampleData
    val d2 = DistributedRowMatrix.fromCOO(env.fromCollection(data2), 40, 40)
    val blockMatrix2 = d2.toBlockMatrix(3, 20)
    val dataMap = blockMatrix2.getDataset.collect().toMap

    dataMap(3).getBlockData(2, 10) shouldBe 42.0

    dataMap(blockMatrix2.getNumBlocks - 1).getBlockData(0, 19) shouldBe 123.0

    d2.numCols shouldBe blockMatrix2.numCols
    d2.numRows shouldBe blockMatrix2.numRows
  }

  "BlockMatrix.toRowMatrix" should "preserve the matrix structure after conversion" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val block0 = Block.apply(
        SparseMatrix.fromCOO(2,
                             2,
                             List(
                                 (0, 0, 3.0),
                                 (1, 0, 0.0),
                                 (0, 1, 1.0),
                                 (1, 1, 12.0)
                             )))

    val block1 = Block.apply(
        SparseMatrix.fromCOO(2,
                             2,
                             List(
                                 (0, 0, 0.0),
                                 (1, 0, 50.0),
                                 (0, 1, 4.0),
                                 (1, 1, 0.0)
                             )))

    val block2 = Block.apply(
        SparseMatrix.fromCOO(2,
                             2,
                             List(
                                 (0, 0, 0.0),
                                 (1, 0, 0.0),
                                 (0, 1, 14.0),
                                 (1, 1, 0.0)
                             )))

    val block3 = Block.apply(
        SparseMatrix.fromCOO(2,
                             2,
                             List(
                                 (0, 0, 0.0),
                                 (1, 0, 18.0),
                                 (0, 1, 60.0),
                                 (1, 1, 0.0)
                             )))

    val blockMatrix = new BlockMatrix(env.fromElements(
                                          (0, block0),
                                          (1, block1),
                                          (2, block2),
                                          (3, block3)
                                      ),
                                      new BlockMapper(4, 4, 2, 2))

    blockMatrix.toRowMatrix.data.collect() shouldBe List(
        IndexedRow(1, SparseVector.fromCOO(4, List((1, 12.0), (2, 50.0)))),
        IndexedRow(
            0, SparseVector.fromCOO(4, List((0, 3.0), (1, 1.0), (3, 4.0)))),
        IndexedRow(2, SparseVector.fromCOO(4, List((1, 14.0), (3, 60.0)))),
        IndexedRow(3, SparseVector.fromCOO(4, List((2, 18.0)))))
  }
}
