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

package org.apache.flink.ml.recommendation

import org.apache.flink.ml.util.FlinkTestBase
import org.scalatest._

import scala.language.postfixOps
import org.apache.flink.api.scala._

class ImplicitALSTest
  extends FlatSpec
    with Matchers
    with FlinkTestBase {

  override val parallelism = 2

  behavior of "The modification of the alternating least squares (ALS) implementation" +
    "for implicit feedback datasets."

  it should "properly compute X^T * X" in {
    import Recommendation._

    val rand = scala.util.Random
    val numBlocks = 3
    // randomly split matrix to blocks
    val blocksX = X
      // add a random block id to every row
      .map { row =>
        (rand.nextInt(numBlocks), row)
      }
      // get the block via grouping
      .groupBy(_._1).values
      // add a block id (-1) to each block
      .map(b => (-1, b.map(_._2)))
      .toSeq

    // use Flink to compute XtX
    val env = ExecutionEnvironment.getExecutionEnvironment

    val distribBlocksX = env.fromCollection(blocksX)

    val XtX = ALS
      .computeXtX(distribBlocksX, implicitFactors)
      .collect().head

    // check XtX size
    XtX.length should be (implicitFactors * (implicitFactors - 1) / 2 + implicitFactors)

    // check result is as expected
    expectedUpperTriangleXtX
      .zip(XtX)
      .foreach { case (expected, result) =>
        result should be (expected +- 0.1)
      }

  }

}
