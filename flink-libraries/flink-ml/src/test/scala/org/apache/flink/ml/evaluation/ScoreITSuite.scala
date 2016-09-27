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


package org.apache.flink.ml.evaluation

import org.apache.flink.api.scala._
import org.apache.flink.ml.util.FlinkTestBase
import org.scalatest.{FlatSpec, Matchers}


class ScoreITSuite extends FlatSpec with Matchers with FlinkTestBase {

  behavior of "Evaluation Score functions"

  it should "work for squared loss" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val yy = env.fromCollection(Seq((0.0, 1.0), (0.0, 0.0), (3.0, 5.0)))

    val loss = RegressionScores.squaredLoss

    val result = loss.evaluate(yy).collect()

    result.length shouldBe 1
    result.head shouldBe (1.6666666666 +- 1e-4)
  }

  it should "work for zero one loss" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val yy = env.fromCollection(Seq(1.0 -> 1.0, 2.0 -> 2.0, 3.0 -> 4.0, 4.0 -> 5.0))

    val loss = ClassificationScores.zeroOneLoss

    val result = loss.evaluate(yy).collect()

    result.length shouldBe 1
    result.head shouldBe (0.5 +- 1e9)
  }

  it should "work for zero one loss applied to signs" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val yy = env.fromCollection(Seq[(Double,Double)](
      -2.3 -> 2.3, -1.0 -> -10.5, 2.0 -> 3.0, 4.0 -> -5.0))

    val loss = RegressionScores.zeroOneSignumLoss

    val result = loss.evaluate(yy).collect()

    result.length shouldBe 1
    result.head shouldBe (0.5 +- 1e9)
  }

  it should "work for accuracy score" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val yy = env.fromCollection(Seq(0.0 -> 0.0, 1.0 -> 1.0, 2.0 -> 2.0, 3.0 -> 2.0))

    val accuracyScore = ClassificationScores.accuracyScore

    val result = accuracyScore.evaluate(yy).collect()

    result.length shouldBe 1
    result.head shouldBe (0.75 +- 1e9)
  }

  it should "calculate the R2 score correctly" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    // List of 50 (i, i + 1.0) tuples, where i the index
    val valueList = Range.Double(0.0, 50.0, 1.0) zip Range.Double(0.0, 50.0, 1.0).map(_ + 1)

    val yy = env.fromCollection(valueList)

    val r2 = RegressionScores.r2Score

    val result = r2.evaluate(yy).collect()

    result.length shouldBe 1
    result.head shouldBe (0.99519807923169268 +- 1e9)
  }

  it should "calculate the R2 score correctly for edge cases" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    // List of 50 (0.0, 1.0) tuples
    val valueList = Array.ofDim[Double](50) zip Array.ofDim[Double](50).map(_ + 1.0)

    val yy = env.fromCollection(valueList)

    val r2 = RegressionScores.r2Score

    val result = r2.evaluate(yy).collect()

    result.length shouldBe 1
    result.head shouldBe (0.0 +- 1e9)
  }
}
