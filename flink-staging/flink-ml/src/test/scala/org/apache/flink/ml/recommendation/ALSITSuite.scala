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

import scala.language.postfixOps

import org.scalatest._

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.test.util.FlinkTestBase
import Recommendation._

class ALSITSuite
  extends FlatSpec
  with Matchers
  with FlinkTestBase {

  override val parallelism = 2

  behavior of "The alternating least squares (ALS) implementation"

  def fixture = new {


    val env = ExecutionEnvironment.getExecutionEnvironment

    val als = ALS()
      .setIterations(iterations)
      .setLambda(lambda)
      .setBlocks(4)
      .setNumFactors(numFactors)
      .setSeed(0L)

    val inputDS = env.fromCollection(data)

    als.fit(inputDS)

    val evaluationData = env.fromCollection(expectedResult)

    val testData = evaluationData.map(idsAndRating => (idsAndRating._1 , idsAndRating._2))
  }

  it should "properly factorize a matrix" in {

    val f =  fixture

    val predictions = f.als.predict(f.testData).collect()

    predictions.length should equal(expectedResult.length)

    val resultMap = expectedResult map {
      case (uID, iID, rating) => (uID, iID) -> rating
    } toMap

    predictions foreach {
      case (uID, iID, rating) => {
        resultMap.isDefinedAt((uID, iID)) should be (true)

        rating should be(resultMap((uID, iID)) +- 0.1)
      }
    }

    val risk = f.als.empiricalRisk(f.inputDS).collect().head

    risk should be (expectedEmpiricalRisk +- 1)
  }

  it should "be possible to get a score for the factorization" in {
    val f = fixture

    val rmse: Double = f.als.score(f.evaluationData).collect().head

    rmse should be < 2.0
  }
}
