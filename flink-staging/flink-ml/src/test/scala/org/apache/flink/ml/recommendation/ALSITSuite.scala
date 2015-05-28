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

class ALSITSuite
  extends FlatSpec
  with Matchers
  with FlinkTestBase {

  override val parallelism = 2

  behavior of "The alternating least squares (ALS) implementation"

  it should "properly factorize a matrix" in {
    import Recommendation._

    val env = ExecutionEnvironment.getExecutionEnvironment

    val als = ALS()
      .setIterations(iterations)
      .setLambda(lambda)
      .setBlocks(4)
      .setNumFactors(numFactors)

    val inputDS = env.fromCollection(data)

    als.fit(inputDS)

    val testData = env.fromCollection(expectedResult.map{
      case (userID, itemID, rating) => (userID, itemID)
    })

    val predictions = als.predict(testData).collect()

    predictions.length should equal(expectedResult.length)

    val resultMap = expectedResult map {
      case (uID, iID, value) => (uID, iID) -> value
    } toMap

    predictions foreach {
      case (uID, iID, value) => {
        resultMap.isDefinedAt(((uID, iID))) should be(true)

        value should be(resultMap((uID, iID)) +- 0.1)
      }
    }

    val risk = als.empiricalRisk(inputDS).collect().apply(0)

    risk should be(expectedEmpiricalRisk +- 1)
  }
}
