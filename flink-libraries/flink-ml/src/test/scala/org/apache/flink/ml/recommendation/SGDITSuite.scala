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

class SGDITSuite
  extends FlatSpec
    with Matchers
    with FlinkTestBase {

  override val parallelism = 2

  behavior of "The distributed stochastic gradient descent (DSGD) implementation" +
    " for matrix factorization."

  it should "properly factorize a matrix" in {
    import Recommendation._

    val env = ExecutionEnvironment.getExecutionEnvironment

    val dsgd = SGDforMatrixFactorization()
      .setIterations(iterations)
      .setLambda(lambda)
      .setBlocks(3)
      .setNumFactors(numFactors)
      .setLearningRate(0.001)
      .setSeed(43L)

    val inputDS = env.fromCollection(data)

    dsgd.fit(inputDS)

    val testData = env.fromCollection(expectedResultSGD.map {
      case (userID, itemID, rating) => (userID, itemID)
    })

    val predictions = dsgd.predict(testData).collect()

    // fixme: remove these from the final version
    val userFacts = dsgd.factorsOption.get._1.collect
    val itemFacts = dsgd.factorsOption.get._2.collect
    predictions.foreach(println)
    println("------------------")
    // fixme: remove these from the final version

    predictions.length should equal(expectedResultSGD.length)

    val resultMap = expectedResultSGD map {
      case (uID, iID, value) => (uID, iID) -> value
    } toMap

    predictions foreach {
      case (uID, iID, value) => {
        resultMap.isDefinedAt((uID, iID)) should be(true)

        value should be(resultMap((uID, iID)) +- 0.1)
      }
    }
  }
}
