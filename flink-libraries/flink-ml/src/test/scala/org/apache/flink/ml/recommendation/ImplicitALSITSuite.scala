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

import org.apache.flink.api.scala._
import org.apache.flink.core.testutils.CommonTestUtils
import org.apache.flink.ml.util.FlinkTestBase
import org.scalatest._

import scala.language.postfixOps

class ImplicitALSITSuite
  extends FlatSpec
    with Matchers
    with FlinkTestBase {

  override val parallelism = 2

  behavior of "The modification of the alternating least squares (ALS) implementation" +
    "for implicit feedback datasets."

  it should "properly factorize matrix" in {
    import Recommendation._

    val env = ExecutionEnvironment.getExecutionEnvironment

    // temporary directory to avoid too few memory segments
    val tempDir = CommonTestUtils.getTempDir + "/"

    // factorize matrix with implicit ALS
    val als = ALS()
      .setIterations(implicitIterations)
      .setLambda(implicitLambda)
      .setBlocks(implicitBlocks)
      .setNumFactors(implicitFactors)
      .setImplicit(true)
      .setAlpha(implicitAlpha)
      .setSeed(implicitSeed)
      .setTemporaryPath(tempDir)

    val inputDS = env.fromCollection(implicitRatings)

    als.fit(inputDS)

    // check predictions on some user-item pairs
    val testData = env.fromCollection(implicitExpectedResult.map{
      case (userID, itemID, rating) => (userID, itemID)
    })

    val predictions = als.predict(testData).collect()

    predictions.length should equal(implicitExpectedResult.length)

    val resultMap = implicitExpectedResult map {
      case (uID, iID, value) => (uID, iID) -> value
    } toMap

    predictions foreach {
      case (uID, iID, value) => {
        resultMap.isDefinedAt((uID, iID)) should be(true)

        value should be(resultMap((uID, iID)) +- 1e-5)
      }
    }

  }

}
