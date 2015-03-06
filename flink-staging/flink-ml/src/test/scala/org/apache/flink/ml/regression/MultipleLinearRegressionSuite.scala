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

package org.apache.flink.ml.regression

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.ml.common.ParameterMap
import org.scalatest.{ShouldMatchers, FlatSpec}

import org.apache.flink.api.scala._

class MultipleLinearRegressionSuite extends FlatSpec with ShouldMatchers {
  behavior of "LinearRegression"

  it should "estimate the correct linear function" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val learner = MultipleLinearRegression()

    import RegressionData._

    val parameters = new ParameterMap

    parameters.add(MultipleLinearRegression.Stepsize, 1.0)
    parameters.add(MultipleLinearRegression.Iterations, 10)
    parameters.add(MultipleLinearRegression.ConvergenceThreshold, 0.001)

    val inputDS = env.fromCollection(data)
    val model = learner.fit(inputDS, parameters)

    val betasList = model.weights.collect

    betasList.size should equal(1)

    val (betas, beta0) = betasList(0)

    expectedBetas.data zip betas.data foreach {
      case (expectedBeta, beta) => {
        beta should be (expectedBeta +- 1)
      }
    }
    beta0 should be (expectedBeta0 +- 0.4)

    val srs = model.squaredResidualSum(inputDS).collect(0)

    srs should be (expectedSquaredResidualSum +- 2)
  }
}
