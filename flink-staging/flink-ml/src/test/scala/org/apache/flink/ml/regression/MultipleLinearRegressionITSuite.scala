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
import org.apache.flink.ml.feature.PolynomialBase
import org.scalatest.{Matchers, FlatSpec}

import org.apache.flink.api.scala._
import org.apache.flink.test.util.FlinkTestBase

class MultipleLinearRegressionITSuite
  extends FlatSpec
  with Matchers
  with FlinkTestBase {

  behavior of "The multipe linear regression implementation"

  it should "estimate a linear function" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)

    val learner = MultipleLinearRegression()

    import RegressionData._

    val parameters = ParameterMap()

    parameters.add(MultipleLinearRegression.Stepsize, 1.0)
    parameters.add(MultipleLinearRegression.Iterations, 10)
    parameters.add(MultipleLinearRegression.ConvergenceThreshold, 0.001)

    val inputDS = env.fromCollection(data)
    val model = learner.fit(inputDS, parameters)

    val weightList = model.weights.collect()

    weightList.size should equal(1)

    val (weights, weight0) = weightList(0)

    expectedWeights zip weights foreach {
      case (expectedWeight, weight) =>
        weight should be (expectedWeight +- 1)
    }
    weight0 should be (expectedWeight0 +- 0.4)

    val srs = model.squaredResidualSum(inputDS).collect().apply(0)

    srs should be (expectedSquaredResidualSum +- 2)
  }

  it should "estimate a cubic function" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)

    val polynomialBase = PolynomialBase()
    val learner = MultipleLinearRegression()

    val pipeline = polynomialBase.chain(learner)

    val inputDS = env.fromCollection(RegressionData.polynomialData)

    val parameters = ParameterMap()
      .add(PolynomialBase.Degree, 3)
      .add(MultipleLinearRegression.Stepsize, 0.002)
      .add(MultipleLinearRegression.Iterations, 100)

    val model = pipeline.fit(inputDS, parameters)

    val weightList = model.weights.collect()

    weightList.size should equal(1)

    val (weights, weight0) = weightList(0)

    RegressionData.expectedPolynomialWeights.zip(weights) foreach {
      case (expectedWeight, weight) =>
        weight should be(expectedWeight +- 0.1)
    }

    weight0 should be(RegressionData.expectedPolynomialWeight0 +- 0.1)

    val transformedInput = polynomialBase.transform(inputDS, parameters)

    val srs = model.squaredResidualSum(transformedInput).collect().apply(0)

    srs should be(RegressionData.expectedPolynomialSquaredResidualSum +- 5)
  }
}
