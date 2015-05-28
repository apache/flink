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
import org.apache.flink.ml.preprocessing.PolynomialFeatures
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

    val mlr = MultipleLinearRegression()

    import RegressionData._

    val parameters = ParameterMap()

    parameters.add(MultipleLinearRegression.Stepsize, 1.0)
    parameters.add(MultipleLinearRegression.Iterations, 10)
    parameters.add(MultipleLinearRegression.ConvergenceThreshold, 0.001)

    val inputDS = env.fromCollection(data)
    mlr.fit(inputDS, parameters)

    val weightList = mlr.weightsOption.get.collect()

    weightList.size should equal(1)

    val (weights, weight0) = weightList(0)

    expectedWeights zip weights foreach {
      case (expectedWeight, weight) =>
        weight should be (expectedWeight +- 1)
    }
    weight0 should be (expectedWeight0 +- 0.4)

    val srs = mlr.squaredResidualSum(inputDS).collect().apply(0)

    srs should be (expectedSquaredResidualSum +- 2)
  }

  it should "estimate a cubic function" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)

    val polynomialBase = PolynomialFeatures()
    val mlr = MultipleLinearRegression()

    val pipeline = polynomialBase.chainPredictor(mlr)

    val inputDS = env.fromCollection(RegressionData.polynomialData)

    val parameters = ParameterMap()
      .add(PolynomialFeatures.Degree, 3)
      .add(MultipleLinearRegression.Stepsize, 0.002)
      .add(MultipleLinearRegression.Iterations, 100)

    pipeline.fit(inputDS, parameters)

    val weightList = mlr.weightsOption.get.collect()

    weightList.size should equal(1)

    val (weights, weight0) = weightList(0)

    RegressionData.expectedPolynomialWeights.zip(weights) foreach {
      case (expectedWeight, weight) =>
        weight should be(expectedWeight +- 0.1)
    }

    weight0 should be(RegressionData.expectedPolynomialWeight0 +- 0.1)

    val transformedInput = polynomialBase.transform(inputDS, parameters)

    val srs = mlr.squaredResidualSum(transformedInput).collect().apply(0)

    srs should be(RegressionData.expectedPolynomialSquaredResidualSum +- 5)
  }

  it should "make (mostly) correct predictions" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val mlr = MultipleLinearRegression()

    import RegressionData._

    val parameters = ParameterMap()

    parameters.add(MultipleLinearRegression.Stepsize, 1.0)
    parameters.add(MultipleLinearRegression.Iterations, 10)
    parameters.add(MultipleLinearRegression.ConvergenceThreshold, 0.001)

    val inputDS = env.fromCollection(data)
    mlr.fit(inputDS, parameters)

    val predictionPairs = mlr.predict(inputDS)

    val absoluteErrorSum = predictionPairs.collect().map{
      case (truth, prediction) => Math.abs(truth - prediction)}.sum

    absoluteErrorSum should be < 50.0
  }
}
