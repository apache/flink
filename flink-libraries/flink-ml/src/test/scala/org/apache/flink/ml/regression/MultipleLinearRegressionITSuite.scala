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

import org.apache.flink.api.scala._
import org.apache.flink.ml.common.{ParameterMap, WeightVector}
import org.apache.flink.ml.preprocessing.PolynomialFeatures
import org.apache.flink.ml.util.FlinkTestBase
import org.scalatest.{FlatSpec, Matchers}

class MultipleLinearRegressionITSuite
  extends FlatSpec
  with Matchers
  with FlinkTestBase {

  behavior of "The multiple linear regression implementation"

  it should "estimate a linear function" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)

    val mlr = MultipleLinearRegression()

    import RegressionData._

    val parameters = ParameterMap()

    parameters.add(MultipleLinearRegression.Stepsize, 2.0)
    parameters.add(MultipleLinearRegression.Iterations, 10)
    parameters.add(MultipleLinearRegression.ConvergenceThreshold, 0.001)

    val inputDS = env.fromCollection(data)
    mlr.fit(inputDS, parameters)

    val weightList = mlr.weightsOption.get.collect()

    weightList.size should equal(1)

    val WeightVector(weights, intercept) = weightList.head

    expectedWeights.toIterator zip weights.valueIterator foreach {
      case (expectedWeight, weight) =>
        weight should be (expectedWeight +- 1)
    }
    intercept should be (expectedWeight0 +- 0.4)

    val srs = mlr.squaredResidualSum(inputDS).collect().head

    srs should be (expectedSquaredResidualSum +- 2)
  }

  it should "work with sparse vectors as input" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val mlr = MultipleLinearRegression()

    val sparseInputDS = env.fromCollection(RegressionData.sparseData)

    val parameters = ParameterMap()

    parameters.add(MultipleLinearRegression.Stepsize, 2.0)
    parameters.add(MultipleLinearRegression.Iterations, 10)
    parameters.add(MultipleLinearRegression.ConvergenceThreshold, 0.001)

    mlr.fit(sparseInputDS, parameters)

    val weightList = mlr.weightsOption.get.collect()

    val WeightVector(weights, intercept) = weightList.head

    RegressionData.expectedWeightsSparseInput.toIterator zip weights.valueIterator foreach {
      case (expectedWeight, weight) =>
        weight should be (expectedWeight +- 1)
    }
    intercept should be (RegressionData.expectedInterceptSparseInput +- 0.4)
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
      .add(MultipleLinearRegression.Stepsize, 0.004)
      .add(MultipleLinearRegression.Iterations, 100)

    pipeline.fit(inputDS, parameters)

    val weightList = mlr.weightsOption.get.collect()

    weightList.size should equal(1)

    val WeightVector(weights, intercept) = weightList.head

    RegressionData.expectedPolynomialWeights.toIterator.zip(weights.valueIterator) foreach {
      case (expectedWeight, weight) =>
        weight should be(expectedWeight +- 0.1)
    }

    intercept should be(RegressionData.expectedPolynomialWeight0 +- 0.1)

    val transformedInput = polynomialBase.transform(inputDS, parameters)

    val srs = mlr.squaredResidualSum(transformedInput).collect().head

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
    val evaluationDS = inputDS.map(x => (x.vector, x.label))

    mlr.fit(inputDS, parameters)

    val predictionPairs = mlr.evaluate(evaluationDS)

    val absoluteErrorSum = predictionPairs.collect().map{
      case (truth, prediction) => Math.abs(truth - prediction)}.sum

    absoluteErrorSum should be < 50.0
  }
}
