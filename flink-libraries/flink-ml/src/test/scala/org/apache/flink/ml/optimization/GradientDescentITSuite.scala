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

package org.apache.flink.ml.optimization

import org.apache.flink.ml.common.{LabeledVector, WeightVector}
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.ml.regression.RegressionData._
import org.apache.flink.ml.util.FlinkTestBase
import org.scalatest.{Matchers, FlatSpec}

import org.apache.flink.api.scala._


class GradientDescentITSuite extends FlatSpec with Matchers with FlinkTestBase {

  // TODO(tvas): Check results again once sampling operators are in place

  behavior of "The Stochastic Gradient Descent implementation"

  it should "correctly solve an L1 regularized regression problem" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)

    val lossFunction = GenericLossFunction(SquaredLoss, LinearPrediction)

    val sgd = GradientDescent()
      .setStepsize(0.01)
      .setIterations(2000)
      .setLossFunction(lossFunction)
      .setRegularizationPenalty(L1Regularization)
      .setRegularizationConstant(0.3)
      
    val inputDS: DataSet[LabeledVector] = env.fromCollection(regularizationData)

    val weightDS = sgd.optimize(inputDS, None)

    val weightList: Seq[WeightVector] = weightDS.collect()

    val weightVector: WeightVector = weightList.head

    val intercept = weightVector.intercept
    val weights = weightVector.weights.asInstanceOf[DenseVector].data

    expectedRegWeights zip weights foreach {
      case (expectedWeight, weight) =>
        weight should be (expectedWeight +- 0.01)
    }

    intercept should be (expectedRegWeight0 +- 0.1)
  }

  it should "correctly perform one step with L2 regularization" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)

    val lossFunction = GenericLossFunction(SquaredLoss, LinearPrediction)

    val sgd = GradientDescent()
      .setStepsize(0.1)
      .setIterations(1)
      .setLossFunction(lossFunction)
      .setRegularizationPenalty(L2Regularization)
      .setRegularizationConstant(1.0)

    val inputDS: DataSet[LabeledVector] = env.fromElements(LabeledVector(1.0, DenseVector(2.0)))
    val currentWeights = new WeightVector(DenseVector(1.0), 1.0)
    val currentWeightsDS = env.fromElements(currentWeights)

    val weightDS = sgd.optimize(inputDS, Some(currentWeightsDS))

    val weightList: Seq[WeightVector] = weightDS.collect()

    weightList.size should equal(1)

    val WeightVector(updatedWeights, updatedIntercept) = weightList.head

    updatedWeights(0) should be (0.5 +- 0.001)
    updatedIntercept should be (0.8 +- 0.01)
  }

  it should "estimate a linear function" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)

    val lossFunction = GenericLossFunction(SquaredLoss, LinearPrediction)

    val sgd = GradientDescent()
      .setStepsize(1.0)
      .setIterations(800)
      .setLossFunction(lossFunction)

    val inputDS = env.fromCollection(data)
    val weightDS = sgd.optimize(inputDS, None)

    val weightList: Seq[WeightVector] = weightDS.collect()

    weightList.size should equal(1)

    val weightVector: WeightVector = weightList.head

    val weights = weightVector.weights.asInstanceOf[DenseVector].data
    val weight0 = weightVector.intercept

    expectedWeights zip weights foreach {
      case (expectedWeight, weight) =>
        weight should be (expectedWeight +- 0.1)
    }
    weight0 should be (expectedWeight0 +- 0.1)
  }

  it should "estimate a linear function without an intercept" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)

    val lossFunction = GenericLossFunction(SquaredLoss, LinearPrediction)

    val sgd = GradientDescent()
      .setStepsize(0.0001)
      .setIterations(100)
      .setLossFunction(lossFunction)

    val inputDS = env.fromCollection(noInterceptData)
    val weightDS = sgd.optimize(inputDS, None)

    val weightList: Seq[WeightVector] = weightDS.collect()

    weightList.size should equal(1)

    val weightVector: WeightVector = weightList.head

    val weights = weightVector.weights.asInstanceOf[DenseVector].data
    val weight0 = weightVector.intercept

    expectedNoInterceptWeights zip weights foreach {
      case (expectedWeight, weight) =>
        weight should be (expectedWeight +- 0.1)
    }
    weight0 should be (expectedNoInterceptWeight0 +- 0.1)
  }

  it should "correctly perform one step of the algorithm with initial weights provided" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)

    val lossFunction = GenericLossFunction(SquaredLoss, LinearPrediction)

    val sgd = GradientDescent()
      .setStepsize(0.1)
      .setIterations(1)
      .setLossFunction(lossFunction)

    val inputDS: DataSet[LabeledVector] = env.fromElements(LabeledVector(1.0, DenseVector(2.0)))
    val currentWeights = new WeightVector(DenseVector(1.0), 1.0)
    val currentWeightsDS = env.fromElements(currentWeights)

    val weightDS = sgd.optimize(inputDS, Some(currentWeightsDS))

    val weightList: Seq[WeightVector] = weightDS.collect()

    weightList.size should equal(1)

    val weightVector: WeightVector = weightList.head

    val updatedIntercept = weightVector.intercept
    val updatedWeight = weightVector.weights(0)

    updatedWeight should be (0.6 +- 0.01)
    updatedIntercept should be (0.8 +- 0.01)

  }

  it should "terminate early if the convergence criterion is reached" in {
    // TODO(tvas): We need a better way to check the convergence of the weights.
    // Ideally we want to have a Breeze-like system, where the optimizers carry a history and that
    // can tell us whether we have converged and at which iteration

    val env = ExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)

    val lossFunction = GenericLossFunction(SquaredLoss, LinearPrediction)

    val sgdEarlyTerminate = GradientDescent()
      .setConvergenceThreshold(1e2)
      .setStepsize(1.0)
      .setIterations(800)
      .setLossFunction(lossFunction)

    val inputDS = env.fromCollection(data)

    val weightDSEarlyTerminate = sgdEarlyTerminate.optimize(inputDS, None)

    val weightListEarly: Seq[WeightVector] = weightDSEarlyTerminate.collect()

    weightListEarly.size should equal(1)

    val weightVectorEarly: WeightVector = weightListEarly.head
    val weightsEarly = weightVectorEarly.weights.asInstanceOf[DenseVector].data
    val weight0Early = weightVectorEarly.intercept

    val sgdNoConvergence = GradientDescent()
      .setStepsize(1.0)
      .setIterations(800)
      .setLossFunction(lossFunction)

    val weightDSNoConvergence = sgdNoConvergence.optimize(inputDS, None)

    val weightListNoConvergence: Seq[WeightVector] = weightDSNoConvergence.collect()

    weightListNoConvergence.size should equal(1)

    val weightVectorNoConvergence: WeightVector = weightListNoConvergence.head
    val weightsNoConvergence = weightVectorNoConvergence.weights.asInstanceOf[DenseVector].data
    val weight0NoConvergence = weightVectorNoConvergence.intercept

    // Since the first optimizer was set to terminate early, its weights should be different
    weightsEarly zip weightsNoConvergence foreach {
      case (earlyWeight, weightNoConvergence) =>
        weightNoConvergence should not be (earlyWeight +- 0.1)
    }
    weight0NoConvergence should not be (weight0Early +- 0.1)
  }

  it should "come up with similar parameter estimates with xu step-size strategy" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)

    val lossFunction = GenericLossFunction(SquaredLoss, LinearPrediction)

    val sgd = GradientDescent()
      .setStepsize(1.0)
      .setIterations(800)
      .setLossFunction(lossFunction)
      .setLearningRateMethod(LearningRateMethod.Xu(-0.75))

    val inputDS = env.fromCollection(data)
    val weightDS = sgd.optimize(inputDS, None)

    val weightList: Seq[WeightVector] = weightDS.collect()

    weightList.size should equal(1)

    val weightVector: WeightVector = weightList.head

    val weights = weightVector.weights.asInstanceOf[DenseVector].data
    val weight0 = weightVector.intercept

    expectedWeights zip weights foreach {
      case (expectedWeight, weight) =>
        weight should be (expectedWeight +- 0.1)
    }
    weight0 should be (expectedWeight0 +- 0.1)
  }
  // TODO: Need more corner cases, see sklearn tests for SGD linear model

}
