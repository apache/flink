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

import org.apache.flink.ml.common.{LabeledVector, WeightVector, ParameterMap}
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.ml.regression.RegressionData._
import org.scalatest.{Matchers, FlatSpec}

import org.apache.flink.api.scala._
import org.apache.flink.test.util.FlinkTestBase


class GradientDescentITSuite extends FlatSpec with Matchers with FlinkTestBase {

  // TODO(tvas): Check results again once sampling operators are in place

  behavior of "The Stochastic Gradient Descent implementation"

  it should "correctly solve an L1 regularized regression problem" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)

    val parameters = ParameterMap()

    parameters.add(IterativeSolver.Stepsize, 0.01)
    parameters.add(IterativeSolver.Iterations, 2000)
    parameters.add(Solver.LossFunction, new SquaredLoss)
    parameters.add(Solver.RegularizationType, new L1Regularization)
    parameters.add(Solver.RegularizationParameter, 0.3)

    val sgd = GradientDescent(parameters)

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

    val parameters = ParameterMap()

    parameters.add(IterativeSolver.Stepsize, 0.1)
    parameters.add(IterativeSolver.Iterations, 1)
    parameters.add(Solver.LossFunction, new SquaredLoss)
    parameters.add(Solver.RegularizationType, new L2Regularization)
    parameters.add(Solver.RegularizationParameter, 1.0)

    val sgd = GradientDescent(parameters)

    val inputDS: DataSet[LabeledVector] = env.fromElements(LabeledVector(1.0, DenseVector(2.0)))
    val currentWeights = new WeightVector(DenseVector(1.0), 1.0)
    val currentWeightsDS = env.fromElements(currentWeights)

    val weightDS = sgd.optimize(inputDS, Some(currentWeightsDS))

    val weightList: Seq[WeightVector] = weightDS.collect()

    weightList.size should equal(1)

    val weightVector: WeightVector = weightList.head

    val updatedIntercept = weightVector.intercept
    val updatedWeight = weightVector.weights(0)

    updatedWeight should be (0.5 +- 0.001)
    updatedIntercept should be (0.8 +- 0.01)
  }

  it should "estimate a linear function" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)

    val parameters = ParameterMap()

    parameters.add(IterativeSolver.Stepsize, 1.0)
    parameters.add(IterativeSolver.Iterations, 800)
    parameters.add(Solver.LossFunction, new SquaredLoss)
    parameters.add(Solver.RegularizationType, new NoRegularization)
    parameters.add(Solver.RegularizationParameter, 0.0)

    val sgd = GradientDescent(parameters)

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

    val parameters = ParameterMap()

    parameters.add(IterativeSolver.Stepsize, 0.0001)
    parameters.add(IterativeSolver.Iterations, 100)
    parameters.add(Solver.LossFunction, new SquaredLoss)
    parameters.add(Solver.RegularizationType, new NoRegularization)
    parameters.add(Solver.RegularizationParameter, 0.0)

    val sgd = GradientDescent(parameters)

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

    val parameters = ParameterMap()

    parameters.add(IterativeSolver.Stepsize, 0.1)
    parameters.add(IterativeSolver.Iterations, 1)
    parameters.add(Solver.LossFunction, new SquaredLoss)
    parameters.add(Solver.RegularizationType, new NoRegularization)
    parameters.add(Solver.RegularizationParameter, 0.0)

    val sgd = GradientDescent(parameters)

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

  // TODO: Need more corner cases

}
