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

import org.apache.flink.ml.common.WeightVector
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.api.scala._
import org.apache.flink.test.util.FlinkTestBase

import org.scalatest.{Matchers, FlatSpec}




class RegularizationITSuite extends FlatSpec with Matchers with FlinkTestBase {

  behavior of "The regularization type implementations"

  it should "not change the loss when no regularization is used" in {

    val env = ExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)

    val regularization = new NoRegularization

    val weightVector = new WeightVector(DenseVector(1.0), 1.0)
    val effectiveStepsize = 1.0
    val regParameter = 0.0
    val gradient = DenseVector(0.0)
    val originalLoss = 1.0

    val adjustedLoss = regularization.regLoss(originalLoss, weightVector.weights, regParameter)

    adjustedLoss should be (originalLoss +- 0.0001)
  }

  it should "correctly apply L1 regularization" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)

    val regularization = new L1Regularization

    val weightVector = new WeightVector(DenseVector(-1.0, 1.0, 0.4, -0.4, 0.0), 1.0)
    val effectiveStepsize = 1.0
    val regParameter = 0.5
    val gradient = DenseVector(0.0, 0.0, 0.0, 0.0, 0.0)

    regularization.takeStep(weightVector.weights,  gradient, effectiveStepsize, regParameter)

    val expectedWeights = DenseVector(-0.5, 0.5, 0.0, 0.0, 0.0)

    weightVector.weights shouldEqual expectedWeights
    weightVector.intercept should be (1.0 +- 0.0001)
  }

  it should "correctly calculate L1 loss"  in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)

    val regularization = new L1Regularization

    val weightVector = new WeightVector(DenseVector(-1.0, 1.0, 0.4, -0.4, 0.0), 1.0)
    val regParameter = 0.5
    val originalLoss = 1.0

    val adjustedLoss = regularization.regLoss(originalLoss, weightVector.weights, regParameter)

    weightVector shouldEqual WeightVector(DenseVector(-1.0, 1.0, 0.4, -0.4, 0.0), 1.0)
    adjustedLoss should be (2.4 +- 0.1)
  }

  it should "correctly adjust the gradient and loss for L2 regularization" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)

    val regularization = new L2Regularization

    val weightVector = new WeightVector(DenseVector(-1.0, 1.0, 0.4, -0.4, 0.0), 1.0)
    val regParameter = 0.5
    val lossGradient = DenseVector(0.0, 0.0, 0.0, 0.0, 0.0)
    val originalLoss = 1.0

    val adjustedLoss = regularization.regularizedLossAndGradient(
      originalLoss,
      weightVector.weights,
      lossGradient,
      regParameter)

    val expectedGradient = DenseVector(-0.5, 0.5, 0.2, -0.2, 0.0)

    weightVector shouldEqual WeightVector(DenseVector(-1.0, 1.0, 0.4, -0.4, 0.0), 1.0)
    adjustedLoss should be (1.58 +- 0.1)
    lossGradient shouldEqual expectedGradient
  }
}
