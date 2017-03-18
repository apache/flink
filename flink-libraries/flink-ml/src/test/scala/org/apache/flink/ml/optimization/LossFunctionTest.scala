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
import org.apache.flink.ml.util.FlinkTestBase
import org.scalatest.{Matchers, FlatSpec}


class LossFunctionTest extends FlatSpec with Matchers {

  behavior of "The optimization Loss Function implementations"

  it should "calculate squared loss and gradient correctly" in {

    val lossFunction = GenericLossFunction(SquaredLoss, LinearPrediction)

    val example = LabeledVector(1.0, DenseVector(2))
    val weightVector = new WeightVector(DenseVector(1.0), 1.0)

    val gradient = lossFunction.gradient(example, weightVector)
    val loss = lossFunction.loss(example, weightVector)

    loss should be (2.0 +- 0.001)

    gradient.weights(0) should be (4.0 +- 0.001)
  }

  it should "calculate logistic loss and gradient correctly" in {

    val lossFunction = GenericLossFunction(LogisticLoss, LinearPrediction)

    val examples = List(
      LabeledVector(1.0, DenseVector(2)),
      LabeledVector(1.0, DenseVector(20)),
      LabeledVector(1.0, DenseVector(-25))
    )

    val weightVector = new WeightVector(DenseVector(1.0), 1.0)
    val expectedLosses = List(0.049, 7.58e-10, 24.0)
    val expectedGradients = List(-0.095, -1.52e-8, 25.0)

    expectedLosses zip examples foreach {
      case (expectedLoss, example) => {
        val loss = lossFunction.loss(example, weightVector)
        loss should be (expectedLoss +- 0.001)
      }
    }

    expectedGradients zip examples foreach {
      case (expectedGradient, example) => {
        val gradient = lossFunction.gradient(example, weightVector)
        gradient.weights(0) should be (expectedGradient +- 0.001)
      }
    }
  }

  it should "calculate hinge loss and gradient correctly" in {

    val lossFunction = GenericLossFunction(HingeLoss, LinearPrediction)

    val examples = List(
      LabeledVector(1.0, DenseVector(2)),
      LabeledVector(1.0, DenseVector(-2))
    )

    val weightVector = new WeightVector(DenseVector(1.0), 1.0)
    val expectedLosses = List(0.0, 2.0)
    val expectedGradients = List(0.0, 2.0)

    expectedLosses zip examples foreach {
      case (expectedLoss, example) => {
        val loss = lossFunction.loss(example, weightVector)
        loss should be (expectedLoss +- 0.001)
      }
    }

    expectedGradients zip examples foreach {
      case (expectedGradient, example) => {
        val gradient = lossFunction.gradient(example, weightVector)
        gradient.weights(0) should be (expectedGradient +- 0.001)
      }
    }
  }
}
