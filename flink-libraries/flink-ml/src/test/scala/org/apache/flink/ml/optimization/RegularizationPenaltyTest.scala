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

import org.apache.flink.ml.math.DenseVector
import org.scalatest.{FlatSpec, Matchers}


class RegularizationPenaltyTest extends FlatSpec with Matchers {

  behavior of "The Regularization Penalty Function implementations"

  it should "correctly update weights and loss with L2 regularization penalty" in {
    val loss =  3.4
    val weights = DenseVector(0.8)
    val gradient = DenseVector(2.0)

    val updatedWeights = L2Regularization.takeStep(weights, gradient, 0.3, 0.01)
    val updatedLoss = L2Regularization.regLoss(loss, updatedWeights, 0.3)

    updatedWeights(0) should be (0.7776 +- 0.001)
    updatedLoss should be (3.4907 +- 0.001)
  }

  it should "correctly update weights and loss with L1 regularization penalty" in {
    val loss =  3.4
    val weights = DenseVector(0.8)
    val gradient = DenseVector(2.0)

    val updatedWeights = L1Regularization.takeStep(weights, gradient, 0.3, 0.01)
    val updatedLoss = L1Regularization.regLoss(loss, updatedWeights, 0.3)

    updatedWeights(0) should be (0.777 +- 0.001)
    updatedLoss should be (3.6331 +- 0.001)
  }

  it should "correctly update weights and loss with no regularization penalty" in {
    val loss =  3.4
    val weights = DenseVector(0.8)
    val gradient = DenseVector(2.0)

    val updatedWeights = NoRegularization.takeStep(weights, gradient, 0.3, 0.01)
    val updatedLoss = NoRegularization.regLoss(loss, updatedWeights, 0.3)

    updatedWeights(0) should be (0.78 +- 0.001)
    updatedLoss should be (3.4 +- 0.001)
  }
}
