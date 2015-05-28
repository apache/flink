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
import org.apache.flink.ml.math.BLAS

trait RegularizationFunction extends Serializable {
  def gradient(weightVector: WeightVector): Option[WeightVector]

  def regularization(weightVector: WeightVector): Double

  def updateWeightVector(
      oldWeightVector: WeightVector,
      gradient: WeightVector,
      learningRate: Double,
      regularizationValue: Double)
    : WeightVector
}

object NoRegularization extends RegularizationFunction {
  override def gradient(weightVector: WeightVector): Option[WeightVector] = None

  override def updateWeightVector(oldWeightVector: WeightVector, gradient: WeightVector,
    learningRate: Double, regularizationValue: Double): WeightVector = {
    val WeightVector(weights, intercept) = oldWeightVector
    BLAS.axpy(-learningRate, gradient.weights, weights)

    WeightVector(weights, intercept - learningRate * gradient.intercept)
  }

  override def regularization(weightVector: WeightVector): Double = 0.0
}

object L2Regularization extends RegularizationFunction {
  override def gradient(weightVector: WeightVector): Option[WeightVector] = {
    Some(weightVector)
  }

  override def updateWeightVector(oldWeightVector: WeightVector, gradient: WeightVector,
    learningRate: Double, regularizationValue: Double): WeightVector = {
    val WeightVector(weights, intercept) = oldWeightVector
    BLAS.axpy(-learningRate, gradient.weights, weights)

    WeightVector(weights, intercept - learningRate * gradient.intercept)
  }

  override def regularization(weightVector: WeightVector): Double = {
    import weightVector._
    0.5 * (BLAS.dot(weights, weights) + intercept * intercept)
  }
}

object L1Regularization extends RegularizationFunction {
  override def gradient(weightVector: WeightVector): Option[WeightVector] = None

  override def updateWeightVector(oldWeightVector: WeightVector, gradient: WeightVector,
    learningRate: Double, regularizationValue: Double): WeightVector = {

    val WeightVector(weights, intercept) = oldWeightVector
    BLAS.axpy(-learningRate, gradient.weights, weights)

    // Apply proximal operator (soft thresholding)
    val shrinkageVal = regularizationValue * learningRate
    var i = 0
    while (i < weights.size) {
      val wi = weights(i)
      weights(i) = math.signum(wi) * math.max(0.0, math.abs(wi) - shrinkageVal)
      i += 1
    }

    WeightVector(weights, intercept - learningRate * gradient.intercept)
  }

  override def regularization(weightVector: WeightVector): Double = {
    weightVector.weights.valueIterator.reduce(Math.abs(_) + Math.abs(_)) +
      Math.abs(weightVector.intercept)
  }
}
