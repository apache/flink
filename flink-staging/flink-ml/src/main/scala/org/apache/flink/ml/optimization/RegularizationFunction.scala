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

import org.apache.flink.ml.math.Vector
import org.apache.flink.ml.math.BLAS

trait RegularizationFunction extends Serializable {
  def gradient(weightVector: Vector): Option[Vector]

  def regularization(weightVector: Vector): Double

  def updateWeights(
      oldWeightVector: Vector,
      gradient: Vector,
      learningRate: Double,
      regularizationValue: Double)
    : Vector
}

object NoRegularization extends RegularizationFunction {
  override def gradient(weightVector: Vector): Option[Vector] = None

  override def updateWeights(weights: Vector, gradient: Vector,
    learningRate: Double, regularizationValue: Double): Vector = {
    BLAS.axpy(-learningRate, gradient, weights)

    weights
  }

  override def regularization(weightVector: Vector): Double = 0.0
}

object L2Regularization extends RegularizationFunction {
  override def gradient(weights: Vector): Option[Vector] = {
    Some(weights)
  }

  override def updateWeights(weights: Vector, gradient: Vector,
    learningRate: Double, regularizationValue: Double): Vector = {
    BLAS.axpy(-learningRate, gradient, weights)

    weights
  }

  override def regularization(weights: Vector): Double = {
    0.5 * BLAS.dot(weights, weights)
  }
}

object L1Regularization extends RegularizationFunction {
  override def gradient(weightVector: Vector): Option[Vector] = None

  override def updateWeights(weights: Vector, gradient: Vector,
    learningRate: Double, regularizationValue: Double): Vector = {

    BLAS.axpy(-learningRate, gradient, weights)

    // Apply proximal operator (soft thresholding)
    val shrinkageVal = regularizationValue * learningRate
    var i = 0
    while (i < weights.size) {
      val wi = weights(i)
      weights(i) = math.signum(wi) * math.max(0.0, math.abs(wi) - shrinkageVal)
      i += 1
    }

    weights
  }

  override def regularization(weights: Vector): Double = {
    weights.valueIterator.reduce(Math.abs(_) + Math.abs(_))
  }
}
