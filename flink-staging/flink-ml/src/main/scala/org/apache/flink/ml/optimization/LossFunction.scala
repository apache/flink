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

import org.apache.flink.ml.common.{WeightVector, LabeledVector}
import org.apache.flink.ml.math.{Vector => FlinkVector, BLAS}

/** Abstract class that implements some of the functionality for common loss functions
  *
  * A loss function determines the loss term $L(w) of the objective function  $f(w) = L(w) +
  * \lambda R(w)$ for prediction tasks, the other being regularization, $R(w)$.
  *
  * We currently only support differentiable loss functions, in the future this class
  * could be changed to DiffLossFunction in order to support other types, such absolute loss.
  */
abstract class LossFunction extends Serializable{

  /** Calculates the loss for a given prediction/truth pair
    *
    * @param prediction The predicted value
    * @param truth The true value
    */
  protected def loss(prediction: Double, truth: Double): Double

  /** Calculates the derivative of the loss function with respect to the prediction
    *
    * @param prediction The predicted value
    * @param truth The true value
    */
  protected def lossDerivative(prediction: Double, truth: Double): Double

  /** Compute the gradient and the loss for the given data.
    * The provided cumGradient is updated in place.
    *
    * @param example The features and the label associated with the example
    * @param weights The current weight vector
    * @param cumGradient The vector to which the gradient will be added to, in place.
    * @return A tuple containing the computed loss as its first element and a the loss derivative as
    *         its second element. The gradient is updated in-place.
    */
  def lossAndGradient(
      example: LabeledVector,
      weights: WeightVector,
      cumGradient: FlinkVector,
      regType: Regularization,
      regParameter: Double,
      predictionFunction: PredictionFunction):
  (Double, Double) = {
    val features = example.vector
    val label = example.label
    // TODO(tvas): We could also provide for the case where we don't want an intercept value
    // i.e. data already centered
    val prediction = predictionFunction.predict(features, weights)
    val predictionGradient = predictionFunction.gradient(features, weights)
    val lossValue: Double = loss(prediction, label)
    // The loss derivative is used to update the intercept
    val lossDeriv = lossDerivative(prediction, label)
    // Restrict the value of the loss derivative to avoid numerical instabilities
    val restrictedLossDeriv: Double = {
      if (lossDeriv < -IterativeSolver.MAX_DLOSS) {
        -IterativeSolver.MAX_DLOSS
      }
      else if (lossDeriv > IterativeSolver.MAX_DLOSS) {
        IterativeSolver.MAX_DLOSS
      }
      else {
        lossDeriv
      }
    }
    // Update the gradient
    BLAS.axpy(restrictedLossDeriv, predictionGradient, cumGradient)
    (lossValue, lossDeriv)
  }
}

trait ClassificationLoss extends LossFunction
trait RegressionLoss extends LossFunction

// TODO(tvas): Implement LogisticLoss, HingeLoss.

/** Squared loss function where $L(w) = \frac{1}{2} (w^{T} x - y)^2$
  *
  */
class SquaredLoss extends RegressionLoss {
  /** Calculates the loss for a given prediction/truth pair
    *
    * @param prediction The predicted value
    * @param truth The true value
    */
  protected override def loss(prediction: Double, truth: Double): Double = {
    0.5 * (prediction - truth) * (prediction - truth)
  }

  /** Calculates the derivative of the loss function with respect to the prediction
    *
    * @param prediction The predicted value
    * @param truth The true value
    */
  protected override def lossDerivative(prediction: Double, truth: Double): Double = {
    prediction - truth
  }

}
