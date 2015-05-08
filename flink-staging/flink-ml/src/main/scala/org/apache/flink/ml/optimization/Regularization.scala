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

import org.apache.flink.ml.math.{Vector => FlinkVector, BLAS}

/** Represents a type of regularization penalty
  *
  * Regularization penalties are used to restrict the optimization problem to solutions with
  * certain desirable characteristics, such as sparsity for the L1 penalty, or penalizing large
  * weights for the L2 penalty.
  *
  * The regularization term, $R(w)$ is added to the objective function, $f(w) = L(w) + \lambda R(w)$
  * where $\lambda$ is the regularization parameter used to tune the amount of regularization
  * applied.
  */
abstract class Regularization extends Serializable {

  /** Updates the weights by taking a step according to the gradient and regularization applied
    *
    * @param oldWeights The weights to be updated
    * @param gradient The gradient according to which we will update the weights
    * @param effectiveStepSize The effective step size for this iteration
    * @param regParameter The regularization parameter, $\lambda$.
    */
  def takeStep(
      oldWeights: FlinkVector,
      gradient: FlinkVector,
      effectiveStepSize: Double,
      regParameter: Double) {
    BLAS.axpy(-effectiveStepSize, gradient, oldWeights)
  }

  /** Adds the regularization term to the loss value
    *
    * @param loss The loss value, before applying regularization.
    * @param weightVector The current vector of weights.
    * @param regularizationParameter The regularization parameter, $\lambda$.
    * @return The loss value with regularization applied.
    */
  def regLoss(loss: Double, weightVector: FlinkVector, regularizationParameter: Double): Double

}

/** Abstract class for regularization penalties that are differentiable
  *
  */
abstract class DiffRegularization extends Regularization {

  /** Compute the regularized gradient loss for the given data.
    * The provided cumGradient is updated in place.
    *
    * @param loss The loss value without regularization.
    * @param weightVector The current vector of weights.
    * @param lossGradient The loss gradient, without regularization. Updated in-place.
    * @param regParameter The regularization parameter, $\lambda$.
    * @return The loss value with regularization applied.
    */
  def regularizedLossAndGradient(
      loss: Double,
      weightVector: FlinkVector,
      lossGradient: FlinkVector,
      regParameter: Double) : Double ={
    val adjustedLoss = regLoss(loss, weightVector, regParameter)
    regGradient(weightVector, lossGradient, regParameter)

    adjustedLoss
  }

  /** Adds the regularization gradient term to the loss gradient. The gradient is updated in place.
    *
    * @param weightVector The current vector of weights
    * @param lossGradient The loss gradient, without regularization. Updated in-place.
    * @param regParameter The regularization parameter, $\lambda$.
    */
  def regGradient(
      weightVector: FlinkVector,
      lossGradient: FlinkVector,
      regParameter: Double)
}

/** Performs no regularization, equivalent to $R(w) = 0$ **/
class NoRegularization extends Regularization {
  /** Adds the regularization term to the loss value
    *
    * @param loss The loss value, before applying regularization
    * @param weightVector The current vector of weights
    * @param regParameter The regularization parameter, $\lambda$
    * @return The loss value with regularization applied.
    */
  override def regLoss(
    loss: Double,
    weightVector: FlinkVector,
    regParameter: Double):  Double = {loss}
}

/** $L_2$ regularization penalty.
  *
  * Penalizes large weights, favoring solutions with more small weights rather than few large ones.
  *
  */
class L2Regularization extends DiffRegularization {

  /** Adds the regularization term to the loss value
    *
    * @param loss The loss value, before applying regularization
    * @param weightVector The current vector of weights
    * @param regParameter The regularization parameter, $\lambda$
    * @return The loss value with regularization applied.
    */
  override def regLoss(loss: Double, weightVector: FlinkVector, regParameter: Double)
    : Double = {
    loss + regParameter * BLAS.dot(weightVector, weightVector) / 2
  }

  /** Adds the regularization gradient term to the loss gradient. The gradient is updated in place.
    *
    * @param weightVector The current vector of weights.
    * @param lossGradient The loss gradient, without regularization. Updated in-place.
    * @param regParameter The regularization parameter, $\lambda$.
    */
  override def regGradient(
      weightVector: FlinkVector,
      lossGradient: FlinkVector,
      regParameter: Double): Unit = {
    BLAS.axpy(regParameter, weightVector, lossGradient)
  }
}

/** $L_1$ regularization penalty.
  *
  * The $L_1$ penalty can be used to drive a number of the solution coefficients to 0, thereby
  * producing sparse solutions.
  *
  */
class L1Regularization extends Regularization {
  /** Calculates and applies the regularization amount and the regularization parameter
    *
    * Implementation was taken from the Apache Spark Mllib library:
    * http://git.io/vfZIT
    *
    * @param oldWeights The weights to be updated
    * @param gradient The gradient according to which we will update the weights
    * @param effectiveStepSize The effective step size for this iteration
    * @param regParameter The regularization parameter to be applied in the case of L1
    *                     regularization
    */
  override def takeStep(
      oldWeights: FlinkVector,
      gradient: FlinkVector,
      effectiveStepSize: Double,
      regParameter: Double) {
    BLAS.axpy(-effectiveStepSize, gradient, oldWeights)

    // Apply proximal operator (soft thresholding)
    val shrinkageVal = regParameter * effectiveStepSize
    var i = 0
    while (i < oldWeights.size) {
      val wi = oldWeights(i)
      oldWeights(i) = math.signum(wi) * math.max(0.0, math.abs(wi) - shrinkageVal)
      i += 1
    }
  }

  /** Adds the regularization term to the loss value
    *
    * @param loss The loss value, before applying regularization.
    * @param weightVector The current vector of weights.
    * @param regularizationParameter The regularization parameter, $\lambda$.
    * @return The loss value with regularization applied.
    */
  override def regLoss(loss: Double, weightVector: FlinkVector, regularizationParameter: Double):
  Double = {
    loss + l1Norm(weightVector) * regularizationParameter
  }

  // TODO(tvas): Replace once we decide on how we deal with vector ops (roll our own or use Breeze)
  /** $L_1$ norm of a Vector **/
  private def l1Norm(vector: FlinkVector) : Double = {
    vector.valueIterator.fold(0.0){(a,b) => math.abs(a) + math.abs(b)}
  }
}
