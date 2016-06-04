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

import org.apache.flink.ml.math.{Vector, BLAS}
import org.apache.flink.ml.math.Breeze._
import breeze.linalg.{norm => BreezeNorm}

/** Represents a type of regularization penalty
  *
  * Regularization penalties are used to restrict the optimization problem to solutions with
  * certain desirable characteristics, such as sparsity for the L1 penalty, or penalizing large
  * weights for the L2 penalty.
  *
  * The regularization term, `R(w)` is added to the objective function, `f(w) = L(w) + lambda*R(w)`
  * where lambda is the regularization parameter used to tune the amount of regularization applied.
  */
trait RegularizationPenalty extends Serializable {

  /** Calculates the new weights based on the gradient and regularization penalty
    *
    * Weights are updated using the gradient descent step `w - learningRate * gradient`
    * with `w` being the weight vector.
    *
    * @param weightVector The weights to be updated
    * @param gradient The gradient used to update the weights
    * @param regularizationConstant The regularization parameter to be applied 
    * @param learningRate The effective step size for this iteration
    * @return Updated weights
    */
  def takeStep(
      weightVector: Vector,
      gradient: Vector,
      regularizationConstant: Double,
      learningRate: Double)
    : Vector

  /** Adds regularization to the loss value
    *
    * @param oldLoss The loss to be updated
    * @param weightVector The weights used to update the loss
    * @param regularizationConstant The regularization parameter to be applied
    * @return Updated loss
    */
  def regLoss(oldLoss: Double, weightVector: Vector, regularizationConstant: Double): Double

}


/** `L_2` regularization penalty.
  *
  * The regularization function is the square of the L2 norm `1/2*||w||_2^2`
  * with `w` being the weight vector. The function penalizes large weights,
  * favoring solutions with more small weights rather than few large ones.
  */
object L2Regularization extends RegularizationPenalty {

  /** Calculates the new weights based on the gradient and L2 regularization penalty
    *
    * The updated weight is `w - learningRate * (gradient + lambda * w)` where
    * `w` is the weight vector, and `lambda` is the regularization parameter.
    *
    * @param weightVector The weights to be updated
    * @param gradient The gradient according to which we will update the weights
    * @param regularizationConstant The regularization parameter to be applied
    * @param learningRate The effective step size for this iteration
    * @return Updated weights
    */
  override def takeStep(
      weightVector: Vector,
      gradient: Vector,
      regularizationConstant: Double,
      learningRate: Double)
    : Vector = {
    // add the gradient of the L2 regularization
    BLAS.axpy(regularizationConstant, weightVector, gradient)

    // update the weights according to the learning rate
    BLAS.axpy(-learningRate, gradient, weightVector)

    weightVector
  }

  /** Adds regularization to the loss value
    *
    * The updated loss is `oldLoss + lambda * 1/2*||w||_2^2` where
    * `w` is the weight vector, and `lambda` is the regularization parameter
    *
    * @param oldLoss The loss to be updated
    * @param weightVector The weights used to update the loss
    * @param regularizationConstant The regularization parameter to be applied
    * @return Updated loss
    */
  override def regLoss(oldLoss: Double, weightVector: Vector, regularizationConstant: Double)
    : Double = {
    val squareNorm = BLAS.dot(weightVector, weightVector)
    oldLoss + regularizationConstant * 0.5 * squareNorm
  }
}

/** `L_1` regularization penalty.
  *
  * The regularization function is the `L1` norm `||w||_1` with `w` being the weight vector.
  * The `L_1` penalty can be used to drive a number of the solution coefficients to 0, thereby
  * producing sparse solutions.
  *
  */
object L1Regularization extends RegularizationPenalty {

  /** Calculates the new weights based on the gradient and L1 regularization penalty
    *
    * Uses the proximal gradient method with L1 regularization to update weights.
    * The updated weight `w - learningRate * gradient` is shrunk towards zero
    * by applying the proximal operator `signum(w) * max(0.0, abs(w) - shrinkageVal)`
    * where `w` is the weight vector, `lambda` is the regularization parameter,
    * and `shrinkageVal` is `lambda*learningRate`.
    *
    * @param weightVector The weights to be updated
    * @param gradient The gradient according to which we will update the weights
    * @param regularizationConstant The regularization parameter to be applied
    * @param learningRate The effective step size for this iteration
    * @return Updated weights
    */
  override def takeStep(
      weightVector: Vector,
      gradient: Vector,
      regularizationConstant: Double,
      learningRate: Double)
    : Vector = {
    // Update weight vector with gradient.
    BLAS.axpy(-learningRate, gradient, weightVector)

    // Apply proximal operator (soft thresholding)
    val shrinkageVal = regularizationConstant * learningRate
    var i = 0
    while (i < weightVector.size) {
      val wi = weightVector(i)
      weightVector(i) = math.signum(wi) *
        math.max(0.0, math.abs(wi) - shrinkageVal)
      i += 1
    }

    weightVector
  }

  /** Adds regularization to the loss value
    *
    * The updated loss is `oldLoss + lambda * ||w||_1` where
    * `w` is the weight vector and `lambda` is the regularization parameter
    *
    * @param oldLoss The loss to be updated
    * @param weightVector The weights used to update the loss
    * @param regularizationConstant The regularization parameter to be applied
    * @return Updated loss
    */
  override def regLoss(oldLoss: Double, weightVector: Vector, regularizationConstant: Double)
    : Double = {
    val norm =  BreezeNorm(weightVector.asBreeze, 1.0)
    oldLoss + norm * regularizationConstant
  }
}

/** No regularization penalty.
  *
  */
object NoRegularization extends RegularizationPenalty {

  /** Calculates the new weights based on the gradient
    *
    * The updated weight is `w - learningRate *gradient` where `w` is the weight vector
    *
    * @param weightVector The weights to be updated
    * @param gradient The gradient according to which we will update the weights
    * @param regularizationConstant The regularization parameter which is ignored
    * @param learningRate The effective step size for this iteration
    * @return Updated weights
    */
  override def takeStep(
                         weightVector: Vector,
                         gradient: Vector,
                         regularizationConstant: Double,
                         learningRate: Double)
  : Vector = {
    // Update the weight vector
    BLAS.axpy(-learningRate, gradient, weightVector)
    weightVector
  }

  /**
   * Returns the unmodified loss value
   *
   * The updated loss is `oldLoss`
   *
   * @param oldLoss The loss to be updated
   * @param weightVector The weights used to update the loss
   * @param regularizationParameter The regularization parameter which is ignored
   * @return Updated loss
   */
  override def regLoss(oldLoss: Double, weightVector: Vector, regularizationParameter: Double)
    : Double = {
    oldLoss
  }
}
