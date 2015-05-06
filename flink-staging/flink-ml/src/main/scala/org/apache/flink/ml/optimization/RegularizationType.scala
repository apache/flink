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

import org.apache.flink.api.scala._
import org.apache.flink.ml.math.{Vector => FlinkVector, BLAS}
import org.apache.flink.ml.math.Breeze._

import breeze.numerics._
import breeze.linalg.{norm => BreezeNorm, max => BreezeMax}



// TODO(tvas): Change name to RegularizationPenalty?
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
abstract class RegularizationType extends Serializable {

  /** Updates the weights by taking a step according to the gradient and regularization applied
    *
    * @param oldWeights The weights to be updated
    * @param gradient The gradient according to which we will update the weights
    * @param effectiveStepSize The effective step size for this iteration
    * @param regParameter The regularization parameter to be applied in the case of L1
    *                     regularization
    */
  def takeStep(
      oldWeights: FlinkVector,
      gradient: FlinkVector,
      effectiveStepSize: Double,
      regParameter: Double) {
    BLAS.axpy(-effectiveStepSize, gradient, oldWeights)
  }

  /** Adds regularization to the loss value **/
  def regLoss(oldLoss: Double, weightVector: FlinkVector, regularizationParameter: Double): Double

}

/** Abstract class for regularization penalties that are differentiable
  *
  */
abstract class DiffRegularizationType extends RegularizationType {

  /** Compute the regularized gradient loss for the given data.
    * The provided cumGradient is updated in place.
    *
    * @param weightVector The current weight vector
    * @param lossGradient The vector to which the gradient will be added to, in place.
    * @return The regularized loss. The gradient is updated in place.
    */
  def regularizedLossAndGradient(
      loss: Double,
      weightVector: FlinkVector,
      lossGradient: FlinkVector,
      regularizationParameter: Double) : Double ={
    val adjustedLoss = regLoss(loss, weightVector, regularizationParameter)
    regGradient(weightVector, lossGradient, regularizationParameter)

    adjustedLoss
  }

  /** Adds regularization gradient to the loss gradient. The gradient is updated in place **/
  def regGradient(
      weightVector: FlinkVector,
      lossGradient: FlinkVector,
      regularizationParameter: Double)
}

/** Performs no regularization, equivalent to $R(w) = 0$ **/
class NoRegularization extends RegularizationType {
  /** Adds regularization to the loss value **/
  override def regLoss(oldLoss: Double, weightVector: FlinkVector, regularizationParameter: Double):
  Double = {oldLoss}
}

/** $L_2$ regularization penalty.
  *
  * Penalizes large weights, favoring solutions with more small weights rather than few large ones.
  *
  */
class L2Regularization extends DiffRegularizationType {

  /** Adds regularization to the loss value **/
  override def regLoss(oldLoss: Double, weightVector: FlinkVector, regParameter: Double)
    : Double = {
    val brzVector = weightVector.asBreeze
    oldLoss + regParameter * (brzVector dot brzVector) / 2
  }

  /** Adds regularization gradient to the loss gradient. The gradient is updated in place **/
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
class L1Regularization extends RegularizationType {
  /** Calculates and applies the regularization amount and the regularization parameter
    *
    * Implementation was taken from the Apache Spark Mllib library:
    * http://git.io/vfZIT
    * @param oldWeights The old weights
    * @param effectiveStepSize The effective step size for this iteration
    * @param regParameter The current regularization parameter
    * @return A tuple whose first element is the updated weight FlinkVector and the second is the
    *         regularization value
    */
  override def takeStep(
      oldWeights: FlinkVector,
      gradient: FlinkVector,
      effectiveStepSize: Double,
      regParameter: Double) {
    BLAS.axpy(-effectiveStepSize, gradient, oldWeights)
    val brzWeights = oldWeights.asBreeze.toDenseVector

    // Apply proximal operator (soft thresholding)
    val shrinkageVal = regParameter * effectiveStepSize
    var i = 0
    while (i < brzWeights.length) {
      val wi = brzWeights(i)
      brzWeights(i) = signum(wi) * BreezeMax(0.0, abs(wi) - shrinkageVal)
      i += 1
    }

    BLAS.copy(brzWeights.fromBreeze, oldWeights)

    // We could maybe define a Breeze Universal function for the proximal operator, and test if it's
    // faster that the for loop + copy above
    //    brzWeights = signum(brzWeights) * max(0.0, abs(brzWeights) - shrinkageVal)

  }

  /** Adds regularization to the loss value **/
  override def regLoss(oldLoss: Double, weightVector: FlinkVector, regularizationParameter: Double):
  Double = {
    oldLoss + BreezeNorm(weightVector.asBreeze, 1.0) * regularizationParameter
  }
}
