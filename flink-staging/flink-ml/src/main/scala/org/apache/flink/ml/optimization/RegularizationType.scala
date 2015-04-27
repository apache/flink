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

import breeze.numerics._
import org.apache.flink.ml.math.{BLAS, Vector}
import org.apache.flink.ml.math.Breeze._
import breeze.linalg.{norm => BreezeNorm, Vector => BreezeVector, max}

// TODO(tvas): Change name to RegularizationPenalty?
abstract class RegularizationType extends Serializable{
  /** Calculates and applies the regularization amount and the regularization parameter
    *
    * @param weights The old weights
    * @param effectiveStepSize The effective step size for this iteration
    * @param regParameter The current regularization parameter
    * @return A tuple whose first element is the updated weight vector and the second is the
    *         new regularization parameter
    */
  def applyRegularization(weights: Vector, effectiveStepSize: Double, regParameter: Double):
  (Vector, Double)
  // TODO(tvas): We are not currently using the regularization value anywhere, but it could be
  // useful to keep a history of it.

}

class NoRegularization extends RegularizationType {
  /** Returns the original weights without any regularization applied
    *
    * @param weights The old weights
    * @param effectiveStepSize The effective step size for this iteration
    * @param regParameter The current regularization parameter
    * @return A tuple whose first element is the updated weight vector and the second is the
    *         regularization value
    */
  override def applyRegularization(weights: Vector,
                                   effectiveStepSize: Double,
                                   regParameter: Double):
  (Vector, Double) = {(weights, 0.0)}
}

class L2Regularization extends RegularizationType {
  /** Calculates and applies the regularization amount and the regularization parameter
    *
    * Implementation was taken from the Apache Spark Mllib library:
    * http://git.io/vfZIT
    * @param weights The old weights
    * @param effectiveStepSize The effective step size for this iteration
    * @param regParameter The current regularization parameter
    * @return A tuple whose first element is the updated weight vector and the second is the
    *         regularization value
    */
  override def applyRegularization(weights: Vector,
                                   effectiveStepSize: Double,
                                   regParameter: Double):
  (Vector, Double) = {

    val brzWeights: BreezeVector[Double] = weights.asBreeze
    brzWeights :*= (1.0 - effectiveStepSize * regParameter)
    val norm = BreezeNorm(brzWeights, 2.0)
    // sklearn chooses instead to keep weights >= 0, can we avoid oscillations better that way?
//    BLAS.scal(1.0 - effectiveStepSize * regParameter, weights)
    (brzWeights.fromBreeze, 0.5 * regParameter * norm * norm)
  }
}

class L1Regularization extends RegularizationType {
  /** Calculates and applies the regularization amount and the regularization parameter
    *
    * Implementation was taken from the Apache Spark Mllib library:
    * http://git.io/vfZIT
    * @param weights The old weights
    * @param effectiveStepSize The effective step size for this iteration
    * @param regParameter The current regularization parameter
    * @return A tuple whose first element is the updated weight vector and the second is the
    *         regularization value
    */
  override def applyRegularization(weights: Vector,
                                   effectiveStepSize: Double,
                                   regParameter: Double):
  (Vector, Double) = {

    val brzWeights: BreezeVector[Double] = weights.asBreeze.toDenseVector

    // Apply proximal operator (soft thresholding)
    val shrinkageVal = regParameter * effectiveStepSize
    var i = 0
    while (i < brzWeights.length) {
      val wi = brzWeights(i)
      brzWeights(i) = signum(wi) * max(0.0, abs(wi) - shrinkageVal)
      i += 1
    }

    // We could maybe define a Breeze Universal function for the proximal operator, and test if it's
    // faster that thefor loop above
//    brzWeights = signum(brzWeights) * max(0.0, abs(brzWeights) - shrinkageVal)

    (brzWeights.fromBreeze, BreezeNorm(brzWeights, 1.0) * regParameter)
  }
}
