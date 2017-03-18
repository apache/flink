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
import org.apache.flink.ml.math.BLAS

/** Abstract class that implements some of the functionality for common loss functions
  *
  * A loss function determines the loss term `L(w)` of the objective function  `f(w) = L(w) +
  * lambda*R(w)` for prediction tasks, the other being regularization, `R(w)`.
  *
  * The regularization is specific to the used optimization algorithm and, thus, implemented there.
  *
  * We currently only support differentiable loss functions, in the future this class
  * could be changed to DiffLossFunction in order to support other types, such as absolute loss.
  */
trait LossFunction extends Serializable {

  /** Calculates the loss given the prediction and label value
    *
    * @param dataPoint
    * @param weightVector
    * @return
    */
  def loss(dataPoint: LabeledVector, weightVector: WeightVector): Double = {
    lossGradient(dataPoint, weightVector)._1
  }

  /** Calculates the gradient of the loss function given a data point and weight vector
    *
    * @param dataPoint
    * @param weightVector
    * @return
    */
  def gradient(dataPoint: LabeledVector, weightVector: WeightVector): WeightVector = {
    lossGradient(dataPoint, weightVector)._2
  }

  /** Calculates the gradient as well as the loss given a data point and the weight vector
    *
    * @param dataPoint
    * @param weightVector
    * @return
    */
  def lossGradient(dataPoint: LabeledVector, weightVector: WeightVector): (Double, WeightVector)
}

/** Generic loss function which lets you build a loss function out of the [[PartialLossFunction]]
  * and the [[PredictionFunction]].
  *
  * @param partialLossFunction
  * @param predictionFunction
  */
case class GenericLossFunction(
    partialLossFunction: PartialLossFunction,
    predictionFunction: PredictionFunction)
  extends LossFunction {

  /** Calculates the gradient as well as the loss given a data point and the weight vector
    *
    * @param dataPoint
    * @param weightVector
    * @return
    */
  def lossGradient(dataPoint: LabeledVector, weightVector: WeightVector): (Double, WeightVector) = {
    val prediction = predictionFunction.predict(dataPoint.vector, weightVector)

    val loss = partialLossFunction.loss(prediction, dataPoint.label)

    val lossDerivative = partialLossFunction.derivative(prediction, dataPoint.label)

    val WeightVector(weightGradient, interceptGradient) =
      predictionFunction.gradient(dataPoint.vector, weightVector)

    BLAS.scal(lossDerivative, weightGradient)

    (loss, WeightVector(weightGradient, lossDerivative * interceptGradient))
  }
}
