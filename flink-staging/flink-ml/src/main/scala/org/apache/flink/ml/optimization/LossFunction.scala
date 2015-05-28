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
trait LossFunction extends Serializable {

  def loss(dataPoint: LabeledVector, weightVector: WeightVector): Double

  def gradient(dataPoint: LabeledVector, weightVector: WeightVector): WeightVector

  def updateWeightVector(
      oldWeightVector: WeightVector,
      gradient: WeightVector,
      learningRate: Double)
    : WeightVector
}

case class GenericLossFunction(
    partialLossFunction: PartialLossFunction,
    predictionFunction: PredictionFunction,
    regularizationFunction: RegularizationFunction,
    regularizationValue: Double)
  extends LossFunction {

  def loss(dataPoint: LabeledVector, weightVector: WeightVector): Double = {
    val prediction = predictionFunction.predict(dataPoint.vector, weightVector)

    partialLossFunction.loss(prediction, dataPoint.label) +
      regularizationValue * regularizationFunction.regularization(weightVector.weights)
  }

  def gradient(dataPoint: LabeledVector, weightVector: WeightVector): WeightVector = {
    val prediction = predictionFunction.predict(dataPoint.vector, weightVector)

    val lossDeriv = partialLossFunction.gradient(prediction, dataPoint.label)

    val WeightVector(predWeightGradient, predIntGradient) =
      predictionFunction.gradient(dataPoint.vector, weightVector)

    regularizationFunction.gradient(weightVector.weights) match {
      case Some(regWeightGradient) => {
        BLAS.scal(regularizationValue, regWeightGradient)
        BLAS.axpy(lossDeriv, predWeightGradient, regWeightGradient)

        WeightVector(regWeightGradient, lossDeriv * predIntGradient)
      }
      case None => {
        BLAS.scal(lossDeriv, predWeightGradient)
        WeightVector(predWeightGradient, lossDeriv * predIntGradient)
      }
    }
  }

  def updateWeightVector(
    oldWeightVector: WeightVector,
    gradient: WeightVector,
    learningRate: Double)
  : WeightVector = {

    val updatedWeigths = regularizationFunction.updateWeights(
      oldWeightVector.weights,
      gradient.weights,
      learningRate,
      regularizationValue)

    WeightVector(updatedWeigths, oldWeightVector.intercept - learningRate * gradient.intercept)
  }
}
