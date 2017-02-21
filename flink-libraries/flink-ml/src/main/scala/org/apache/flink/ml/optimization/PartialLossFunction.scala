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

/** Represents loss functions which can be used with the [[GenericLossFunction]].
  *
  */
trait PartialLossFunction extends Serializable {
  /** Calculates the loss depending on the label and the prediction
    *
    * @param prediction The predicted value
    * @param label The true value
    * @return The loss
    */
  def loss(prediction: Double, label: Double): Double

  /** Calculates the derivative of the [[PartialLossFunction]]
    * 
    * @param prediction The predicted value
    * @param label The true value
    * @return The derivative of the loss function
    */
  def derivative(prediction: Double, label: Double): Double
}

/** Squared loss function which can be used with the [[GenericLossFunction]]
  *
  * The [[SquaredLoss]] function implements `1/2 (prediction - label)^2`
  */
object SquaredLoss extends PartialLossFunction {

  /** Calculates the loss depending on the label and the prediction
    *
    * @param prediction The predicted value
    * @param label The true value
    * @return The loss
    */
  override def loss(prediction: Double, label: Double): Double = {
    0.5 * (prediction - label) * (prediction - label)
  }

  /** Calculates the derivative of the [[PartialLossFunction]]
    *
    * @param prediction The predicted value
    * @param label The true value
    * @return The derivative of the loss function
    */
  override def derivative(prediction: Double, label: Double): Double = {
    prediction - label
  }
}

/** Logistic loss function which can be used with the [[GenericLossFunction]]
  *
  *
  * The [[LogisticLoss]] function implements `-y*log(g(prediction))-(1-y)*log(g(prediction))`
  * for binary classification with label in {0, 1}.Where function g is the sigmoid function.
  * The sigmoid function is defined as:
  *
  * `g(z) = 1 / (1+exp(-z))`
  */
object LogisticLoss extends PartialLossFunction {

  /** Calculates the loss depending on the label and the prediction
    *
    * @param prediction The predicted value
    * @param label The true value
    * @return The loss
    */
  override def loss(prediction: Double, label: Double): Double = {
    if (label > 0) {
      math.log(1 + math.exp(prediction))
    } else {
      math.log(1 + math.exp(prediction)) - prediction
    }
  }

  /** Calculates the derivative of the loss function with respect to the prediction
    *
    * @param prediction The predicted value
    * @param label The true value
    * @return The derivative of the loss function
    */
  override def derivative(prediction: Double, label: Double): Double = {
    (1.0 / (1.0 + math.exp(-prediction))) - label
  }
}

/** Hinge loss function which can be used with the [[GenericLossFunction]]
  *
  * The [[HingeLoss]] function implements `max(0, 1 - prediction*label)`
  * for binary classification with label in {-1, 1}
  */
object HingeLoss extends PartialLossFunction {
  /** Calculates the loss for a given prediction/truth pair
    *
    * @param prediction The predicted value
    * @param label The true value
    * @return The loss
    */
  override def loss(prediction: Double, label: Double): Double = {
    val z = prediction * label
    math.max(0, 1 - z)
  }

  /** Calculates the derivative of the loss function with respect to the prediction
    *
    * @param prediction The predicted value
    * @param label The true value
    * @return The derivative of the loss function
    */
  override def derivative(prediction: Double, label: Double): Double = {
    val z = prediction * label
    if (z <= 1) {
      -label
    }
    else {
      0
    }
  }
}
