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
  * The [[LogisticLoss]] function implements `log(1 + -exp(prediction*label))`
  * for binary classification with label in {-1, 1}
  */
object LogisticLoss extends PartialLossFunction {

  /** Calculates the loss depending on the label and the prediction
    *
    * @param prediction The predicted value
    * @param label The true value
    * @return The loss
    */
  override def loss(prediction: Double, label: Double): Double = {
    val z = prediction * label

    // based on implementation in scikit-learn
    // approximately equal and saves the computation of the log
    if (z > 18) {
      math.exp(-z)
    }
    else if (z < -18) {
      -z
    }
    else {
      math.log(1 + math.exp(-z))
    }
  }

  /** Calculates the derivative of the loss function with respect to the prediction
    *
    * @param prediction The predicted value
    * @param label The true value
    * @return The derivative of the loss function
    */
  override def derivative(prediction: Double, label: Double): Double = {
    val z = prediction * label

    // based on implementation in scikit-learn
    // approximately equal and saves the computation of the log
    if (z > 18) {
      label * math.exp(-z)
    }
    else if (z < -18) {
      -label
    }
    else {
      -label/(math.exp(z) + 1)
    }
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
