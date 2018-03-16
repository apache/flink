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

package org.apache.flink.ml.regression

import org.apache.flink.api.scala.DataSet
import org.apache.flink.ml.math.{Breeze, Vector}
import org.apache.flink.ml.common._

import org.apache.flink.api.scala._
import org.apache.flink.ml.optimization.LearningRateMethod.LearningRateMethodTrait

import org.apache.flink.ml.optimization._
import org.apache.flink.ml.pipeline.{PredictOperation, FitOperation, Predictor}


/** Multiple linear regression using the ordinary least squares (OLS) estimator.
  *
  * The linear regression finds a solution to the problem
  *
  * `y = w_0 + w_1*x_1 + w_2*x_2 ... + w_n*x_n = w_0 + w^T*x`
  *
  * such that the sum of squared residuals is minimized
  *
  * `min_{w, w_0} \sum (y - w^T*x - w_0)^2`
  *
  * The minimization problem is solved by (stochastic) gradient descent. For each labeled vector
  * `(x,y)`, the gradient is calculated. The weighted average of all gradients is subtracted from
  * the current value `w` which gives the new value of `w_new`. The weight is defined as
  * `stepsize/math.sqrt(iteration)`.
  *
  * The optimization runs at most a maximum number of iterations or, if a convergence threshold has
  * been set, until the convergence criterion has been met. As convergence criterion the relative
  * change of the sum of squared residuals is used:
  *
  * `(S_{k-1} - S_k)/S_{k-1} < \rho`
  *
  * with S_k being the sum of squared residuals in iteration k and `\rho` being the convergence
  * threshold.
  *
  * At the moment, the whole partition is used for SGD, making it effectively a batch gradient
  * descent. Once a sampling operator has been introduced, the algorithm can be optimized.
  *
  * @example
  *          {{{
  *             val mlr = MultipleLinearRegression()
  *               .setIterations(10)
  *               .setStepsize(0.5)
  *               .setConvergenceThreshold(0.001)
  *
  *             val trainingDS: DataSet[LabeledVector] = ...
  *             val testingDS: DataSet[Vector] = ...
  *
  *             mlr.fit(trainingDS)
  *
  *             val predictions = mlr.predict(testingDS)
  *          }}}
  *
  * =Parameters=
  *
  *  - [[org.apache.flink.ml.regression.MultipleLinearRegression.Iterations]]:
  *  Maximum number of iterations.
  *
  *  - [[org.apache.flink.ml.regression.MultipleLinearRegression.Stepsize]]:
  *  Initial step size for the gradient descent method.
  *  This value controls how far the gradient descent method moves in the opposite direction of the
  *  gradient. Tuning this parameter might be crucial to make it stable and to obtain a better
  *  performance.
  *
  *  - [[org.apache.flink.ml.regression.MultipleLinearRegression.ConvergenceThreshold]]:
  *  Threshold for relative change of sum of squared residuals until convergence.
  *
  *  - [[LearningRateMethodTrait]]:
  *  The method used to calculate the effective learning rate for each iteration step. See
  *  [[LearningRateMethod]] for all supported methods.
  *
  */
class MultipleLinearRegression extends Predictor[MultipleLinearRegression] {
  import org.apache.flink.ml._
  import MultipleLinearRegression._

  // Stores the weights of the linear model after the fitting phase
  var weightsOption: Option[DataSet[WeightVector]] = None

  def setIterations(iterations: Int): MultipleLinearRegression = {
    parameters.add(Iterations, iterations)
    this
  }

  def setStepsize(stepsize: Double): MultipleLinearRegression = {
    parameters.add(Stepsize, stepsize)
    this
  }

  def setConvergenceThreshold(convergenceThreshold: Double): MultipleLinearRegression = {
    parameters.add(ConvergenceThreshold, convergenceThreshold)
    this
  }

  def setLearningRateMethod(
      learningRateMethod: LearningRateMethodTrait)
    : MultipleLinearRegression = {
    parameters.add(LearningRateMethodValue, learningRateMethod)
    this
  }

  def squaredResidualSum(input: DataSet[LabeledVector]): DataSet[Double] = {
    weightsOption match {
      case Some(weights) => {
        input.mapWithBcVariable(weights){
          (dataPoint, weights) => lossFunction.loss(dataPoint, weights)
        }.reduce {
          _ + _
        }
      }

      case None => {
        throw new RuntimeException("The MultipleLinearRegression has not been fitted to the " +
          "data. This is necessary to learn the weight vector of the linear function.")
      }
    }

  }
}

object MultipleLinearRegression {

  val WEIGHTVECTOR_BROADCAST = "weights_broadcast"

  val lossFunction = GenericLossFunction(SquaredLoss, LinearPrediction)

  // ====================================== Parameters =============================================

  case object Stepsize extends Parameter[Double] {
    val defaultValue = Some(0.1)
  }

  case object Iterations extends Parameter[Int] {
    val defaultValue = Some(10)
  }

  case object ConvergenceThreshold extends Parameter[Double] {
    val defaultValue = None
  }

  case object LearningRateMethodValue extends Parameter[LearningRateMethodTrait] {
    val defaultValue = None
  }

  // ======================================== Factory methods ======================================

  def apply(): MultipleLinearRegression = {
    new MultipleLinearRegression()
  }

  // ====================================== Operations =============================================

  /** Trains the linear model to fit the training data. The resulting weight vector is stored in
    * the [[MultipleLinearRegression]] instance.
    *
    */
  implicit val fitMLR = new FitOperation[MultipleLinearRegression, LabeledVector] {
    override def fit(
      instance: MultipleLinearRegression,
      fitParameters: ParameterMap,
      input: DataSet[LabeledVector])
    : Unit = {
      val map = instance.parameters ++ fitParameters

      // retrieve parameters of the algorithm
      val numberOfIterations = map(Iterations)
      val stepsize = map(Stepsize)
      val convergenceThreshold = map.get(ConvergenceThreshold)
      val learningRateMethod = map.get(LearningRateMethodValue)

      val lossFunction = GenericLossFunction(SquaredLoss, LinearPrediction)

      val optimizer = GradientDescent()
        .setIterations(numberOfIterations)
        .setStepsize(stepsize)
        .setLossFunction(lossFunction)

      convergenceThreshold match {
        case Some(threshold) => optimizer.setConvergenceThreshold(threshold)
        case None =>
      }

      learningRateMethod match {
        case Some(method) => optimizer.setLearningRateMethod(method)
        case None =>
      }

      instance.weightsOption = Some(optimizer.optimize(input, None))
    }
  }

  implicit def predictVectors[T <: Vector] = {
    new PredictOperation[MultipleLinearRegression, WeightVector, T, Double]() {
      override def getModel(self: MultipleLinearRegression, predictParameters: ParameterMap)
        : DataSet[WeightVector] = {
        self.weightsOption match {
          case Some(weights) => weights


          case None => {
            throw new RuntimeException("The MultipleLinearRegression has not been fitted to the " +
              "data. This is necessary to learn the weight vector of the linear function.")
          }
        }
      }
      override def predict(value: T, model: WeightVector): Double = {
        import Breeze._
        val WeightVector(weights, weight0) = model
        val dotProduct = value.asBreeze.dot(weights.asBreeze)
        dotProduct + weight0
      }
    }
  }
}
