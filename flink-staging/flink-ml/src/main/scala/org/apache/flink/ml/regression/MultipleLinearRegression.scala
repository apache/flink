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
import org.apache.flink.ml.math.Vector
import org.apache.flink.ml.common._

import org.apache.flink.api.scala._

import org.apache.flink.ml.optimization.{LinearPrediction, SquaredLoss, GenericLossFunction,
SimpleGradientDescent}
import org.apache.flink.ml.pipeline.{FitOperation, PredictOperation, Predictor}

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

  import org.apache.flink.ml._

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

      val lossFunction = GenericLossFunction(SquaredLoss, LinearPrediction)

      val optimizer = SimpleGradientDescent()
        .setIterations(numberOfIterations)
        .setStepsize(stepsize)
        .setLossFunction(lossFunction)

      convergenceThreshold match {
        case Some(threshold) => optimizer.setConvergenceThreshold(threshold)
        case None =>
      }

      instance.weightsOption = Some(optimizer.optimize(input, None))
    }
  }

  /** Calculates the predictions for new data with respect to the learned linear model.
    *
    * @tparam T Testing data type for which the prediction is calculated. Has to be a subtype of
    *           [[Vector]]
    * @return [[PredictOperation]] which calculates for a given vector it's label according to the
    *        linear model. The result of this [[PredictOperation]] is a [[LabeledVector]]
    */
  implicit def predictVectors[T <: Vector] = {
    new PredictOperation[MultipleLinearRegression, T, LabeledVector] {
      override def predict(
        instance: MultipleLinearRegression,
        predictParameters: ParameterMap,
        input: DataSet[T])
      : DataSet[LabeledVector] = {
        instance.weightsOption match {
          case Some(weights) => {
            input.mapWithBcVariable(weights) {
              (dataPoint, weights) =>
                LabeledVector(LinearPrediction.predict(dataPoint, weights), dataPoint)
            }
          }

          case None => {
            throw new RuntimeException("The MultipleLinearRegression has not been fitted to the " +
              "data. This is necessary to learn the weight vector of the linear function.")
          }
        }
      }
    }
  }

  /** Calculates the predictions for labeled data with respect to the learned linear model.
    *
    * @return A DataSet[(Double, Double)] where each tuple is a (truth, prediction) pair.
    */
  implicit def predictLabeledVectors = {
    new PredictOperation[MultipleLinearRegression, LabeledVector, (Double, Double)] {
      override def predict(
        instance: MultipleLinearRegression,
        predictParameters: ParameterMap,
        input: DataSet[LabeledVector])
      : DataSet[(Double, Double)] = {
        instance.weightsOption match {
          case Some(weights) => {
            input.mapWithBcVariable(weights) {
              (labeledVector, weights) => {
                (labeledVector.label, LinearPrediction.predict(labeledVector.vector, weights))
              }
            }
          }

          case None => {
            throw new RuntimeException("The MultipleLinearRegression has not been fitted to the " +
              "data. This is necessary to learn the weight vector of the linear function.")
          }
        }
      }
    }
  }
}
