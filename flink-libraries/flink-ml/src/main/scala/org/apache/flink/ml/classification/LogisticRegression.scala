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

package org.apache.flink.ml.classification

import org.apache.flink.api.scala._
import org.apache.flink.ml.common._
import org.apache.flink.ml.math._
import org.apache.flink.ml.optimization._
import org.apache.flink.ml.pipeline.{PredictOperation, Predictor}
import org.apache.flink.ml.regression.{GeneralizedLinearModel, WithIterativeSolver, WithRegularizationOption}

/** Logistic regression using the ordinary least squares (OLS) estimator.
  *
  * The logistic regression minimizes the following cost function:
  *
  * `-y*log(g(xw))-(1-y)*log(g(xw))`
  *
  * The label y is in {0, 1}, where function g is the sigmoid function.
  * The sigmoid function is defined as:
  *
  * `g(z) = 1 / (1+exp(-z))`
  *
  * The minimization problem is solved by (stochastic) gradient descent. For each labeled vector
  * `(x,y)`, the gradient is calculated. The weighted average of all gradients is subtracted from
  * the current value `w` which gives the new value of `w_new`. The weight is defined as
  * `stepsize/math.sqrt(iteration)`(default).
  *
  * @example
  *          {{{
  *             val mlr = LogisticRegression()
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
  *  - [[org.apache.flink.ml.regression.WithIterativeSolver.Iterations]]:
  *  Maximum number of iterations.
  *
  *  - [[org.apache.flink.ml.regression.WithIterativeSolver.Stepsize]]:
  *  Initial step size for the gradient descent method.
  *  This value controls how far the gradient descent method moves in the opposite direction of the
  *  gradient. Tuning this parameter might be crucial to make it stable and to obtain a better
  *  performance.
  *
  *  - [[org.apache.flink.ml.regression.WithIterativeSolver.ConvergenceThreshold]]:
  *  Threshold for relative change of sum of squared residuals until convergence.
  *
  *  - [[org.apache.flink.ml.regression.WithIterativeSolver.LearningRateMethodValue]]:
  *  The method used to calculate the effective learning rate for each iteration step. See
  *  [[LearningRateMethod]] for all supported methods.
  *
  */
class LogisticRegression
  extends Predictor[LogisticRegression]
    with GeneralizedLinearModel[LogisticRegression]
    with WithRegularizationOption[LogisticRegression]
    with WithIterativeSolver[LogisticRegression] {
  import LogisticRegression._

  val _solver: GradientDescent = GradientDescent()
    .setLossFunction(lossFunction)

  protected def solver: GradientDescent = _solver
}

object LogisticRegression {
  val lossFunction = GenericLossFunction(LogisticLoss, LinearPrediction)

  def apply(): LogisticRegression = {
    new LogisticRegression()
  }

  implicit def predictVectors[T <: Vector] = {
    new PredictOperation[LogisticRegression, WeightVector, T, Double]() {
      override def getModel(self: LogisticRegression, predictParameters: ParameterMap)
      : DataSet[WeightVector] = {
        self.weightsOption match {
          case Some(weights) => weights
          case None => {
            throw new RuntimeException("The LogisticRegression has not been fitted to the " +
              "data. This is necessary to learn the weight vector of the linear function.")
          }
        }
      }

      override def predict(value: T, model: WeightVector): Double = {
        import Breeze._
        val WeightVector(weights, weight0) = model
        val dotProduct = value.asBreeze.dot(weights.asBreeze) + weight0
        val score = 1.0 / (1.0 + math.exp(-dotProduct))
        if (score > 0.5) 1.0 else 0.0
      }
    }
  }
}

