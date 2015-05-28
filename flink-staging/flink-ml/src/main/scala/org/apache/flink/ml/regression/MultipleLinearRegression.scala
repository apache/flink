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

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.DataSet
import org.apache.flink.configuration.Configuration
import org.apache.flink.ml.math.{DenseVector, BLAS, Vector, vector2Array}
import org.apache.flink.ml.common._

import org.apache.flink.api.scala._

import com.github.fommil.netlib.BLAS.{ getInstance => blas }
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
  * The optimization runs at most a maximum number of iteratinos or, if a convergence threshold has
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

  import MultipleLinearRegression._

  // Stores the weights of the linear model after the fitting phase
  var weightsOption: Option[DataSet[(Array[Double], Double)]] = None

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
        input.map {
          new SquaredResiduals
        }.withBroadcastSet(weights, WEIGHTVECTOR_BROADCAST).reduce {
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

      // calculate dimension of the feature vectors
      val dimension = input.map{_.vector.size}.reduce {
        (a, b) =>
          require(a == b, "All input vector must have the same dimension.")
          a
      }

      input.flatMap{
        t =>
          Seq(t)
      }

      // initial weight vector is set to 0
      val initialWeightVector = createInitialWeightVector(dimension)

      // check if a convergence threshold has been set
      val resultingWeightVector = convergenceThreshold match {
        case Some(convergence) =>

          // we have to calculate for each weight vector the sum of squared residuals
          val initialSquaredResidualSum = input.map {
            new SquaredResiduals
          }.withBroadcastSet(initialWeightVector, WEIGHTVECTOR_BROADCAST).reduce {
            _ + _
          }

          // combine weight vector with current sum of squared residuals
          val initialWeightVectorWithSquaredResidualSum = initialWeightVector.
            crossWithTiny(initialSquaredResidualSum).setParallelism(1)

          // start SGD iteration
          val resultWithResidual = initialWeightVectorWithSquaredResidualSum.
            iterateWithTermination(numberOfIterations) {
            weightVectorSquaredResidualDS =>

              // extract weight vector and squared residual sum
              val weightVector = weightVectorSquaredResidualDS.map{_._1}
              val squaredResidualSum = weightVectorSquaredResidualDS.map{_._2}

              // TODO: Sample from input to realize proper SGD
              val newWeightVector = input.map {
                new LinearRegressionGradientDescent
              }.withBroadcastSet(weightVector, WEIGHTVECTOR_BROADCAST).reduce {
                (left, right) =>
                  val (leftBetas, leftBeta0, leftCount) = left
                  val (rightBetas, rightBeta0, rightCount) = right

                  blas.daxpy(leftBetas.length, 1.0, rightBetas, 1, leftBetas, 1)

                  (leftBetas, leftBeta0 + rightBeta0, leftCount + rightCount)
              }.map {
                new LinearRegressionWeightsUpdate(stepsize)
              }.withBroadcastSet(weightVector, WEIGHTVECTOR_BROADCAST)

              // calculate the sum of squared residuals for the new weight vector
              val newResidual = input.map {
                new SquaredResiduals
              }.withBroadcastSet(newWeightVector, WEIGHTVECTOR_BROADCAST).reduce {
                _ + _
              }

              // check if the relative change in the squared residual sum is smaller than the
              // convergence threshold. If yes, then terminate => return empty termination data set
              val termination = squaredResidualSum.crossWithTiny(newResidual).setParallelism(1).
                filter{
                pair => {
                  val (residual, newResidual) = pair

                  if (residual <= 0) {
                    false
                  } else {
                    math.abs((residual - newResidual)/residual) >= convergence
                  }
                }
              }

              // result for new iteration
              (newWeightVector cross newResidual, termination)
          }

          // remove squared residual sum to only return the weight vector
          resultWithResidual.map{_._1}

        case None =>
          // No convergence criterion
          initialWeightVector.iterate(numberOfIterations) {
            weightVector => {

              // TODO: Sample from input to realize proper SGD
              input.map {
                new LinearRegressionGradientDescent
              }.withBroadcastSet(weightVector, WEIGHTVECTOR_BROADCAST).reduce {
                (left, right) =>
                  val (leftBetas, leftBeta0, leftCount) = left
                  val (rightBetas, rightBeta0, rightCount) = right

                  blas.daxpy(leftBetas.length, 1, rightBetas, 1, leftBetas, 1)
                  (leftBetas, leftBeta0 + rightBeta0, leftCount + rightCount)
              }.map {
                new LinearRegressionWeightsUpdate(stepsize)
              }.withBroadcastSet(weightVector, WEIGHTVECTOR_BROADCAST)
            }
          }
      }

      instance.weightsOption = Some(resultingWeightVector)
    }
  }

  /** Creates a DataSet with one zero vector. The zero vector has dimension d, which is given
    * by the dimensionDS.
    *
    * @param dimensionDS DataSet with one element d, denoting the dimension of the returned zero
    *                    vector
    * @return DataSet of a zero vector of dimension d
    */
  private def createInitialWeightVector(dimensionDS: DataSet[Int]):
  DataSet[(Array[Double], Double)] = {
    dimensionDS.map {
      dimension =>
        val values = Array.fill(dimension)(0.0)
        (values, 0.0)
    }
  }

  /** Calculates the predictions for new data with respect to the learned linear model.
    *
    * @tparam T Testing data type for which the prediction is calculated. Has to be a subtype of
    *           [[Vector]]
    * @return
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
            input.map(new LinearRegressionPrediction[T])
              .withBroadcastSet(weights, WEIGHTVECTOR_BROADCAST)
          }

          case None => {
            throw new RuntimeException("The MultipleLinearRegression has not been fitted to the " +
              "data. This is necessary to learn the weight vector of the linear function.")
          }
        }
      }
    }
  }

  private class LinearRegressionPrediction[T <: Vector] extends RichMapFunction[T, LabeledVector] {
    private var weights: Array[Double] = null
    private var weight0: Double = 0


    @throws(classOf[Exception])
    override def open(configuration: Configuration): Unit = {
      val t = getRuntimeContext
        .getBroadcastVariable[(Array[Double], Double)](WEIGHTVECTOR_BROADCAST)

      val weightsPair = t.get(0)

      weights = weightsPair._1
      weight0 = weightsPair._2
    }

    override def map(value: T): LabeledVector = {
      val dotProduct = blas.ddot(weights.length, weights, 1, vector2Array(value), 1)

      val prediction = dotProduct + weight0

      LabeledVector(prediction, value)
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
            input.map(new LinearRegressionLabeledPrediction)
              .withBroadcastSet(weights, WEIGHTVECTOR_BROADCAST)
          }

          case None => {
            throw new RuntimeException("The MultipleLinearRegression has not been fitted to the " +
              "data. This is necessary to learn the weight vector of the linear function.")
          }
        }
      }
    }
  }

  private class LinearRegressionLabeledPrediction
    extends RichMapFunction[LabeledVector, (Double, Double)] {
    private var weights: Array[Double] = null
    private var weight0: Double = 0


    @throws(classOf[Exception])
    override def open(configuration: Configuration): Unit = {
      val t = getRuntimeContext
        .getBroadcastVariable[(Array[Double], Double)](WEIGHTVECTOR_BROADCAST)

      val weightsPair = t.get(0)

      weights = weightsPair._1
      weight0 = weightsPair._2
    }

    override def map(labeledVector: LabeledVector ): (Double, Double) = {

      val truth = labeledVector.label
      val dotProduct = BLAS.dot(DenseVector(weights), labeledVector.vector)

      val prediction = dotProduct + weight0

      (truth, prediction)
    }
  }
}

//--------------------------------------------------------------------------------------------------
//  Flink function definitions
//--------------------------------------------------------------------------------------------------

/** Calculates for a labeled vector and the current weight vector its squared residual:
  *
  * `(y - (w^Tx + w_0))^2`
  *
  * The weight vector is received as a broadcast variable.
  */
private class SquaredResiduals extends RichMapFunction[LabeledVector, Double] {
  import MultipleLinearRegression.WEIGHTVECTOR_BROADCAST

  var weightVector: Array[Double] = null
  var weight0: Double = 0.0

  @throws(classOf[Exception])
  override def open(configuration: Configuration): Unit = {
    val list = this.getRuntimeContext.
      getBroadcastVariable[(Array[Double], Double)](WEIGHTVECTOR_BROADCAST)

    val weightsPair = list.get(0)

    weightVector = weightsPair._1
    weight0 = weightsPair._2
  }

  override def map(value: LabeledVector): Double = {
    val array = vector2Array(value.vector)
    val label = value.label

    val dotProduct = blas.ddot(weightVector.length, weightVector, 1, array, 1)

    val residual = dotProduct + weight0 - label

    residual * residual
  }
}

/** Calculates for a labeled vector and the current weight vector the gradient minimizing the
  * OLS equation. The gradient is given by:
  *
  * `dw = 2*(w^T*x + w_0 - y)*x`
  * `dw_0 = 2*(w^T*x + w_0 - y)`
  *
  * The weight vector is received as a broadcast variable.
  */
private class LinearRegressionGradientDescent extends
RichMapFunction[LabeledVector, (Array[Double], Double, Int)] {

  import MultipleLinearRegression.WEIGHTVECTOR_BROADCAST

  var weightVector: Array[Double] = null
  var weight0: Double = 0.0

  @throws(classOf[Exception])
  override def open(configuration: Configuration): Unit = {
    val list = this.getRuntimeContext.
      getBroadcastVariable[(Array[Double], Double)](WEIGHTVECTOR_BROADCAST)

    val weightsPair = list.get(0)

    weightVector = weightsPair._1
    weight0 = weightsPair._2
  }

  override def map(value: LabeledVector): (Array[Double], Double, Int) = {
    val x = vector2Array(value.vector)
    val label = value.label

    val dotProduct = blas.ddot(weightVector.length, weightVector, 1, x, 1)

    val error = dotProduct + weight0 - label

    // reuse vector x
    val weightsGradient = x

    blas.dscal(weightsGradient.length, 2*error, weightsGradient, 1)

    val weight0Gradient = 2 * error

    (weightsGradient, weight0Gradient, 1)
  }
}

/** Calculates the new weight vector based on the partial gradients. In order to do that,
  * all partial gradients are averaged and weighted by the current stepsize. This update value is
  * added to the current weight vector.
  *
  * @param stepsize Initial value of the step size used to update the weight vector
  */
private class LinearRegressionWeightsUpdate(val stepsize: Double) extends
RichMapFunction[(Array[Double], Double, Int), (Array[Double], Double)] {

  import MultipleLinearRegression.WEIGHTVECTOR_BROADCAST

  var weights: Array[Double] = null
  var weight0: Double = 0.0

  @throws(classOf[Exception])
  override def open(configuration: Configuration): Unit = {
    val list = this.getRuntimeContext.
      getBroadcastVariable[(Array[Double], Double)](WEIGHTVECTOR_BROADCAST)

    val weightsPair = list.get(0)

    weights = weightsPair._1
    weight0 = weightsPair._2
  }

  override def map(value: (Array[Double], Double, Int)): (Array[Double], Double) = {
    val weightsGradient = value._1
    blas.dscal(weightsGradient.length, 1.0/value._3, weightsGradient, 1)

    val weight0Gradient = value._2 / value._3

    val iteration = getIterationRuntimeContext.getSuperstepNumber

    // scale initial stepsize by the inverse square root of the iteration number to make it
    // decreasing
    val effectiveStepsize = stepsize/math.sqrt(iteration)

    val newWeights = weights.clone
    blas.daxpy(newWeights.length, -effectiveStepsize, weightsGradient, 1, newWeights, 1)
    val newWeight0 = weight0 - effectiveStepsize * weight0Gradient

    (newWeights, newWeight0)
  }
}
