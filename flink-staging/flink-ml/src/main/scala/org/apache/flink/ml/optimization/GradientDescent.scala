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

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.ml.common._
import org.apache.flink.ml.math._
import org.apache.flink.ml.optimization.IterativeSolver.{Iterations, Stepsize}
import org.apache.flink.ml.optimization.Solver._

/** This [[Solver]] performs Stochastic Gradient Descent optimization using mini batches
  *
  * For each labeled vector in a mini batch the gradient is computed and added to a partial
  * gradient. The partial gradients are then summed and divided by the size of the batches. The
  * average gradient is then used to updated the weight values, including regularization.
  *
  * At the moment, the whole partition is used for SGD, making it effectively a batch gradient
  * descent. Once a sampling operator has been introduced, the algorithm can be optimized
  *
  * @param runParameters The parameters to tune the algorithm. Currently these include:
  *                      [[Solver.LossFunction]] for the loss function to be used,
  *                      [[Solver.RegularizationType]] for the type of regularization,
  *                      [[Solver.RegularizationParameter]] for the regularization parameter,
  *                      [[IterativeSolver.Iterations]] for the maximum number of iteration,
  *                      [[IterativeSolver.Stepsize]] for the learning rate used.
  */
class GradientDescent(runParameters: ParameterMap) extends IterativeSolver {

  import Solver.WEIGHTVECTOR_BROADCAST

  var parameterMap: ParameterMap = parameters ++ runParameters

  /** Performs one iteration of Stochastic Gradient Descent using mini batches
    *
    * @param data A Dataset of LabeledVector (label, features) pairs
    * @param currentWeights A Dataset with the current weights to be optimized as its only element
    * @return A Dataset containing the weights after one stochastic gradient descent step
    */
  private def SGDStep(data: DataSet[(LabeledVector)], currentWeights: DataSet[WeightVector]):
  DataSet[WeightVector] = {

    // TODO: Sample from input to realize proper SGD
    data.map {
      new GradientCalculation
    }.withBroadcastSet(currentWeights, WEIGHTVECTOR_BROADCAST).reduce {
      (left, right) =>
        val (leftGradVector, leftLoss, leftCount) = left
        val (rightGradVector, rightLoss, rightCount) = right
        // Add the left gradient to the right one
        BLAS.axpy(1.0, leftGradVector.weights, rightGradVector.weights)
        val gradients = WeightVector(
          rightGradVector.weights, leftGradVector.intercept + rightGradVector.intercept)

        (gradients , leftLoss + rightLoss, leftCount + rightCount)
    }.map {
      new WeightsUpdate
    }.withBroadcastSet(currentWeights, WEIGHTVECTOR_BROADCAST)
  }

  /** Provides a solution for the given optimization problem
    *
    * @param data A Dataset of LabeledVector (label, features) pairs
    * @param initWeights The initial weights that will be optimized
    * @return The weights, optimized for the provided data.
    */
  override def optimize(
    data: DataSet[LabeledVector],
    initWeights: Option[DataSet[WeightVector]]): DataSet[WeightVector] = {
    // TODO: Faster way to do this?
    val dimensionsDS = data.map(_.vector.size).reduce((a, b) => b)

    val numberOfIterations: Int = parameterMap(Iterations)

    // Initialize weights
    val initialWeightsDS: DataSet[WeightVector] = initWeights match {
      // Ensure provided weight vector is a DenseVector
      case Some(wvDS) => {
        wvDS.map{wv => {
          val denseWeights = wv.weights match {
            case dv: DenseVector => dv
            case sv: SparseVector => sv.toDenseVector
          }
          WeightVector(denseWeights, wv.intercept)
        }

        }
      }
      case None => createInitialWeightVector(dimensionsDS)
    }

    // Perform the iterations
    // TODO: Enable convergence stopping criterion, as in Multiple Linear regression
    initialWeightsDS.iterate(numberOfIterations) {
      weightVector => {
        SGDStep(data, weightVector)
      }
    }
  }

  /** Mapping function that calculates the weight gradients from the data.
    *
    */
  private class GradientCalculation extends
    RichMapFunction[LabeledVector, (WeightVector, Double, Int)] {

    var weightVector: WeightVector = null

    @throws(classOf[Exception])
    override def open(configuration: Configuration): Unit = {
      val list = this.getRuntimeContext.
        getBroadcastVariable[WeightVector](WEIGHTVECTOR_BROADCAST)

      weightVector = list.get(0)
    }

    override def map(example: LabeledVector): (WeightVector, Double, Int) = {

      val lossFunction = parameterMap(LossFunction)
      val regType = parameterMap(RegularizationType)
      val regParameter = parameterMap(RegularizationParameter)
      val predictionFunction = parameterMap(PredictionFunctionParameter)
      val dimensions = example.vector.size
      // TODO(tvas): Any point in carrying the weightGradient vector for in-place replacement?
      // The idea in spark is to avoid object creation, but here we have to do it anyway
      val weightGradient = new DenseVector(new Array[Double](dimensions))

      // TODO(tvas): Indentation here?
      val (loss, lossDeriv) = lossFunction.lossAndGradient(
                                example,
                                weightVector,
                                weightGradient,
                                regType,
                                regParameter,
                                predictionFunction)

      (new WeightVector(weightGradient, lossDeriv), loss, 1)
    }
  }

  /** Performs the update of the weights, according to the given gradients and regularization type.
    *
    */
  private class WeightsUpdate() extends
  RichMapFunction[(WeightVector, Double, Int), WeightVector] {

    var weightVector: WeightVector = null

    @throws(classOf[Exception])
    override def open(configuration: Configuration): Unit = {
      val list = this.getRuntimeContext.
        getBroadcastVariable[WeightVector](WEIGHTVECTOR_BROADCAST)

      weightVector = list.get(0)
    }

    override def map(gradientLossAndCount: (WeightVector, Double, Int)): WeightVector = {
      val regType = parameterMap(RegularizationType)
      val regParameter = parameterMap(RegularizationParameter)
      val stepsize = parameterMap(Stepsize)
      val weightGradients = gradientLossAndCount._1
      val lossSum = gradientLossAndCount._2
      val count = gradientLossAndCount._3

      // Scale the gradients according to batch size
      BLAS.scal(1.0/count, weightGradients.weights)

      // Calculate the regularized loss and, if the regularization is differentiable, add the
      // regularization term to the gradient as well, in-place
      // Note(tvas): adjustedLoss is never used currently, but I'd like to leave it here for now.
      // We can probably maintain a loss history as the optimization package grows towards a
      // Breeze-like interface (see breeze.optimize.FirstOrderMinimizer)
      val adjustedLoss = {
        regType match {
          case x: DiffRegularization => {
            x.regularizedLossAndGradient(
              lossSum / count,
              weightVector.weights,
              weightGradients.weights,
              regParameter)
          }
          case x: Regularization => {
            x.regLoss(
              lossSum / count,
              weightVector.weights,
              regParameter)
          }
        }
      }

      val weight0Gradient = weightGradients.intercept / count

      val iteration = getIterationRuntimeContext.getSuperstepNumber

      // Scale initial stepsize by the inverse square root of the iteration number
      // TODO(tvas): There are more ways to determine the stepsize, possible low-effort extensions
      // here
      val effectiveStepsize = stepsize/math.sqrt(iteration)

      // Take the gradient step for the intercept
      weightVector.intercept -= effectiveStepsize * weight0Gradient

      // Take the gradient step for the weight vector, possibly applying regularization
      // TODO(tvas): This should be moved to a takeStep() function that takes regType plus all these
      // arguments, this would decouple the update step from the regularization classes
      regType.takeStep(weightVector.weights, weightGradients.weights,
        effectiveStepsize, regParameter)

      weightVector
    }
  }
}

object GradientDescent {
  def apply(): GradientDescent = {
    new GradientDescent(new ParameterMap())
  }

  def apply(parameterMap: ParameterMap): GradientDescent = {
    new GradientDescent(parameterMap)
  }
}



