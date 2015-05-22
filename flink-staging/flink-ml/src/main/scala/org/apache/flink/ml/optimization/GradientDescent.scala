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
import org.apache.flink.ml.optimization.IterativeSolver.{ConvergenceThreshold, Iterations, Stepsize}
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
  *  The parameters to tune the algorithm are:
  *                      [[Solver.LossFunction]] for the loss function to be used,
  *                      [[Solver.RegularizationType]] for the type of regularization,
  *                      [[Solver.RegularizationParameter]] for the regularization parameter,
  *                      [[IterativeSolver.Iterations]] for the maximum number of iteration,
  *                      [[IterativeSolver.Stepsize]] for the learning rate used.
  *                      [[IterativeSolver.ConvergenceThreshold]] when provided the algorithm will
  *                      stop the iterations if the relative change in the value of the objective
  *                      function between successive iterations is is smaller than this value.
  */
class GradientDescent() extends IterativeSolver {

  import Solver.WEIGHTVECTOR_BROADCAST

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
    * @param initialWeights The initial weights that will be optimized
    * @return The weights, optimized for the provided data.
    */
  override def optimize(
    data: DataSet[LabeledVector],
    initialWeights: Option[DataSet[WeightVector]]): DataSet[WeightVector] = {
    val numberOfIterations: Int = parameters(Iterations)
    val convergenceThresholdOption: Option[Double] = parameters.get(ConvergenceThreshold)

    // Initialize weights
    val initialWeightsDS: DataSet[WeightVector] = createInitialWeightsDS(initialWeights, data)

    // Perform the iterations
    val optimizedWeights = convergenceThresholdOption match {
      // No convergence criterion
      case None =>
        initialWeightsDS.iterate(numberOfIterations) {
          weightVectorDS => {
            SGDStep(data, weightVectorDS)
          }
        }
      case Some(convergence) =>
        // Calculates the regularized loss, from the data and given weights
        def lossCalculation(data: DataSet[LabeledVector], weightDS: DataSet[WeightVector]):
        DataSet[Double] = {
          data
            .map {new LossCalculation}.withBroadcastSet(weightDS, WEIGHTVECTOR_BROADCAST)
            .reduce {
              (left, right) =>
                val (leftLoss, leftCount) = left
                val (rightLoss, rightCount) = right
                (leftLoss + rightLoss, rightCount + leftCount)
            }
            .map{new RegularizedLossCalculation}.withBroadcastSet(weightDS, WEIGHTVECTOR_BROADCAST)
        }
        // We have to calculate for each weight vector the sum of squared residuals,
        // and then sum them and apply regularization
        val initialLossSumDS = lossCalculation(data, initialWeightsDS)

        // Combine weight vector with the current loss
        val initialWeightsWithLossSum = initialWeightsDS.
          crossWithTiny(initialLossSumDS).setParallelism(1)

        val resultWithLoss = initialWeightsWithLossSum.
          iterateWithTermination(numberOfIterations) {
          weightsWithLossSum =>

            // Extract weight vector and loss
            val previousWeightsDS = weightsWithLossSum.map{_._1}
            val previousLossSumDS = weightsWithLossSum.map{_._2}

            val currentWeightsDS = SGDStep(data, previousWeightsDS)

            val currentLossSumDS = lossCalculation(data, currentWeightsDS)

            // Check if the relative change in the loss is smaller than the
            // convergence threshold. If yes, then terminate i.e. return empty termination data set
            val termination = previousLossSumDS.crossWithTiny(currentLossSumDS).setParallelism(1).
              filter{
              pair => {
                val (previousLoss, currentLoss) = pair

                if (previousLoss <= 0) {
                  false
                } else {
                  math.abs((previousLoss - currentLoss)/previousLoss) >= convergence
                }
              }
            }

            // Result for new iteration
            (currentWeightsDS cross currentLossSumDS, termination)
        }
        // Return just the weights
        resultWithLoss.map{_._1}
    }
    optimizedWeights
  }

  /** Calculates the loss value, given a labeled vector and the current weight vector
    *
    * The weight vector is received as a broadcast variable.
    */
  private class LossCalculation extends RichMapFunction[LabeledVector, (Double, Int)] {

    var weightVector: WeightVector = null

    @throws(classOf[Exception])
    override def open(configuration: Configuration): Unit = {
      val list = this.getRuntimeContext.
        getBroadcastVariable[WeightVector](WEIGHTVECTOR_BROADCAST)

      weightVector = list.get(0)
    }

    override def map(example: LabeledVector): (Double, Int) = {
      val lossFunction = parameters(LossFunction)
      val predictionFunction = parameters(PredictionFunction)

      val loss = lossFunction.lossValue(
        example,
        weightVector,
        predictionFunction)

      (loss, 1)
    }
  }

/** Calculates the regularized loss value, given the loss and the current weight vector
  *
  * The weight vector is received as a broadcast variable.
  */
private class RegularizedLossCalculation extends RichMapFunction[(Double, Int), Double] {

  var weightVector: WeightVector = null

  @throws(classOf[Exception])
  override def open(configuration: Configuration): Unit = {
    val list = this.getRuntimeContext.
      getBroadcastVariable[WeightVector](WEIGHTVECTOR_BROADCAST)

    weightVector = list.get(0)
  }

  override def map(lossAndCount: (Double, Int)): Double = {
    val (lossSum, count) = lossAndCount
    val regType = parameters(RegularizationType)
    val regParameter = parameters(RegularizationParameter)

    val regularizedLoss = {
      regType.regLoss(
        lossSum/count,
        weightVector.weights,
        regParameter)
    }
    regularizedLoss
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
      val regType = parameters(RegularizationType)
      val regParameter = parameters(RegularizationParameter)
      val stepsize = parameters(Stepsize)
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
    new GradientDescent()
  }
}



