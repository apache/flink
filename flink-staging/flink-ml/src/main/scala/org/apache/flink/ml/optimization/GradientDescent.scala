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

import org.apache.flink.api.scala._
import org.apache.flink.ml.common._
import org.apache.flink.ml.math._
import org.apache.flink.ml.optimization.IterativeSolver.{ConvergenceThreshold, Iterations, Stepsize}
import org.apache.flink.ml.optimization.Solver._
import org.apache.flink.ml._

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
  *                      [[Solver.RegularizationParameter]] for the regularization parameter,
  *                      [[IterativeSolver.Iterations]] for the maximum number of iteration,
  *                      [[IterativeSolver.Stepsize]] for the learning rate used.
  *                      [[IterativeSolver.ConvergenceThreshold]] when provided the algorithm will
  *                      stop the iterations if the relative change in the value of the objective
  *                      function between successive iterations is is smaller than this value.
  */
class GradientDescent() extends IterativeSolver {

  /** Performs one iteration of Stochastic Gradient Descent using mini batches
    *
    * @param data A Dataset of LabeledVector (label, features) pairs
    * @param currentWeights A Dataset with the current weights to be optimized as its only element
    * @return A Dataset containing the weights after one stochastic gradient descent step
    */
  private def SGDStep(
      data: DataSet[(LabeledVector)],
      currentWeights: DataSet[WeightVector],
      lossFunction: LossFunction,
      stepsize: Double)
    : DataSet[WeightVector] = {

    data.mapWithBcVariable(currentWeights){
      (data, weightVector) => (lossFunction.gradient(data, weightVector), 1)
    }.reduce{
      (left, right) =>
        val (leftGradVector, leftCount) = left
        val (rightGradVector, rightCount) = right
        // Add the left gradient to the right one
        BLAS.axpy(1.0, leftGradVector.weights, rightGradVector.weights)
        val gradients = WeightVector(
          rightGradVector.weights, leftGradVector.intercept + rightGradVector.intercept)

        (gradients , leftCount + rightCount)
    }.mapWithBcVariableIteration(currentWeights){
      (gradientCount, weightVector, iteration) => {
        val (WeightVector(weights, intercept), count) = gradientCount

        BLAS.scal(1.0/count, weights)

        val gradient = WeightVector(weights, intercept/count)

        val learningRate = stepsize/Math.sqrt(iteration)

        lossFunction.updateWeightVector(weightVector, gradient, learningRate)
      }
    }
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
    val lossFunction = parameters(LossFunction)
    val stepsize = parameters(Stepsize)

    // Initialize weights
    val initialWeightsDS: DataSet[WeightVector] = createInitialWeightsDS(initialWeights, data)

    // Perform the iterations
    val optimizedWeights = convergenceThresholdOption match {
      // No convergence criterion
      case None =>
        initialWeightsDS.iterate(numberOfIterations) {
          weightVectorDS => {
            SGDStep(data, weightVectorDS, lossFunction, stepsize)
          }
        }
      case Some(convergence) =>
        // Calculates the regularized loss, from the data and given weights
        def calculateLoss(data: DataSet[LabeledVector], weightDS: DataSet[WeightVector])
        : DataSet[Double] = {
          data.mapWithBcVariable(weightDS){
            (data, weightVector) => (lossFunction.loss(data, weightVector), 1)
          }.reduce{
            (left, right) => (left._1 + right._1, left._2 + right._2)
          }.map {
            lossCount => lossCount._1 / lossCount._2
          }
        }
        // We have to calculate for each weight vector the sum of squared residuals,
        // and then sum them and apply regularization
        val initialLossSumDS = calculateLoss(data, initialWeightsDS)

        // Combine weight vector with the current loss
        val initialWeightsWithLossSum = initialWeightsDS.mapWithBcVariable(initialLossSumDS){
          (weights, loss) => (weights, loss)
        }

        val resultWithLoss = initialWeightsWithLossSum.iterateWithTermination(numberOfIterations) {
          weightsWithPreviousLossSum =>

            // Extract weight vector and loss
            val previousWeightsDS = weightsWithPreviousLossSum.map{_._1}
            val previousLossSumDS = weightsWithPreviousLossSum.map{_._2}

            val currentWeightsDS = SGDStep(data, previousWeightsDS, lossFunction, stepsize)

            val currentLossSumDS = calculateLoss(data, currentWeightsDS)

            // Check if the relative change in the loss is smaller than the
            // convergence threshold. If yes, then terminate i.e. return empty termination data set
            val termination = previousLossSumDS.filterWithBcVariable(currentLossSumDS){
              (previousLoss, currentLoss) => {
                if (previousLoss <= 0) {
                  false
                } else {
                  Math.abs((previousLoss - currentLoss)/previousLoss) >= convergence
                }
              }
            }

            // Result for new iteration
            (currentWeightsDS.mapWithBcVariable(currentLossSumDS)((w, l) => (w, l)), termination)
        }
        // Return just the weights
        resultWithLoss.map{_._1}
    }
    optimizedWeights
  }
}

object GradientDescent {
  def apply(): GradientDescent = {
    new GradientDescent()
  }
}



