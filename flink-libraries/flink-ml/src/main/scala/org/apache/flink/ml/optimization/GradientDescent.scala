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
import org.apache.flink.ml.optimization.IterativeSolver._
import org.apache.flink.ml.optimization.LearningRateMethod.LearningRateMethodTrait
import org.apache.flink.ml.optimization.Solver._
import org.apache.flink.ml._

/** Base class which performs Stochastic Gradient Descent optimization using mini batches.
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
  *                      [[Solver.RegularizationPenaltyValue]] for the regularization penalty.
  *                      [[Solver.RegularizationConstant]] for the regularization parameter,
  *                      [[IterativeSolver.Iterations]] for the maximum number of iteration,
  *                      [[IterativeSolver.LearningRate]] for the learning rate used.
  *                      [[IterativeSolver.ConvergenceThreshold]] when provided the algorithm will
  *                      stop the iterations if the relative change in the value of the objective
  *                      function between successive iterations is is smaller than this value.
  *                      [[IterativeSolver.LearningRateMethodValue]] determines functional form of
  *                      effective learning rate.
  */
class GradientDescent extends IterativeSolver {

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
    val learningRate = parameters(LearningRate)
    val regularizationPenalty = parameters(RegularizationPenaltyValue)
    val regularizationConstant = parameters(RegularizationConstant)
    val learningRateMethod = parameters(LearningRateMethodValue)

    // Initialize weights
    val initialWeightsDS: DataSet[WeightVector] = createInitialWeightsDS(initialWeights, data)

    // Perform the iterations
    convergenceThresholdOption match {
      // No convergence criterion
      case None =>
        optimizeWithoutConvergenceCriterion(
          data,
          initialWeightsDS,
          numberOfIterations,
          regularizationPenalty,
          regularizationConstant,
          learningRate,
          lossFunction,
          learningRateMethod)
      case Some(convergence) =>
        optimizeWithConvergenceCriterion(
          data,
          initialWeightsDS,
          numberOfIterations,
          regularizationPenalty,
          regularizationConstant,
          learningRate,
          convergence,
          lossFunction,
          learningRateMethod)
    }
  }

  def optimizeWithConvergenceCriterion(
      dataPoints: DataSet[LabeledVector],
      initialWeightsDS: DataSet[WeightVector],
      numberOfIterations: Int,
      regularizationPenalty: RegularizationPenalty,
      regularizationConstant: Double,
      learningRate: Double,
      convergenceThreshold: Double,
      lossFunction: LossFunction,
      learningRateMethod: LearningRateMethodTrait)
    : DataSet[WeightVector] = {
    // We have to calculate for each weight vector the sum of squared residuals,
    // and then sum them and apply regularization
    val initialLossSumDS = calculateLoss(dataPoints, initialWeightsDS, lossFunction)

    // Combine weight vector with the current loss
    val initialWeightsWithLossSum = initialWeightsDS.mapWithBcVariable(initialLossSumDS){
      (weights, loss) => (weights, loss)
    }

    val resultWithLoss = initialWeightsWithLossSum.iterateWithTermination(numberOfIterations) {
      weightsWithPreviousLossSum =>

        // Extract weight vector and loss
        val previousWeightsDS = weightsWithPreviousLossSum.map{_._1}
        val previousLossSumDS = weightsWithPreviousLossSum.map{_._2}

        val currentWeightsDS = SGDStep(
          dataPoints,
          previousWeightsDS,
          lossFunction,
          regularizationPenalty,
          regularizationConstant,
          learningRate,
          learningRateMethod)

        val currentLossSumDS = calculateLoss(dataPoints, currentWeightsDS, lossFunction)

        // Check if the relative change in the loss is smaller than the
        // convergence threshold. If yes, then terminate i.e. return empty termination data set
        val termination = previousLossSumDS.filterWithBcVariable(currentLossSumDS){
          (previousLoss, currentLoss) => {
            if (previousLoss <= 0) {
              false
            } else {
              scala.math.abs((previousLoss - currentLoss)/previousLoss) >= convergenceThreshold
            }
          }
        }

        // Result for new iteration
        (currentWeightsDS.mapWithBcVariable(currentLossSumDS)((w, l) => (w, l)), termination)
    }
    // Return just the weights
    resultWithLoss.map{_._1}
  }

  def optimizeWithoutConvergenceCriterion(
      data: DataSet[LabeledVector],
      initialWeightsDS: DataSet[WeightVector],
      numberOfIterations: Int,
      regularizationPenalty: RegularizationPenalty,
      regularizationConstant: Double,
      learningRate: Double,
      lossFunction: LossFunction,
      optimizationMethod: LearningRateMethodTrait)
    : DataSet[WeightVector] = {
    initialWeightsDS.iterate(numberOfIterations) {
      weightVectorDS => {
        SGDStep(data,
                weightVectorDS,
                lossFunction,
                regularizationPenalty,
                regularizationConstant,
                learningRate,
                optimizationMethod)
      }
    }
  }

  /** Performs one iteration of Stochastic Gradient Descent using mini batches
    *
    * @param data A Dataset of LabeledVector (label, features) pairs
    * @param currentWeights A Dataset with the current weights to be optimized as its only element
    * @param lossFunction The loss function to be used
    * @param regularizationPenalty The regularization penalty to be used
    * @param regularizationConstant The regularization parameter
    * @param learningRate The effective step size for this iteration
    * @param learningRateMethod The learning rate used
    *
    * @return A Dataset containing the weights after one stochastic gradient descent step
    */
  private def SGDStep(
    data: DataSet[(LabeledVector)],
    currentWeights: DataSet[WeightVector],
    lossFunction: LossFunction,
    regularizationPenalty: RegularizationPenalty,
    regularizationConstant: Double,
    learningRate: Double,
    learningRateMethod: LearningRateMethodTrait)
  : DataSet[WeightVector] = {

    data.mapWithBcVariable(currentWeights){
      (data, weightVector) => (lossFunction.gradient(data, weightVector), 1)
    }.reduce{
      (left, right) =>
        val (leftGradVector, leftCount) = left
        val (rightGradVector, rightCount) = right

        // make the left gradient dense so that the following reduce operations (left fold) reuse
        // it. This strongly depends on the underlying implementation of the ReduceDriver which
        // always passes the new input element as the second parameter
        val result = leftGradVector.weights match {
          case d: DenseVector => d
          case s: SparseVector => s.toDenseVector
        }

        // Add the right gradient to the result
        BLAS.axpy(1.0, rightGradVector.weights, result)
        val gradients = WeightVector(
          result, leftGradVector.intercept + rightGradVector.intercept)

        (gradients , leftCount + rightCount)
    }.mapWithBcVariableIteration(currentWeights){
      (gradientCount, weightVector, iteration) => {
        val (WeightVector(weights, intercept), count) = gradientCount

        BLAS.scal(1.0/count, weights)

        val gradient = WeightVector(weights, intercept/count)
        val effectiveLearningRate = learningRateMethod.calculateLearningRate(
          learningRate,
          iteration,
          regularizationConstant)

        val newWeights = takeStep(
          weightVector.weights,
          gradient.weights,
          regularizationPenalty,
          regularizationConstant,
          effectiveLearningRate)

        WeightVector(
          newWeights,
          weightVector.intercept - effectiveLearningRate * gradient.intercept)
      }
    }
  }

  /** Calculates the new weights based on the gradient
    *
    * @param weightVector The weights to be updated
    * @param gradient The gradient according to which we will update the weights
    * @param regularizationPenalty The regularization penalty to apply
    * @param regularizationConstant The regularization parameter
    * @param learningRate The effective step size for this iteration
    * @return Updated weights
    */
  def takeStep(
    weightVector: Vector,
    gradient: Vector,
    regularizationPenalty: RegularizationPenalty,
    regularizationConstant: Double,
    learningRate: Double
    ): Vector = {
    regularizationPenalty.takeStep(weightVector, gradient, regularizationConstant, learningRate)
  }

  /** Calculates the regularized loss, from the data and given weights.
    *
    * @param data A Dataset of LabeledVector (label, features) pairs
    * @param weightDS A Dataset with the current weights to be optimized as its only element
    * @param lossFunction The loss function to be used
    * @return A Dataset with the regularized loss as its only element
    */
  private def calculateLoss(
      data: DataSet[LabeledVector],
      weightDS: DataSet[WeightVector],
      lossFunction: LossFunction)
    : DataSet[Double] = {
    data.mapWithBcVariable(weightDS){
      (data, weightVector) => (lossFunction.loss(data, weightVector), 1)
    }.reduce{
      (left, right) => (left._1 + right._1, left._2 + right._2)
    }.map {
      lossCount => lossCount._1 / lossCount._2
    }
  }
}


/** Implementation of a Gradient Descent solver.
  *
  */
object GradientDescent {
  def apply() = new GradientDescent
}
