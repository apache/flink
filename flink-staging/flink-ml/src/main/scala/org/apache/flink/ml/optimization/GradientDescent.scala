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

class GradientDescent(runParameters: ParameterMap) extends IterativeSolver {

  import Solver.WEIGHTVECTOR_BROADCAST

  var parameterMap: ParameterMap = parameters ++ runParameters

  // TODO(tvas): Use once we have proper sampling in place
//  case object MiniBatchFraction extends Parameter[Double] {
//    val defaultValue = Some(1.0)
//  }
//
//  def setMiniBatchFraction(fraction: Double): GradientDescent = {
//    parameterMap.add(MiniBatchFraction, fraction)
//    this
//  }

  /** Performs one iteration of Stochastic Gradient Descent
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
        val (leftGradientVector, leftCount) = left
        val (rightGradientVector, rightCount) = right

        BLAS.axpy(1.0, leftGradientVector.weights, rightGradientVector.weights)
        (new WeightVector(
          rightGradientVector.weights,
          leftGradientVector.intercept + rightGradientVector.intercept),
          leftCount + rightCount)
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
  override def optimize(data: DataSet[LabeledVector], initWeights: Option[DataSet[WeightVector]]):
  DataSet[WeightVector] = {
    // TODO: Faster way to do this?
    val dimensionsDS = data.map(_.vector.size).reduce((a, b) => b)

    val numberOfIterations: Int = parameterMap(Iterations)


    val initialWeightsDS: DataSet[WeightVector] = initWeights match {
      //TODO(tvas): Need to figure out if and where we want to pass initial weights
      case Some(x) => x
      case None => createInitialWeightVector(dimensionsDS)
    }

    // We need the weights vector to initialize the regularization value.
    val initWeightsVector = initialWeightsDS.collect()(0).weights

    val initRegVal = parameterMap(RegularizationType)
      .applyRegularization(initWeightsVector, 0.0, parameterMap(RegularizationParameter))._2

    // TODO: Is there a way to initialize the regularization parameter without collect()?
    // The following code should give us a dataset with the initial reg. parameter, but we still
    // need to call collect to retrieve it. Question is: call collect here, or on the weights as we
    // currently do?
    //val initRegValue: DataSet[Double] = initialWeights.map {x => parameterMap(RegularizationType)
    //  .applyRegularization(x.weights, 0.0, parameterMap(RegularizationParameter))._2}
    //  .reduce(_ + _)

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
  RichMapFunction[LabeledVector, (WeightVector, Int)] {

    var weightVector: WeightVector = null

    @throws(classOf[Exception])
    override def open(configuration: Configuration): Unit = {
      val list = this.getRuntimeContext.
        getBroadcastVariable[WeightVector](WEIGHTVECTOR_BROADCAST)

      weightVector = list.get(0)
    }

    override def map(example: LabeledVector): (WeightVector, Int) = {

      val lossFunction = parameterMap(LossFunction)
      //TODO(tvas): Should throw an error if Dimensions has not been defined
      val dimensions = example.vector.size
      // TODO(tvas): Any point in carrying the weightGradient vector for in-place replacement?
      // The idea in spark is to avoid object creation, but here we have to do it anyway
      val weightGradient = new DenseVector(new Array[Double](dimensions))

      val (loss, lossDeriv) = lossFunction.lossAndGradient(example, weightVector, weightGradient)

      // Restrict the value of the loss derivative to avoid numerical instabilities
      val restrictedLossDeriv: Double = {
        if (lossDeriv < -IterativeSolver.MAX_DLOSS) {
          -IterativeSolver.MAX_DLOSS
        }
        else if (lossDeriv > IterativeSolver.MAX_DLOSS) {
          IterativeSolver.MAX_DLOSS
        }
        else {
          lossDeriv
        }
      }

      (new WeightVector(weightGradient, restrictedLossDeriv), 1)
    }
  }

  /** Performs the update of the weights, according to the given gradients and regularization type.
    *
    */
  private class WeightsUpdate() extends
  RichMapFunction[(WeightVector, Int), WeightVector] {


    var weightVector: WeightVector = null

    @throws(classOf[Exception])
    override def open(configuration: Configuration): Unit = {
      val list = this.getRuntimeContext.
        getBroadcastVariable[WeightVector](WEIGHTVECTOR_BROADCAST)

      weightVector = list.get(0)
    }

    override def map(gradientsAndCount: (WeightVector, Int)): WeightVector = {
      val regularizationType = parameterMap(RegularizationType)
      val regularizationParameter = parameterMap(RegularizationParameter)
      val stepsize = parameterMap(Stepsize)
      val weightGradients = gradientsAndCount._1
      val count = gradientsAndCount._2

      // Scale the gradients according to batch size
      BLAS.scal(1.0/count, weightGradients.weights)

      val weight0Gradient = weightGradients.intercept / count

      val iteration = getIterationRuntimeContext.getSuperstepNumber

      // scale initial stepsize by the inverse square root of the iteration number to make it
      // decreasing
      // TODO(tvas): There are more ways to determine the stepsize, possible low-effort extensions
      // here
      val effectiveStepsize = stepsize/math.sqrt(iteration)

      val newWeights = weightVector.weights.copy
      // Take the gradient step
      BLAS.axpy(-effectiveStepsize, weightGradients.weights, newWeights)
      val newWeight0 = weightVector.intercept - effectiveStepsize * weight0Gradient

      // Apply the regularization
      val (updatedWeights, regVal) = regularizationType.applyRegularization(
        newWeights, effectiveStepsize, regularizationParameter)

      new WeightVector(updatedWeights, newWeight0)
    }
  }
}

object GradientDescent {
  def apply(parameterMap: ParameterMap): GradientDescent = {
    new GradientDescent(parameterMap)
  }
}



