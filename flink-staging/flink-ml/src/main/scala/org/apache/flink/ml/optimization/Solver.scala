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

import org.apache.flink.api.scala.DataSet
import org.apache.flink.ml.common._
import org.apache.flink.ml.math.{Vector => FlinkVector, BLAS, DenseVector}
import org.apache.flink.api.scala._
import org.apache.flink.ml.optimization.IterativeSolver._
import org.apache.flink.ml.optimization.Solver._

/** Base class for optimization algorithms
 *
 */
abstract class Solver extends Serializable with WithParameters {

  /** Provides a solution for the given optimization problem
    *
    * @param data A Dataset of LabeledVector (input, output) pairs
    * @param initialWeights The initial weight that will be optimized
    * @return A Vector of weights optimized to the given problem
    */
  def optimize(
    data: DataSet[LabeledVector],
    initialWeights: Option[DataSet[WeightVector]]): DataSet[WeightVector]

  /** Creates a DataSet with one zero vector. The zero vector has dimension d, which is given
    * by the dimensionDS.
    *
    * @param dimensionDS DataSet with one element d, denoting the dimension of the returned zero
    *                    vector
    * @return DataSet of a zero vector of dimension d
    */
  def createInitialWeightVector(dimensionDS: DataSet[Int]):  DataSet[WeightVector] = {
    dimensionDS.map {
      dimension =>
        val values = Array.fill(dimension)(0.0)
        new WeightVector(DenseVector(values), 0.0)
    }
  }

  //Setters for parameters
  def setLossFunction(lossFunction: LossFunction): Solver = {
    parameters.add(LossFunction, lossFunction)
    this
  }

  def setRegularizationType(regularization: Regularization): Solver = {
    parameters.add(RegularizationType, regularization)
    this
  }

  def setRegularizationParameter(regularizationParameter: Double): Solver = {
    parameters.add(RegularizationParameter, regularizationParameter)
    this
  }

  def setPredictionFunction(predictionFunction: PredictionFunction): Solver = {
    parameters.add(PredictionFunctionParameter, predictionFunction)
    this
  }
}

object Solver {
  // TODO(tvas): Does this belong in IterativeSolver instead?
  val WEIGHTVECTOR_BROADCAST = "weights_broadcast"

  // Define parameters for Solver
  case object LossFunction extends Parameter[LossFunction] {
    // TODO(tvas): Should depend on problem, here is where differentiating between classification
    // and regression could become useful
    val defaultValue = Some(new SquaredLoss)
  }

  case object RegularizationType extends Parameter[Regularization] {
    val defaultValue = Some(new NoRegularization)
  }

  case object RegularizationParameter extends Parameter[Double] {
    val defaultValue = Some(0.0) // TODO(tvas): Properly initialize this, ensure Parameter > 0!
  }

  case object PredictionFunctionParameter extends Parameter[PredictionFunction] {
    val defaultValue = Some(new LinearPrediction)
  }
}

/** An abstract class for iterative optimization algorithms
  *
  * See [[https://en.wikipedia.org/wiki/Iterative_method Iterative Methods on Wikipedia]] for more
  * info
  */
abstract class IterativeSolver extends Solver {

  //Setters for parameters
  def setIterations(iterations: Int): IterativeSolver = {
    parameters.add(Iterations, iterations)
    this
  }

  def setStepsize(stepsize: Double): IterativeSolver = {
    parameters.add(Stepsize, stepsize)
    this
  }
}

object IterativeSolver {

  val MAX_DLOSS: Double = 1e12

  // Define parameters for IterativeSolver
  case object Stepsize extends Parameter[Double] {
    val defaultValue = Some(0.1)
  }

  case object Iterations extends Parameter[Int] {
    val defaultValue = Some(10)
  }
}
