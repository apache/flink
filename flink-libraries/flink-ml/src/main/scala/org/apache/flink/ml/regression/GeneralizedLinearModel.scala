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

import org.apache.flink.api.scala._
import org.apache.flink.ml.common._
import org.apache.flink.ml.math._
import org.apache.flink.ml.optimization.LearningRateMethod.LearningRateMethodTrait
import org.apache.flink.ml.optimization._
import org.apache.flink.ml.pipeline.{FitOperation, PredictOperation, Predictor}


trait GeneralizedLinearModel[Self] extends WithParameters {
  import GeneralizedLinearModel._

  protected def solver: Solver

  var weightsOption: Option[DataSet[WeightVector]] = None

  def setInitialWeightDS(initialWeights: DataSet[WeightVector]): this.type = {
    parameters.add(InitWeightsDS, initialWeights)
    this
  }

  def setRegularizationConstant(regularizationConstant: Double): this.type = {
    parameters.add(RegularizationConstant, regularizationConstant)
    this
  }

}

object GeneralizedLinearModel {
  import WithIterativeSolver._
  import WithRegularizationOption._

  case object InitWeightsDS extends Parameter[DataSet[WeightVector]] {
    val defaultValue = None
  }

  case object RegularizationConstant extends Parameter[Double] {
    val defaultValue = Some(0.0001)
  }

  private def setIterativeSolverParam(solver: IterativeSolver, parameters: ParameterMap) = {
    val numberOfIterations = parameters(Iterations)
    val stepSize = parameters(Stepsize)
    val convergenceThreshold = parameters.get(ConvergenceThreshold)
    val learningRateMethod = parameters.get(LearningRateMethodValue)

    solver.setIterations(numberOfIterations)
      .setStepsize(stepSize)

    convergenceThreshold match {
      case Some(threshold) => solver.setConvergenceThreshold(threshold)
      case None =>
    }

    learningRateMethod match {
      case Some(method) => solver.setLearningRateMethod(method)
      case None =>
    }
  }

  implicit def fitMLR[Self <: GeneralizedLinearModel[Self]] =
    new FitOperation[Self, LabeledVector] {
      override def fit(
                        instance: Self,
                        fitParameters: ParameterMap,
                        input: DataSet[LabeledVector])
      : Unit = {
        val parameters = instance.parameters ++ fitParameters

        instance.solver match {
          case s: IterativeSolver => setIterativeSolverParam(s, parameters)
        }

        if (instance.isInstanceOf[WithRegularizationOption[Self]]) {
          instance.solver.setRegularizationPenalty(parameters(RegularizationPenaltyValue))
        }
        instance.solver.setRegularizationConstant(parameters(RegularizationConstant))

        instance.weightsOption = Some(instance.solver.createInitialWeightsDS(
          parameters.get(InitWeightsDS),
          input))
        instance.weightsOption = Some(instance.solver.optimize(input, instance.weightsOption))
      }
    }
}

trait WithRegularizationOption[Self] extends GeneralizedLinearModel[Self] {
  import WithRegularizationOption._

  def setRegularizationType(regularizationType: RegularizationPenalty): this.type = {
    parameters.add(RegularizationPenaltyValue, regularizationType)
    this
  }
}

object WithRegularizationOption {

  case object RegularizationPenaltyValue extends Parameter[RegularizationPenalty] {
    val defaultValue = Some(NoRegularization)
  }
}

trait WithIterativeSolver[Self] extends GeneralizedLinearModel[Self] {
  import WithIterativeSolver._

  protected def solver: IterativeSolver

  def setIterations(iterations: Int): this.type = {
    parameters.add(Iterations, iterations)
    this
  }

  def setStepsize(stepsize: Double): this.type = {
    parameters.add(Stepsize, stepsize)
    this
  }

  def setConvergenceThreshold(convergenceThreshold: Double): this.type = {
    parameters.add(ConvergenceThreshold, convergenceThreshold)
    this
  }

  def setLearningRateMethod(learningRateMethod: LearningRateMethodTrait): this.type = {
    parameters.add(LearningRateMethodValue, learningRateMethod)
    this
  }
}

object WithIterativeSolver {
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
}
