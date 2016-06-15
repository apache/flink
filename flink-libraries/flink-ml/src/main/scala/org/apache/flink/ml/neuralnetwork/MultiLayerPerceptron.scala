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

package org.apache.flink.ml.neuralnetwork

import breeze.linalg.{DenseMatrix => BreezeDenseMatrix}

import org.apache.flink.api.scala._

import org.apache.flink.ml.common._
import org.apache.flink.ml.math.Vector
import org.apache.flink.ml.optimization.{MultiLayerPerceptronPrediction,
                                        IterativeSolver,
                                        SquaredLoss}

import org.apache.flink.api.scala.DataSet

import org.apache.flink.ml.pipeline.{PredictOperation, FitOperation, Predictor}


/**
 * Multi-Layer Perceptron to estimate data point
 *
 * Neural networks use a directed graph consisting of activation functions (nodes) and parameter
 * weights (edges) to solve complex problems with interdependent variables.
 * One of the simplest types of neural networks (sometimes referred to as Artificial Neural
 * Networks or *ANN*s) is the multiple-layer perceptron (or *MLP*).  The Multi-Layer Perceptron is
 * a feed forward neural network that has multiple layers which are fully connected with
 * *non-linear* activation function at each node and weights at each layer.
 *
 *
 *
 * =Parameters=
 *
 * - [[org.apache.flink.ml.neuralnetwork.MultiLayerPerceptron.HiddenLayerNetworkArchitecture]]
 *  A list of integer specifying the number of nodes to have in each hidden layer of the network
 *
 *  - [[org.apache.flink.ml.neuralnetwork.MultiLayerPerceptron.Optimizer]]
 *  An [[IterativeSolver]] to be used in optimizing the multi-layer perceptron
 *
 *  - [[org.apache.flink.ml.neuralnetwork.MultiLayerPerceptron.ActivationFunc]]
 *  An [[ActivationFunction]] that is continuous and differentiable to be used at each node in the
 *  network
 *
 */
class MultiLayerPerceptron extends Predictor[MultiLayerPerceptron] {
  import MultiLayerPerceptron._

  var weightsOption: Option[DataSet[WeightVector]] = None

  var predictionFunctionOption: Option[MultiLayerPerceptronPrediction] = None

  var lossFunction: Option[GenericMLPLossFunction] = None

  var networkArchitecture = List(0)

  def setHiddenLayerArchitecture(arch: List[Int]): MultiLayerPerceptron = {
    parameters.add(HiddenLayerNetworkArchitecture, arch)
    this
  }

  def setOptimizer[A <: IterativeSolver](optimizer: IterativeSolver): MultiLayerPerceptron = {
    parameters.add(Optimizer, optimizer)
    this
  }

  def setActivatoinFunction(f: ActivationFunction): MultiLayerPerceptron = {
    parameters.add(ActivationFunc, f)
    this
  }

  def squaredResidualSum(input: DataSet[LabeledVector]): DataSet[Double] = {
    import scala.math.pow
    val pf = predictionFunctionOption.get
    weightsOption match {
      case Some(weights) => {
        input.mapWithBcVariable(weights){ // convenience function to be revoved soon.
          (dataPoint, weights) => pow(dataPoint.label - pf.predict(dataPoint.vector, weights), 2)
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

object MultiLayerPerceptron {

  val WEIGHTVECTOR_BROADCAST = "weights_broadcast"

  case object HiddenLayerNetworkArchitecture extends Parameter[List[Int]] {
    val defaultValue = Some(List(5,5))
  }

  case object Optimizer extends Parameter[IterativeSolver] {
    val defaultValue = None
  }

  case object ActivationFunc extends Parameter[ActivationFunction] {
    import org.apache.flink.ml.neuralnetwork.elliotsSquashActivationFn
    val defaultValue = Some(elliotsSquashActivationFn)
  }

  // ======================================== Factory methods ======================================

  def apply(): MultiLayerPerceptron = {
    new MultiLayerPerceptron()
  }

  // ====================================== Operations =============================================

  /** Trains the linear model to fit the training data. The resulting weight vector is stored in
    * the [[MultiLayerPerceptron]] instance.
    *
    */
  implicit val fitMLP =  {
    new FitOperation[MultiLayerPerceptron, LabeledVector] {
      override def fit(
                        instance: MultiLayerPerceptron,
                        fitParameters: ParameterMap,
                        input: DataSet[LabeledVector]): Unit = {

        val map = instance.parameters ++ fitParameters

        val hiddenLayers = map(HiddenLayerNetworkArchitecture)
        val inputVectorSample = input.first(1).collect()(0)
        val inputDim = inputVectorSample.vector.size
        val outputDim = 1 // change this when you introduce vector targets
        val actFunc = map(ActivationFunc)
        instance.networkArchitecture = List(inputDim) ::: hiddenLayers ::: List(outputDim)

        instance.predictionFunctionOption = instance.predictionFunctionOption match {
          case Some(pfo) => Some(pfo)
          case None => Some(new MultiLayerPerceptronPrediction(instance.networkArchitecture,
                                                                actFunc))
        }

        val predFunc = instance.predictionFunctionOption.get

        // retrieve parameters of the algorithm
        val initialWeightsDS: DataSet[WeightVector] = instance.weightsOption match {
          case Some(weights) => weights
          case None => {
            val initialWeights = predFunc.makeWeightVector(
                                                buildUWeightSet(instance.networkArchitecture ))
            input.map(o => initialWeights)
          }

        }

        instance.lossFunction = Some(GenericMLPLossFunction(SquaredLoss,
                                      predFunc,
                                      instance.networkArchitecture ))


        val optimizer = map(Optimizer)
          .setLossFunction(instance.lossFunction.get)


        instance.weightsOption = Some(optimizer.optimize(input, Some(initialWeightsDS)))
      }
    }
  }

  implicit def predictVectors[T <: Vector] = {
    new PredictOperation[MultiLayerPerceptron, WeightVector, T, Double]() {

      var predictionFunction: MultiLayerPerceptronPrediction = _

      override def getModel(self: MultiLayerPerceptron, predictParameters: ParameterMap)
      : DataSet[WeightVector] = {
        predictionFunction = self.predictionFunctionOption.get
        self.weightsOption match {
          case Some(weights) => weights
          case None => {
            throw new RuntimeException("The MultiLayerPerceptron has not been fitted to the " +
              "data. This is necessary to learn the weight vector of the neural net function.")
          }
        }
      }

      override def predict(value: T, model: WeightVector): Double = {
        predictionFunction.predict( value, model )
      }
    }
  }

  private def buildUWeightSet(arch: List[Int]): Array[BreezeDenseMatrix[Double]] = {
    // A 'U' Weight set is for parameters moving up e.g. Xs, lower layers
    var uTemplate = scala.collection.mutable.ArrayBuffer.empty[BreezeDenseMatrix[Double]]
    for (i <- (1 until arch.length)){
      uTemplate += BreezeDenseMatrix.rand[Double](arch(i), arch(i-1))
    }
    uTemplate.toArray
  }
}
