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

import breeze.linalg.{DenseVector => BreezeDenseVector, DenseMatrix => BreezeDenseMatrix}

import org.apache.flink.ml.common.{LabeledVector, WeightVector}
import org.apache.flink.ml.math._
import org.apache.flink.ml.math.Breeze.{Vector2BreezeConverter, Breeze2VectorConverter}
import org.apache.flink.ml.optimization.{ PartialLossFunction,
                                          MultiLayerPerceptronPrediction,
                                          LossFunction}

case class GenericMLPLossFunction(
                                   partialLossFunction: PartialLossFunction,
                                   predictionFunction: MultiLayerPerceptronPrediction,
                                   arch: List[Int]) extends LossFunction {

  /** Calculates the gradient as well as the loss given a data point and the weight vector
    *
    * @param dataPoint
    * @param weightVector
    * @return
    */

  // Make this spit out / take weight vectors, not reg vetors and you're gtg
  def makeWeightVector(U: Array[BreezeDenseMatrix[Double]]): WeightVector = {
    val fVector = DenseVector( U.map(_.toDenseVector)
                                .reduceLeft(BreezeDenseVector.vertcat(_,_)).data )
    WeightVector( fVector, 0)
  }

  def makeWeightArray(v: WeightVector, arch: List[Int]): Array[BreezeDenseMatrix[Double]] = {
    val weightVector = Vector2BreezeConverter(v.weights).asBreeze.toDenseVector
    val breakPoints = arch.iterator.sliding(2).toList.map(o => o(0) * o(1)).scanLeft(0)(_ + _)
    var U = new Array[BreezeDenseMatrix[Double]](arch.length-1)
    // takes weight vector and gives back weight array
    for (l <- (0 to arch.length - 2)){
      U(l) = new BreezeDenseMatrix( arch(l + 1),
                                    arch(l),
                                    weightVector.data.slice(breakPoints(l), breakPoints(l + 1)))
    }
    U
  }

  def lossGradient(dataPoint: LabeledVector, weightVector: WeightVector): (Double, WeightVector) = {
    val ffr = predictionFunction.feedForward(weightVector, dataPoint.vector)
    val L = arch.length - 1
    // BP1
    var delta = new Array[BreezeDenseVector[Double]](L + 1)
    val f = predictionFunction.f
    val loss = partialLossFunction.derivative(ffr.A(L).data(0), dataPoint.label)
    delta(L) = loss * f.derivative(ffr.Z( L ))

    val grad = predictionFunction.gradient(ffr, delta)

    (loss, grad)
  }
}

