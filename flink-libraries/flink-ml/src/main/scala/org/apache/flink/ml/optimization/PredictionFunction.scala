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

import org.apache.flink.ml.common.WeightVector
import org.apache.flink.ml.math.{Vector => FlinkVector, Breeze, DenseVector, BLAS}
import org.apache.flink.ml.neuralnetwork.ActivationFunction

import breeze.linalg.{  Vector => BreezeVector,
SparseVector => BreezeSparseVector,
DenseVector => BreezeDenseVector,
DenseMatrix => BreezeDenseMatrix}

import org.apache.flink.ml.math.Breeze.{Vector2BreezeConverter, Breeze2VectorConverter}


/** An abstract class for prediction functions to be used in optimization **/
abstract class PredictionFunction extends Serializable {
  def predict(features: FlinkVector, weights: WeightVector): Double

  def gradient(features: FlinkVector, weights: WeightVector): WeightVector
}

/** A linear prediction function **/
object LinearPrediction extends PredictionFunction {
  override def predict(features: FlinkVector, weightVector: WeightVector): Double = {
    BLAS.dot(features, weightVector.weights) + weightVector.intercept
  }

  override def gradient(features: FlinkVector, weights: WeightVector): WeightVector = {
    WeightVector(features.copy, 1)
  }

}

case class MultiLayerPerceptronPrediction(
                                         arch: List[Int],
                                         f: ActivationFunction
                                           ) extends Serializable {
  // Todo: this is used multiple times- move to one spot
  def makeWeightArray(v: WeightVector): Array[BreezeDenseMatrix[Double]] = {
    val weightVector = Vector2BreezeConverter(v.weights).asBreeze.toDenseVector
    val breakPoints = arch.iterator.sliding(2).toList.map(o => o(0) * o(1)).scanLeft(0)(_ + _)
    var U = new Array[BreezeDenseMatrix[Double]](arch.length-1)
    // takes weight vector and gives back weight array
    for (l <- (0 to arch.length - 2)){
      U(l) = new BreezeDenseMatrix(arch(l + 1), arch(l),
                                    weightVector.data.slice(breakPoints(l), breakPoints(l + 1)))
    }
    U
  }

  def makeWeightVector(U: Array[BreezeDenseMatrix[Double]]): WeightVector = {
    val fVector = DenseVector( U.map(_.toDenseVector)
                                .reduceLeft(BreezeDenseVector.vertcat(_,_))
                                .data )
    WeightVector( fVector, 0)
  }

  case class FeedForwardResult(
                                U: Array[BreezeDenseMatrix[Double]],
                                A: Array[BreezeDenseVector[Double]],
                                Z: Array[BreezeDenseVector[Double]]
                                ) extends Serializable

  def feedForward(weightVector: WeightVector, features: FlinkVector): FeedForwardResult  = {
    val L = arch.length - 1
    val U = makeWeightArray(weightVector)
    var A = new Array[BreezeDenseVector[Double]](L + 1);
    A(0) = Vector2BreezeConverter(features).asBreeze.toDenseVector
    var Z = new Array[BreezeDenseVector[Double]](L + 1);
    // Feed Forward
    for (l <- (0 until L)){
      Z(l + 1) = U(l) * A(l)
      A(l + 1) = f.func( Z(l + 1) )
    }
    FeedForwardResult(U, A, Z)
  }

  def predict(features: FlinkVector, weights: WeightVector): Double = {
    feedForward(weights, features).A.last(0)
  }

  def gradient(ffr: FeedForwardResult,
               delta: Array[BreezeDenseVector[Double]]): WeightVector = {

    val L = arch.length - 1
    // BP1

    // BP2
    for (l <- (L -1 until 0 by -1)){
      delta(l) = (ffr.U(l).t * delta(l + 1)) :* f.derivative(ffr.Z(l))
    }

    // BP4
    var grads = new Array[BreezeDenseMatrix[Double]](L)
    for (l <- (0 to L-1)){
      grads(l) = BreezeDenseMatrix.tabulate(ffr.U(l).rows, ffr.U(l).cols){
        case (i, j) => ffr.A(l)(j)*delta(l + 1)(i)
      }
    }

    makeWeightVector(grads)
  }
}
