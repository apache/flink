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

import breeze.linalg.{DenseVector => BreezeDenseVector}
import breeze.numerics.{sigmoid, tanh}

/**
 * Represents Activation Functions which can be used with [[MultiLayerPerceptron]]
 */
abstract class ActivationFunction extends Serializable {
  /**
   * Applies a function to a [[BreezeDenseVector]]
   * @param x
   * @return
   */
  def func(x: BreezeDenseVector[Double]): BreezeDenseVector[Double]

  /**
   *  Applied the derivative of the function to a [[BreezeDenseVector]]
   * @param x
   * @return
   */
  def derivative(x: BreezeDenseVector[Double]): BreezeDenseVector[Double]
}

/**
 * A sigmoid function
 * https://en.wikipedia.org/wiki/Sigmoid_function
 */
object sigmoidActivationFn extends ActivationFunction {
  import breeze.numerics.sigmoid

  /**
   * Applies sigmoid function to a [[BreezeDenseVector]]
   * @param x input [[BreezeDenseVector]]
   * @return function applied to input [[BreezeDenseVector]]
   */
  override def func(x: BreezeDenseVector[Double]): BreezeDenseVector[Double] ={
    sigmoid(x)
  }

  /**
   * Applies a derivative of sigmoid function to a [[BreezeDenseVector]]
   * @param x input [[BreezeDenseVector]]
   * @return derivative of function applied to input [[BreezeDenseVector]]
   */
  override def derivative(x: BreezeDenseVector[Double]): BreezeDenseVector[Double] = {
    sigmoid(x) :* (1.0 - sigmoid(x))
  }
}

/**
 * A hyperbolic tangent function
 * http://mathworld.wolfram.com/HyperbolicTangent.html
 */
object tanhActivationFn extends ActivationFunction {
  import breeze.numerics.tanh
  /**
   * Applies a hyperbolic tangent function to a [[BreezeDenseVector]]
   * @param x input [[BreezeDenseVector]]
   * @return function applied to input [[BreezeDenseVector]]
   */
  override def func(x: BreezeDenseVector[Double]): BreezeDenseVector[Double] = {
    tanh(x)
  }

  /**
   * Applies a derivative of hyperbolic tangent function to a [[BreezeDenseVector]]
   * @param x input [[BreezeDenseVector]]
   * @return derivative of function applied to input [[BreezeDenseVector]]
   */
  override def derivative(x: BreezeDenseVector[Double]): BreezeDenseVector[Double] ={
    1.0 - tanh(x) :* tanh(x)
  }
}

/**
 * fast computing activation function by David Elliot
 * http://ufnalski.edu.pl/zne/ci_2014/papers/Elliott_TR_93-8.pdf
 */
object elliotsSquashActivationFn extends ActivationFunction {
  import breeze.numerics.abs
  /**
   * Applies a Elliot's function to a [[BreezeDenseVector]]
   * @param x input [[BreezeDenseVector]]
   * @return function applied to input [[BreezeDenseVector]]
   */
  override def func(x: BreezeDenseVector[Double]): BreezeDenseVector[Double] ={
    x / ( abs(x) + 1.0 )
  }

  /**
   * Applies derivative of Elliot's function to a [[BreezeDenseVector]]
   * @param x input [[BreezeDenseVector]]
   * @return derivative of function applied to input [[BreezeDenseVector]]
   */
  override def derivative(x: BreezeDenseVector[Double]): BreezeDenseVector[Double] = {
    1.0 / (( abs(x) + 1.0 ) :* ( abs(x) + 1.0))
  }
}

