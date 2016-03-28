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

/**
 * Docs
 */



import breeze.linalg.{DenseVector => BreezeDenseVector}
import breeze.numerics.{sigmoid, tanh}

abstract class ActivationFunction extends Serializable {
  def func(x: BreezeDenseVector[Double]): BreezeDenseVector[Double]

  def derivative(x: BreezeDenseVector[Double]): BreezeDenseVector[Double]
}


object sigmoidActivationFn extends ActivationFunction {
  import breeze.numerics.sigmoid
  override def func(x: BreezeDenseVector[Double]): BreezeDenseVector[Double] ={
    sigmoid(x)
  }

  override def derivative(x: BreezeDenseVector[Double]): BreezeDenseVector[Double] = {
    sigmoid(x) :* (1.0 - sigmoid(x))
  }
}

object tanhActivationFn extends ActivationFunction {
  import breeze.numerics.tanh
  override def func(x: BreezeDenseVector[Double]): BreezeDenseVector[Double] = {
    tanh(x)
  }

  override def derivative(x: BreezeDenseVector[Double]): BreezeDenseVector[Double] ={
    1.0 - tanh(x) :* tanh(x)
  }
}

/**
 * Elliots Fast Squash Function
 *  by David Elliot
 * http://ufnalski.edu.pl/zne/ci_2014/papers/Elliott_TR_93-8.pdf
 *
 */

object elliotsSquashActivationFn extends ActivationFunction {
  import breeze.numerics.abs
  override def func(x: BreezeDenseVector[Double]): BreezeDenseVector[Double] ={
    x / ( abs(x) + 1.0 )
  }

  override def derivative(x: BreezeDenseVector[Double]): BreezeDenseVector[Double] = {
    1.0 / (( abs(x) + 1.0 ) :* ( abs(x) + 1.0))
  }
}

