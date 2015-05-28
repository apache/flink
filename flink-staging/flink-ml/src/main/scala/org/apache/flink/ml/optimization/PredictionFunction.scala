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
import org.apache.flink.ml.math.{Vector => FlinkVector, BLAS}

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
