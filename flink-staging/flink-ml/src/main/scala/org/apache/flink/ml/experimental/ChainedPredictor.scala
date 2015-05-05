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

package org.apache.flink.ml.experimental

import org.apache.flink.api.scala.DataSet
import org.apache.flink.ml.common.ParameterMap

case class ChainedPredictor[T <: Transformer[T], P <: Predictor[P]](transformer: T, predictor: P)
  extends Predictor[ChainedPredictor[T, P]]{}

object ChainedPredictor{
  implicit def chainedPredictOperation[
      T <: Transformer[T],
      P <: Predictor[P],
      Input,
      Testing,
      Prediction](
      implicit transform: TransformOperation[T, Input, Testing],
      predictor: PredictOperation[P, Testing, Prediction])
    : PredictOperation[ChainedPredictor[T, P], Input, Prediction] = {

    new PredictOperation[ChainedPredictor[T, P], Input, Prediction] {
      override def predict(
          instance: ChainedPredictor[T, P],
          predictParameters: ParameterMap,
          input: DataSet[Input])
        : DataSet[Prediction] = {

        val testing = instance.transformer.transform(input, predictParameters)
        instance.predictor.predict(testing, predictParameters)
      }
    }
  }

  implicit def chainedFitOperation[L <: Transformer[L], R <: Transformer[R], I, T](implicit
    leftFitOperation: FitOperation[L, I],
    leftTransformOperation: TransformOperation[L, I, T],
    rightFitOperation: FitOperation[R, T]): FitOperation[ChainedTransformer[L, R], I] = {
    new FitOperation[ChainedTransformer[L, R], I] {
      override def fit(
          instance: ChainedTransformer[L, R],
          fitParameters: ParameterMap,
          input: DataSet[I])
        : Unit = {
        instance.left.fit(input, fitParameters)
        val intermediateResult = instance.left.transform(input, fitParameters)
        instance.right.fit(intermediateResult, fitParameters)
      }
    }
  }
}
