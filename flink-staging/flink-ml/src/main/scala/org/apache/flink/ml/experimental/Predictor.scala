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

import scala.reflect.ClassTag

import org.apache.flink.api.scala.DataSet
import org.apache.flink.ml.common.{ParameterMap, WithParameters}

trait Predictor[Self] extends Estimator[Self] with WithParameters with Serializable {
  that: Self =>

  def predict[Testing, Prediction](
      input: DataSet[Testing],
      predictParameters: ParameterMap = ParameterMap.Empty)(implicit
      predictor: PredictOperation[Self, Testing, Prediction])
    : DataSet[Prediction] = {
    predictor.predict(this, predictParameters, input)
  }
}

object Predictor{
  implicit def fallbackPredictOperation[Self: ClassTag, Testing: ClassTag, Prediction: ClassTag]
    : PredictOperation[Self, Testing, Prediction] = {
    new PredictOperation[Self, Testing, Prediction] {
      override def predict(
          instance: Self,
          predictParameters: ParameterMap,
          input: DataSet[Testing])
        : DataSet[Prediction] = {
        val self = implicitly[ClassTag[Self]]
        val testing = implicitly[ClassTag[Testing]]
        val prediction = implicitly[ClassTag[Prediction]]

        throw new RuntimeException("There is no PredictOperation defined for " + self.runtimeClass +
          " which takes a DataSet[" + testing.runtimeClass + "] as input and returns a DataSet[" +
          prediction.runtimeClass + "]")
      }
    }
  }

  implicit def fallbackChainedPredictOperation[
      L <: Transformer[L],
      R <: Predictor[R],
      LI,
      LO,
      RI,
      RO](implicit
      leftTransformOperation: TransformOperation[L, LI, LO],
      rightPredictOperation: PredictOperation[R, RI, RO]
      )
    : PredictOperation[ChainedPredictor[L, R], LI, RO] = {
    new PredictOperation[ChainedPredictor[L, R], LI, RO] {
      override def predict(
          instance: ChainedPredictor[L, R],
          predictParameters: ParameterMap,
          input: DataSet[LI]): DataSet[RO] = {
        instance.transformer.transform(input, predictParameters)
        instance.predictor.predict(null, predictParameters)
      }
    }
  }
}

abstract class PredictOperation[Self, Testing, Prediction]{
  def predict(
      instance: Self,
      predictParameters: ParameterMap,
      input: DataSet[Testing])
    : DataSet[Prediction]
}
