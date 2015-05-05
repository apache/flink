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

trait Estimator[Self] extends WithParameters with Serializable {
  that: Self =>

  def fit[Training](
      input: DataSet[Training],
      fitParameters: ParameterMap = ParameterMap.Empty)(implicit
      fitOperation: FitOperation[Self, Training]): Unit = {
    fitOperation.fit(this, fitParameters, input)
  }
}

object Estimator{
  implicit def fallbackFitOperation[Self: ClassTag, Training: ClassTag]
    : FitOperation[Self, Training] = {
    new FitOperation[Self, Training]{
      override def fit(
          instance: Self,
          fitParameters: ParameterMap,
          input: DataSet[Training])
        : Unit = {
        new FitOperation[Self, Training] {
          override def fit(
                instance: Self,
                fitParameters: ParameterMap,
                input: DataSet[Training])
            : Unit = {
            val self = implicitly[ClassTag[Self]]
            val training = implicitly[ClassTag[Training]]

            throw new RuntimeException("There is no FitOperation defined for " + self.runtimeClass +
              " which trains on a DataSet[" + training.runtimeClass + "]")
          }
        }
      }
    }
  }

  implicit def fallbackChainedFitOperationTransformer[
      L <: Transformer[L],
      R <: Transformer[R],
      LI,
      LO,
      RI](implicit
      leftFitOperation: FitOperation[L, LI],
      leftTransformOperation: TransformOperation[L, LI, LO],
      rightFitOperaiton: FitOperation[R, RI])
    : FitOperation[ChainedTransformer[L, R], LI] = {
    new FitOperation[ChainedTransformer[L, R], LI] {
      override def fit(
          instance: ChainedTransformer[L, R],
          fitParameters: ParameterMap,
          input: DataSet[LI]): Unit = {
        instance.left.fit(input, fitParameters)
        instance.left.transform(input, fitParameters)
        instance.right.fit(null, fitParameters)
      }
    }
  }

  implicit def fallbackChainedFitOperationPredictor[
  L <: Transformer[L],
  R <: Predictor[R],
  LI,
  LO,
  RI](implicit
    leftFitOperation: FitOperation[L, LI],
    leftTransformOperation: TransformOperation[L, LI, LO],
    rightFitOperaiton: FitOperation[R, RI])
  : FitOperation[ChainedPredictor[L, R], LI] = {
    new FitOperation[ChainedPredictor[L, R], LI] {
      override def fit(
          instance: ChainedPredictor[L, R],
          fitParameters: ParameterMap,
          input: DataSet[LI]): Unit = {
        instance.transformer.fit(input, fitParameters)
        instance.transformer.transform(input, fitParameters)
        instance.predictor.fit(null, fitParameters)
      }
    }
  }
}

trait FitOperation[Self, Training]{
  def fit(instance: Self, fitParameters: ParameterMap,  input: DataSet[Training]): Unit
}
