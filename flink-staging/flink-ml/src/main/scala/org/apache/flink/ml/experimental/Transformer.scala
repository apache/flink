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

trait Transformer[Self <: Transformer[Self]]
  extends Estimator[Self]
  with WithParameters
  with Serializable {
  that: Self =>

  def transform[I, O](input: DataSet[I], transformParameters: ParameterMap = ParameterMap.Empty)
                     (implicit transformOperation: TransformOperation[Self, I, O]): DataSet[O] = {
    transformOperation.transform(that, transformParameters, input)
  }

  def chainTransformer[T <: Transformer[T]](transformer: T): ChainedTransformer[Self, T] = {
    ChainedTransformer(this, transformer)
  }

  def chainPredictor[P <: Predictor[P]](predictor: P): ChainedPredictor[Self, P] = {
    ChainedPredictor(this, predictor)
  }
}

object Transformer{
  implicit def fallbackChainedTransformOperation[
      L <: Transformer[L],
      R <: Transformer[R],
      LI,
      LO,
      RI,
      RO]
      (implicit transformLeft: TransformOperation[L, LI, LO],
      transformRight: TransformOperation[R, RI, RO])
    : TransformOperation[ChainedTransformer[L,R], LI, RO] = {

    new TransformOperation[ChainedTransformer[L, R], LI, RO] {
      override def transform(
          chain: ChainedTransformer[L, R],
          transformParameters: ParameterMap,
          input: DataSet[LI]): DataSet[RO] = {
        transformLeft.transform(chain.left, transformParameters, input)
        transformRight.transform(chain.right, transformParameters, null)
      }
    }
  }

  implicit def fallbackTransformOperation[
      Self: ClassTag,
      IN: ClassTag,
      OUT: ClassTag]
    : TransformOperation[Self, IN, OUT] = {
    new TransformOperation[Self, IN, OUT] {
      override def transform(
          instance: Self,
          transformParameters: ParameterMap,
          input: DataSet[IN])
        : DataSet[OUT] = {
        val self = implicitly[ClassTag[Self]]
        val in = implicitly[ClassTag[IN]]
        val out = implicitly[ClassTag[OUT]]

        throw new RuntimeException("There is no TransformOperation defined for " +
          self.runtimeClass +  " which takes a DataSet[" + in.runtimeClass +
          "] as input and transforms it into a DataSet[" + out.runtimeClass + "]")
      }
    }
  }
}

abstract class TransformOperation[Self, IN, OUT] extends Serializable{
  def transform(instance: Self, transformParameters: ParameterMap, input: DataSet[IN]): DataSet[OUT]
}
