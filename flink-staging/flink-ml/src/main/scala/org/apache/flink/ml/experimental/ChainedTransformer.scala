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

case class ChainedTransformer[L <: Transformer[L], R <: Transformer[R]](left: L, right: R)
  extends Transformer[ChainedTransformer[L, R]] {
}

object ChainedTransformer{
  implicit def chainedTransformOperation[
      L <: Transformer[L],
      R <: Transformer[R],
      I,
      T,
      O](implicit
      transformLeft: TransformOperation[L, I, T],
      transformRight: TransformOperation[R, T, O])
    : TransformOperation[ChainedTransformer[L,R], I, O] = {

    new TransformOperation[ChainedTransformer[L, R], I, O] {
      override def transform(
          chain: ChainedTransformer[L, R],
          transformParameters: ParameterMap,
          input: DataSet[I]): DataSet[O] = {
        val intermediateResult = transformLeft.transform(chain.left, transformParameters, input)
        transformRight.transform(chain.right, transformParameters, intermediateResult)
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
          input: DataSet[I]): Unit = {
        instance.left.fit(input, fitParameters)
        val intermediateResult = instance.left.transform(input, fitParameters)
        instance.right.fit(intermediateResult, fitParameters)
      }
    }
  }
}
