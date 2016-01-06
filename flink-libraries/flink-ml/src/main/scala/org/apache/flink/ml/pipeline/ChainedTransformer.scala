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

package org.apache.flink.ml.pipeline

import org.apache.flink.api.scala.DataSet
import org.apache.flink.ml.common.ParameterMap

/** [[Transformer]] which represents the chaining of two [[Transformer]].
  *
  * A [[ChainedTransformer]] can be treated as regular [[Transformer]]. Upon calling the fit or
  * transform operation, the data is piped through all [[Transformer]] of the pipeline.
  *
  * The pipeline mechanism has been inspired by scikit-learn
  *
  * @param left Left [[Transformer]] of the pipeline
  * @param right Right [[Transformer]] of the pipeline
  * @tparam L Type of the left [[Transformer]]
  * @tparam R Type of the right [[Transformer]]
  */
case class ChainedTransformer[L <: Transformer[L], R <: Transformer[R]](left: L, right: R)
  extends Transformer[ChainedTransformer[L, R]] {
}

object ChainedTransformer{

  /** [[TransformDataSetOperation]] implementation for [[ChainedTransformer]].
    *
    * First the transform operation of the left [[Transformer]] is called with the input data. This
    * generates intermediate data which is fed to the right [[Transformer]]'s transform operation.
    *
    * @param transformOpLeft [[TransformDataSetOperation]] for the left [[Transformer]]
    * @param transformOpRight [[TransformDataSetOperation]] for the right [[Transformer]]
    * @tparam L Type of the left [[Transformer]]
    * @tparam R Type of the right [[Transformer]]
    * @tparam I Type of the input data
    * @tparam T Type of the intermediate output data
    * @tparam O Type of the output data
    * @return
    */
  implicit def chainedTransformOperation[
      L <: Transformer[L],
      R <: Transformer[R],
      I,
      T,
      O](implicit
      transformOpLeft: TransformDataSetOperation[L, I, T],
      transformOpRight: TransformDataSetOperation[R, T, O])
    : TransformDataSetOperation[ChainedTransformer[L,R], I, O] = {

    new TransformDataSetOperation[ChainedTransformer[L, R], I, O] {
      override def transformDataSet(
          chain: ChainedTransformer[L, R],
          transformParameters: ParameterMap,
          input: DataSet[I]): DataSet[O] = {
        val intermediateResult = transformOpLeft.transformDataSet(
          chain.left,
          transformParameters,
          input)
        transformOpRight.transformDataSet(chain.right, transformParameters, intermediateResult)
      }
    }
  }

  /** [[FitOperation]] implementation for [[ChainedTransformer]].
    *
    * First the fit operation of the left [[Transformer]] is called with the input data. Then
    * the data is transformed by this [[Transformer]] and the given to the fit operation of the
    * right [[Transformer]].
    *
    * @param leftFitOperation [[FitOperation]] for the left [[Transformer]]
    * @param leftTransformOperation [[TransformDataSetOperation]] for the left [[Transformer]]
    * @param rightFitOperation [[FitOperation]] for the right [[Transformer]]
    * @tparam L Type of the left [[Transformer]]
    * @tparam R Type of the right [[Transformer]]
    * @tparam I Type of the input data
    * @tparam T Type of the intermediate output data
    * @return
    */
  implicit def chainedFitOperation[L <: Transformer[L], R <: Transformer[R], I, T](implicit
      leftFitOperation: FitOperation[L, I],
      leftTransformOperation: TransformDataSetOperation[L, I, T],
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
