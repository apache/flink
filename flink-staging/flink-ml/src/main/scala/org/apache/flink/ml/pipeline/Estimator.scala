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

import scala.reflect.ClassTag

import org.apache.flink.api.scala.DataSet
import org.apache.flink.ml.common.{ParameterMap, WithParameters}

/** Base trait for Flink's pipeline operators.
  *
  * An estimator can be fitted to input data. In order to do that the implementing class has
  * to provide an implementation of a [[FitOperation]] with the correct input type. In order to make
  * the [[FitOperation]] retrievable by the Scala compiler, the implementation should be placed
  * in the companion object of the implementing class.
  *
  * The pipeline mechanism has been inspired by scikit-learn
  *
  * @tparam Self
  */
trait Estimator[Self] extends WithParameters with Serializable {
  that: Self =>

  /** Fits the estimator to the given input data. The fitting logic is contained in the
    * [[FitOperation]]. The computed state will be stored in the implementing class.
    *
    * @param training Training data
    * @param fitParameters Additional parameters for the [[FitOperation]]
    * @param fitOperation [[FitOperation]] which encapsulates the algorithm logic
    * @tparam Training Type of the training data
    * @return
    */
  def fit[Training](
      training: DataSet[Training],
      fitParameters: ParameterMap = ParameterMap.Empty)(implicit
      fitOperation: FitOperation[Self, Training]): Unit = {
    fitOperation.fit(this, fitParameters, training)
  }
}

object Estimator{

  /** Fallback [[FitOperation]] type class implementation which is used if no other
    * [[FitOperation]] with the right input types could be found in the scope of the implementing
    * class. The fallback [[FitOperation]] makes the system fail in the pre-flight phase by
    * throwing a [[RuntimeException]] which states the reason for the failure. Usually the error
    * is a missing [[FitOperation]] implementation for the input types or the wrong chaining
    * of pipeline operators which have incompatible input/output types.
    *
     * @tparam Self Type of the pipeline operator
    * @tparam Training Type of training data
    * @return
    */
  implicit def fallbackFitOperation[Self: ClassTag, Training: ClassTag]
    : FitOperation[Self, Training] = {
    new FitOperation[Self, Training]{
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

  /** Fallback [[FitOperation]] type class implementation for [[ChainedTransformer]]. The fallback
    * implementation is used if the Scala compiler could not instantiate the chained fit operation
    * defined in the companion object of [[ChainedTransformer]]. This is usually the case if either
    * a [[FitOperation]] or a [[TransformOperation]] could not be instantiated for one of the
    * leaves of the chained transformer. The fallback [[FitOperation]] calls the first the
    * fit operation of the left transformer, then the transform operation of the left transformer
    * and last the fit operation of the right transformer.
    *
    * @param leftFitOperation [[FitOperation]] of the left transformer
    * @param leftTransformOperation [[TransformOperation]] of the left transformer
    * @param rightFitOperaiton [[FitOperation]] of the right transformer
    * @tparam L Type of left transformer
    * @tparam R Type of right transformer
    * @tparam LI Input type of left transformer's [[FitOperation]]
    * @tparam LO Output type of left transformer's [[TransformOperation]]
    * @return
    */
  implicit def fallbackChainedFitOperationTransformer[
      L <: Transformer[L],
      R <: Transformer[R],
      LI,
      LO](implicit
      leftFitOperation: FitOperation[L, LI],
      leftTransformOperation: TransformOperation[L, LI, LO],
      rightFitOperaiton: FitOperation[R, LO])
    : FitOperation[ChainedTransformer[L, R], LI] = {
    new FitOperation[ChainedTransformer[L, R], LI] {
      override def fit(
          instance: ChainedTransformer[L, R],
          fitParameters: ParameterMap,
          input: DataSet[LI]): Unit = {
        instance.left.fit(input, fitParameters)
        val intermediate = instance.left.transform(input, fitParameters)
        instance.right.fit(intermediate, fitParameters)
      }
    }
  }

  /** Fallback [[FitOperation]] type class implementation for [[ChainedPredictor]]. The fallback
    * implementation is used if the Scala compiler could not instantiate the chained fit operation
    * defined in the companion object of [[ChainedPredictor]]. This is usually the case if either
    * a [[FitOperation]] or a [[TransformOperation]] could not be instantiated for one of the
    * leaves of the chained transformer. The fallback [[FitOperation]] calls the first the
    * fit operation of the left transformer, then the transform operation of the left transformer
    * and last the fit operation of the right transformer.
    *
    * @param leftFitOperation [[FitOperation]] of the left transformer
    * @param leftTransformOperation [[TransformOperation]] of the left transformer
    * @param rightFitOperaiton [[FitOperation]] of the right transformer
    * @tparam L Type of left transformer
    * @tparam R Type of right transformer
    * @tparam LI Input type of left transformer's [[FitOperation]]
    * @tparam LO Output type of left transformer's [[TransformOperation]]
    * @return
    */
  implicit def fallbackChainedFitOperationPredictor[
  L <: Transformer[L],
  R <: Predictor[R],
  LI,
  LO](implicit
    leftFitOperation: FitOperation[L, LI],
    leftTransformOperation: TransformOperation[L, LI, LO],
    rightFitOperaiton: FitOperation[R, LO])
  : FitOperation[ChainedPredictor[L, R], LI] = {
    new FitOperation[ChainedPredictor[L, R], LI] {
      override def fit(
          instance: ChainedPredictor[L, R],
          fitParameters: ParameterMap,
          input: DataSet[LI]): Unit = {
        instance.transformer.fit(input, fitParameters)
        val intermediate = instance.transformer.transform(input, fitParameters)
        instance.predictor.fit(intermediate, fitParameters)
      }
    }
  }
}

/** Type class for the fit operation of an [[Estimator]].
  *
  * The [[FitOperation]] contains a self type parameter so that the Scala compiler looks into
  * the companion object of this class to find implicit values.
  *
  * @tparam Self Type of the [[Estimator]] subclass for which the [[FitOperation]] is defined
  * @tparam Training Type of the training data
  */
trait FitOperation[Self, Training]{
  def fit(instance: Self, fitParameters: ParameterMap,  input: DataSet[Training]): Unit
}
