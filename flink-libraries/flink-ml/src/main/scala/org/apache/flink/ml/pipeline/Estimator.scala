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
import scala.reflect.runtime.universe._

import org.apache.flink.api.scala.DataSet
import org.apache.flink.ml.common.{FlinkMLTools, ParameterMap, WithParameters}

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
trait Estimator[Self] extends WithParameters {
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
    FlinkMLTools.registerFlinkMLTypes(training.getExecutionEnvironment)
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
  implicit def fallbackFitOperation[
      Self: TypeTag,
      Training: TypeTag]
    : FitOperation[Self, Training] = {
    new FitOperation[Self, Training]{
      override def fit(
          instance: Self,
          fitParameters: ParameterMap,
          input: DataSet[Training])
        : Unit = {
        val self = typeOf[Self]
        val training = typeOf[Training]

        throw new RuntimeException("There is no FitOperation defined for " + self +
          " which trains on a DataSet[" + training + "]")
      }
    }
  }

  /** Fallback [[PredictDataSetOperation]] if a [[Predictor]] is called with a not supported input
    * data type. The fallback [[PredictDataSetOperation]] lets the system fail with a
    * [[RuntimeException]] stating which input and output data types were inferred but for which no
    * [[PredictDataSetOperation]] could be found.
    *
    * @tparam Self Type of the [[Predictor]]
    * @tparam Testing Type of the testing data
    * @return
    */
  implicit def fallbackPredictOperation[
      Self: TypeTag,
      Testing: TypeTag]
    : PredictDataSetOperation[Self, Testing, Any] = {
    new PredictDataSetOperation[Self, Testing, Any] {
      override def predictDataSet(
          instance: Self,
          predictParameters: ParameterMap,
          input: DataSet[Testing])
        : DataSet[Any] = {
        val self = typeOf[Self]
        val testing = typeOf[Testing]

        throw new RuntimeException("There is no PredictOperation defined for " + self +
          " which takes a DataSet[" + testing + "] as input.")
      }
    }
  }

  /** Fallback [[TransformDataSetOperation]] for [[Transformer]] which do not support the input or
    * output type with which they are called. This is usually the case if pipeline operators are
    * chained which have incompatible input/output types. In order to detect these failures, the
    * fallback [[TransformDataSetOperation]] throws a [[RuntimeException]] with the corresponding
    * input/output types. Consequently, a wrong pipeline will be detected at pre-flight phase of
    * Flink and thus prior to execution time.
    *
    * @tparam Self Type of the [[Transformer]] for which the [[TransformDataSetOperation]] is
    *              defined
    * @tparam IN Input data type of the [[TransformDataSetOperation]]
    * @return
    */
  implicit def fallbackTransformOperation[
  Self: TypeTag,
  IN: TypeTag]
  : TransformDataSetOperation[Self, IN, Any] = {
    new TransformDataSetOperation[Self, IN, Any] {
      override def transformDataSet(
        instance: Self,
        transformParameters: ParameterMap,
        input: DataSet[IN])
      : DataSet[Any] = {
        val self = typeOf[Self]
        val in = typeOf[IN]

        throw new RuntimeException("There is no TransformOperation defined for " +
          self +  " which takes a DataSet[" + in +
          "] as input.")
      }
    }
  }

  implicit def fallbackEvaluateOperation[
      Self: TypeTag,
      Testing: TypeTag]
    : EvaluateDataSetOperation[Self, Testing, Any] = {
    new EvaluateDataSetOperation[Self, Testing, Any] {
      override def evaluateDataSet(
        instance: Self,
        predictParameters: ParameterMap,
        input: DataSet[Testing])
      : DataSet[(Any, Any)] = {
        val self = typeOf[Self]
        val testing = typeOf[Testing]

        throw new RuntimeException("There is no PredictOperation defined for " + self +
          " which takes a DataSet[" + testing + "] as input.")
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
