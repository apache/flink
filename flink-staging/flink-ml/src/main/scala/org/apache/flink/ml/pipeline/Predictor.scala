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
import org.apache.flink.ml.common.{FlinkMLTools, ParameterMap, WithParameters}

/** Predictor trait for Flink's pipeline operators.
  *
  * A [[Predictor]] calculates predictions for testing data based on the model it learned during
  * the fit operation (training phase). In order to do that, the implementing class has to provide
  * a [[FitOperation]] and a [[PredictOperation]] implementation for the correct types. The implicit
  * values should be put into the scope of the companion object of the implementing class to make
  * them retrievable for the Scala compiler.
  *
  * The pipeline mechanism has been inspired by scikit-learn
  *
  * @tparam Self Type of the implementing class
  */
trait Predictor[Self] extends Estimator[Self] with WithParameters with Serializable {
  that: Self =>

  /** Predict testing data according the learned model. The implementing class has to provide
    * a corresponding implementation of [[PredictOperation]] which contains the prediction logic.
    *
    * @param testing Testing data which shall be predicted
    * @param predictParameters Additional parameters for the prediction
    * @param predictor [[PredictOperation]] which encapsulates the prediction logic
    * @tparam Testing Type of the testing data
    * @tparam Prediction Type of the prediction data
    * @return
    */
  def predict[Testing, Prediction](
      testing: DataSet[Testing],
      predictParameters: ParameterMap = ParameterMap.Empty)(implicit
      predictor: PredictOperation[Self, Testing, Prediction])
    : DataSet[Prediction] = {
    FlinkMLTools.registerFlinkMLTypes(testing.getExecutionEnvironment)
    predictor.predict(this, predictParameters, testing)
  }
}

object Predictor{

  /** Fallback [[PredictOperation]] if a [[Predictor]] is called with a not supported input data
    * type. The fallback [[PredictOperation]] lets the system fail with a [[RuntimeException]]
    * stating which input and output data types were inferred but for which no [[PredictOperation]]
    * could be found.
    *
    * @tparam Self Type of the [[Predictor]]
    * @tparam Testing Type of the testing data
    * @return
    */
  implicit def fallbackPredictOperation[Self: ClassTag, Testing: ClassTag]
    : PredictOperation[Self, Testing, Any] = {
    new PredictOperation[Self, Testing, Any] {
      override def predict(
          instance: Self,
          predictParameters: ParameterMap,
          input: DataSet[Testing])
        : DataSet[Any] = {
        val self = implicitly[ClassTag[Self]]
        val testing = implicitly[ClassTag[Testing]]

        throw new RuntimeException("There is no PredictOperation defined for " + self.runtimeClass +
          " which takes a DataSet[" + testing.runtimeClass + "] as input.")
      }
    }
  }

  /** Fallback [[PredictOperation]] for a [[ChainedPredictor]] if a [[TransformOperation]] for
    * one of the [[Transformer]] and its respective types or the [[PredictOperation]] for the
    * [[Predictor]] and its respective type could not be found. This is usually the case, if the
    * the pipeline contains pipeline operators which work on incompatible types.
    *
    * The fallback [[PredictOperation]] first transforms the input data by calling the transform
    * method of the [[Transformer]] and then the predict method of the [[Predictor]].
    *
    * @param leftTransformOperation [[TransformOperation]] of the [[Transformer]]
    * @param rightPredictOperation [[PredictOperation]] of the [[Predictor]]
    * @tparam L Type of the [[Transformer]]
    * @tparam R Type of the [[Predictor]]
    * @tparam LI Input type of the [[Transformer]]
    * @tparam LO Output type of the [[Transformer]]
    * @tparam RO Prediction type of the [[Predictor]]
    * @return
    */
  implicit def fallbackChainedPredictOperation[
      L <: Transformer[L],
      R <: Predictor[R],
      LI,
      LO,
      RO](implicit
      leftTransformOperation: TransformOperation[L, LI, LO],
      rightPredictOperation: PredictOperation[R, LO, RO]
      )
    : PredictOperation[ChainedPredictor[L, R], LI, RO] = {
    new PredictOperation[ChainedPredictor[L, R], LI, RO] {
      override def predict(
          instance: ChainedPredictor[L, R],
          predictParameters: ParameterMap,
          input: DataSet[LI]): DataSet[RO] = {
        val intermediate = instance.transformer.transform(input, predictParameters)
        instance.predictor.predict(intermediate, predictParameters)
      }
    }
  }
}

/** Type class for the predict operation of [[Predictor]].
  *
  * Predictors have to implement this trait and make the result available as an implicit value or
  * function in the scope of their companion objects.
  *
  * The first type parameter is the type of the implementing [[Predictor]] class so that the Scala
  * compiler includes the companion object of this class in the search scope for the implicit
  * values.
  *
  * @tparam Self Type of [[Predictor]] implementing class
  * @tparam Testing Type of testing data
  * @tparam Prediction Type of predicted data
  */
trait PredictOperation[Self, Testing, Prediction]{
  def predict(
      instance: Self,
      predictParameters: ParameterMap,
      input: DataSet[Testing])
    : DataSet[Prediction]
}
