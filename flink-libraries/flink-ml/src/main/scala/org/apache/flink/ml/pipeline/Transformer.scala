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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.DataSet
import org.apache.flink.ml._
import org.apache.flink.ml.common.{FlinkMLTools, ParameterMap, WithParameters}

import scala.reflect.ClassTag

/** Transformer trait for Flink's pipeline operators.
  *
  * A Transformer transforms a [[DataSet]] of an input type into a [[DataSet]] of an output type.
  * Furthermore, a [[Transformer]] is also an [[Estimator]], because some transformations depend
  * on the training data. In order to do that the implementing class has to provide a
  * [[TransformDataSetOperation]] and [[FitOperation]] implementation. The Scala compiler finds
  * these implicit values if it is put in the scope of the companion object of the implementing
  * class.
  *
  * [[Transformer]] can be chained with other [[Transformer]] and [[Predictor]] to create
  * pipelines. These pipelines can consist of an arbitrary number of [[Transformer]] and at most
  * one trailing [[Predictor]].
  *
  * The pipeline mechanism has been inspired by scikit-learn
  *
  * @tparam Self
  */
trait Transformer[Self <: Transformer[Self]]
  extends Estimator[Self]
  with WithParameters
  with Serializable {
  that: Self =>

  /** Transform operation which transforms an input [[DataSet]] of type I into an output [[DataSet]]
    * of type O. The actual transform operation is implemented within the
    * [[TransformDataSetOperation]].
    *
    * @param input Input [[DataSet]] of type I
    * @param transformParameters Additional parameters for the [[TransformDataSetOperation]]
    * @param transformOperation [[TransformDataSetOperation]] which encapsulates the algorithm's
    *                          logic
    * @tparam Input Input data type
    * @tparam Output Output data type
    * @return
    */
  def transform[Input, Output](
      input: DataSet[Input],
      transformParameters: ParameterMap = ParameterMap.Empty)
      (implicit transformOperation: TransformDataSetOperation[Self, Input, Output])
    : DataSet[Output] = {
    FlinkMLTools.registerFlinkMLTypes(input.getExecutionEnvironment)
    transformOperation.transformDataSet(that, transformParameters, input)
  }

  /** Chains two [[Transformer]] to form a [[ChainedTransformer]].
    *
    * @param transformer Right side transformer of the resulting pipeline
    * @tparam T Type of the [[Transformer]]
    * @return
    */
  def chainTransformer[T <: Transformer[T]](transformer: T): ChainedTransformer[Self, T] = {
    ChainedTransformer(this, transformer)
  }

  /** Chains a [[Transformer]] with a [[Predictor]] to form a [[ChainedPredictor]].
    *
    * @param predictor Trailing [[Predictor]] of the resulting pipeline
    * @tparam P Type of the [[Predictor]]
    * @return
    */
  def chainPredictor[P <: Predictor[P]](predictor: P): ChainedPredictor[Self, P] = {
    ChainedPredictor(this, predictor)
  }
}

object Transformer{
  implicit def defaultTransformDataSetOperation[
      Instance <: Estimator[Instance],
      Model,
      Input,
      Output](
      implicit transformOperation: TransformOperation[Instance, Model, Input, Output],
      outputTypeInformation: TypeInformation[Output],
      outputClassTag: ClassTag[Output])
    : TransformDataSetOperation[Instance, Input, Output] = {
    new TransformDataSetOperation[Instance, Input, Output] {
      override def transformDataSet(
          instance: Instance,
          transformParameters: ParameterMap,
          input: DataSet[Input])
        : DataSet[Output] = {
        val resultingParameters = instance.parameters ++ transformParameters
        val model = transformOperation.getModel(instance, resultingParameters)

        input.mapWithBcVariable(model){
          (element, model) => transformOperation.transform(element, model)
        }
      }
    }
  }
}

/** Type class for a transform operation of [[Transformer]]. This works on [[DataSet]] of elements.
  *
  * The [[TransformDataSetOperation]] contains a self type parameter so that the Scala compiler
  * looks into the companion object of this class to find implicit values.
  *
  * @tparam Instance Type of the [[Transformer]] for which the [[TransformDataSetOperation]] is
  *                  defined
  * @tparam Input Input data type
  * @tparam Output Output data type
  */
trait TransformDataSetOperation[Instance, Input, Output] extends Serializable{
  def transformDataSet(
      instance: Instance,
      transformParameters: ParameterMap,
      input: DataSet[Input])
    : DataSet[Output]
}

/** Type class for a transform operation which works on a single element and the corresponding model
  * of the [[Transformer]].
  *
  * @tparam Instance
  * @tparam Model
  * @tparam Input
  * @tparam Output
  */
trait TransformOperation[Instance, Model, Input, Output] extends Serializable{

  /** Retrieves the model of the [[Transformer]] for which this operation has been defined.
    *
    * @param instance
    * @param transformParameters
    * @return
    */
  def getModel(instance: Instance, transformParameters: ParameterMap): DataSet[Model]

  /** Transforms a single element with respect to the model associated with the respective
    * [[Transformer]]
    *
    * @param element
    * @param model
    * @return
    */
  def transform(element: Input, model: Model): Output
}
