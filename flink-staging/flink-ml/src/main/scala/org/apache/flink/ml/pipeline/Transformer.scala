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

/** Transformer trait for Flink's pipeline operators.
  *
  * A Transformer transforms a [[DataSet]] of an input type into a [[DataSet]] of an output type.
  * Furthermore, a [[Transformer]] is also an [[Estimator]], because some transformations depend
  * on the training data. In order to do that the implementing class has to provide a
  * [[TransformOperation]] and [[FitOperation]] implementation. The Scala compiler finds these
  * implicit values if it is put in the scope of the companion object of the implementing class.
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

  /** Transform operation which transforms an input [[DataSet]] of type I into an ouptut [[DataSet]]
    * of type O. The actual transform operation is implemented within the [[TransformOperation]].
    *
    * @param input Input [[DataSet]] of type I
    * @param transformParameters Additional parameters for the [[TransformOperation]]
    * @param transformOperation [[TransformOperation]] which encapsulates the algorithm's logic
    * @tparam Input Input data type
    * @tparam Output Ouptut data type
    * @return
    */
  def transform[Input, Output](
      input: DataSet[Input],
      transformParameters: ParameterMap = ParameterMap.Empty)
      (implicit transformOperation: TransformOperation[Self, Input, Output])
    : DataSet[Output] = {
    transformOperation.transform(that, transformParameters, input)
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

  /** Fallback [[TransformOperation]] for [[ChainedTransformer]] which is used if no suitable
    * [[TransformOperation]] implementation can be found. This implementation is used if there is no
    * [[TransformOperation]] for one of the leaves of the [[ChainedTransformer]] for the given
    * input types. This is usually the case if one [[Transformer]] does not support the transform
    * operation for the input type.
    *
    * The fallback [[TransformOperation]] for [[ChainedTransformer]] calls first the transform
    * operation of the left transformer and then the transform operation of the right transformer.
    * That way the fallback [[TransformOperation]] for a [[Transformer]] will be called which
    * will fail the job in the pre-flight phase by throwing an exception.
    *
    * @param transformLeft Left [[Transformer]] of the pipeline
    * @param transformRight Right [[Transformer]] of the pipeline
    * @tparam L Type of the left [[Transformer]]
    * @tparam R Type of the right [[Transformer]]
    * @tparam LI Input type of left transformer's [[TransformOperation]]
    * @tparam LO Output type of left transformer's [[TransformOperation]]
    * @tparam RO Output type of right transformer's [[TransformOperation]]
    * @return
    */
  implicit def fallbackChainedTransformOperation[
      L <: Transformer[L],
      R <: Transformer[R],
      LI,
      LO,
      RO]
      (implicit transformLeft: TransformOperation[L, LI, LO],
      transformRight: TransformOperation[R, LO, RO])
    : TransformOperation[ChainedTransformer[L,R], LI, RO] = {

    new TransformOperation[ChainedTransformer[L, R], LI, RO] {
      override def transform(
          chain: ChainedTransformer[L, R],
          transformParameters: ParameterMap,
          input: DataSet[LI]): DataSet[RO] = {
        val intermediate = transformLeft.transform(chain.left, transformParameters, input)
        transformRight.transform(chain.right, transformParameters, intermediate)
      }
    }
  }

  /** Fallback [[TransformOperation]] for [[Transformer]] which do not support the input or output
    * type with which they are called. This is usualy the case if pipeline operators are chained
    * which have incompatible input/output types. In order to detect these failures, the fallback
    * [[TransformOperation]] throws a [[RuntimeException]] with the corresponding input/output
    * types. Consequently, a wrong pipeline will be detected at pre-flight phase of Flink and
    * thus prior to execution time.
    *
    * @tparam Self Type of the [[Transformer]] for which the [[TransformOperation]] is defined
    * @tparam IN Input data type of the [[TransformOperation]]
    * @tparam OUT Output data type of the [[TransformOperation]]
    * @return
    */
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

/** Type class for a transform operation of [[Transformer]].
  *
  * The [[TransformOperation]] contains a self type parameter so that the Scala compiler looks into
  * the companion object of this class to find implicit values.
  *
  * @tparam Self Type of the [[Transformer]] for which the [[TransformOperation]] is defined
  * @tparam Input Input data type
  * @tparam Output Ouptut data type
  */
abstract class TransformOperation[Self, Input, Output] extends Serializable{
  def transform(
      instance: Self,
      transformParameters: ParameterMap,
      input: DataSet[Input])
    : DataSet[Output]
}
