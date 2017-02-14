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
package org.apache.flink.ml.preprocessing

import breeze.linalg
import breeze.linalg.normalize
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala._
import org.apache.flink.ml.common.{LabeledVector, Parameter, ParameterMap}
import org.apache.flink.ml.math.{BreezeVectorConverter, Norm, Vector}
import org.apache.flink.ml.pipeline.{FitOperation, TransformDataSetOperation, Transformer}
import org.apache.flink.ml.preprocessing.Normalizer.NormPValue
import org.apache.flink.ml.math.Breeze._
import org.apache.flink.ml.preprocessing.validation.DataValidation

import scala.reflect.ClassTag

/** Scales observations, so that all samples have unit measure. L^p norm is used.
  * By default for [[Normalizer]] transformer p=2.
  *
  * This transformer takes a subtype of  [[Vector]] of values and maps it to a
  * scaled subtype of [[Vector]] such that each sample has unit measure.
  *
  * This transformer can be prepended to all [[Transformer]] and
  * [[org.apache.flink.ml.pipeline.Predictor]] implementations which expect as input a subtype
  * of [[Vector]].
  *
  *
  * @example
  *          {{{
  *            val trainingDS: DataSet[Vector] = env.fromCollection(data)
  *            val transformer = Normalizer().setNorm(Norm.L2)
  *            val transformedDS = transformer.transform(trainingDS)
  *          }}}
  *
  * =Parameters=
  *
  * - [[org.apache.flink.ml.preprocessing.Normalizer.NormPValue]]: The p value for the norm to use.
  * By default 2.0 is used.
  */
class Normalizer extends Transformer[Normalizer] {
  /** Sets the norm value to use for the transform of the data
    *
    * @param p the user-specified p value.
    * @return the Normalizer with the p value set to the user-specified value.
    */
  def setNorm(p: Double): Normalizer = {
    parameters.add(NormPValue, p)
    this
  }
}

object Normalizer {

  case object NormPValue extends Parameter[Double] {
    override val defaultValue: Option[Double] = Some(Norm.L2)
  }

  def apply(): Normalizer = {
    new Normalizer()
  }

  /** Traditionally this would train the transformer with some parameters.
    * For this case since we are transforming the data per sample, this is a redundant step.
    * See also: http://scikit-learn.org/stable/modules/preprocessing.html
    *
    * @tparam T Input data type which is a subtype of [[Vector]]
    * @return [[FitOperation]] training the [[Normalizer]] on subtypes of [[Vector]]
    */
  implicit def fitVectorNormalizer[T <: Vector] = new FitOperation[Normalizer, T] {
    override def fit(instance: Normalizer, fitParameters: ParameterMap, input: DataSet[T])
    : Unit = {}
  }

  /** Traditionally this would train the transformer with some parameters.
    * For this case since we are transforming the data per sample, this is a redundant step.
    * See also: http://scikit-learn.org/stable/modules/preprocessing.html
    */
  implicit val fitLabeledVectorNormalizer: FitOperation[Normalizer, LabeledVector] = {
    new FitOperation[Normalizer, LabeledVector] {
      override def fit(
        instance: Normalizer,
        fitParameters: ParameterMap,
        input: DataSet[LabeledVector])
      : Unit = {}
    }
  }

  /** [[TransformDataSetOperation]] which scales input data of subtype of [[Vector]] with respect to
    * the calculated minimum and maximum of the training data. The minimum and maximum
    * values of the resulting data is configurable.
    *
    * @tparam T Type of the input and output data which has to be a subtype of [[Vector]]
    * @return [[TransformDataSetOperation]] scaling subtypes of [[Vector]] such that the feature
    *        values are in the configured range
    */
  implicit def transformVectors[T <: Vector : BreezeVectorConverter : TypeInformation : ClassTag]
  = {
    new TransformDataSetOperation[Normalizer, T, T] {
      override def transformDataSet(
        instance: Normalizer,
        transformParameters: ParameterMap,
        input: DataSet[T])
      : DataSet[T] = {
        val resultingParameters = instance.parameters ++ transformParameters
        val pValue = resultingParameters(NormPValue)
        input.map { vector =>
          if (DataValidation.isFiniteVector(vector)) {
            normalizeVector(vector, pValue)
          } else {
            throw new RuntimeException("Input contains NaN or Infinity...")
          }
        }
      }
    }
  }

  implicit val transformLabeledVectors = {
    new TransformDataSetOperation[Normalizer, LabeledVector, LabeledVector] {
      override def transformDataSet(instance: Normalizer,
        transformParameters: ParameterMap,
        input: DataSet[LabeledVector])
      : DataSet[LabeledVector] = {
        val resultingParameters = instance.parameters ++ transformParameters
        val pValue = resultingParameters(NormPValue)
        input.map { labeledVector =>
          val LabeledVector(label, vector) = labeledVector

          if (DataValidation.isFiniteVector(vector)) {
            LabeledVector(label, normalizeVector(vector, pValue))
          } else {
            throw new RuntimeException("Input contains NaN or Infinity...")
          }
        }
      }
    }
  }

  /** Scales a vector according to a p-norm so that the sum of the scaled vector is 1.
    * Scikit-learn sets norm to 1.0 if zero is found.
    **/
  private def normalizeVector[T <: Vector: BreezeVectorConverter](vector: T, pValue: Double): T = {
    val normalized = normalize(vector.asBreeze, pValue)
    normalized.fromBreeze
  }
}

