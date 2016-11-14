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
import breeze.linalg.{max, min}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.ml._
import org.apache.flink.ml.common.{LabeledVector, Parameter, ParameterMap}
import org.apache.flink.ml.math.Breeze._
import org.apache.flink.ml.math.{BreezeVectorConverter, Vector}
import org.apache.flink.ml.pipeline.{TransformDataSetOperation, FitOperation,
Transformer}
import org.apache.flink.ml.preprocessing.MinMaxScaler.{Max, Min}

import scala.reflect.ClassTag

/** Scales observations, so that all features are in a user-specified range.
  * By default for [[MinMaxScaler]] transformer range = [0,1].
  *
  * This transformer takes a subtype of  [[Vector]] of values and maps it to a
  * scaled subtype of [[Vector]] such that each feature lies between a user-specified range.
  *
  * This transformer can be prepended to all [[Transformer]] and
  * [[org.apache.flink.ml.pipeline.Predictor]] implementations which expect as input a subtype
  * of [[Vector]] or a [[LabeledVector]].
  *
  * @example
  * {{{
  *               val trainingDS: DataSet[Vector] = env.fromCollection(data)
  *               val transformer = MinMaxScaler().setMin(-1.0)
  *
  *               transformer.fit(trainingDS)
  *               val transformedDS = transformer.transform(trainingDS)
  * }}}
  *
  * =Parameters=
  *
  * - [[Min]]: The minimum value of the range of the transformed data set; by default equal to 0
  * - [[Max]]: The maximum value of the range of the transformed data set; by default
  * equal to 1
  */
class MinMaxScaler extends Transformer[MinMaxScaler] {

  private [preprocessing] var metricsOption: Option[
      DataSet[(linalg.Vector[Double], linalg.Vector[Double])]
    ] = None

  /** Sets the minimum for the range of the transformed data
    *
    * @param min the user-specified minimum value.
    * @return the MinMaxScaler instance with its minimum value set to the user-specified value.
    */
  def setMin(min: Double): MinMaxScaler = {
    parameters.add(Min, min)
    this
  }

  /** Sets the maximum for the range of the transformed data
    *
    * @param max the user-specified maximum value.
    * @return the MinMaxScaler instance with its minimum value set to the user-specified value.
    */
  def setMax(max: Double): MinMaxScaler = {
    parameters.add(Max, max)
    this
  }
}

object MinMaxScaler {

  // ====================================== Parameters =============================================

  case object Min extends Parameter[Double] {
    override val defaultValue: Option[Double] = Some(0.0)
  }

  case object Max extends Parameter[Double] {
    override val defaultValue: Option[Double] = Some(1.0)
  }

  // ==================================== Factory methods ==========================================

  def apply(): MinMaxScaler = {
    new MinMaxScaler()
  }

  // ====================================== Operations =============================================

  /** Trains the [[MinMaxScaler]] by learning the minimum and maximum of each feature of the
    * training data. These values are used in the transform step to transform the given input data.
    *
    * @tparam T Input data type which is a subtype of [[Vector]]
    * @return [[FitOperation]] training the [[MinMaxScaler]] on subtypes of [[Vector]]
    */
  implicit def fitVectorMinMaxScaler[T <: Vector] = new FitOperation[MinMaxScaler, T] {
    override def fit(instance: MinMaxScaler, fitParameters: ParameterMap, input: DataSet[T])
    : Unit = {
      val metrics = extractFeatureMinMaxVectors(input)

      instance.metricsOption = Some(metrics)
    }
  }

  /** Trains the [[MinMaxScaler]] by learning the minimum and maximum of the features of the
    * training data which is of type [[LabeledVector]]. The minimum and maximum are used to
    * transform the given input data.
    *
    */
  implicit val fitLabeledVectorMinMaxScaler = {
    new FitOperation[MinMaxScaler, LabeledVector] {
      override def fit(
        instance: MinMaxScaler,
        fitParameters: ParameterMap,
        input: DataSet[LabeledVector])
      : Unit = {
        val vectorDS = input.map(_.vector)
        val metrics = extractFeatureMinMaxVectors(vectorDS)

        instance.metricsOption = Some(metrics)
      }
    }
  }

  /** Calculates in one pass over the data the features' minimum and maximum values.
    *
    * @param dataSet The data set for which we want to calculate the minimum and maximum values.
    * @return  DataSet containing a single tuple of two vectors (minVector, maxVector).
    *          The first vector represents the minimum values vector and the second is the maximum
    *          values vector.
    */
  private def extractFeatureMinMaxVectors[T <: Vector](dataSet: DataSet[T])
  : DataSet[(linalg.Vector[Double], linalg.Vector[Double])] = {

    val minMax = dataSet.map {
      v => (v.asBreeze, v.asBreeze)
    }.reduce {
      (minMax1, minMax2) => {

        val tempMinimum = min(minMax1._1, minMax2._1)
        val tempMaximum = max(minMax1._2, minMax2._2)

        (tempMinimum, tempMaximum)
      }
    }
    minMax
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
    new TransformDataSetOperation[MinMaxScaler, T, T] {
      override def transformDataSet(
        instance: MinMaxScaler,
        transformParameters: ParameterMap,
        input: DataSet[T])
      : DataSet[T] = {

        val resultingParameters = instance.parameters ++ transformParameters
        val min = resultingParameters(Min)
        val max = resultingParameters(Max)

        instance.metricsOption match {
          case Some(metrics) => {
            input.mapWithBcVariable(metrics) {
              (vector, metrics) => {
                val (broadcastMin, broadcastMax) = metrics
                scaleVector(vector, broadcastMin, broadcastMax, min, max)
              }
            }
          }

          case None =>
            throw new RuntimeException("The MinMaxScaler has not been fitted to the data. " +
              "This is necessary to estimate the minimum and maximum of the data.")
        }
      }
    }
  }

  implicit val transformLabeledVectors = {
    new TransformDataSetOperation[MinMaxScaler, LabeledVector, LabeledVector] {
      override def transformDataSet(instance: MinMaxScaler,
        transformParameters: ParameterMap,
        input: DataSet[LabeledVector]): DataSet[LabeledVector] = {
        val resultingParameters = instance.parameters ++ transformParameters
        val min = resultingParameters(Min)
        val max = resultingParameters(Max)

        instance.metricsOption match {
          case Some(metrics) => {
            input.mapWithBcVariable(metrics) {
              (labeledVector, metrics) => {
                val (broadcastMin, broadcastMax) = metrics
                val LabeledVector(label, vector) = labeledVector

                LabeledVector(label, scaleVector(vector, broadcastMin, broadcastMax, min, max))
              }
            }
          }

          case None =>
            throw new RuntimeException("The MinMaxScaler has not been fitted to the data. " +
              "This is necessary to estimate the minimum and maximum of the data.")
        }
      }
    }
  }

  /** Scales a vector such that it's features lie in the range [min, max]
    *
    * @param vector Vector to scale
    * @param broadcastMin Vector containing for each feature the minimal value in the training set
    * @param broadcastMax Vector containing for each feature the maximal value in the training set
    * @param min Minimal value of range
    * @param max Maximal value of range
    * @tparam T Type of [[Vector]]
    * @return Scaled feature vector
    */
  private def scaleVector[T <: Vector: BreezeVectorConverter](
      vector: T,
      broadcastMin: linalg.Vector[Double],
      broadcastMax: linalg.Vector[Double],
      min: Double,
      max: Double)
    : T = {
    var myVector = vector.asBreeze

    //handle the case where a feature takes only one value
    val rangePerFeature = (broadcastMax - broadcastMin)
    for (i <- 0 until rangePerFeature.size) {
      if (rangePerFeature(i) == 0.0) {
        rangePerFeature(i)= 1.0
      }
    }

    myVector -= broadcastMin
    myVector :/= rangePerFeature
    myVector = (myVector :* (max - min)) + min
    myVector.fromBreeze
  }
}
