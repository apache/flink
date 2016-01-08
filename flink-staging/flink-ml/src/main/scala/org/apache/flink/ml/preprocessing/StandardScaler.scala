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
import breeze.numerics.sqrt
import breeze.numerics.sqrt._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.ml._
import org.apache.flink.ml.common.{LabeledVector, Parameter, ParameterMap}
import org.apache.flink.ml.math.Breeze._
import org.apache.flink.ml.math.{BreezeVectorConverter, Vector}
import org.apache.flink.ml.pipeline.{TransformOperation, FitOperation, Transformer}
import org.apache.flink.ml.preprocessing.StandardScaler.{Mean, Std}

import scala.reflect.ClassTag

/** Scales observations, so that all features have a user-specified mean and standard deviation.
  * By default for [[StandardScaler]] transformer mean=0.0 and std=1.0.
  *
  * This transformer takes a subtype of  [[Vector]] of values and maps it to a
  * scaled subtype of [[Vector]] such that each feature has a user-specified mean and standard
  * deviation.
  *
  * This transformer can be prepended to all [[Transformer]] and
  * [[org.apache.flink.ml.pipeline.Predictor]] implementations which expect as input a subtype
  * of [[Vector]].
  *
  * @example
  *          {{{
  *            val trainingDS: DataSet[Vector] = env.fromCollection(data)
  *            val transformer = StandardScaler().setMean(10.0).setStd(2.0)
  *
  *            transformer.fit(trainingDS)
  *            val transformedDS = transformer.transform(trainingDS)
  *          }}}
  *
  * =Parameters=
  *
  * - [[Mean]]: The mean value of transformed data set; by default equal to 0
  * - [[Std]]: The standard deviation of the transformed data set; by default
  * equal to 1
  */
class StandardScaler extends Transformer[StandardScaler] {

  private[preprocessing] var metricsOption: Option[
      DataSet[(linalg.Vector[Double], linalg.Vector[Double])]
    ] = None

  /** Sets the target mean of the transformed data
    *
    * @param mu the user-specified mean value.
    * @return the StandardScaler instance with its mean value set to the user-specified value
    */
  def setMean(mu: Double): StandardScaler = {
    parameters.add(Mean, mu)
    this
  }

  /** Sets the target standard deviation of the transformed data
    *
    * @param std the user-specified std value. In case the user gives 0.0 value as input,
    *            the std is set to the default value: 1.0.
    * @return the StandardScaler instance with its std value set to the user-specified value
    */
  def setStd(std: Double): StandardScaler = {
    if (std == 0.0) {
      return this
    }
    parameters.add(Std, std)
    this
  }
}

object StandardScaler {

  // ====================================== Parameters =============================================

  case object Mean extends Parameter[Double] {
    override val defaultValue: Option[Double] = Some(0.0)
  }

  case object Std extends Parameter[Double] {
    override val defaultValue: Option[Double] = Some(1.0)
  }

  // ==================================== Factory methods ==========================================

  def apply(): StandardScaler = {
    new StandardScaler()
  }

  // ====================================== Operations =============================================

  /** Trains the [[org.apache.flink.ml.preprocessing.StandardScaler]] by learning the mean and
    * standard deviation of the training data. These values are used inthe transform step
    * to transform the given input data.
    *
    * @tparam T Input data type which is a subtype of [[Vector]]
    * @return
    */
  implicit def fitVectorStandardScaler[T <: Vector] = new FitOperation[StandardScaler, T] {
    override def fit(instance: StandardScaler, fitParameters: ParameterMap, input: DataSet[T])
      : Unit = {
      val metrics = extractFeatureMetrics(input)

      instance.metricsOption = Some(metrics)
    }
  }

  /** Trains the [[StandardScaler]] by learning the mean and standard deviation of the training
    * data which is of type [[LabeledVector]]. The mean and standard deviation are used to
    * transform the given input data.
    *
    */
  implicit val fitLabeledVectorStandardScaler = {
    new FitOperation[StandardScaler, LabeledVector] {
      override def fit(
          instance: StandardScaler,
          fitParameters: ParameterMap,
          input: DataSet[LabeledVector])
        : Unit = {
        val vectorDS = input.map(_.vector)
        val metrics = extractFeatureMetrics(vectorDS)

        instance.metricsOption = Some(metrics)
      }
    }
  }

  /** Calculates in one pass over the data the features' mean and standard deviation.
    * For the calculation of the Standard deviation with one pass over the data,
    * the Youngs & Cramer algorithm was used:
    * [[http://www.cs.yale.edu/publications/techreports/tr222.pdf]]
    *
    *
    * @param dataSet The data set for which we want to calculate mean and variance
    * @return  DataSet containing a single tuple of two vectors (meanVector, stdVector).
    *          The first vector represents the mean vector and the second is the standard
    *          deviation vector.
    */
  private def extractFeatureMetrics[T <: Vector](dataSet: DataSet[T])
  : DataSet[(linalg.Vector[Double], linalg.Vector[Double])] = {
    val metrics = dataSet.map{
      v => (1.0, v.asBreeze, linalg.Vector.zeros[Double](v.size))
    }.reduce{
      (metrics1, metrics2) => {
        /* We use formula 1.5b of the cited technical report for the combination of partial
           * sum of squares. According to 1.5b:
           * val temp1 : m/n(m+n)
           * val temp2 : n/m
           */
        val temp1 = metrics1._1 / (metrics2._1 * (metrics1._1 + metrics2._1))
        val temp2 = metrics2._1 / metrics1._1
        val tempVector = (metrics1._2 * temp2) - metrics2._2
        val tempS = (metrics1._3 + metrics2._3) + (tempVector :* tempVector) * temp1

        (metrics1._1 + metrics2._1, metrics1._2 + metrics2._2, tempS)
      }
    }.map{
      metric => {
        val varianceVector = sqrt(metric._3 / metric._1)

        for (i <- 0 until varianceVector.size) {
          if (varianceVector(i) == 0.0) {
            varianceVector.update(i, 1.0)
          }
        }
        (metric._2 / metric._1, varianceVector)
      }
    }
    metrics
  }

  /** [[TransformOperation]] which scales input data of subtype of [[Vector]] with respect to
    * the calculated mean and standard deviation of the training data. The mean and standard
    * deviation of the resulting data is configurable.
    *
    * @tparam T Type of the input and output data which has to be a subtype of [[Vector]]
    * @return
    */
  implicit def transformVectors[T <: Vector: BreezeVectorConverter: TypeInformation: ClassTag] = {
    new TransformOperation[StandardScaler, T, T] {
      override def transform(
        instance: StandardScaler,
        transformParameters: ParameterMap,
        input: DataSet[T])
      : DataSet[T] = {

        val resultingParameters = instance.parameters ++ transformParameters
        val mean = resultingParameters(Mean)
        val std = resultingParameters(Std)

        instance.metricsOption match {
          case Some(metrics) => {
            input.mapWithBcVariable(metrics){
              (vector, metrics) => {
                val (broadcastMean, broadcastStd) = metrics
                scaleVector(vector, broadcastMean, broadcastStd, mean, std)
              }
            }
          }

          case None =>
            throw new RuntimeException("The StandardScaler has not been fitted to the data. " +
              "This is necessary to estimate the mean and standard deviation of the data.")
        }
      }
    }
  }

  implicit val transformLabeledVectors = {
    new TransformOperation[StandardScaler, LabeledVector, LabeledVector] {
      override def transform(instance: StandardScaler, transformParameters: ParameterMap, input:
      DataSet[LabeledVector]): DataSet[LabeledVector] = {
        val resultingParameters = instance.parameters ++ transformParameters
        val mean = resultingParameters(Mean)
        val std = resultingParameters(Std)

        instance.metricsOption match {
          case Some(metrics) => {
            input.mapWithBcVariable(metrics){
              (labeledVector, metrics) => {
                val (broadcastMean, broadcastStd) = metrics
                val LabeledVector(label, vector) = labeledVector

                LabeledVector(label, scaleVector(vector, broadcastMean, broadcastStd, mean, std))
              }
            }
          }

          case None =>
            throw new RuntimeException("The StandardScaler has not been fitted to the data. " +
              "This is necessary to estimate the mean and standard deviation of the data.")
        }
      }
    }
  }

  /** Scales the given vector such that it has the given mean and std
    *
    * @param vector Vector to be scaled
    * @param dataMean Mean of the training data
    * @param dataStd Standard deviation of the training data
    * @param mean Mean of the scaled data
    * @param std Standard deviation of the scaled data
    * @tparam T Type of [[Vector]]
    * @return Scaled vector
    */
  private def scaleVector[T <: Vector: BreezeVectorConverter](
      vector: T,
      dataMean: linalg.Vector[Double],
      dataStd: linalg.Vector[Double],
      mean: Double,
      std: Double)
    : T = {
    var myVector = vector.asBreeze

    myVector -= dataMean
    myVector :/= dataStd
    myVector = (myVector :* std) + mean
    myVector.fromBreeze
  }
}
