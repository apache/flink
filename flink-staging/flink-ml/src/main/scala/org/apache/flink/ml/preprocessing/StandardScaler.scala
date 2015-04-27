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
import org.apache.flink.api.common.functions._
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.ml.common.{Parameter, ParameterMap, Transformer}
import org.apache.flink.ml.math.Breeze._
import org.apache.flink.ml.math.Vector
import org.apache.flink.ml.preprocessing.StandardScaler.{Mean, Std}

/** Scales observations, so that all features have a user-specified mean and standard deviation.
  * By default for [[StandardScaler]] transformer mean=0.0 and std=1.0.
  *
  * This transformer takes a [[Vector]] of values and maps it to a
  * scaled [[Vector]] such that each feature has a user-specified mean and standard deviation.
  *
  * This transformer can be prepended to all [[Transformer]] and
  * [[org.apache.flink.ml.common.Learner]] implementations which expect an input of
  * [[Vector]].
  *
  * @example
  *          {{{
  *            val trainingDS: DataSet[Vector] = env.fromCollection(data)
  *            val transformer = StandardScaler().setMean(10.0).setStd(2.0)
  *
  *            transformer.transform(trainingDS)
  *          }}}
  *
  * =Parameters=
  *
  * - [[StandardScaler.Mean]]: The mean value of transformed data set; by default equal to 0
  * - [[StandardScaler.Std]]: The standard deviation of the transformed data set; by default
  * equal to 1
  */
class StandardScaler extends Transformer[Vector, Vector] with Serializable {

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

  override def transform(input: DataSet[Vector], parameters: ParameterMap):
  DataSet[Vector] = {
    val resultingParameters = this.parameters ++ parameters
    val mean = resultingParameters(Mean)
    val std = resultingParameters(Std)

    val featureMetrics = extractFeatureMetrics(input)

    input.map(new RichMapFunction[Vector, Vector]() {

      var broadcastMean: linalg.Vector[Double] = null
      var broadcastStd: linalg.Vector[Double] = null

      override def open(parameters: Configuration): Unit = {
        val broadcastedMetrics = getRuntimeContext().getBroadcastVariable[(linalg.Vector[Double],
          linalg.Vector[Double])]("broadcastedMetrics").get(0)
        broadcastMean = broadcastedMetrics._1
        broadcastStd = broadcastedMetrics._2
      }

      override def map(vector: Vector): Vector = {
        var myVector = vector.asBreeze

        myVector -= broadcastMean
        myVector :/= broadcastStd
        myVector = (myVector :* std) + mean
        return myVector.fromBreeze
      }
    }).withBroadcastSet(featureMetrics, "broadcastedMetrics")
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
  private def extractFeatureMetrics(dataSet: DataSet[Vector])
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
}

object StandardScaler {

  case object Mean extends Parameter[Double] {
    override val defaultValue: Option[Double] = Some(0.0)
  }

  case object Std extends Parameter[Double] {
    override val defaultValue: Option[Double] = Some(1.0)
  }

  def apply(): StandardScaler = {
    new StandardScaler()
  }
}
