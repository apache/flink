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

import java.lang.Iterable
import breeze.linalg
import breeze.linalg.DenseVector
import breeze.numerics.sqrt
import breeze.numerics.sqrt._
import org.apache.flink.api.common.functions._
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.ml.common.{Parameter, ParameterMap, Transformer}
import org.apache.flink.ml.math.Breeze._
import org.apache.flink.ml.math.Vector
import org.apache.flink.ml.preprocessing.StandardScaler.{Mean, Std}
import org.apache.flink.util.Collector

/** Scales observations, so that all features have a user-specified mean and standard deviation.
  * By default for StandardScaler transformer mean=0.0 and std=1.0.
  *
  * This transformer takes a a Vector of values and maps it into the
  * scaled Vector that each feature has a user-specified mean and standard deviation.
  *
  * This transformer can be prepended to all [[Transformer]] and
  * [[org.apache.flink.ml.common.Learner]] implementations which expect an input of
  * [[Vector]].
  *
  * @example
  * {{{
  *                                          val trainingDS: DataSet[Vector] = env.fromCollection
  *                                          (data)
  *
  *                                          val transformer = StandardScaler().setMean(10.0).setStd
  *                                          (2.0)
  *
  *                                          transformer.transform(trainingDS)
  * }}}
  *
  * =Parameters=
  *
  * - [[StandardScaler.Mean]]: The mean value of transformed data set; by default equal to 0
  * - [[StandardScaler.Std]]: The standard deviation of the transformed data set; by default
  * equal to 1
  */
class StandardScaler extends Transformer[Vector, Vector] with Serializable {

  def setMean(mu: Double): StandardScaler = {
    parameters.add(Mean, mu)
    this
  }

  def setStd(std: Double): StandardScaler = {
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

        myVector :-= broadcastMean
        myVector :/= broadcastStd
        myVector = (myVector :* std) :+ mean
        return myVector.fromBreeze
      }
    }).withBroadcastSet(featureMetrics, "broadcastedMetrics")
  }

  /** Calculates in one pass over the data the features' mean and standard deviation.
    * For the calculation of the Standard deviation with one pass over the data,
    * the Youngs & Cramer algorithm was used:
    * http://www.cs.yale.edu/publications/techreports/tr222.pdf
    *
    *
    * @param dataSet The data set for which we want to calculate mean and variance
    * @return  DataSet containing a single tuple of two vectors (meanVector, stdVector).
    *          The first vector represents the mean vector and the second is the standard
    *          deviation vector.
    */
  private def extractFeatureMetrics(dataSet: DataSet[Vector]):
  DataSet[(linalg.Vector[Double], linalg.Vector[Double])] = {

    val metrics = dataSet.combineGroup(new GroupCombineFunction[Vector,
      (Double, linalg.Vector[Double], linalg.Vector[Double])] {

      override def combine(vector: Iterable[Vector],
        out: Collector[(Double, linalg.Vector[Double], linalg.Vector[Double])]): Unit = {

        var counter = 0.0
        var T: linalg.Vector[Double] = null
        var S: linalg.Vector[Double] = null
        val iter = vector.iterator()

        while (iter.hasNext) {
          counter += 1.0
          val tempVector = iter.next().asBreeze
          if (counter == 1.0) {
            T = tempVector
            S = DenseVector.zeros[Double](tempVector.size)
          }
          else {
            T :+= tempVector
            val temp = tempVector :* counter :- T
            S :+= (temp :* temp) :* (1 / (counter * (counter - 1.0)))
          }
        }
        out.collect(counter, T, S)
      }
    }).reduce(new CalculateMetrics())
      .map(new MapFunction[(Double, linalg.Vector[Double], linalg.Vector[Double]),
      (linalg.Vector[Double], linalg.Vector[Double])] {
      override def map(metrics: (Double, linalg.Vector[Double], linalg.Vector[Double])):
      (linalg.Vector[Double], linalg.Vector[Double]) = {
        val varianceVector = sqrt(metrics._3 :/ metrics._1)
        return (metrics._2 :/ metrics._1, varianceVector)
      }
    })
    metrics
  }

  /** CalculateMetrics extends ReduceFunction.
    * This reduce function takes as input all the calculated sub-results as a
    * tuple of three elements (numberOfElements,featuresMeanSubVector,featuresStdSubVector>
    *
    * @return A data set containing a single tuple of three elements:
    *         (totalNumberOfElements,featuresMeanVector,featuresStdVector)
    */
  final class CalculateMetrics extends ReduceFunction[(Double, linalg.Vector[Double],
    linalg.Vector[Double])] {
    override def reduce(metrics1: (Double, linalg.Vector[Double], linalg.Vector[Double]),
      metrics2: (Double, linalg.Vector[Double], linalg.Vector[Double])):
    (Double, linalg.Vector[Double], linalg.Vector[Double]) = {
      val temp1 = metrics1._1 / (metrics2._1 * (metrics1._1 + metrics2._1))
      val temp2 = (metrics1._1 + metrics2._1) / metrics1._1
      val tempVector = (metrics1._2 :* temp2) - metrics2._2

      val tempS = (metrics1._3 + metrics2._3) + (tempVector :* tempVector) :* temp1
      return (metrics1._1 + metrics2._1, metrics1._2 + metrics2._2, tempS)
    }
  }

}

object StandardScaler {

  case object Mean extends Parameter[Double] {
    override val defaultValue: Option[Double] = Some(0.0)
  }

  case object Std extends Parameter[Double] {
    override val defaultValue: Option[Double] = Some(1.0)
  }

}
