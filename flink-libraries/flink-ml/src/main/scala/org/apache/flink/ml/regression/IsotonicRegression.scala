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

package org.apache.flink.ml.regression

import java.util.Arrays.binarySearch

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.scala._
import org.apache.flink.ml.common.{Parameter, ParameterMap}
import org.apache.flink.ml.pipeline.{FitOperation, PredictOperation, Predictor}

import scala.collection.mutable.ArrayBuffer

case class IsotonicRegressionModel(boundaries: Array[Double], predictions: Array[Double])

/**
  * Isotonic regression.
  * Currently implemented using parallelized pool adjacent violators algorithm.
  * Only univariate (single feature) algorithm supported.
  *
  * Sequential PAV implementation based on:
  * Tibshirani, Ryan J., Holger Hoefling, and Robert Tibshirani.
  * "Nearly-isotonic regression." Technometrics 53.1 (2011): 54-61.
  * Available from [[http://www.stat.cmu.edu/~ryantibs/papers/neariso.pdf]]
  *
  * Sequential PAV parallelization based on:
  * Kearsley, Anthony J., Richard A. Tapia, and Michael W. Trosset.
  * "An approach to parallelizing isotonic regression."
  * Applied Mathematics and Parallel Computing. Physica-Verlag HD, 1996. 141-147.
  * Available from [[http://softlib.rice.edu/pub/CRPC-TRs/reports/CRPC-TR96640.pdf]]
  *
  * @see [[http://en.wikipedia.org/wiki/Isotonic_regression Isotonic regression (Wikipedia)]]
  *
  *      This is a port from the implementation in Apache Spark.
  * @example
  * {{{
  *             val ir = IsotonicRegression()
  *               .setIsotonic(true)
  *
  *             val trainingDS: DataSet[(Double,Double,Double)] = ...
  *             val testingDS: DataSet[(Double)] = ...
  *
  *             mlr.fit(trainingDS)
  *
  *             val predictions = mlr.predict(testingDS)
  *          }}}
  *
  * =Parameters=
  *
  * - [[org.apache.flink.ml.regression.IsotonicRegression.Isotonic]]:
  * true if labels shall be ascending, false if labels shall be descending.
  *
  */
class IsotonicRegression extends Predictor[IsotonicRegression] {

  var isotonic = true

  var model: Option[DataSet[IsotonicRegressionModel]] = None

  def setIsotonic(isotonic: Boolean): this.type = {
    this.isotonic = isotonic
    this
  }

}

object IsotonicRegression {

  // ====================================== Parameters ===========================================

  case object Isotonic extends Parameter[Boolean] {
    val defaultValue = Some(true)
  }

  // ======================================== Factory methods ====================================

  def apply(): IsotonicRegression = {
    new IsotonicRegression()
  }

  // ====================================== Operations ===========================================

  class AdjacentPoolViolatersMapper extends MapFunction[Array[(Double, Double, Double)], Array[
    (Double, Double, Double)]] {

    /**
      * Performs a pool adjacent violators algorithm (PAV).
      *
      * @param input Input data of tuples (label, feature, weight).
      * @return Result tuples (label, feature, weight) where labels were updated
      *         to form a monotone sequence as per isotonic regression definition.
      */
    override def map(input: Array[(Double, Double, Double)]):
    Array[(Double, Double, Double)] = {
      if (input.isEmpty) {
        return Array.empty
      }

      var pooled = true
      while (pooled) {
        val n = input.length - 1
        var i = 0
        pooled = false
        while (i < n) {
          var k = i
          while (k < n && input(k)._1 >= input(k + 1)._1) {
            k += 1
          }
          if (input(i)._1 != input(k)._1) {
            var numerator = 0.0
            var denominator = 0.0
            for (j <- i until k + 1) {
              numerator += input(j)._1 * input(j)._3
              denominator += input(j)._3
            }
            if(denominator == 0) {
              denominator = 1
            }
            val ratio = numerator / denominator
            for (j <- i until k + 1) {
              input(j) = (ratio, input(j)._2, input(j)._3)
            }
            pooled = true
          }
          i = k + 1
        }
      }

      val compressed = ArrayBuffer.empty[(Double, Double, Double)]

      var (curLabel, curFeature, curWeight) = input.head
      var rightBound = curFeature

      var i = 1
      while (i < input.length) {
        val (label, feature, weight) = input(i)
        if (label == curLabel) {
          curWeight += weight
          rightBound = feature
        } else {
          compressed += ((curLabel, curFeature, curWeight))
          if (rightBound > curFeature) {
            compressed += ((curLabel, rightBound, 0.0))
          }
          curLabel = label
          curFeature = feature
          curWeight = weight
          rightBound = curFeature
        }
        i += 1
      }
      compressed += ((curLabel, curFeature, curWeight))
      if (rightBound > curFeature) {
        compressed += ((curLabel, rightBound, 0.0))
      }

      compressed.toArray
    }
  }


  implicit val fitIR = new FitOperation[IsotonicRegression, (Double, Double, Double)] {

    override def fit(instance: IsotonicRegression,
                     fitParameters: ParameterMap,
                     input: DataSet[(Double, Double, Double)]): Unit = {

      val isotonic = instance.isotonic

      val preprocessedInput = if (isotonic) {
        input
      } else {
        input.map(x => (-x._1, x._2, x._3))
      }

      val parallelStepResult = preprocessedInput
        .partitionByRange(1)
        .mapPartition(partition => {
          val buffer = new ArrayBuffer[(Double, Double, Double)]
          buffer ++= partition
          Seq(buffer.sortBy(x => (x._2, x._1)).toArray)
        })
        .map(new AdjacentPoolViolatersMapper)
        .mapPartition(partitions => {
          val buffer = new ArrayBuffer[(Double, Double, Double)]
          for (partition <- partitions) {
            buffer ++= partition
          }
          Seq(buffer.sortBy(x => (x._2, x._1)).toArray)
        })
        .setParallelism(1)
        .map(new AdjacentPoolViolatersMapper)
        .map(arr => {
          val boundaries = new ArrayBuffer[Double]
          val predictions = new ArrayBuffer[Double]
          for (x <- arr) {
            predictions += (if (isotonic) x._1 else -x._1)
            boundaries += x._2
          }
          IsotonicRegressionModel(boundaries.toArray, predictions.toArray)
        })

      instance.model = Some(parallelStepResult)
    }
  }

  implicit val predictIR =
    new PredictOperation[IsotonicRegression, IsotonicRegressionModel, Double, Double] {

      override def getModel(self: IsotonicRegression,
                            predictParameters: ParameterMap): DataSet[IsotonicRegressionModel] = {
        self.model match {
          case Some(model) => model
          case None => throw new RuntimeException("The IsotonicRegression has not " +
            "been fitted to the data. This is necessary to learn the model.")
        }
      }

      private def linearInterpolation(x1: Double, y1: Double,
                                      x2: Double, y2: Double, x: Double): Double = {
        y1 + (y2 - y1) * (x - x1) / (x2 - x1)
      }

      override def predict(value: Double, model: IsotonicRegressionModel): Double = {
        val boundaries = model.boundaries
        val predictions = model.predictions

        val foundIndex = binarySearch(boundaries, value)
        val insertIndex = -foundIndex - 1

        // Find if the index was lower than all values,
        // higher than all values, in between two values or exact match.
        if (insertIndex == 0) {
          predictions.head
        } else if (insertIndex == boundaries.length) {
          predictions.last
        } else if (foundIndex < 0) {
          linearInterpolation(
            boundaries(insertIndex - 1),
            predictions(insertIndex - 1),
            boundaries(insertIndex),
            predictions(insertIndex),
            value)
        } else {
          predictions(foundIndex)
        }
      }
    }

}

