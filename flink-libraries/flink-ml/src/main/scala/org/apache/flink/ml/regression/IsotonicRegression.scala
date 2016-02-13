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
  * PAV implementation based on:
  * - Isotonic Regression implementation in the scikit-learn Python module for machine learning:
  *   [[https://github.com/scikit-learn/scikit-learn/blob/c9572494a82b364529374aafca15660a7366e2c4/sklearn/_isotonic.pyx]]
  * - Isotonic Regression implementation in Apache Spark:
  *   [[https://github.com/apache/spark/blob/master/mllib/src/main/scala/org/apache/spark/mllib/regression/IsotonicRegression.scala]]
  *
  * @see [[http://en.wikipedia.org/wiki/Isotonic_regression Isotonic regression (Wikipedia)]]
  *
  * @example {{{
  *   val ir = IsotonicRegression().setIsotonic(true)
  *
  *   val trainingDS: DataSet[(Double,Double,Double)] = ...
  *   val testingDS: DataSet[(Double)] = ...
  *
  *   mlr.fit(trainingDS)
  *
  *   val predictions = mlr.predict(testingDS)
  * }}}
  *
  * =Parameters=
  *
  * - [[org.apache.flink.ml.regression.IsotonicRegression.Isotonic]]:
  * true if labels shall be ascending, false if labels shall be descending.
  *
  */
class IsotonicRegression extends Predictor[IsotonicRegression] {

  // Indicates whether labels shall be ascending or descending.
  var isotonic = true

  // The IR model that will be created from training data and that will be used for predictiions.
  var model: Option[DataSet[IsotonicRegressionModel]] = None

	/** @param isotonic True if the labels shall be ascending, false if they shall be descending
    * @return This IsotonicRegression instance
    */
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

	/**
    * Map function that can be applied to a data partition and that runs the PAV algorithm on that data.
    */
  class AdjacentPoolViolatersMapper extends MapFunction[Array[(Double, Double, Double)], Array[
    (Double, Double, Double)]] {

    /**
      * Performs the PAV algorithm on the provided data.
      *
      * @param input List of input data in the form of tuples (label, feature, weight).
      * @return List of resulting tuples (label, feature, weight) where labels were updated
      *         to form a monotone sequence as per isotonic regression definition.
      */
    override def map(input: Array[(Double, Double, Double)]):
    Array[(Double, Double, Double)] = {
      if (input.isEmpty) {
        return Array.empty
      }

      var pooled = true // Indicates whether any data points were pooled in the current PAV iteration.
      while (pooled) {
        val n = input.length - 1
        var i = 0
        pooled = false

        // Iterate over all data points.
        while (i < n) {
          // Check if the data points following the current one form a monotonically decreasing sequence. After the
          // while loop, k will contain the index of the last data point of that monotonically decreasing sequence. If
          // the points following the current one don't form such a sequence, k will still point to the current data
          // point.
          var k = i
          while (k < n && input(k)._1 >= input(k + 1)._1) {
            k += 1
          }

          // If the Y values of the current data point and the data point at index k differ, we know that all data
          // points between the current one and the one at index k form a monotonically decreasing sequence, in which
          // case we want to combine them into a single pool.
          if (input(i)._1 != input(k)._1) {
            val dataPointCount = k - i
            var weightedSum = 0.0
            var sumOfWeights = 0.0

            for (j <- i until k + 1) {
              weightedSum += input(j)._1 * input(j)._3
              sumOfWeights += input(j)._3
            }

            // If the sum of weights is 0, we can't use that for calculating the weighted average. But since we know
            // that all data points we want to pool have a weight of 0, we can just pretend they all have a weight of 1
            // by using the number of data points as the sum of weights.
            if (sumOfWeights == 0) {
              sumOfWeights = dataPointCount
            }

            // Calculate the weighted average and pool all data points between i and k by setting their Y values to that
            // weighted average.
            val weightedAverage = weightedSum / sumOfWeights
            for (j <- i until k + 1) {
              input(j) = (weightedAverage, input(j)._2, input(j)._3)
            }

            pooled = true
          }

          // Continue with the next data point after the current pool or simply with the next data point if the current
          // one is not part of a pool (in which case k still equals i).
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
          // If the next Y value is equal to the current Y value, add its weight to the current weight and move the
          // current right boundary to the right, effectively removing the next data point by merging its information
          // into the current one.
          curWeight += weight
          rightBound = feature
        } else {
          // If the next data point's Y value differs from the current Y value, add the current data point to the
          // result. If the current point actually compresses multiple data points (if it contains information of more
          // than one data point), we have to add another tuple so we don't lose the information on where the right
          // boundary is.
          compressed += ((curLabel, curFeature, curWeight))
          if (rightBound > curFeature) {
            compressed += ((curLabel, rightBound, 0.0))
          }

          // Continue with the next data point.
          curLabel = label
          curFeature = feature
          curWeight = weight
          rightBound = curFeature
        }
        i += 1
      }

      // Add the final tuple(s) to the result, as the above loop continued early.
      compressed += ((curLabel, curFeature, curWeight))
      if (rightBound > curFeature) {
        compressed += ((curLabel, rightBound, 0.0))
      }

      // Return the compressed result as an array.
      compressed.toArray
    }
  }

  implicit val fitIR = new FitOperation[IsotonicRegression, (Double, Double, Double)] {

    override def fit(instance: IsotonicRegression,
                     fitParameters: ParameterMap,
                     input: DataSet[(Double, Double, Double)]): Unit = {

      val isotonic = instance.isotonic

      // In the antitonic case, simply invert all Y values to make the data isotonic.
      val preprocessedInput = if (isotonic) {
        input
      } else {
        input.map(x => (-x._1, x._2, x._3))
      }

      // First, run the PAV algorithm in parallel on separate partitions. Then, collect the combined results of that
      // parallel step on a single node (parallelism = 1) and run the algorithm again on that combined data. Finally,
      // create a new IR model from the resulting boundaries and predictions.
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

			/**
        * Returns an instance's IR model, if available, or throw a [[RuntimeException]] if this instance wasn't given
        * any training data yet that it could have created a model on.
        * @param self The instance whose model should be returned
        * @param predictParameters The parameters for the prediction
        * @return A DataSet with the model representation as its only element
        */
      override def getModel(self: IsotonicRegression, predictParameters: ParameterMap):
      DataSet[IsotonicRegressionModel] = {
        self.model match {
          case Some(model) => model
          case None => throw new RuntimeException("The IsotonicRegression has not " +
            "been fitted to the data. This is necessary to learn the model.")
        }
      }

			/**
        * Linear interpolation that will be used for predicting the label of a feature that lies between two features
        * that actually exist in the model.
				*/
      private def linearInterpolation(x1: Double, y1: Double,
                                      x2: Double, y2: Double, x: Double): Double = {
        y1 + (y2 - y1) * (x - x1) / (x2 - x1)
      }

			/**
        * Predict the label for a given feature value.
        * @param value The unlabeled example on which we make the prediction
        * @param model The model representation of the prediciton algorithm
        * @return A label for the provided example of type [[Prediction]]
        */
      override def predict(value: Double, model: IsotonicRegressionModel): Double = {
        val boundaries = model.boundaries
        val predictions = model.predictions

        val foundIndex = binarySearch(boundaries, value)
        val insertIndex = -foundIndex - 1

        if (insertIndex == 0) {
          // If provided feature is lower than the model's first data point, simply return the label of that first model
          // data point.
          predictions.head
        } else if (insertIndex == boundaries.length) {
          // If provided feature is larger than the model's last data point, simply return the label of that last model
          // data point.
          predictions.last
        } else if (foundIndex < 0) {
          // If provided feature lies between two features that exist in the model, return a linear interpolation of
          // these data points as the prediction.
          linearInterpolation(
            boundaries(insertIndex - 1),
            predictions(insertIndex - 1),
            boundaries(insertIndex),
            predictions(insertIndex),
            value)
        } else {
          // If provided feature matches a feature that exists in the model exactly, return that feature's label as the
          // prediction.
          predictions(foundIndex)
        }
      }
    }

}

