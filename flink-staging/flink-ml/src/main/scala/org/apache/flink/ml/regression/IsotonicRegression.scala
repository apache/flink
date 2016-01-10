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
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._
import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.ml.pipeline.{FitOperation, PredictOperation, Predictor}

import scala.collection.mutable.ArrayBuffer
;

case class IsotonicRegressionModel(boundaries: Array[Double], predictions: Array[Double])

/**
 * ported from org.apache.spark.mllib.regression.IsotonicRegression
 *
 * https://github.com/apache/spark/blob/3e7e05f5ee763925ed60410d7de04cf36b723de1
 *      /mllib/src/main/scala/org/apache/spark/mllib/regression/IsotonicRegression.scala
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

    println("v11")

    class AdjacentPoolViolatersMapper extends MapFunction[Array[(Double, Double, Double)], Array[
        (Double, Double, Double)]] {

        /**
         * Performs a pool adjacent violators algorithm (PAV).
         * Uses approach with single processing of data where violators
         * in previously processed data created by pooling are fixed immediately.
         * Uses optimization of discovering monotonicity violating sequences (blocks).
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

            // Pools sub array within given bounds assigning weighted average value to all
            // elements.
            def pool(input: Array[(Double, Double, Double)], start: Int, end: Int): Unit = {
                val poolSubArray = input.slice(start, end + 1)

                val weightedSum = poolSubArray.map(lp => lp._1 * lp._3).sum
                val weight = poolSubArray.map(_._3).sum

                var i = start
                while (i <= end) {
                    input(i) = (weightedSum / weight, input(i)._2, input(i)._3)
                    i = i + 1
                }
            }

            var i = 0
            val len = input.length
            while (i < len) {
                var j = i

                // Find monotonicity violating sequence, if any.
                while (j < len - 1 && input(j)._1 > input(j + 1)._1) {
                    j = j + 1
                }

                // If monotonicity was not violated, move to next data point.
                if (i == j) {
                    i = i + 1
                } else {
                    // Otherwise pool the violating sequence
                    // and check if pooling caused monotonicity violation in previously processed
                    // points.
                    while (i >= 0 && input(i)._1 > input(i + 1)._1) {
                        pool(input, i, j)
                        i = i - 1
                    }

                    i = j
                }
            }
            // For points having the same prediction, we only keep two boundary points.
            val compressed = ArrayBuffer.empty[(Double, Double, Double)]

            var (curLabel, curFeature, curWeight) = input.head
            var rightBound = curFeature
            def merge(): Unit = {
                compressed += ((curLabel, curFeature, curWeight))
                if (rightBound > curFeature) {
                    compressed += ((curLabel, rightBound, 0.0))
                }
            }
            i = 1
            while (i < input.length) {
                val (label, feature, weight) = input(i)
                if (label == curLabel) {
                    curWeight += weight
                    rightBound = feature
                } else {
                    merge()
                    curLabel = label
                    curFeature = feature
                    curWeight = weight
                    rightBound = curFeature
                }
                i += 1
            }
            merge()

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
                .setParallelism(1)
                .mapPartition(partitions => {
                    val buffer = new ArrayBuffer[(Double, Double, Double)]
                    for (partition <- partitions) {
                        buffer ++= partition
                    }
                    Seq(buffer.sortBy(x => (x._2, x._1)).toArray)
                })
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

            override def getModel(self: IsotonicRegression, predictParameters: ParameterMap):
            DataSet[IsotonicRegressionModel] =
                self.model match {
                    case Some(model) => model
                    case None => throw new RuntimeException("The IsotonicRegression has not " +
                        "been " +
                        "fitted to the data. This is necessary to learn the model.")
                }

            override def predict(value: Double, model: IsotonicRegressionModel): Double = {

                def linearInterpolation(x1: Double, y1: Double, x2: Double, y2: Double, x:
                Double):
                Double = {
                    y1 + (y2 - y1) * (x - x1) / (x2 - x1)
                }

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

