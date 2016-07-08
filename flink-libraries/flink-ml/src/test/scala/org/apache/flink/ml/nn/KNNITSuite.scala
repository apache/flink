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

package org.apache.flink.ml.nn

import org.apache.flink.api.common.operators.base.CrossOperatorBase.CrossHint
import org.apache.flink.api.scala._
import org.apache.flink.ml.classification.Classification
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.ml.metrics.distances.{ManhattanDistanceMetric,
SquaredEuclideanDistanceMetric}
import org.apache.flink.ml.util.FlinkTestBase
import org.scalatest.{FlatSpec, Matchers}

class KNNITSuite extends FlatSpec with Matchers with FlinkTestBase {
  behavior of "The KNN Join Implementation"

  it should "throw an exception when the given K is not valid" in {
    intercept[IllegalArgumentException] {
      KNN().setK(0)
    }
  }

  it should "throw an exception when the given count of blocks is not valid" in {
    intercept[IllegalArgumentException] {
      KNN().setBlocks(0)
    }
  }

  // calculate answer
  val answer = Classification.trainingData.map {
    v => (v.vector, SquaredEuclideanDistanceMetric().distance(DenseVector(0.0, 0.0), v.vector))
  }.sortBy(_._2).take(3).map(_._1).toArray

  it should "calculate kNN join correctly without using a Quadtree" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    // prepare data
    val trainingSet = env.fromCollection(Classification.trainingData).map(_.vector)
    val testingSet = env.fromElements(DenseVector(0.0, 0.0))

    val knn = KNN()
      .setK(3)
      .setBlocks(10)
      .setDistanceMetric(SquaredEuclideanDistanceMetric())
      .setUseQuadTree(false)
      .setSizeHint(CrossHint.SECOND_IS_SMALL)

    // run knn join
    knn.fit(trainingSet)
    val result = knn.predict(testingSet).collect()

    result.size should be(1)
    result.head._1 should be(DenseVector(0.0, 0.0))
    result.head._2 should be(answer)
  }

  it should "calculate kNN join correctly with a Quadtree" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    // prepare data
    val trainingSet = env.fromCollection(Classification.trainingData).map(_.vector)
    val testingSet = env.fromElements(DenseVector(0.0, 0.0))

    val knn = KNN()
      .setK(3)
      .setBlocks(2) // blocks set to 2 to make sure initial quadtree box is partitioned
      .setDistanceMetric(SquaredEuclideanDistanceMetric())
      .setUseQuadTree(true)
      .setSizeHint(CrossHint.SECOND_IS_SMALL)

    // run knn join
    knn.fit(trainingSet)
    val result = knn.predict(testingSet).collect()

    result.size should be(1)
    result.head._1 should be(DenseVector(0.0, 0.0))
    result.head._2 should be(answer)
  }

  it should "throw an exception when using a Quadtree with an incompatible metric" in {
    intercept[IllegalArgumentException] {
      val env = ExecutionEnvironment.getExecutionEnvironment

      // prepare data
      val trainingSet = env.fromCollection(Classification.trainingData).map(_.vector)
      val testingSet = env.fromElements(DenseVector(0.0, 0.0))

      val knn = KNN()
        .setK(3)
        .setBlocks(10)
        .setDistanceMetric(ManhattanDistanceMetric())
        .setUseQuadTree(true)

      // run knn join
      knn.fit(trainingSet)
      knn.predict(testingSet).collect()
    }
  }

}

