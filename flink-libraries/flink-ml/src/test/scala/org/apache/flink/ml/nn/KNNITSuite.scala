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
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.ml.metrics.distances.{ManhattanDistanceMetric,
SquaredEuclideanDistanceMetric}
import org.apache.flink.test.util.FlinkTestBase
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

  it should "throw an exception setExact is true and setUseLSH is true" in {
    intercept[IllegalArgumentException] {
      KNN().setK(3)
        .setUseLSH(true)
        .setExact(true)
    }

    intercept[IllegalArgumentException] {
      KNN().setK(3)
        .setExact(true)
        .setUseLSH(true)
    }
  }

  it should "throw an exception setExact is true and setUseZvalues is true" in {
    intercept[IllegalArgumentException] {
      KNN().setK(3)
        .setUseZValues(true)
        .setExact(true)
    }

    intercept[IllegalArgumentException] {
      KNN().setK(3)
        .setExact(true)
        .setUseZValues(true)
    }
  }

  it should "throw an exception setExact is false and setUseQuadtree is true" in {
    intercept[IllegalArgumentException] {
      KNN().setK(3)
        .setUseQuadTree(true)
        .setExact(false)
    }

   }

  val env = ExecutionEnvironment.getExecutionEnvironment

  // prepare data
  val trainingSet = env.fromCollection(Classification.trainingData).map(_.vector)
  val testingSet = env.fromElements(DenseVector(0.0, 0.0))

  // for approximate methods add a single point very close to (0,0)
  val trainingSetApprox = trainingSet.union(
    env.fromElements(
      DenseVector(0.000001, 0.000001),
      DenseVector(0.000002, 0.000002),
      DenseVector(0.000003, 0.000003)))

  // add another point for approx (hack, now only can have one point)
  val testingSetApprox = env.fromElements(DenseVector(0.0, 0.0))

  // calculate answer
  val answer = Classification.trainingData.map {
    v => (v.vector, SquaredEuclideanDistanceMetric().distance(DenseVector(0.0, 0.0), v.vector))
  }.sortBy(_._2).take(3).map(_._1).toArray

  // answer for exact search
  val answerForApproxBenchmark = Classification.trainingData.union(Seq(
    LabeledVector(1.0000, DenseVector(0.000001, 0.000001)),
    LabeledVector(1.0000, DenseVector(0.000002, 0.000002)),
    LabeledVector(1.0000, DenseVector(0.000003, 0.000003))))
    .map {
      v => (v.vector, SquaredEuclideanDistanceMetric().distance(DenseVector(0.0, 0.0), v.vector))
    }.sortBy(_._2).take(3).map(_._1).toArray

  it should "calculate exact kNN join correctly without using a Quadtree" in {

    val knn = KNN()
      .setK(3)
      .setBlocks(10)
      .setDistanceMetric(SquaredEuclideanDistanceMetric())
      .setExact(true)
      .setUseQuadTree(false)
      .setSizeHint(CrossHint.SECOND_IS_SMALL)

    // run knn join
    knn.fit(trainingSet)
    val result = knn.predict(testingSet).collect()

    result.head._2.foreach(x => println(x))
    result.size should be(1)
    result.head._1 should be(DenseVector(0.0, 0.0))
    result.head._2 should be(answer)

  }

  it should "calculate exact kNN join correctly with a Quadtree" in {

    val knn = KNN()
      .setK(3)
      .setBlocks(2) // blocks set to 2 to make sure initial quadtree box is partitioned
      .setDistanceMetric(SquaredEuclideanDistanceMetric())
      .setUseQuadTree(true)
      .setExact(true)
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
      val knn = KNN()
        .setK(3)
        .setBlocks(10)
        .setDistanceMetric(ManhattanDistanceMetric())
        .setExact(true)
        .setUseQuadTree(true)

      // run knn join
      knn.fit(trainingSet)
      val result = knn.predict(testingSet).collect()

    }
  }

  it should "calculate approximate kNN join correctly using z-values when 3 training" +
    " points are much closer to a given test point" in {

    val knn = KNN()
      .setK(3)
      .setBlocks(1)
      .setDistanceMetric(SquaredEuclideanDistanceMetric())
      .setSizeHint(CrossHint.SECOND_IS_SMALL)
      .setExact(false)
      .setUseQuadTree(false)

    // run knn join
    knn.fit(trainingSetApprox)
    val result = knn.predict(testingSetApprox).collect()

    result.size should be(1)
    result.head._1 should be(DenseVector(0.0, 0.0))
    result.head._2 should be(answerForApproxBenchmark)
  }

  it should "calculate approximate kNN join correctly using lsh when 3 training" +
    " points are much closer to a given test point" in {

    val knn = KNN()
      .setK(3)
      .setBlocks(1)
      .setDistanceMetric(SquaredEuclideanDistanceMetric())
      .setSizeHint(CrossHint.SECOND_IS_SMALL)
      .setExact(false)
      .setUseQuadTree(false)
      .setUseLSH(true)

    // run knn join
    knn.fit(trainingSetApprox)
    val result = knn.predict(testingSetApprox).collect()

    result.size should be(1)
    result.head._1 should be(DenseVector(0.0, 0.0))
    result.head._2 should be(answerForApproxBenchmark)
  }

}
