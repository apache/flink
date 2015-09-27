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

import org.apache.flink.api.scala._
import org.apache.flink.ml.classification.Classification
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.ml.metrics.distances.SquaredEuclideanDistanceMetric
import org.apache.flink.test.util.FlinkTestBase
import org.scalatest.{FlatSpec, Matchers}

// used only to setup and time the KNN algorithm

class KNNBenchmark extends FlatSpec {

    val env = ExecutionEnvironment.getExecutionEnvironment

    //// Generate trainingSet
    val r = scala.util.Random
    val nFill = 20000

  val rSeq = Seq.fill(nFill)(DenseVector(r.nextGaussian, r.nextGaussian))
  /// GENERATE RANDOM SET OF POINTS IN [0,1]x[0,1]
  val trainingSet = env.fromCollection(rSeq)

  val rSeqTest = Seq.fill(nFill)(DenseVector(r.nextGaussian, r.nextGaussian))
  val testingSet = env.fromCollection(rSeqTest)


    val rSeq2D = Seq.fill(nFill)(DenseVector(r.nextGaussian, r.nextGaussian))
    /// GENERATE RANDOM SET OF POINTS IN [0,1]x[0,1]
    val trainingSet2D = env.fromCollection(rSeq2D)

    val rSeqTest2D = Seq.fill(nFill)(DenseVector(r.nextGaussian, r.nextGaussian))
    val testingSet2D = env.fromCollection(rSeqTest2D)

  val rSeq3D = Seq.fill(nFill)(DenseVector(r.nextGaussian, r.nextGaussian, r.nextGaussian))

  /// GENERATE RANDOM SET OF POINTS IN [0,1]x[0,1]
  val trainingSet3D = env.fromCollection(rSeq3D)

  val rSeqTest3D = Seq.fill(nFill)(DenseVector(r.nextGaussian, r.nextGaussian, r.nextGaussian))
  val testingSet3D = env.fromCollection(rSeqTest3D)


  val rSeq4D = Seq.fill(nFill)(DenseVector(r.nextGaussian, r.nextGaussian, r.nextGaussian, r.nextGaussian))

  /// GENERATE RANDOM SET OF POINTS IN [0,1]x[0,1]
  val trainingSet4D = env.fromCollection(rSeq4D)

  val rSeqTest4D = Seq.fill(nFill)(DenseVector(r.nextGaussian, r.nextGaussian, r.nextGaussian, r.nextGaussian))
  val testingSet4D = env.fromCollection(rSeqTest4D)

  /////////////////////////////////////////2d

  var t0 = System.nanoTime()
  //// ACTUAL CALL TO kNN
  ///FIRST SET UP PARAMETERS
  val knn = KNN()
    .setK(3)
    .setBlocks(4)
    .setDistanceMetric(SquaredEuclideanDistanceMetric())

  // ACTUAL kNN COMPUTATION
  // run knn join
  knn.fit(trainingSet)
  val result = knn.predict(testingSet).collect()

  var tf = System.nanoTime()


  /////////////////////////////////////////2d

  var t02D = System.nanoTime()
  //// ACTUAL CALL TO kNN
  ///FIRST SET UP PARAMETERS
  val knn2D = KNN()
    .setK(3)
    .setBlocks(4)
    .setDistanceMetric(SquaredEuclideanDistanceMetric())

  // ACTUAL kNN COMPUTATION
  // run knn join
  knn2D.fit(trainingSet2D)
  val result2D = knn2D.predict(testingSet2D).collect()

  var tf2D = System.nanoTime()



  //////////////////////////////////////////3d
  var t03D = System.nanoTime()
  //// ACTUAL CALL TO kNN
  ///FIRST SET UP PARAMETERS
  val knn3D = KNN()
    .setK(3)
    .setBlocks(4)
    .setDistanceMetric(SquaredEuclideanDistanceMetric())

  // ACTUAL kNN COMPUTATION
  // run knn join
  knn3D.fit(trainingSet3D)
  val result3D = knn3D.predict(testingSet3D).collect()

  var tf3D = System.nanoTime()



  //////////////////////////////////////////4d
  var t04D = System.nanoTime()
  //// ACTUAL CALL TO kNN
  ///FIRST SET UP PARAMETERS
  val knn4D = KNN()
    .setK(3)
    .setBlocks(4)
    .setDistanceMetric(SquaredEuclideanDistanceMetric())

  // ACTUAL kNN COMPUTATION
  // run knn join
  knn4D.fit(trainingSet4D)
  val result4D = knn4D.predict(testingSet4D).collect()

  var tf4D = System.nanoTime()

  println("Elapsed time first 2D =       : " + (tf - t0)/1000000000 + "s")
  println("Elapsed time 2D =       : " + (tf2D - t02D)/1000000000 + "s")
  println("Elapsed time 3D =       : " + (tf3D - t03D)/1000000000 + "s")
  println("Elapsed time 4D =       : " + (tf4D - t04D)/1000000000 + "s")

  println("")
}
