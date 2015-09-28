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

class KNNBenchmark_2Guass extends FlatSpec {

    val env = ExecutionEnvironment.getExecutionEnvironment

    //// Generate trainingSet
    val r = scala.util.Random
    val nFill = 20000

  val rSeq01 = Seq.fill(nFill)(DenseVector(r.nextGaussian, r.nextGaussian))
  val rSeq02 = Seq.fill(nFill)(DenseVector(r.nextGaussian + 5.0, r.nextGaussian + 2.0))

  val rSeq1 = rSeq01 ++ rSeq02

  /// GENERATE RANDOM SET OF POINTS IN [0,1]x[0,1]
  val trainingSet = env.fromCollection(rSeq1)

  var rSeqTest = Seq.fill(2*nFill)(DenseVector(r.nextGaussian, r.nextGaussian))
  val testingSet = env.fromCollection(rSeqTest)

///////////////////////////////////////////////////
  val rSeq12 = Seq.fill(nFill)(DenseVector(r.nextGaussian, r.nextGaussian))
  val rSeq22 = Seq.fill(nFill)(DenseVector(r.nextGaussian + 5.0, r.nextGaussian + 2.0))

  val rSeq2 = rSeq12 ++ rSeq22

  /// GENERATE RANDOM SET OF POINTS IN [0,1]x[0,1]
  val trainingSet2 = env.fromCollection(rSeq2)

  var rSeqTest2 = Seq.fill(2*nFill)(DenseVector(r.nextGaussian, r.nextGaussian))
  val testingSet2 = env.fromCollection(rSeqTest2)


  ////////////////////////
  val rSeq13= Seq.fill(nFill)(DenseVector(r.nextGaussian, r.nextGaussian))
  val rSeq23 = Seq.fill(nFill)(DenseVector(0.5*r.nextGaussian + 50.0, 0.5*r.nextGaussian + 20.0))

  val rSeq3 = rSeq13 ++ rSeq23

  /// GENERATE RANDOM SET OF POINTS IN [0,1]x[0,1]
  val trainingSet3 = env.fromCollection(rSeq3)

  var rSeqTest3 = Seq.fill(2*nFill)(DenseVector(1*r.nextGaussian, 1*r.nextGaussian))
  val testingSet3 = env.fromCollection(rSeqTest3)

/////////////////////////////////////////
 val rSeq14= Seq.fill(nFill)(DenseVector(r.nextGaussian, r.nextGaussian))
  val rSeq24 = Seq.fill(nFill)(DenseVector(0.5*r.nextGaussian + 500.0, 0.5*r.nextGaussian + 200.0))

  val rSeq4 = rSeq14 ++ rSeq24

  /// GENERATE RANDOM SET OF POINTS IN [0,1]x[0,1]
  val trainingSet4 = env.fromCollection(rSeq4)

  var rSeqTest4 = Seq.fill(2*nFill)(DenseVector(2*r.nextGaussian, 2*r.nextGaussian))
  val testingSet4 = env.fromCollection(rSeqTest4)

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
  knn2D.fit(trainingSet2)
  val result2D = knn2D.predict(testingSet2).collect()

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
  knn3D.fit(trainingSet3)
  val result3D = knn3D.predict(testingSet3).collect()

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
  knn4D.fit(trainingSet4)
  val result4D = knn4D.predict(testingSet4).collect()

  var tf4D = System.nanoTime()

  println("Elapsed time first 2D =       : " + (tf - t0)/1000000000 + "s")
  println("Elapsed time 2D =       : " + (tf2D - t02D)/1000000000 + "s")
  println("Elapsed time 3D =       : " + (tf3D - t03D)/1000000000 + "s")
  println("Elapsed time 4D =       : " + (tf4D - t04D)/1000000000 + "s")

  println("")
}
