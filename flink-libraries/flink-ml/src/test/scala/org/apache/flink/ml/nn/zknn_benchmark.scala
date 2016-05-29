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
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.ml.metrics.distances.SquaredEuclideanDistanceMetric
import org.scalatest.{FlatSpec, Matchers}

// used only to setup and time the KNN algorithm

class zknn_benchmark extends FlatSpec {

  val env = ExecutionEnvironment.getExecutionEnvironment

  // Generate trainingSet
  val r = scala.util.Random

  val  nFill = 80000
  /////////////////////////////////////////6d
  val rSeq6D = Seq.fill(nFill)(DenseVector(r.nextDouble + 1.0, r.nextDouble + 1.0,
    r.nextDouble + 1.0, r.nextDouble + 1.0,
    r.nextDouble + 1.0, r.nextDouble + 1.0))

  // GENERATE RANDOM SET OF POINTS IN [0,1]x[0,1]
  val trainingSet6D = env.fromCollection(rSeq6D)

  val rSeqTest6D = Seq.fill(nFill)(DenseVector(r.nextDouble + 1.0, r.nextDouble + 1.0,
    r.nextDouble + 1.0, r.nextDouble + 1.0,
    r.nextDouble + 1.0, r.nextDouble + 1.0))

  val testingSet6D = env.fromCollection(rSeqTest6D)

  //////////////////////////////////////////////
  val zknn1 = KNN()
    .setK(3)
    .setBlocks(4)

  // ACTUAL kNN COMPUTATION
  // run knn join
  zknn1.fit(trainingSet6D)
  val result_approx1 = zknn1.predict(testingSet6D).collect()
/*

  // exact
  var t0_exact = System.nanoTime()
  // ACTUAL CALL TO kNN
  // FIRST SET UP PARAMETERS
  val knn = KNN()
    .setK(3)
    .setBlocks(4)
    .setExact(true)
    .setDistanceMetric(SquaredEuclideanDistanceMetric())

  // ACTUAL kNN COMPUTATION
  // run knn join

  knn.fit(trainingSet6D)
  val result = knn.predict(testingSet6D).collect()

  var tf_exact = System.nanoTime()

*/
  // approx
  var t0_approx = System.nanoTime()
  // ACTUAL CALL TO kNN
  // FIRST SET UP PARAMETERS
  val zknn = KNN()
    .setK(3)
    .setBlocks(4)
    .setDistanceMetric(SquaredEuclideanDistanceMetric())

  // ACTUAL kNN COMPUTATION
  // run knn join
  zknn.fit(trainingSet6D)
  val result_approx = zknn.predict(testingSet6D).collect()

  var tf_approx = System.nanoTime()

  println("Elapsed time approx =       : " + (tf_approx - t0_approx)/1000000000 + "s")
  //println("Elapsed time exact =       : " + (tf_exact - t0_exact)/1000000000 + "s")
}
