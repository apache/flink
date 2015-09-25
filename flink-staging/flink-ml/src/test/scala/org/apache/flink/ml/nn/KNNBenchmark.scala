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

    ////////// Uniform case
    //// Generate trainingSet
    val r = scala.util.Random
    val rSeq = Seq.fill(10000)(DenseVector(r.nextGaussian, r.nextGaussian, r.nextGaussian, r.nextGaussian))

    /// GENERATE RANDOM SET OF POINTS IN [0,1]x[0,1]
    val trainingSet = env.fromCollection(rSeq)

    val rSeqTest = Seq.fill(10000)(DenseVector(r.nextGaussian, r.nextGaussian, r.nextGaussian, r.nextGaussian))
    val testingSet = env.fromCollection(rSeqTest)

  val t0 = System.nanoTime()
  //// ACTUAL CALL TO kNN
    ///FIRST SET UP PARAMETERS
    val knn = KNN()
      .setK(3)
      .setBlocks(5)
      .setDistanceMetric(SquaredEuclideanDistanceMetric())

    // ACTUAL kNN COMPUTATION
    // run knn join
    knn.fit(trainingSet)
    val result = knn.predict(testingSet).collect()

    val tf = System.nanoTime()
    println("MyElapsed time =       : " + (tf - t0)/1000000000 + "s")
}
