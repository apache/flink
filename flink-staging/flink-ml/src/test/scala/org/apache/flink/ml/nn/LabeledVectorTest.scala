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

//import scala.util.Random
//import org.apache.flink.api.scala.DataSetUtils._

class LabeledVectorTest extends FlatSpec with Matchers with FlinkTestBase {
  behavior of "The KNN Join Implementation"


  /*
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
*/


  it should "calculate kNN join correctly" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    // prepare data

    // FROM http://massapi.com/method/org/apache/flink/api/java/ExecutionEnvironment.fromCollection-8.html
    // env.fromCollection "Creates a DataSet from the given non-empty collection"

    //val trainingSet = env.fromCollection(Classification.trainingData).map(_.vector)

    val trainingSetLV = env.fromCollection(Classification.trainingData)


    ///// TEST TO FILTER DATA:  NEEDED WHEN DETERMINING ALL POINTS BELONGING TO A GIVEN BOX
    //println("LV.filter(label==2.0) ")

    val tlv2 = trainingSetLV.filter{ _.label==2.0}
    tlv2.print

    //trainingSet.print

    //// Generate alternate trainingSet

    val r = scala.util.Random
    val rSeq = Seq.fill(10000)(DenseVector(r.nextGaussian, r.nextGaussian, r.nextGaussian))

    /// GENERATE RANDOM SET OF POINTS IN [0,1]x[0,1]
    // val trainingSet= env.fromCollection(Seq.fill(100)(DenseVector(r.nextFloat, r.nextFloat)))
     val trainingSet= env.fromCollection(rSeq)

    //val rSeqTest = Seq.fill(10000)(DenseVector(r.nextFloat, r.nextFloat))
    //val testingSet = env.fromCollection(rSeqTest)

    val testingSet = env.fromElements(DenseVector(0.0, 0.0, 0.0)) /// single point

    //// BELOW SEEMS TO RUN kNN FOR A SINGLE POINT TO COMPARE WITH OUTPUT OF knn
    // calculate answer
   /*
    val answer = Classification.trainingData.map {
      v => (v.vector, SquaredEuclideanDistanceMetric().distance(DenseVector(0.0, 0.0), v.vector))
    }.sortBy(_._2).take(3).map(_._1).toArray
   */

    val answer = rSeq.map {
      v => (v, SquaredEuclideanDistanceMetric().distance(DenseVector(0.0, 0.0, 0.0), v))
    }.sortBy(_._2).take(3).map(_._1).toArray

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

    //// TEST AGAINST `val answer = ...` STEP
    result.size should be(1)
    result.head._1 should be(DenseVector(0.0, 0.0, 0.0))
    result.head._2 should be(answer)

  }
}
