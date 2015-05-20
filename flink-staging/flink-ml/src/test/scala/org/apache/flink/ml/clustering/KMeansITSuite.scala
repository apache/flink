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

package org.apache.flink.ml.clustering

import org.apache.flink.api.scala._
import org.apache.flink.ml.math
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.test.util.FlinkTestBase
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable

class KMeansITSuite extends FlatSpec with Matchers with FlinkTestBase {

  behavior of "The KMeans implementation"

  it should "data points are clustered into 'K' cluster centers" in {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val learner = KMeans().
      setInitialCentroids(env.fromCollection(Clustering.centroidData)).
      setNumIterations(Clustering.iterations)

    val trainingDS = env.fromCollection(Clustering.trainingData)

    val model = learner.fit(trainingDS)
    val centroidsResult = model.centroids.collect()

    val centroidsExpected = Clustering.expectedCentroids

    // the sizes must match
    centroidsResult.length should be === centroidsExpected.length

    // create a lookup table for better matching
    val expectedMap = centroidsExpected map (e => e.label->e.vector.asInstanceOf[DenseVector]) toMap

    // each of the results must be in lookup table
    centroidsResult foreach(result => {
      val expectedVector = expectedMap.get(result.label).get

      // the type must match (not None)
      expectedVector shouldBe a [math.DenseVector]

      val expectedData = expectedVector.asInstanceOf[DenseVector].data
      val resultData = result.vector.asInstanceOf[DenseVector].data

      // match the individual values of the vector
      expectedData zip resultData foreach {
        case (expectedVector, entryVector) =>
          entryVector should be(expectedVector +- 0.00001)
      }
    })

  }

}
