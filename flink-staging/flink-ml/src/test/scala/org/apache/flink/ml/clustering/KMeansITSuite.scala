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

class KMeansITSuite extends FlatSpec with Matchers with FlinkTestBase {

  behavior of "The KMeans implementation"

  def fixture = new {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val kmeans = KMeans().
      setInitialCentroids(env.fromCollection(Clustering.centroidData)).
      setNumIterations(Clustering.iterations)

    val trainingDS = env.fromCollection(Clustering.trainingData)

    kmeans.fit(trainingDS)
  }

  it should "data points are clustered into 'K' cluster centers" in {
    val f = fixture

    val centroidsResult = f.kmeans.centroids.get.collect()

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

  it should "predict points to cluster centers" in {
    val f = fixture

    val vectorsWithExpectedLabels = Clustering.testData
    // create a lookup table for better matching
    val expectedMap = vectorsWithExpectedLabels map (v =>
      v.vector.asInstanceOf[DenseVector] -> v.label
      ) toMap

    // calculate the vector to cluster mapping on the plain vectors
    val plainVectors = vectorsWithExpectedLabels.map(v => v.vector)
    val predictedVectors = f.kmeans.predict(f.env.fromCollection(plainVectors))

    // check if all vectors were labeled correctly
    predictedVectors.collect() foreach (result => {
      val expectedLabel = expectedMap.get(result.vector.asInstanceOf[DenseVector]).get
      result.label should be(expectedLabel)
    })

  }

}
