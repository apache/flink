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

package org.apache.flink.ml.metrics.distances

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import org.apache.flink.ml.math.DenseVector
import org.scalatest.{FlatSpec, Matchers}

class DistanceMetricSuite extends FlatSpec with Matchers {
  val EPSILON = 1e-8

  behavior of "Distance Measures"

  it should "calculate Euclidean distance correctly" in {
    val vec1 = DenseVector(1, 9)
    val vec2 = DenseVector(5, 6)

    EuclideanDistanceMetric().distance(vec1, vec2) should be(5)
  }

  it should "calculate square value of Euclidean distance correctly" in {
    val vec1 = DenseVector(1, 9)
    val vec2 = DenseVector(5, 6)

    SquaredEuclideanDistanceMetric().distance(vec1, vec2) should be(25)
  }

  it should "calculate Chebyshev distance correctly" in {
    val vec1 = DenseVector(0, 3, 6)
    val vec2 = DenseVector(0, 0, 0)

    ChebyshevDistanceMetric().distance(vec1, vec2) should be(6)
  }

  it should "calculate Cosine distance correctly" in {
    val vec1 = DenseVector(1, 0)
    val vec2 = DenseVector(2, 2)

    CosineDistanceMetric().distance(vec1, vec2) should be((1 - math.sqrt(2) / 2) +- EPSILON)
  }

  it should "calculate Manhattan distance correctly" in {
    val vec1 = DenseVector(0, 0, 0, 1, 1, 1)
    val vec2 = DenseVector(1, 1, 1, 0, 0, 0)

    ManhattanDistanceMetric().distance(vec1, vec2) should be(6)
  }

  it should "calculate Minkowski distance correctly" in {
    val vec1 = DenseVector(0, 0, 1, 1, 0)
    val vec2 = DenseVector(1, 1, 0, 1, 2)

    MinkowskiDistanceMetric(3).distance(vec1, vec2) should be(math.pow(11, 1.0 / 3) +- EPSILON)
  }

  it should "calculate Tanimoto distance correctly" in {
    val vec1 = DenseVector(0, 1, 1)
    val vec2 = DenseVector(1, 1, 0)

    TanimotoDistanceMetric().distance(vec1, vec2) should be(1 - (1.0 / (2 + 2 - 1)) +- EPSILON)
  }

  it should "be serialized" in {
    val metric = EuclideanDistanceMetric()
    val byteOutput = new ByteArrayOutputStream()
    val output = new ObjectOutputStream(byteOutput)

    output.writeObject(metric)
    output.close()

    val byteInput = new ByteArrayInputStream(byteOutput.toByteArray)
    val input = new ObjectInputStream(byteInput)

    val restoredMetric = input.readObject().asInstanceOf[DistanceMetric]

    restoredMetric should be(an[EuclideanDistanceMetric])
  }
}
