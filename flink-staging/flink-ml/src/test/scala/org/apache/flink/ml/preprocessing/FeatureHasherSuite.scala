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
package org.apache.flink.ml.preprocessing

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.ml.math.SparseVector
import org.apache.flink.test.util.FlinkTestBase
import org.scalatest.{Matchers, FlatSpec}

import org.apache.flink.api.scala._

class FeatureHasherSuite
  extends FlatSpec
  with Matchers
  with FlinkTestBase {

  behavior of "Flink's Feature Hasher"

  it should "transform a sequence of strings into a sparse vector of given size" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    env.setParallelism (2)

    val numFeatures = 16
    val input = Seq(
      Seq("TEST1", "TEST2", "TEST3"),
      Seq("TEST1", "TEST1", "TEST2"),
      Seq("TEST1"),
      "Two households both alike in dignity In fair Verona where we lay our scene".split(" ").toSeq
    )

    val expectedResults = List(
      SparseVector.fromCOO(numFeatures, Map((0, 1.0), (4, 1.0), (12, -1.0))),
      SparseVector.fromCOO(numFeatures, Map((4, 2.0), (12, -1.0))),
      SparseVector.fromCOO(numFeatures, Map((4, 1.0))),
      SparseVector.fromCOO(numFeatures, Map((0, 1.0), (2, -2.0), (5, 1.0), (10, 1.0), (12, -1.0), (13, -1.0),
        (14, -2.0), (15, 1.0)))
    )

    val inputDS = env.fromCollection(input)

    val transformer = FeatureHasher ()
      .setNumFeatures(numFeatures).setNonNegative(false)

    val transformedDS = transformer.transform (inputDS)

    val results = transformedDS.collect()

    for((result, expectedResult) <- results zip expectedResults) {
      result.equalsVector(expectedResult) should be(true)
    }
  }
}

