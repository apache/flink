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

package org.apache.flink.ml.feature

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.client.CliFrontendTestUtils
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.DenseVector
import org.junit.{BeforeClass, Test}
import org.scalatest.ShouldMatchers

import org.apache.flink.api.scala._

class PolynomialBaseITCase extends ShouldMatchers {

  @Test
  def testMapElementToPolynomialVectorSpace (): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    env.setParallelism (2)

    val input = Seq (
    LabeledVector (DenseVector (1), 1.0),
    LabeledVector (DenseVector (2), 2.0)
    )

    val inputDS = env.fromCollection (input)

    val transformer = PolynomialBase ()
    .setDegree (3)

    val transformedDS = transformer.transform (inputDS)

    val expectedMap = List (
    (1.0 -> DenseVector (1.0, 1.0, 1.0) ),
    (2.0 -> DenseVector (8.0, 4.0, 2.0) )
    ) toMap

    val result = transformedDS.collect

    for (entry <- result) {
    expectedMap.contains (entry.label) should be (true)
    entry.vector should equal (expectedMap (entry.label) )
    }
  }

  @Test
  def testMapVectorToPolynomialVectorSpace(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)

    val input = Seq(
      LabeledVector(DenseVector(2, 3), 1.0),
      LabeledVector(DenseVector(2, 3, 4), 2.0)
    )

    val expectedMap = List(
      (1.0 -> DenseVector(8.0, 12.0, 18.0, 27.0, 4.0, 6.0, 9.0, 2.0, 3.0)),
      (2.0 -> DenseVector(8.0, 12.0, 16.0, 18.0, 24.0, 32.0, 27.0, 36.0, 48.0, 64.0, 4.0, 6.0, 8.0,
        9.0, 12.0, 16.0, 2.0, 3.0, 4.0))
    ) toMap

    val inputDS = env.fromCollection(input)

    val transformer = PolynomialBase()
      .setDegree(3)

    val transformedDS = transformer.transform(inputDS)

    val result = transformedDS.collect

    for(entry <- result) {
      expectedMap.contains(entry.label) should be(true)
      entry.vector should equal(expectedMap(entry.label))
    }
  }

  @Test
  def testReturnEmptyVectorIfDegreeIsZero(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)

    val input = Seq(
      LabeledVector(DenseVector(2, 3), 1.0),
      LabeledVector(DenseVector(2, 3, 4), 2.0)
    )

    val inputDS = env.fromCollection(input)

    val transformer = PolynomialBase()
      .setDegree(0)

    val transformedDS = transformer.transform(inputDS)

    val result = transformedDS.collect

    val expectedMap = List(
      (1.0 -> DenseVector()),
      (2.0 -> DenseVector())
    ) toMap

    for(entry <- result) {
      expectedMap.contains(entry.label) should be(true)
      entry.vector should equal(expectedMap(entry.label))
    }
  }
}

object PolynomialBaseITCase {
  @BeforeClass
  def setup(): Unit = {
    CliFrontendTestUtils.pipeSystemOutToNull()
  }
}
