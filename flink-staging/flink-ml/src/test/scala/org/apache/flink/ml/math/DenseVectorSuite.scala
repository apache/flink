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

package org.apache.flink.ml.math

import org.scalatest.{Matchers, FlatSpec}

class DenseVectorSuite extends FlatSpec with Matchers {

  behavior of "Flink's DenseVector"

  it should "contain the initialization data" in {
    val data = Array.range(1,10)

    val vector = DenseVector(data)

    assertResult(data.length)(vector.size)

    data.zip(vector.map(_._2)).foreach{case (expected, actual) => assertResult(expected)(actual)}
  }

  it should "fail in case of an illegal element access" in {
    val size = 10

    val vector = DenseVector.zeros(size)

    intercept[IllegalArgumentException] {
      vector(-1)
    }

    intercept[IllegalArgumentException] {
      vector(size)
    }
  }
}
