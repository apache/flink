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

import org.apache.flink.ml.math.{Vector, DenseVector}
import org.apache.flink.test.util.FlinkTestBase
import org.scalatest.{Matchers, FlatSpec}


class FeatureHasherSuite
  extends FlatSpec
  with Matchers
  with FlinkTestBase {

  behavior of "Flink's Feature Hasher"

  it should "map "

}

object FeatureHasherData {
  val data: Seq[Vector] = List(
    DenseVector(Array("TEST1"))
  )
}
