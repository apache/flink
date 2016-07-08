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

package org.apache.flink.ml.optimization

import org.apache.flink.ml.common.WeightVector
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.api.scala._
import org.apache.flink.ml.util.FlinkTestBase

import org.scalatest.{Matchers, FlatSpec}

class PredictionFunctionITSuite extends FlatSpec with Matchers with FlinkTestBase {

  behavior of "The optimization framework prediction functions"

  it should "correctly calculate linear predictions" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)

    val predFunction = LinearPrediction

    val weightVector = new WeightVector(DenseVector(-1.0, 1.0, 0.4, -0.4, 0.0), 1.0)
    val features = DenseVector(1.0, 1.0, 1.0, 1.0, 1.0)

    val prediction = predFunction.predict(features, weightVector)

    prediction should be (1.0 +- 0.001)
  }

  it should "correctly calculate the gradient for linear predictions" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)

    val predFunction = LinearPrediction

    val weightVector = new WeightVector(DenseVector(-1.0, 1.0, 0.4, -0.4, 0.0), 1.0)
    val features = DenseVector(1.0, 1.0, 1.0, 1.0, 1.0)

    val gradient = predFunction.gradient(features, weightVector)

    gradient shouldEqual WeightVector(DenseVector(1.0, 1.0, 1.0, 1.0, 1.0), 1.0)
  }

}
