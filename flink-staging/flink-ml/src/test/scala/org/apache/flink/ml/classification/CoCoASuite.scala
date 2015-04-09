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

package org.apache.flink.ml.classification

import org.scalatest.{FlatSpec, Matchers}

import org.apache.flink.api.scala._
import org.apache.flink.test.util.FlinkTestBase

class CoCoAITSuite extends FlatSpec with Matchers with FlinkTestBase {

  behavior of "The CoCoA implementation"

  it should "train a SVM" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val learner = CoCoA().
    setBlocks(env.getParallelism).
    setIterations(100).
    setLocalIterations(100).
    setRegularization(0.002).
    setStepsize(0.1).
    setSeed(0)

    val trainingDS = env.fromCollection(Classification.trainingData)

    val model = learner.fit(trainingDS)

    val weightVector = model.weights.collect().apply(0)

    weightVector.valuesIterator.zip(Classification.expectedWeightVector.valueIterator).foreach {
      case (weight, expectedWeight) =>
        weight should be(expectedWeight +- 0.1)
    }
  }
}
