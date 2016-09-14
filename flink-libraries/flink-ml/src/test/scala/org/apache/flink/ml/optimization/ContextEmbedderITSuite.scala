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

import org.apache.flink.api.scala._
import org.apache.flink.ml.util.FlinkTestBase
import org.scalatest.{FlatSpec, Matchers}


class ContextEmbedderITSuite extends FlatSpec with Matchers with FlinkTestBase {
  behavior of "ContextEmbedder for generic context learning implementation"

  it should "form an initial vector set given a training corpus and modify" in {
    val context =
      Seq(Context(0, Seq(0,0,0,0)),
        Context(1, Seq(0,0,0,0)),
        Context(2, Seq(0,0,0,0)))

    val env = ExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)

    val dataSet = env.fromCollection(context)

    val initializedWeights = new ContextEmbedder[Int]
      .setBatchSize(context.size)
      .setTargetCount(0)
      .setVectorSize(100)
      .setIterations(100)
      .createInitialWeightsDS(None, dataSet)

    val initializedWeightsLocal = initializedWeights.collect().head

    initializedWeightsLocal.leafMap.size should be (context.map(_.target).distinct.size)
    initializedWeightsLocal.innerMap.size should be (initializedWeightsLocal.leafMap.size - 1)
    initializedWeightsLocal.leafVectors.size should be (initializedWeightsLocal.leafMap.size)
    initializedWeightsLocal.innerVectors.size should be (initializedWeightsLocal.innerMap.size)

    initializedWeightsLocal.leafVectors.head._2.length should be (100)

    val learnedWeights = new ContextEmbedder[Int]
      .setBatchSize(context.size)
      .setIterations(100)
      .optimize(dataSet, Option(initializedWeights)).collect().head

    learnedWeights.leafVectors.get(0).get should not be
      initializedWeightsLocal.leafVectors.get(0).get
  }
}
