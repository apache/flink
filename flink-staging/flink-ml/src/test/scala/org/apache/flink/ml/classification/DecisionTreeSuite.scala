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

import org.apache.flink.api.scala._
import org.apache.flink.test.util.FlinkTestBase
import org.scalatest.{FlatSpec, Matchers}

class DecisionTreeSuite extends FlatSpec with Matchers with FlinkTestBase {

  behavior of "The Decision Tree implementation"

  it should "train a decision tree" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val learner = DecisionTree().setMaxBins(5).setDepth(20).setDimension(4).setClasses(3)

    val trainingDS = env.fromCollection(Classification.IrisTrainingData).setParallelism(4)

    learner.fit(trainingDS)
    println("Accuracy:" +
      learner.testAccuracy(
        env.fromCollection(Classification.IrisTestingData).setParallelism(4),
        learner.treeOption.get))
  }
}