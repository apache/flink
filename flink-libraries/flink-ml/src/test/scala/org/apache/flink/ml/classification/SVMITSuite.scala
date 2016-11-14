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

import org.apache.flink.ml.util.FlinkTestBase
import org.scalatest.{FlatSpec, Matchers}
import org.apache.flink.ml.math.DenseVector

import org.apache.flink.api.scala._

class SVMITSuite extends FlatSpec with Matchers with FlinkTestBase {

  behavior of "The SVM using CoCoA implementation"

  it should "train a SVM" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val svm = SVM().
    setBlocks(env.getParallelism).
    setIterations(100).
    setLocalIterations(100).
    setRegularization(0.002).
    setStepsize(0.1).
    setSeed(0)

    val trainingDS = env.fromCollection(Classification.trainingData)

    svm.fit(trainingDS)

    val weightVector = svm.weightsOption.get.collect().head

    weightVector.valueIterator.zip(Classification.expectedWeightVector.valueIterator).foreach {
      case (weight, expectedWeight) =>
        weight should be(expectedWeight +- 0.1)
    }
  }

  it should "make (mostly) correct predictions" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val svm = SVM().
      setBlocks(env.getParallelism).
      setIterations(100).
      setLocalIterations(100).
      setRegularization(0.002).
      setStepsize(0.1).
      setSeed(0)

    val trainingDS = env.fromCollection(Classification.trainingData)

    val test = trainingDS.map(x => (x.vector, x.label))

    svm.fit(trainingDS)

    val predictionPairs = svm.evaluate(test)

    val absoluteErrorSum = predictionPairs.collect().map{
      case (truth, prediction) => Math.abs(truth - prediction)}.sum

    absoluteErrorSum should be < 15.0
  }

  it should "be possible to get the raw decision function values" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val svm = SVM().
      setBlocks(env.getParallelism)
      .setOutputDecisionFunction(false)

    val customWeights = env.fromElements(DenseVector(1.0, 1.0, 1.0))

    svm.weightsOption = Option(customWeights)

    val test = env.fromElements(DenseVector(5.0, 5.0, 5.0))

    val thresholdedPrediction = svm.predict(test).map(vectorLabel => vectorLabel._2).collect().head

    thresholdedPrediction should be (1.0 +- 1e-9)

    svm.setOutputDecisionFunction(true)

    val rawPrediction = svm.predict(test).map(vectorLabel => vectorLabel._2).collect().head

    rawPrediction should be (15.0 +- 1e-9)


  }
}
