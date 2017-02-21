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
import org.apache.flink.ml.common.LabeledVector

import org.apache.flink.api.scala._

import scala.util.Random

object LogisticRegressionITSuite {
  /* This code is originally from the Apache Spark project. */
  def generateLogisticInput(
                             offset: Double,
                             scale: Double,
                             nPoints: Int,
                             seed: Int): Seq[LabeledVector] = {
    val rnd = new Random(seed)
    val x1 = Array.fill[Double](nPoints)(rnd.nextGaussian())

    val y = (0 until nPoints).map { i =>
      val p = 1.0 / (1.0 + math.exp(-(offset + scale * x1(i))))
      if (rnd.nextDouble() < p) 1.0 else 0.0
    }

    val testData = (0 until nPoints).map(i => LabeledVector(y(i), DenseVector(Array(x1(i)))))
    testData
  }
}

class LogisticRegressionITSuite extends FlatSpec with Matchers with FlinkTestBase {
  import LogisticRegression._

  behavior of "The Logistic Regression using GD implementation"

  it should "train a LR" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val lr = LogisticRegression().
      setIterations(100).
      setStepsize(10)

    val nPoints = 10000
    val A = 2.0
    val B = -1.5

    val trainingDS = env.fromCollection(
      LogisticRegressionITSuite.generateLogisticInput(A, B, nPoints, 42))

    val test = env.fromCollection(
      LogisticRegressionITSuite.generateLogisticInput(A, B, nPoints, 17))
      .map(x => (x.vector, x.label))

    lr.fit(trainingDS)

    val weightVector = lr.weightsOption.get.collect().head

    weightVector.weights(0) should be(B +- 0.05)
    weightVector.intercept should be(A +- 0.05)

    val predictionPairs = lr.evaluate(test)

    val absoluteErrorSum = predictionPairs.collect().map{
      case (truth, prediction) => Math.abs(truth - prediction)}.sum

    absoluteErrorSum / nPoints should be < 0.20
  }
}
