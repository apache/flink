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

package org.apache.flink.ml.regression

import org.apache.flink.api.scala._
import org.apache.flink.test.util.FlinkTestBase
import org.scalatest.{FlatSpec, Matchers}

class IsotonicRegressionITSuite extends FlatSpec with Matchers with FlinkTestBase {

  behavior of "The isotonic regression implementation"

  it should "correctly build model and predict data with increasing isotonic regression model" in {
    /*
     The following result could be re-produced with sklearn.
     > from sklearn.isotonic import IsotonicRegression
     > x = range(9)
     > y = [1, 2, 3, 1, 6, 17, 16, 17, 18]
     > ir = IsotonicRegression(x, y)
     > print ir.predict(x)
     array([  1. ,   2. ,   2. ,   2. ,   6. ,  16.5,  16.5,  17. ,  18. ])
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    val dataset = generateIsotonicInput(env, Seq(1, 2, 3, 1, 6, 17, 16, 17, 18))
    val ir = new IsotonicRegression().setIsotonic(true)
    ir.fit(dataset)

    ir.predict(env.fromCollection(0.0 to 9.0 by 1.0)).collect().map { case (_, x) => x } should be
    Seq(1, 2, 2, 2, 6, 16.5, 16.5, 17, 18)

    val model = ir.model.get.collect().head
    model.boundaries should be(Seq(0, 1, 3, 4, 5, 6, 7, 8))
    model.predictions should be(Seq(1, 2, 2, 6, 16.5, 16.5, 17.0, 18.0))
    ir.isotonic should be(true)
  }

  it should "build empty prediction-model for empty input" in {
    val ir = runIsotonicRegression(Seq(), true)
    val model = ir.model.get.collect().head
    model.predictions should be (Seq())
  }

  it should "build one-sized prediction-model for one-sized input" in {
    val ir = runIsotonicRegression(Seq(1.0), isotonic = true)
    val model = ir.model.get.collect().head
    model.predictions should be(Seq(1.0))
  }

  it should "build same prediction-model as input for strictly increasing sequence" in {
    val ir = runIsotonicRegression(Seq(1, 2, 3, 4, 5), isotonic = true)
    val model = ir.model.get.collect().head
    model.predictions should be(Seq(1, 2, 3, 4, 5))
  }

  it should "build prediction-model spanning whole data for strictly decreasing sequence" in {
    val ir = runIsotonicRegression(Seq(5, 4, 3, 2, 1), isotonic = true)
    val model = ir.model.get.collect().head
    model.boundaries should be(Seq(0, 4))
    model.predictions should be(Seq(3, 3))
  }

  it should "pool data correctly when the last element violates monotonicity" in {
    val ir = runIsotonicRegression(Seq(1, 2, 3, 4, 2), isotonic = true)
    val model = ir.model.get.collect().head
    model.boundaries should be(Seq(0, 1, 2, 4))
    model.predictions should be(Seq(1, 2, 3, 3))
  }

  it should "pool data correctly when the first element violates monotonicity" in {
    val ir = runIsotonicRegression(Seq(4, 2, 3, 4, 5), isotonic = true)
    val model = ir.model.get.collect().head
    model.boundaries should be(Seq(0, 2, 3, 4))
    model.predictions should be(Seq(3, 3, 4, 5))
  }

  it should "work properly with negative labels" in {
    val ir = runIsotonicRegression(Seq(-1, -2, 0, 1, -1), isotonic = true)
    val model = ir.model.get.collect().head
    model.boundaries should be(Seq(0, 1, 2, 4))
    model.predictions should be(Seq(-1.5, -1.5, 0, 0))
  }

  it should "work properly with unordered input" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    val trainDS = generateIsotonicInput(env, Seq(1, 2, 3, 4, 5), reverse = true)
    val ir = new IsotonicRegression().setIsotonic(true)
    ir.fit(trainDS)

    val model = ir.model.get.collect().head
    model.predictions should be(Seq(1, 2, 3, 4, 5))
  }

  it should "work properly with weighted input" in {
    val ir = runIsotonicRegression(Seq(1, 2, 3, 4, 2), Seq(1, 1, 1, 1, 2), isotonic = true)
    val model = ir.model.get.collect().head
    model.boundaries should be(Seq(0, 1, 2, 4))
    model.predictions should be(Seq(1, 2, 2.75, 2.75))
  }

  it should "work properly with weights lower than 1" in {
    val ir = runIsotonicRegression(Seq(1, 2, 3, 2, 1), Seq(1, 1, 1, 0.1, 0.1), isotonic = true)
    val model = ir.model.get.collect().head
    model.boundaries should be(Seq(0, 1, 2, 4))
    model.predictions should be(Seq(1, 2, 3.3 / 1.2, 3.3 / 1.2))
  }

  it should "work properly with negative weights" in {
    val ir = runIsotonicRegression(Seq(1, 2, 3, 2, 1), Seq(-1, 1, -3, 1, -5), isotonic = true)
    val model = ir.model.get.collect().head
    model.boundaries should be(Seq(0.0, 1.0, 4.0))
    model.predictions should be(Seq(1.0, 10.0 / 6, 10.0 / 6))
  }

  it should "work properly with zero weights" in {
    val ir = runIsotonicRegression(Seq(1, 2, 3, 2, 1), Seq(0, 0, 0, 1, 0), isotonic = true)
    val model = ir.model.get.collect().head
    model.boundaries should be(Seq(0.0, 1.0, 4.0))
    model.predictions should be(Seq(1, 2, 2))
  }

  it should "predict labels correctly" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ir = runIsotonicRegression(Seq(1, 2, 7, 1, 2), isotonic = true)

    ir.predict(env.fromElements(-2d)).collect().head._2 should be(1)
    ir.predict(env.fromElements(-1d)).collect().head._2 should be(1)
    ir.predict(env.fromElements(0.5)).collect().head._2 should be(1.5)
    ir.predict(env.fromElements(0.75)).collect().head._2 should be(1.75)
    ir.predict(env.fromElements(1d)).collect().head._2 should be(2)
    ir.predict(env.fromElements(2d)).collect().head._2 should be(10d / 3)
    ir.predict(env.fromElements(9d)).collect().head._2 should be(10d / 3)
  }

  it should "predict labels correctly with duplicate features" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    val trainDS = env.fromCollection(
      Seq[(Double, Double, Double)]((2, 1, 1), (1, 1, 1), (4, 2, 1), (2, 2, 1), (6, 3, 1), (5, 3,
        1)))
    val ir = new IsotonicRegression().setIsotonic(true)
    ir.fit(trainDS)

    ir.predict(env.fromElements(0d)).collect().head._2 should be(1)
    ir.predict(env.fromElements(1.5)).collect().head._2 should be(2)
    ir.predict(env.fromElements(2.5)).collect().head._2 should be(4.5)
    ir.predict(env.fromElements(4d)).collect().head._2 should be(6)
  }

  it should "predict labels correctly (antitonic)" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ir = runIsotonicRegression(Seq(7, 5, 3, 5, 1), isotonic = false)
    ir.predict(env.fromElements(-2d)).collect().head._2 should be(7)
    ir.predict(env.fromElements(-1d)).collect().head._2 should be(7)
    ir.predict(env.fromElements(0.5)).collect().head._2 should be(6)
    ir.predict(env.fromElements(0.75)).collect().head._2 should be(5.5)
    ir.predict(env.fromElements(1d)).collect().head._2 should be(5)
    ir.predict(env.fromElements(2d)).collect().head._2 should be(4)
    ir.predict(env.fromElements(9d)).collect().head._2 should be(1)
  }

  it should "predict labels correctly with duplicate features (antitonic)" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    val trainDS = env.fromCollection(
      Seq[(Double, Double, Double)]((5, 1, 1), (6, 1, 1), (2, 2, 1), (4, 2, 1), (1, 3, 1), (2, 3,
        1)))
    val ir = new IsotonicRegression().setIsotonic(false)
    ir.fit(trainDS)

    ir.predict(env.fromElements(0d)).collect().head._2 should be(6)
    ir.predict(env.fromElements(1.5)).collect().head._2 should be(4.5)
    ir.predict(env.fromElements(2.5)).collect().head._2 should be(2)
    ir.predict(env.fromElements(4d)).collect().head._2 should be(1)
  }

  private def runIsotonicRegression(labels: Seq[Double], weights: Seq[Double], isotonic: Boolean)
  : IsotonicRegression = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    val dataset = generateIsotonicInput(env, labels, weights, reverse = false)
    val ir = new IsotonicRegression().setIsotonic(isotonic)
    ir.fit(dataset)
    ir
  }

  private def runIsotonicRegression(labels: Seq[Double], isotonic: Boolean): IsotonicRegression = {
    runIsotonicRegression(labels, Array.fill(labels.size)(1d), isotonic)
  }

  private def generateIsotonicInput(env: ExecutionEnvironment, labels: Seq[Double], weights:
  Seq[Double], reverse: Boolean): DataSet[
    (Double, Double, Double)] = {
    val tuples = labels.zipWithIndex.map { case (label, i) => (label, i.toDouble, weights(i)) }
    env.fromCollection(if (reverse) {
      tuples.reverse
    } else {
      tuples
    })
  }

  private def generateIsotonicInput(env: ExecutionEnvironment, labels: Seq[Double], reverse:
  Boolean = false): DataSet[
    (Double, Double, Double)] = {
    generateIsotonicInput(env, labels, Array.fill(labels.size)(1d), reverse)
  }
}
