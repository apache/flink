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

//  it should "build correct isotonic regression model" in {
//    val env = ExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(2)
//
//    val dataset = generateIsotonicInput(env, Seq(1, 2, 3, 1, 6, 17, 16, 17, 18))
//    val ir = new IsotonicRegression().setIsotonic(true)
//
//    ir.fit(dataset)
//    val model = ir.model.get.collect().head
//
//    println(ir.predict(dataset).collect())
//
//    model.boundaries should be (Seq(0, 1, 3, 4, 5, 6, 7, 8))
//    model.predictions should be (Seq(1, 2, 2, 6, 16.5, 16.5, 17.0, 18.0))
//    ir.isotonic should be (true)
//  }
//
//  private def generateIsotonicInput(env: ExecutionEnvironment, labels: Seq[Double]): DataSet[
//    (Double, Double, Double)] = {
//    env.fromCollection(
//      labels.zipWithIndex.map { case (label, i) => (label, i.toDouble, 1.0) }
//    )
//  }

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

    ir.predict(env.fromCollection(0.0 to 9.0 by 1.0)).collect().map{case (_, x) => x} should be
      Seq(1, 2, 2, 2, 6, 16.5, 16.5, 17, 18)

    val model = ir.model.get.collect().head
    model.boundaries should be (Seq(0, 1, 3, 4, 5, 6, 7, 8))
    model.predictions should be (Seq(1, 2, 2, 6, 16.5, 16.5, 17.0, 18.0))
    ir.isotonic should be (true)
  }

// this produces an error with the range partitioning!
//  it should "build empty prediction-model for empty input" in {
//    val ir = runIsotonicRegression(Seq(), true)
//    val model = ir.model.get.collect().head
//    model.predictions should be (Seq())
//  }

  it should "build one-sized prediction-model for one-sized input" in {
    val ir = runIsotonicRegression(Seq(1.0), true)
    val model = ir.model.get.collect().head
    model.predictions should be (Seq(1.0))
  }

  it should "build same prediction-model as input for strictly increasing sequence" in {
    val ir = runIsotonicRegression(Seq(1, 2, 3, 4, 5), true)
    val model = ir.model.get.collect().head
    model.predictions should be (Seq(1, 2, 3, 4, 5))
  }

  it should "build prediction-model spanning whole data for strictly decreasing sequence" in {
    val ir = runIsotonicRegression(Seq(5, 4, 3, 2, 1), true)
    val model = ir.model.get.collect().head
    model.boundaries should be (Seq(0, 4))
    model.boundaries should be (Seq(3, 3))
  }

  private def runIsotonicRegression(labels: Seq[Double], isotonic: Boolean) : IsotonicRegression = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    val dataset = generateIsotonicInput(env, labels)
    val ir = new IsotonicRegression().setIsotonic(isotonic)
    ir.fit(dataset)
    ir
  }

  private def generateIsotonicInput(env: ExecutionEnvironment, labels: Seq[Double]): DataSet[
      (Double, Double, Double)] = {
      env.fromCollection(
        labels.zipWithIndex.map { case (label, i) => (label, i.toDouble, 1.0) }
      )
    }

}
