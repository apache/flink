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
package org.apache.flink.ml.evaluation

import org.apache.flink.api.scala._
import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.ml.preprocessing.StandardScaler
import org.apache.flink.ml.regression.MultipleLinearRegression
import org.apache.flink.ml.regression.RegressionData._
import org.apache.flink.test.util.FlinkTestBase
import org.scalatest.{FlatSpec, Matchers}

class ScorerITSuite  extends FlatSpec with Matchers with FlinkTestBase {

  behavior of "the Scorer class"

  def fixture =  new {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val loss = RegressionScores.squaredLoss

    val scorer = new Scorer(loss)

    val mlr = MultipleLinearRegression()

    val parameters = ParameterMap()

    parameters.add(MultipleLinearRegression.Stepsize, 1.0)
    parameters.add(MultipleLinearRegression.Iterations, 10)
    parameters.add(MultipleLinearRegression.ConvergenceThreshold, 0.001)

    val inputDS = env.fromCollection(data)
  }

  it should "work for squared loss" in {

    val f =  fixture

    f.mlr.fit(f.inputDS, f.parameters)

    val evaluationDS = f.inputDS.map(x => (x.vector, x.label))

    val mse = f.scorer.evaluate(evaluationDS, f.mlr).collect().head

    mse should be < 2.0
  }

  it should "be possible to obtain scores for a chained predictor" in {
    val f = fixture

    val scaler = StandardScaler()

    val chainedMLR = scaler.chainPredictor(f.mlr)

    chainedMLR.fit(f.inputDS, f.parameters)

    val evaluationDS = f.inputDS.map(x => (x.vector, x.label))

    val mse = f.scorer.evaluate(evaluationDS, chainedMLR).collect().head

    mse should be < 2.0
  }
}
