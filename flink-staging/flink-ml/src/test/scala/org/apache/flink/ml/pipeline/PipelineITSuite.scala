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

package org.apache.flink.ml.pipeline

import org.apache.flink.api.scala._
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.ml.preprocessing.{PolynomialFeatures, StandardScaler}
import org.apache.flink.ml.regression.MultipleLinearRegression
import org.apache.flink.test.util.FlinkTestBase
import org.scalatest.{Matchers, FlatSpec}

class PipelineITSuite extends FlatSpec with Matchers with FlinkTestBase {
  behavior of "Flink's pipelines"

  it should "support chaining of compatible transformer" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val vData = List(DenseVector(1.0, 2.0, 3.0), DenseVector(2.0, 3.0, 4.0))
    val lvData = List(LabeledVector(1.0, DenseVector(1.0, 1.0, 1.0)),
      LabeledVector(2.0, DenseVector(2.0, 2.0, 2.0)))

    val vectorData = env.fromCollection(vData)
    val labeledVectorData = env.fromCollection(lvData)

    val expectedScaledVectorSet = Set(
      DenseVector(1.0, 1.0, 1.0, 1.0, 1.0, 1.0, -1.0, -1.0, -1.0),
      DenseVector(1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)
    )

    val expectedScaledLabeledVectorSet = Set(
      LabeledVector(1.0, DenseVector(1.0, 3.0, 5.0, 9.0, 15.0, 25.0, -1.0, -3.0, -5.0)),
      LabeledVector(2.0, DenseVector(1.0, -1.0, -3.0, 1.0, 3.0, 9.0, 1.0, -1.0, -3.0))
    )

    val scaler = StandardScaler()
    val polyFeatures = PolynomialFeatures().setDegree(2)

    val pipeline = scaler.chainTransformer(polyFeatures)

    pipeline.fit(vectorData)

    val scaledVectorDataDS = pipeline.transform(vectorData)
    val scaledLabeledVectorDataDS = pipeline.transform(labeledVectorData)

    val scaledVectorData = scaledVectorDataDS.collect()
    val scaledLabeledVectorData = scaledLabeledVectorDataDS.collect()

    scaledVectorData.size should be(expectedScaledVectorSet.size)

    for(scaledVector <- scaledVectorData){
      expectedScaledVectorSet should contain(scaledVector)
    }

    scaledLabeledVectorData.size should be(expectedScaledLabeledVectorSet.size)

    for(scaledLabeledVector <- scaledLabeledVectorData) {
      expectedScaledLabeledVectorSet should contain(scaledLabeledVector)
    }
  }

  it should "throw an exception when the pipeline operators are not compatible" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val scaler = StandardScaler()
    val mlr = MultipleLinearRegression()

    val vData = List(DenseVector(1.0, 2.0, 3.0), DenseVector(2.0, 3.0, 4.0))
    val vectorData = env.fromCollection(vData)

    val pipeline = scaler.chainPredictor(mlr)

    val exception = intercept[RuntimeException] {
      pipeline.fit(vectorData)
    }

    exception.getMessage should equal("There is no FitOperation defined for class org.apache." +
      "flink.ml.regression.MultipleLinearRegression which trains on a " +
      "DataSet[class org.apache.flink.ml.math.DenseVector]")
  }

  it should "throw an exception when the input data is not supported" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val dData = List(1.0, 2.0, 3.0)
    val doubleData = env.fromCollection(dData)

    val scaler = StandardScaler()
    val polyFeatures = PolynomialFeatures()

    val pipeline = scaler.chainTransformer(polyFeatures)

    val exception = intercept[RuntimeException] {
      pipeline.fit(doubleData)
    }

    exception.getMessage should equal("There is no FitOperation defined for class org.apache." +
      "flink.ml.preprocessing.StandardScaler which trains on a DataSet[double]")
  }

  it should "support multiple transformers and a predictor" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val data = List(LabeledVector(1.0, DenseVector(1.0, 2.0)),
      LabeledVector(2.0, DenseVector(2.0, 3.0)),
      LabeledVector(3.0, DenseVector(3.0, 4.0)))

    val trainingData = env.fromCollection(data)

    val chainedScalers2 = StandardScaler().chainTransformer(StandardScaler())
    val chainedScalers3 = chainedScalers2.chainTransformer(StandardScaler())
    val chainedScalers4 = chainedScalers3.chainTransformer(StandardScaler())
    val chainedScalers5 = chainedScalers4.chainTransformer(StandardScaler())

    val predictor = MultipleLinearRegression()


    val pipeline = chainedScalers5.chainPredictor(predictor)

    pipeline.fit(trainingData)

    val weightVector = predictor.weightsOption.get.collect().head

    weightVector._1.foreach{
      _ should be (0.367282 +- 0.01)
    }

    weightVector._2 should be (1.3131727 +- 0.01)
  }
}
