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
import org.apache.flink.ml.classification.SVM
import org.apache.flink.ml.common.{ParameterMap, LabeledVector}
import org.apache.flink.ml.math._
import org.apache.flink.ml.preprocessing.{PolynomialFeatures, StandardScaler}
import org.apache.flink.ml.regression.MultipleLinearRegression
import org.apache.flink.ml.util.FlinkTestBase
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
    val labeledVectors = List(LabeledVector(1.0, DenseVector(1.0, 2.0)),
      LabeledVector(2.0, DenseVector(2.0, 3.0)),
      LabeledVector(3.0, DenseVector(3.0, 4.0)))
    val labeledData = env.fromCollection(labeledVectors)
    val doubles = List(1.0, 2.0, 3.0)
    val doubleData = env.fromCollection(doubles)

    val pipeline = scaler.chainPredictor(mlr)

    val exceptionFit = intercept[RuntimeException] {
      pipeline.fit(vectorData)
    }

    exceptionFit.getMessage should equal("There is no FitOperation defined for org.apache." +
      "flink.ml.regression.MultipleLinearRegression which trains on a " +
      "DataSet[org.apache.flink.ml.math.DenseVector]")

    // fit the pipeline so that the StandardScaler won't fail when predict is called on the pipeline
    pipeline.fit(labeledData)

    // make sure that we have TransformOperation[StandardScaler, Double, Double]
    implicit val standardScalerDoubleTransform =
      new TransformDataSetOperation[StandardScaler, Double, Double] {
        override def transformDataSet(instance: StandardScaler, transformParameters: ParameterMap,
          input: DataSet[Double]): DataSet[Double] = {
          input
        }
      }

    val exceptionPredict = intercept[RuntimeException] {
      pipeline.predict(doubleData)
    }

    exceptionPredict.getMessage should equal("There is no PredictOperation defined for " +
      "org.apache.flink.ml.regression.MultipleLinearRegression which takes a " +
      "DataSet[Double] as input.")
  }

  it should "throw an exception when the input data is not supported" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val dData = List(1.0, 2.0, 3.0)
    val doubleData = env.fromCollection(dData)

    val scaler = StandardScaler()
    val polyFeatures = PolynomialFeatures()

    val pipeline = scaler.chainTransformer(polyFeatures)

    val exceptionFit = intercept[RuntimeException] {
      pipeline.fit(doubleData)
    }

    exceptionFit.getMessage should equal("There is no FitOperation defined for org.apache." +
      "flink.ml.preprocessing.StandardScaler which trains on a DataSet[Double]")

    val exceptionTransform = intercept[RuntimeException] {
      pipeline.transform(doubleData)
    }

    exceptionTransform.getMessage should equal("There is no TransformOperation defined for " +
      "org.apache.flink.ml.preprocessing.StandardScaler which takes a DataSet[Double] as input.")
  }

  it should "support multiple transformers and a predictor" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val data = List(LabeledVector(1.0, DenseVector(1.0, 2.0)),
      LabeledVector(2.0, DenseVector(2.0, 3.0)),
      LabeledVector(3.0, DenseVector(3.0, 4.0)))
    val testing = data.map(_.vector)
    val evaluation = data.map(x => (x.vector, x.label))

    val trainingData = env.fromCollection(data)
    val testingData = env.fromCollection(testing)
    val evaluationData = env.fromCollection(evaluation)

    val chainedScalers2 = StandardScaler().chainTransformer(StandardScaler())
    val chainedScalers3 = chainedScalers2.chainTransformer(StandardScaler())
    val chainedScalers4 = chainedScalers3.chainTransformer(StandardScaler())
    val chainedScalers5 = chainedScalers4.chainTransformer(StandardScaler())

    val predictor = MultipleLinearRegression()
    
    val pipeline = chainedScalers5.chainPredictor(predictor)

    pipeline.fit(trainingData)

    val weightVector = predictor.weightsOption.get.collect().head

    weightVector.weights.valueIterator.foreach{
      _ should be (0.268050 +- 0.01)
    }

    weightVector.intercept should be (0.807924 +- 0.01)

    val predictionDS = pipeline.predict(testingData)

    val predictionResult = predictionDS.collect()

    val evaluationDS = pipeline.evaluate(evaluationData)

    val evaluationResult = evaluationDS.collect()

    predictionResult.size should be(testing.size)
    evaluationResult.size should be(evaluation.size)
  }

  it should "throw an exception when the input data is not supported by a predictor" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val data = List(1.0, 2.0, 3.0)
    val doubleData = env.fromCollection(data)

    val svm = SVM()

    intercept[RuntimeException] {
      svm.fit(doubleData)
    }

    intercept[RuntimeException] {
      svm.predict(doubleData)
    }
  }
}
