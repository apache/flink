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

import java.io.File
import javax.xml.transform.stream.StreamSource

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.ml.common.{ParameterMap, WeightVector}
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.ml.preprocessing.PolynomialFeatures
import org.apache.flink.test.util.FlinkTestBase
import org.dmg.pmml._
import org.jpmml.model.JAXBUtil
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

class MultipleLinearRegressionITSuite
  extends FlatSpec
  with Matchers
  with FlinkTestBase {

  behavior of "The multipe linear regression implementation"

  it should "estimate a linear function" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)

    val mlr = MultipleLinearRegression()

    import RegressionData._

    val parameters = ParameterMap()

    parameters.add(MultipleLinearRegression.Stepsize, 2.0)
    parameters.add(MultipleLinearRegression.Iterations, 10)
    parameters.add(MultipleLinearRegression.ConvergenceThreshold, 0.001)

    val inputDS = env.fromCollection(data)
    mlr.fit(inputDS, parameters)

    val weightList = mlr.weightsOption.get.collect()

    weightList.size should equal(1)

    val WeightVector(weights, intercept) = weightList.head

    expectedWeights.toIterator zip weights.valueIterator foreach {
      case (expectedWeight, weight) =>
        weight should be (expectedWeight +- 1)
    }
    intercept should be (expectedWeight0 +- 0.4)

    val srs = mlr.squaredResidualSum(inputDS).collect().head

    srs should be (expectedSquaredResidualSum +- 2)
  }

  it should "estimate a cubic function" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)

    val polynomialBase = PolynomialFeatures()
    val mlr = MultipleLinearRegression()

    val pipeline = polynomialBase.chainPredictor(mlr)

    val inputDS = env.fromCollection(RegressionData.polynomialData)

    val parameters = ParameterMap()
      .add(PolynomialFeatures.Degree, 3)
      .add(MultipleLinearRegression.Stepsize, 0.004)
      .add(MultipleLinearRegression.Iterations, 100)

    pipeline.fit(inputDS, parameters)

    val weightList = mlr.weightsOption.get.collect()

    weightList.size should equal(1)

    val WeightVector(weights, intercept) = weightList.head

    RegressionData.expectedPolynomialWeights.toIterator.zip(weights.valueIterator) foreach {
      case (expectedWeight, weight) =>
        weight should be(expectedWeight +- 0.1)
    }

    intercept should be(RegressionData.expectedPolynomialWeight0 +- 0.1)

    val transformedInput = polynomialBase.transform(inputDS, parameters)

    val srs = mlr.squaredResidualSum(transformedInput).collect().head

    srs should be(RegressionData.expectedPolynomialSquaredResidualSum +- 5)
  }

  it should "make (mostly) correct predictions" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val mlr = MultipleLinearRegression()

    import RegressionData._

    val parameters = ParameterMap()

    parameters.add(MultipleLinearRegression.Stepsize, 1.0)
    parameters.add(MultipleLinearRegression.Iterations, 10)
    parameters.add(MultipleLinearRegression.ConvergenceThreshold, 0.001)

    val inputDS = env.fromCollection(data)
    val evaluationDS = inputDS.map(x => (x.vector, x.label))

    mlr.fit(inputDS, parameters)

    val predictionPairs = mlr.evaluate(evaluationDS)

    val absoluteErrorSum = predictionPairs.collect().map{
      case (truth, prediction) => Math.abs(truth - prediction)}.sum

    absoluteErrorSum should be < 50.0
  }

  it should "export a valid PMML object" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val mlr = MultipleLinearRegression()
    mlr.weightsOption = Some(env.fromElements(
      WeightVector(DenseVector(scala.Array(1.0, 2.0)), 3.0))
    )

    val tempFile = File.createTempFile("mlr-export-test", null)
    mlr.exportToPMML(tempFile.toString)

    // ------------ VERIFY THE EXPORT ---------------------------
    val pmml = JAXBUtil.unmarshalPMML(new StreamSource(tempFile))

    // header verification
    pmml.getHeader.getApplication.getName should equal("Flink ML")
    pmml.getHeader.getDescription should equal("Multiple Linear Regression")

    // dictionary verification
    val dataFields = pmml.getDataDictionary.getDataFields.asScala
    dataFields.size should equal(3)
    val field1 = dataFields.find(_.getName.getValue == "field_0")
    val field2 = dataFields.find(_.getName.getValue == "field_1")
    val field3 = dataFields.find(_.getName.getValue == "prediction")
    field1 should not be None
    field2 should not be None
    field3 should not be None
    field1.get.getDataType should equal(DataType.DOUBLE)
    field2.get.getDataType should equal(DataType.DOUBLE)
    field3.get.getDataType should equal(DataType.DOUBLE)
    field1.get.getOpType should equal(OpType.CONTINUOUS)
    field2.get.getOpType should equal(OpType.CONTINUOUS)
    field3.get.getOpType should equal(OpType.CONTINUOUS)

    // get the model
    pmml.getModels.size() should equal(1)
    val model = pmml.getModels.get(0) match {
      case m: RegressionModel => m
      case default => throw new Exception("Invalid model type")
    }

    // verify model
    model.getFunctionName should equal(MiningFunctionType.REGRESSION)
    val schema = model.getMiningSchema.getMiningFields.asScala
    schema.size should equal(3)
    val schema1 = schema.find(_.getName.getValue == "field_0")
    val schema2 = schema.find(_.getName.getValue == "field_1")
    val schema3 = schema.find(_.getName.getValue == "prediction")
    schema1 should not be None
    schema2 should not be None
    schema3 should not be None
    schema1.get.getUsageType should equal(FieldUsageType.ACTIVE)
    schema2.get.getUsageType should equal(FieldUsageType.ACTIVE)
    schema3.get.getUsageType should equal(FieldUsageType.PREDICTED)
    model.getRegressionTables.get(0).getIntercept should equal(3.0)
    val predictors = model.getRegressionTables.get(0).getNumericPredictors.asScala
    predictors.size should equal(2)
    predictors.find(_.getName.getValue == "field_0").get.getCoefficient should equal(1.0)
    predictors.find(_.getName.getValue == "field_1").get.getCoefficient should equal(2.0)
  }
}
