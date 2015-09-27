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

import java.io.File
import javax.xml.transform.stream.StreamSource

import org.apache.flink.api.scala._
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.test.util.FlinkTestBase
import org.dmg.pmml._
import org.jpmml.model.JAXBUtil
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

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

  it should "export a valid PMML object" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val svm = SVM().setThreshold(0.5)
    svm.weightsOption = Some(env.fromElements(DenseVector(scala.Array(1.0, 2.0))))

    val tempFile = File.createTempFile("svm-export-test", null)
    svm.exportToPMML(tempFile.toString)

    // ------------ VERIFY THE EXPORT ---------------------------
    val pmml = JAXBUtil.unmarshalPMML(new StreamSource(tempFile))

    // header verification
    pmml.getHeader.getApplication.getName should equal("Flink ML")
    pmml.getHeader.getDescription should equal("Support Vector Machine")

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
    field3.get.getDataType should equal(DataType.STRING)
    field1.get.getOpType should equal(OpType.CONTINUOUS)
    field2.get.getOpType should equal(OpType.CONTINUOUS)
    field3.get.getOpType should equal(OpType.CATEGORICAL)
    val values = field3.get.getValues.asScala.map(_.getValue)
    values.size should equal(2)
    values should contain("NO")
    values should contain("YES")

    // get the model
    pmml.getModels.size() should equal(1)
    val model = pmml.getModels.get(0) match {
      case m: SupportVectorMachineModel => m
      case default => throw new Exception("Invalid model type")
    }

    // verify the model
    model.getFunctionName should equal(MiningFunctionType.CLASSIFICATION)
    model.getSvmRepresentation should equal(SvmRepresentationType.COEFFICIENTS)
    model.getThreshold.toDouble should equal(0.5)
    model.getKernel match {
      case _: LinearKernel =>
      case default => throw new Exception("Invalid kernel type")
    }
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
    val ws = model.getSupportVectorMachines.get(0).getCoefficients.getCoefficients.asScala
    ws.head.getValue.toDouble should equal(1.0)
    ws.last.getValue.toDouble should equal(2.0)
  }

}
