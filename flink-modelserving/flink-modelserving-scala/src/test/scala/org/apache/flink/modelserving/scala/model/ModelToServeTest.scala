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

package org.apache.flink.modelserving.scala.model

import org.apache.flink.modelserving.scala.model.tensorflow.Field
import org.apache.flink.model.modeldescriptor.ModelDescriptor

import java.io._
import java.nio.file.{Files, Paths}

import com.google.protobuf.ByteString
import org.junit.Assert.{assertArrayEquals, assertEquals, assertNotEquals, assertTrue}
import org.junit.Test

/**
  * Tests for {@link ModelT0Serve}.
  */
class ModelToServeTest {

  private val tfmodeloptimized = "model/TF/optimized/optimized_WineQuality.pb"
  private val tfmodelsaved = "model/TF/saved/"
  private val pmmlmodel = "model/PMML/winequalityDecisionTreeClassification.pmml"
  private val name = "test"
  private val description = "test"
  private val dataType = "simple"

  private val bundleTag = "serve"
  private val bundleSignature = "serving_default"
  private val bundleInputs = "inputs"
  private val input = Field("image_tensor", null, Seq(-1, -1, -1, 3))
  private val bundleoutputs = Seq("detection_classes", "detection_boxes", "num_detections",
    "detection_scores")
  private val output = Seq(
    Field("detection_classes", null, Seq(-1, 100)),
    Field("detection_boxes", null, Seq(-1, 100, 4)),
    Field("num_detections", null, Seq(-1)),
    Field("detection_scores", null, Seq(-1, 100)))

  ModelToServe.setResolver(new SimpleFactoryResolver)

  @Test
  def testTFOptimized(): Unit = {
    val model = getModel(tfmodeloptimized)
    // Build input record
    val record = getbinaryContent(Some(model), Option.empty, ModelDescriptor.ModelType.TENSORFLOW)
    // Convert input record
    val result = ModelToServe.fromByteArray(record).toOption
    // validate it
    validateModelToServe(result, Some(model), Option.empty, ModelDescriptor.ModelType.TENSORFLOW)
    // Build TF model
    val tf = ModelToServe.toModel[Double, Double](result.get)
    // Validate
    assertTrue("TF Model created correctly", tf.isDefined)
    valdateTFModel(tf.get)
    // Simply copy the model
    val copyDirect = ModelToServe.copy(tf)
    assertEquals("Copy equal to source", tf.get, copyDirect.get)
    // Create model from binary
    val direct = ModelToServe.restore[Double, Double](
      ModelDescriptor.ModelType.TENSORFLOW.value, model)
    // Validate it
    valdateTFModel(direct.get)
  }

  @Test
  def testTFOptimizedBadData(): Unit = {
    val model = Array[Byte]()
    // Build input record
    val record = getbinaryContent(Some(model), Option.empty, ModelDescriptor.ModelType.TENSORFLOW)
    // Convert input record
    val result = ModelToServe.fromByteArray(record).toOption
    // validate it
    validateModelToServe(result, Some(model), Option.empty, ModelDescriptor.ModelType.TENSORFLOW)
    // Build TF model
    val tf = ModelToServe.toModel[Double, Double](result.get)
    // Validate
    assertTrue("TF Model is not created", tf.isEmpty)
  }

  @Test
  def testTFBundled(): Unit = {
    // Get TF model from File
    val classLoader = getClass.getClassLoader
    val file = new File(classLoader.getResource(tfmodelsaved).getFile)
    val model = file.getPath
    // Build input record
    val record = getbinaryContent(Option.empty, Some(model),
      ModelDescriptor.ModelType.TENSORFLOWSAVED)
    // Convert input record
    val result = ModelToServe.fromByteArray(record).toOption
    // validate it
    validateModelToServe(result, Option.empty, Some(model),
      ModelDescriptor.ModelType.TENSORFLOWSAVED)
    // Build TF model
    val tf = ModelToServe.toModel[Double, Double](result.get)

    // Validate
    assertTrue("TF Model created correctly",tf.isDefined)
    valdateTFBundleModel(tf.get)

    // Simply copy the model
    val copyDirect = ModelToServe.copy(tf)

    assertEquals("Copy equal to source", tf.get, copyDirect.get)

    // Create model from binary
    val direct = ModelToServe.restore[Double, Double](
      ModelDescriptor.ModelType.TENSORFLOWSAVED.value, model.getBytes)

    // Validate it
    valdateTFBundleModel(direct.get)

  }

  @Test
  def testTFBundledBadData(): Unit = {
    val model = new String()
    // Build input record
    val record = getbinaryContent(Option.empty, Some(model),
      ModelDescriptor.ModelType.TENSORFLOWSAVED)
    // Convert input record
    val result = ModelToServe.fromByteArray(record).toOption
    // validate it
    validateModelToServe(result, Option.empty, Some(model),
      ModelDescriptor.ModelType.TENSORFLOWSAVED)
    // Build TF model
    val tf = ModelToServe.  toModel(result.get)
    // Validate
    assertTrue("TF Model is not created",tf.isEmpty)
  }

  private def valdateTFModel(tf: Model[Double, Double]): Unit = {
    assertTrue(tf.isInstanceOf[SimpleTensorflowModel])
    val tfModel = tf.asInstanceOf[SimpleTensorflowModel]
    assertNotEquals("Graph created", null, tfModel.getGrapth)
    assertNotEquals("Session created",null, tfModel.getSession)
  }

  private def valdateTFBundleModel(tf: Model[Double, Double]): Unit = {
    assertTrue(tf.isInstanceOf[SimpleTensorflowBundleModel])
    val tfModel = tf.asInstanceOf[SimpleTensorflowBundleModel]
    assertNotEquals("Graph created",null, tfModel.getGraph)
    assertNotEquals("Session created",null, tfModel.getSession)
    assertEquals("Number of Tags is correct",1, tfModel.getTags.size)
    assertEquals("Name of Tag is correct",bundleTag, tfModel.getTags(0))
    assertEquals("Number of Signatures is correct",1, tfModel.getSignatures.size)
    val sigEntry = tfModel.getSignatures.toList(0)
    assertEquals("Signature name is correct",bundleSignature, sigEntry._1)
    val sign = sigEntry._2
    assertEquals("Number of Inputs is correct",1, sign.inputs.toList.length)
    val inputEntry = sign.inputs.toList(0)
    assertEquals("Input name is correct",bundleInputs, inputEntry._1)
    assertEquals("Input name is correct",input.name, inputEntry._2.name)
    assertArrayEquals("Input shape is correct",input.shape.toArray, inputEntry._2.shape.toArray)
    assertEquals("Number of outputs is correct",4, sign.outputs.toList.length)
    val outputIterator = output.iterator
    for (outputName <- bundleoutputs) {
      val current = sign.outputs.get(outputName)
      assertTrue("Output name is correct",current.isDefined)
      val field = outputIterator.next
      assertEquals("Output name is correct",field.name, current.get.name)
      assertArrayEquals("Output shape is correct",field.shape.toArray, current.get.shape.toArray)
    }
  }

  private def validateModelToServe(modelToServe: Option[ModelToServe], model: Option[Array[Byte]],
                                   location: Option[String], `type`: ModelDescriptor.ModelType):
  Unit = {
    assertTrue("Model is created", modelToServe.isDefined)
    assertEquals("Model type is correct", `type`, modelToServe.get.modelType)
    assertEquals("Data type is correct", dataType, modelToServe.get.dataType)
    assertEquals("Model name is correct", name, modelToServe.get.name)
    assertEquals("Model description is correct", description, modelToServe.get.description)
    model match {
      case Some(data) => assertArrayEquals("Model data is correct", data, modelToServe.get.model)
      case _ => assertEquals("Model location is correct", location.get, modelToServe.get.location)
    }
  }

  private def getModel(fileName: String) : Array[Byte] = {
    val classLoader = getClass.getClassLoader
    val file = new File(classLoader.getResource(fileName).getFile)
    Files.readAllBytes(Paths.get(file.getPath))
  }

  private def getbinaryContent(pByteArray: Option[Array[Byte]], location: Option[String],
                               `type`: ModelDescriptor.ModelType) : Array[Byte] = {
    val record = pByteArray match {
      case Some(data) => ModelDescriptor(name, description, dataType, `type`)
        .withData(ByteString.copyFrom(data))
      case _ => ModelDescriptor(name, description, dataType, `type`).withLocation(location.get)
    }
    val bos = new ByteArrayOutputStream
    record.writeTo(bos)
    bos.toByteArray
  }
}
