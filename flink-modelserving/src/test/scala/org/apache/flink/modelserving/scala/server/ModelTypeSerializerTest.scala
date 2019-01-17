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

package org.apache.flink.modelserving.scala.server

import org.apache.flink.model.modeldescriptor.ModelDescriptor
import org.apache.flink.modelserving.java.server.SerializerTestBase
import org.apache.flink.modelserving.scala.model.{Model, ModelToServe, SimpleFactoryResolver}
import org.apache.flink.modelserving.scala.server.typeschema.ModelTypeSerializer

import java.io.File
import java.nio.file.{Files, Paths}

/**
  * Tests for the {@link ModelTypeSerializer}.
  */
class ModelTypeSerializerTest extends SerializerTestBase[Option[Model]] {

  private val tfmodeloptimized = "model/TF/optimized/optimized_WineQuality.pb"
  private val tfmodelsaved = "model/TF/saved/"

  ModelToServe.setResolver(new SimpleFactoryResolver)

  override protected def createSerializer() = new ModelTypeSerializer

  override protected def getLength: Int = -1

  override protected def getTypeClass: Class[Option[Model]] = classOf[Option[Model]]

  override protected def getTestData: Array[Option[Model]] = {
    // Get TF Optimized model from file
    var model = getModel(tfmodeloptimized)
    val tfoptimized = ModelToServe.restore(ModelDescriptor.ModelType.TENSORFLOW.value, model)
    // Get TF bundled model location
    val classLoader = getClass.getClassLoader
    val file = new File(classLoader.getResource(tfmodelsaved).getFile)
    val location = file.getPath
    // Create model from location
    val tfbundled = ModelToServe.restore(ModelDescriptor.ModelType.TENSORFLOWSAVED.value,
      location.getBytes)
    Array[Option[Model]](Option.empty, tfoptimized, tfbundled)
  }

  private def getModel(fileName: String) = {
    val classLoader = getClass.getClassLoader
    val file = new File(classLoader.getResource(fileName).getFile)
    Files.readAllBytes(Paths.get(file.getPath))
  }
}
