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

package org.apache.flink.examples.modelserving.scala.model

import org.apache.flink.modelserving.wine.winerecord.WineRecord
import org.apache.flink.modelserving.scala.model.{Model, ModelFactory, ModelToServe}
import org.apache.flink.modelserving.scala.model.tensorflow.TensorFlowBundleModel

/**
  * Implementation of TensorFlow bundled model for Wine.
  */
class WineTensorFlowBundledModel(inputStream: Array[Byte])
  extends TensorFlowBundleModel[WineRecord, Double](inputStream) {

  /**
    * Score data.
    *
    * @param input object to score.
    * @return scoring result
    */
  override def score(input: WineRecord): Double = {
    // Create input tensor
    val modelInput = WineTensorFlowModel.toTensor(input)
    // Serve model using tensorflow APIs
    val signature = parsedSign.head._2
    val tinput = signature.inputs.head._2
    val toutput= signature.outputs.head._2
    val result = session.runner.feed(tinput.name, modelInput).fetch(toutput.name).run().get(0)
    // process result
    val resultshape = result.shape
    // get result tensor
    var resultensor = Array.ofDim[Float](
      resultshape(0).asInstanceOf[Int], resultshape(1).asInstanceOf[Int])
    result.copyTo(resultensor)
    // Get result
    resultensor(0).indices.maxBy(resultensor(0)).toDouble
  }
}

/**
  * Implementation of TensorFlow bundled model factory.
  */
object WineTensorFlowBundledModel extends ModelFactory[WineRecord, Double] {

  /**
    * Creates a new TensorFlow bundled model.
    *
    * @param descriptor model to serve representation of PMML model.
    * @return model
    */
  override def create(input: ModelToServe): Option[Model[WineRecord, Double]] =
    try
      Some(new WineTensorFlowBundledModel(input.location.getBytes))
    catch {
      case t: Throwable => None
    }

  /**
    * Restore PMML model from binary.
    *
    * @param bytes binary representation of PMML model.
    * @return model
    */
  override def restore(bytes: Array[Byte]) = new WineTensorFlowBundledModel(bytes)
}

