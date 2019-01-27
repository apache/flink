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
import org.apache.flink.modelserving.scala.model.tensorflow.TensorFlowModel

import org.tensorflow.Tensor

/**
  * Implementation of tensorflow model (optimized) for wine.
  */
class WineTensorFlowModel(inputStream: Array[Byte])
  extends TensorFlowModel[WineRecord, Double](inputStream){

  import WineTensorFlowModel._

  /**
    * Score data.
    *
    * @param input object to score.
    */
  override def score(input: WineRecord): Double = {

    // Create input tensor
    val modelInput  = toTensor(input)
    // Serve model using tensorflow APIs
    val result = session.runner.feed("dense_1_input", modelInput).fetch("dense_3/Sigmoid").
      run().get(0)
    // Get result shape
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
  * Implementation of tensorflow (optimized) model factory for wine.
  */
object WineTensorFlowModel extends  ModelFactory[WineRecord, Double] {

  /**
    * Convert wine record into input tensor for serving.
    *
    * @param record Wine record.
    * @return tensor
    */
  def toTensor(record : WineRecord) : Tensor[_] = {
    val data = Array(
      record.fixedAcidity.toFloat,
      record.volatileAcidity.toFloat,
      record.citricAcid.toFloat,
      record.residualSugar.toFloat,
      record.chlorides.toFloat,
      record.freeSulfurDioxide.toFloat,
      record.totalSulfurDioxide.toFloat,
      record.density.toFloat,
      record.pH.toFloat,
      record.sulphates.toFloat,
      record.alcohol.toFloat
    )
    Tensor.create(Array(data))
  }

  /**
    * Creates a new tensorflow (optimized) model.
    *
    * @param descriptor model to serve representation of tensorflow model.
    * @return model
    */
  override def create(input: ModelToServe): Option[Model[WineRecord, Double]] = try
    Some(new WineTensorFlowModel(input.model))
  catch {
    case t: Throwable => None
  }

  /**
    * Restore tensorflow (optimised) model from binary.
    *
    * @param bytes binary representation of tensorflow model.
    * @return model
    */
  override def restore(bytes: Array[Byte]) = new WineTensorFlowModel(bytes)
}
