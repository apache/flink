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
import org.apache.flink.modelserving.scala.model.pmml.PMMLModel
import org.jpmml.evaluator.Computable

import scala.collection.JavaConversions._
import scala.collection._

/**
  * Implementation of PMML model for Wine.
  */
class WinePMMLModel(inputStream: Array[Byte]) extends PMMLModel(inputStream) {

  /**
    * Score data.
    *
    * @param input object to score.
    * @return scoring result
    */
  override def score(input: AnyVal): AnyVal = {
    // Convert input
    val inputs = input.asInstanceOf[WineRecord]
    // Clear arguments (from previous run)
    arguments.clear()
    // Populate input based on record
    inputFields.foreach(field => {
      arguments.put(field.getName, field.prepare(getValueByName(inputs, field.getName.getValue)))
    })
    // Calculate Output// Calculate Output
    val result = evaluator.evaluate(arguments)

    // Prepare output
    result.get(tname) match {
      case c : Computable => c.getResult.toString.toDouble
      case v : Any => v.asInstanceOf[Double]
    }
  }

  /**
    * Support function to get values.
    *
    * @param inputs input record.
    * @return value
    */
  private def getValueByName(inputs : WineRecord, name: String) : Double =
    WinePMMLModel. names.get(name) match {
      case Some(index) => {
        val v = inputs.getFieldByNumber(index + 1)
        v.asInstanceOf[Double]
      }
      case _ => .0
    }}

/**
  * Implementation of PMML model factory.
  */
object WinePMMLModel extends ModelFactory {

  private val names = Map("fixed acidity" -> 0,
    "volatile acidity" -> 1,"citric acid" ->2,"residual sugar" -> 3,
    "chlorides" -> 4,"free sulfur dioxide" -> 5,"total sulfur dioxide" -> 6,
    "density" -> 7,"pH" -> 8,"sulphates" ->9,"alcohol" -> 10)

  /**
    * Creates a new PMML model.
    *
    * @param descriptor model to serve representation of PMML model.
    * @return model
    */
  override def create(input: ModelToServe): Option[Model] =
    try
      Some(new WinePMMLModel(input.model))
    catch {
      case t: Throwable => None
    }

  /**
    * Restore PMML model from binary.
    *
    * @param bytes binary representation of PMML model.
    * @return model
    */
  override def restore(bytes: Array[Byte]) = new WinePMMLModel(bytes)
}

