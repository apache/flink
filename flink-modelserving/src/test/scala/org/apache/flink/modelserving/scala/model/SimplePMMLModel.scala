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

import java.util

import org.apache.flink.modelserving.scala.model.pmml.PMMLModel
import org.dmg.pmml.{FieldName, PMML}
import org.jpmml.evaluator.{Evaluator, InputField}

/**
  * Implementation of PMML model for testing.
  */
class SimplePMMLModel (inputStream: Array[Byte]) extends PMMLModel(inputStream) {

  /**
    * Score data.
    *
    * @param input object to score.
    */
  override def score(input: AnyVal): AnyVal = null.asInstanceOf[AnyVal]

  // Getters for validation
  /**
    * Get PMML model.
    *
    * @return PMML.
    */
  def getPmml: PMML = pmml

  /**
    * Get PMML Evaluator.
    *
    * @return PMML Evaluator.
    */
  def getEvaluator: Evaluator = evaluator

  /**
    * Get PMML model's input field.
    *
    * @return PMML model input field.
    */
  def getTname: FieldName = tname

  /**
    * Get PMML model output fields.
    *
    * @return PMML model output fields.
    */
  def getInputFields: util.List[InputField] = inputFields
}

/**
  * Implementation of PMML model factory.
  */
object SimplePMMLModel extends ModelFactory {

  /**
    * Creates a new PMML model.
    *
    * @param descriptor model to serve representation of PMML model.
    * @return model
    */
  override def create(input: ModelToServe): Option[Model] =
    try
      Some(new SimplePMMLModel(input.model))
    catch {
      case t: Throwable => None
    }

  /**
    * Restore PMML model from binary.
    *
    * @param bytes binary representation of PMML model.
    * @return model
    */
  override def restore(bytes: Array[Byte]) = new SimplePMMLModel(bytes)
}
