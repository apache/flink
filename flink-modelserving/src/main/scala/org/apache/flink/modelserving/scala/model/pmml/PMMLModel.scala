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

package org.apache.flink.modelserving.scala.model.pmml

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import org.apache.flink.annotation.Public
import org.apache.flink.model.modeldescriptor.ModelDescriptor
import org.apache.flink.modelserving.scala.model.{Model, ModelFactory}
import org.dmg.pmml.{FieldName, PMML}
import org.jpmml.evaluator.visitors.{ExpressionOptimizer, FieldOptimizer, PredicateOptimizer}
import org.jpmml.evaluator.visitors.GeneralRegressionModelOptimizer
import org.jpmml.evaluator.visitors.{NaiveBayesModelOptimizer, RegressionModelOptimizer}
import org.jpmml.evaluator.{FieldValue, ModelEvaluatorFactory, TargetField}
import org.jpmml.model.PMMLUtil

import scala.collection._

/**
  * Base class for PMML model processing.
  */
@Public
abstract class PMMLModel(inputStream: Array[Byte]) extends Model {

  /**
    * Creates a new PMML model.
    *
    * @param input binary representation of PMML model.
    */
  // Arguments array
  var arguments = mutable.Map[FieldName, FieldValue]()

  // Marshall PMML
  val pmml = PMMLUtil.unmarshal(new ByteArrayInputStream(inputStream))

  // Optimize model// Optimize model
  PMMLModelBase.optimize(pmml)

  // Create and verify evaluator
  val evaluator = ModelEvaluatorFactory.newInstance.newModelEvaluator(pmml)
  evaluator.verify()

  // Get input/target fields
  val inputFields = evaluator.getInputFields
  val target: TargetField = evaluator.getTargetFields.get(0)
  val tname = target.getName

  /**
    * Clean up PMML model.
    */
  override def cleanup(): Unit = {}

  /**
    * Get bytes representation of PMML model.
    *
    * @return binary representation of the PMML model.
    */
  override def toBytes : Array[Byte] = inputStream

  /**
    * Get model'a type.
    *
    * @return PMML model type.
    */
  override def getType: Long = ModelDescriptor.ModelType.PMML.value

  /**
    * Compare 2 PMML models. They are equal if their binary content is same.
    *
    * @param obj other model.
    * @return boolean specifying whether models are the same.
    */
  override def equals(obj: Any): Boolean = obj match {
    case pmmlModel: PMMLModel =>
      pmmlModel.toBytes.toList == inputStream.toList
    case _ => false
  }
}

/**
  * Base class for PMML model processing - static methods.
  */
object PMMLModelBase{

  // List of PMML optimizers (https://groups.google.com/forum/#!topic/jpmml/rUpv8hOuS3A)
  private val optimizers = Array(new ExpressionOptimizer, new FieldOptimizer,
    new PredicateOptimizer, new GeneralRegressionModelOptimizer,
    new NaiveBayesModelOptimizer, new RegressionModelOptimizer)

  /**
    * Optimize PMML model.
    *
    * @param pmml original pmml.
    * @return .
    */
  def optimize(pmml : PMML): Unit = this.synchronized {
    optimizers.foreach(opt =>
      try
        opt.applyTo(pmml)
      catch {
        case t: Throwable => {
          println(s"Error optimizing model for optimizer $opt")
          t.printStackTrace()
        }
      }
    )
  }
}
