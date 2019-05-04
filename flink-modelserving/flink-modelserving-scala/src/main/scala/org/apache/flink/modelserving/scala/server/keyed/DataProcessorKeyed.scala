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

package org.apache.flink.modelserving.scala.server.keyed

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.modelserving.scala.model.{DataToServe, Model, ModelToServe,
  ModelToServeStats, ServingResult}
import org.apache.flink.modelserving.scala.server.typeschema.ModelTypeSerializer
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.util.Collector

object DataProcessorKeyed {
  def apply[RECORD, RESULT]() = new DataProcessorKeyed[RECORD, RESULT]
}

/**
  * Data Processer (keyed) - the main implementation, that brings together model and data
  * to process (based on key).
  */
class DataProcessorKeyed[RECORD, RESULT] extends
  CoProcessFunction[DataToServe[RECORD], ModelToServe, ServingResult[RESULT]]{


  // current model state
  var modelState: ValueState[ModelToServeStats] = _

  // Current model
  var currentModel : ValueState[Option[Model[RECORD, RESULT]]] = _

  /**
    * Open execution. Called when an instance is created.
    *
    * @param parameters Flink configuration.
    */
  override def open(parameters: Configuration): Unit = {

    // Model state descriptor
    val modelStateDesc = new ValueStateDescriptor[ModelToServeStats](
      "currentModelState",                        // state name
      createTypeInformation[ModelToServeStats])           // type information
    modelStateDesc.setQueryable("currentModelState")      // Expose it for queryable state
    // Create Model state
    modelState = getRuntimeContext.getState(modelStateDesc)
    // Model descriptor
    val modelDesc = new ValueStateDescriptor[Option[Model[RECORD, RESULT]]](
      "currentModel",                               // state name
      new ModelTypeSerializer[RECORD, RESULT])              // type information
    // Create current model state
    currentModel = getRuntimeContext.getState(modelDesc)
  }

  /**
    * Process model. Invoked every time when a new model arrives.
    *
    * @param model Model to serve.
    * @param ctx   Flink execution context.
    * @param out   result's collector.
    */
  override def processElement2(model: ModelToServe,
       ctx: CoProcessFunction[DataToServe[RECORD], ModelToServe, ServingResult[RESULT]]#Context,
                               out: Collector[ServingResult[RESULT]]): Unit = {

    // Ensure that the state is initialized
    if(currentModel.value == null) currentModel.update(None)

    println(s"New model - $model")
    // Create a model
    ModelToServe.toModel[RECORD, RESULT](model) match {
      case Some(md) =>
        // close current model first
        currentModel.value.foreach(_.cleanup())
        // Update model
        currentModel.update(Some(md))                     // Create a new model
        modelState.update(new ModelToServeStats (model))  // Create a new model state
      case _ =>   // Model creation failed, continue
        println(s"Model creation for model $model failed")
    }
  }

  /**
    * Process data. Invoked every time when a new data element to be processed arrives.
    *
    * @param value Data to serve.
    * @param ctx   Flink execution context.
    * @param out   result's collector.
    */
  override def processElement1(record: DataToServe[RECORD],
       ctx: CoProcessFunction[DataToServe[RECORD], ModelToServe, ServingResult[RESULT]]#Context,
                               out: Collector[ServingResult[RESULT]]): Unit = {

    // Ensure that the state is initialized
    if(currentModel.value == null) currentModel.update(None)

    // Actually process data
    currentModel.value match {
      case Some(model) => {
        val start = System.currentTimeMillis()
        // Actually serve
        val result = model.score(record.getRecord)
        val duration = System.currentTimeMillis() - start
        // Update state
        modelState.update(modelState.value().incrementUsage(duration))
        // Write result out
        out.collect(ServingResult(duration, result))
      }
      case _ =>
    }
  }
}
