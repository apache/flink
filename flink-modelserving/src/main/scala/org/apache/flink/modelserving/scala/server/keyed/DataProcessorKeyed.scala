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
  def apply() = new DataProcessorKeyed
}

/**
  * Data Processer (keyed) - the main implementation, that brings together model and data
  * to process (based on key).
  */
class DataProcessorKeyed extends CoProcessFunction[DataToServe, ModelToServe, ServingResult]{

  // current model state
  var modelState: ValueState[ModelToServeStats] = _
  // New model state
  var newModelState: ValueState[ModelToServeStats] = _

  // Current model
  var currentModel : ValueState[Option[Model]] = _
  // New model
  var newModel : ValueState[Option[Model]] = _

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
    // New Model state descriptor
    val newModelStateDesc = new ValueStateDescriptor[ModelToServeStats](
      "newModelState",                             // state name
      createTypeInformation[ModelToServeStats])            // type information
    // Create new model state
    newModelState = getRuntimeContext.getState(newModelStateDesc)
    // Model descriptor
    val modelDesc = new ValueStateDescriptor[Option[Model]](
      "currentModel",                               // state name
      new ModelTypeSerializer)                              // type information
    // Create current model state
    currentModel = getRuntimeContext.getState(modelDesc)
    // New model state descriptor
    val newModelDesc = new ValueStateDescriptor[Option[Model]](
      "newModel",                                    // state name
      new ModelTypeSerializer)                               // type information
    // Create new model state
    newModel = getRuntimeContext.getState(newModelDesc)
  }

  /**
    * Process model. Invoked every time when a new model arrives.
    *
    * @param model Model to serve.
    * @param ctx   Flink execution context.
    * @param out   result's collector.
    */
  override def processElement2(model: ModelToServe, ctx: CoProcessFunction
    [DataToServe, ModelToServe, ServingResult]#Context, out: Collector[ServingResult]): Unit = {

    // Ensure that the state is initialized
    if(newModel.value == null) newModel.update(None)
    if(currentModel.value == null) currentModel.update(None)

    println(s"New model - $model")
    // Create a model
    ModelToServe.toModel(model) match {
      case Some(md) =>
        newModel.update (Some(md))                            // Create a new model
        newModelState.update (new ModelToServeStats (model))  // Create a new model state
      case _ =>   // Model creation failed, continue
    }
  }

  /**
    * Process data. Invoked every time when a new data element to be processed arrives.
    *
    * @param value Data to serve.
    * @param ctx   Flink execution context.
    * @param out   result's collector.
    */
  override def processElement1(record: DataToServe, ctx: CoProcessFunction
    [DataToServe, ModelToServe, ServingResult]#Context, out: Collector[ServingResult]): Unit = {

    // Ensure that the state is initialized
    if(newModel.value == null) newModel.update(None)
    if(currentModel.value == null) currentModel.update(None)
    // See if we have update for the model
    newModel.value.foreach { model =>
      // close current model first
      currentModel.value.foreach(_.cleanup())
      // Update model
      currentModel.update(newModel.value)
      modelState.update(newModelState.value())
      newModel.update(None)
    }

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
