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


package org.apache.flink.modelserving.scala.server.partitioned

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.modelserving.scala.model.{DataToServe, Model, ModelToServe, ModelWithType, ServingResult}
import org.apache.flink.modelserving.scala.server.typeschema.ModelWithTypeSerializer
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, Map}

object DataProcessorMap{
  def apply() = new DataProcessorMap
}

/**
  * Data Processer (map) - the main implementation, that brings together model and data to
  * process. Model is distributed to all instances, while data is send to arbitrary one.
  */
class DataProcessorMap extends RichCoFlatMapFunction[DataToServe, ModelToServe, ServingResult]
  with CheckpointedFunction {

  // Current models
  private var currentModels = mutable.Map[String, Model]()
  // New models
  private var newModels = mutable.Map[String, Model]()

  // Checkpointing state
  @transient private var checkpointedState: ListState[ModelWithType] = _

  /**
    * Create snapshot execution state.
    *
    * @param context Flink execution context.
    */
  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    // Clear checkpointing state
    checkpointedState.clear()
    // Populate checkpointing state
    currentModels.foreach(
      entry => checkpointedState.add(new ModelWithType(true, entry._1, Some(entry._2))))
    newModels.foreach(
      entry => checkpointedState.add(new ModelWithType(false, entry._1, Some(entry._2))))
  }

  /**
    * Restore state from checkpoint.
    *
    * @param context Flink execution context.
    */
  override def initializeState(context: FunctionInitializationContext): Unit = {
    // Checkpointing descriptor
    val checkPointDescriptor = new ListStateDescriptor[ModelWithType] (
        "modelState",
        new ModelWithTypeSerializer)
    // Get checkpointing data
    checkpointedState = context.getOperatorStateStore.getListState (checkPointDescriptor)

    // If restored
    if (context.isRestored) {
      // Create state
      val nm = new ListBuffer[(String, Model)]()
      val cm = new ListBuffer[(String, Model)]()
      checkpointedState.get().iterator().asScala.foreach(modelWithType => {
        // For each model in the checkpointed state
        modelWithType.model match {
          case Some(model) =>                     // Model is present
            modelWithType.isCurrent match {
              case true => cm += (modelWithType.dataType -> model)  // Its a current model
              case _ => nm += (modelWithType.dataType -> model)     // Its a new model
            }
          case _ =>
        }
      })
      // Convert lists into maps
      currentModels = mutable.Map(cm: _*)
      newModels = mutable.Map(nm: _*)
    }
  }

  /**
    * Process model. Invoked every time when a new model arrives.
    *
    * @param model Model to serve.
    * @param out   result's collector.
    */
  override def flatMap2(model: ModelToServe, out: Collector[ServingResult]): Unit = {

    println(s"New model - $model")
    ModelToServe.toModel(model) match {                     // Inflate model
      case Some(md) => newModels += (model.dataType -> md)  // Save a new model
      case _ =>
    }
  }

  /**
    * Process data. Invoked every time when a new data element to be processed arrives.
    *
    * @param record Data to serve.
    * @param out    result's collector.
    */
  override def flatMap1(record: DataToServe, out: Collector[ServingResult]): Unit = {
    // See if we need to update
    newModels.contains(record.getType) match {    // There is a new model for this type
      case true =>
        currentModels.contains(record.getType) match {  // There is currently a model
          case true => currentModels(record.getType).cleanup()  // Cleanup
          case _ =>
        }
        // Update current models and remove a model from new models
        currentModels += (record.getType -> newModels(record.getType))
        newModels -= record.getType
      case _ =>
    }
    // actually process
    currentModels.contains(record.getType) match {
      case true =>
        val start = System.currentTimeMillis()
        // Actual serving
        val result = currentModels(record.getType).score(record.getRecord)
        val duration = System.currentTimeMillis() - start
        // write result out
        out.collect(ServingResult(duration, result))
      case _ =>
    }
  }
}
