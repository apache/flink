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
import scala.collection.mutable.{ListBuffer, Map}

object DataProcessorMap{
  def apply[RECORD, RESULT]() = new DataProcessorMap[RECORD, RESULT]
}

/**
  * Data Processer (map) - the main implementation, that brings together model and data to
  * process. Model is distributed to all instances, while data is send to arbitrary one.
  */
class DataProcessorMap[RECORD, RESULT] extends
  RichCoFlatMapFunction[DataToServe[RECORD], ModelToServe, ServingResult[RESULT]]
  with CheckpointedFunction {

  // Current models
  private var currentModels = Map[String, Model[RECORD, RESULT]]()

  // Checkpointing state
  @transient private var checkpointedState: ListState[ModelWithType[RECORD, RESULT]] = _

  /**
    * Create snapshot execution state.
    *
    * @param context Flink execution context.
    */
  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    // Clear checkpointing state
    checkpointedState.clear()
    // Populate checkpointing state
    currentModels.foreach(entry => if(entry._2 != null) checkpointedState.
      add(new ModelWithType[RECORD, RESULT](entry._1, Some(entry._2))))
  }

  /**
    * Restore state from checkpoint.
    *
    * @param context Flink execution context.
    */
  override def initializeState(context: FunctionInitializationContext): Unit = {
    // Checkpointing descriptor
    val checkPointDescriptor = new ListStateDescriptor[ModelWithType[RECORD, RESULT]] (
      "modelState",
      new ModelWithTypeSerializer[RECORD, RESULT])
    // Get checkpointing data
    checkpointedState = context.getOperatorStateStore.getListState (checkPointDescriptor)

    // If restored
    if (context.isRestored) {
      // Create state
      currentModels = Map(checkpointedState.get().iterator().asScala.toList.map(modelWithType =>
        (modelWithType.dataType -> modelWithType.model.get)): _*)
    }
  }

  /**
    * Process model. Invoked every time when a new model arrives.
    *
    * @param model Model to serve.
    * @param out   result's collector.
    */
  override def flatMap2(model: ModelToServe, out: Collector[ServingResult[RESULT]]): Unit = {

    println(s"New model - $model")
    ModelToServe.toModel[RECORD, RESULT](model) match {     // Inflate model
      case Some(md) =>
        currentModels.get(model.dataType).map(_.cleanup())

        // Now update the current models with the new model for the record type
        // and remove the new model from new models temporary placeholder.
        currentModels += (model.dataType -> md)
      case _ =>
    }
  }

  /**
    * Process data. Invoked every time when a new data element to be processed arrives.
    *
    * @param record Data to serve.
    * @param out    result's collector.
    */
  override def flatMap1(record: DataToServe[RECORD], out: Collector[ServingResult[RESULT]]):
  Unit = {
    // actually process
    currentModels.get(record.getType) match {
      case Some(model) =>
        val start = System.currentTimeMillis()
        // Actual serving
        val result = model.score(record.getRecord)
        val duration = System.currentTimeMillis() - start
        // write result out
        out.collect(ServingResult(duration, result))
      case _ =>
    }
  }
}
