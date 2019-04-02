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

package org.apache.flink.modelserving.java.server.partitioned;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.modelserving.java.model.DataConverter;
import org.apache.flink.modelserving.java.model.DataToServe;
import org.apache.flink.modelserving.java.model.Model;
import org.apache.flink.modelserving.java.model.ModelToServe;
import org.apache.flink.modelserving.java.model.ModelWithType;
import org.apache.flink.modelserving.java.model.ServingResult;
import org.apache.flink.modelserving.java.server.typeschema.ModelWithTypeSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

/**
 * Data Processer (map) - the main implementation, that brings together model and data to process.
 * Model is distributed to all instances, while data is send to arbitrary one.
 */
public class DataProcessorMap<RECORD, RESULT> extends RichCoFlatMapFunction<DataToServe<RECORD>,
	ModelToServe, ServingResult<RESULT>> implements CheckpointedFunction {

    // Current models
	Map<String, Model<RECORD, RESULT>> currentModels = new HashMap<>();

    // Checkpointing state
	private transient ListState<ModelWithType<RECORD, RESULT>> checkpointedState = null;

	/**
	 * Create snapshot execution state.
	 * @param context Flink execution context.
	 */
	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // Clear checkponting state
		checkpointedState.clear();
        // Copy current state
		for (Map.Entry<String, Model<RECORD, RESULT>> entry : currentModels.entrySet()) {
			if (entry.getValue() != null){
				checkpointedState.add(
					new ModelWithType(entry.getKey(), Optional.of(entry.getValue())));
			}
		}
	}

	/**
	 * Restore state from checkpoint.
	 * @param context Flink execution context.
	 */
	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
        // Descriptor
		ListStateDescriptor<ModelWithType<RECORD, RESULT>> descriptor = new ListStateDescriptor<>(
			"modelState", new ModelWithTypeSerializer<RECORD, RESULT>());
        // Read checkpoint
		checkpointedState = context.getOperatorStateStore().getListState (descriptor);
		if (context.isRestored()) {                 // If restored
			Iterator<ModelWithType<RECORD, RESULT>> iterator = checkpointedState.get().iterator();
			while (iterator.hasNext()){              // For every restored
				ModelWithType<RECORD, RESULT> current = iterator.next();
				if (current.getModel().isPresent()){ // It contains model
					currentModels.put(current.getDataType(), current.getModel().get());
				}
			}
		}
	}

	/**
	 * Process data. Invoked every time when a new data element to be processed arrives.
	 * @param record Data to serve.
	 * @param out result's collector.
	 */
	@Override
	public void flatMap1(DataToServe<RECORD> record, Collector<ServingResult<RESULT>> out)
		throws Exception {
		if (currentModels.containsKey(record.getType())){
			// We have the model for this data type
			long start = System.currentTimeMillis();
            // Actually serve data
			Object result = currentModels.get(record.getType()).score(record.getRecord());
			long duration = System.currentTimeMillis() - start;
            // Write result out
			out.collect(new ServingResult(duration, result));
		}
	}

	/**
	 * Process model. Invoked every time when a new model arrives.
	 * @param model Model to serve.
	 * @param out result's collector.
	 */
	@Override
	public void flatMap2(ModelToServe model, Collector<ServingResult<RESULT>> out) throws Exception {
		System.out.println("New model - " + model);
        // Create model
		Optional<Model<RECORD, RESULT>> m = DataConverter.toModel(model);
		if (m.isPresent()) {               // If creation successful
			// If there is currently model of this type in use?
			if (currentModels.containsKey(model.getDataType())){
				currentModels.get(model.getDataType()).cleanup();
			}
			// Update current state
			currentModels.put(model.getDataType(), m.get());
		}
		else {
			System.out.println("Model creation for " + model + " failed");
		}
	}
}
