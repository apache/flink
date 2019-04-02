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

package org.apache.flink.modelserving.java.server.keyed;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.modelserving.java.model.DataConverter;
import org.apache.flink.modelserving.java.model.DataToServe;
import org.apache.flink.modelserving.java.model.Model;
import org.apache.flink.modelserving.java.model.ModelToServe;
import org.apache.flink.modelserving.java.model.ModelToServeStats;
import org.apache.flink.modelserving.java.model.ServingResult;
import org.apache.flink.modelserving.java.server.typeschema.ModelTypeSerializer;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Optional;

/**
 * Data Processer (keyed) - the main implementation, that brings together model and data to process (based on key).
 */
public class DataProcessorKeyed<RECORD, RESULT> extends CoProcessFunction<DataToServe<RECORD>, ModelToServe, ServingResult<RESULT>> {

    // In Flink class instance is created not for key, but rater key groups
    // https://ci.apache.org/projects/flink/flink-docs-release-1.6/dev/stream/state/state.html#keyed-state-and-operator-state
    // As a result, any key specific sate data has to be in the key specific state

    // current model state
	ValueState<ModelToServeStats> modelState;
    // Current model
	ValueState<Model<RECORD, RESULT>> currentModel;

	/**
	 * Open execution. Called when an instance is created.
	 * @param parameters Flink configuration.
	 */
	@Override
	public void open(Configuration parameters){
        // Model state descriptor
		ValueStateDescriptor<ModelToServeStats> modeStatelDesc = new ValueStateDescriptor<>(
			"currentModelState",   // state name
			TypeInformation.of(new TypeHint<ModelToServeStats>() {})); // type information
        // Allow access from queryable client
		modeStatelDesc.setQueryable("currentModelState");
        // Create model state
		modelState = getRuntimeContext().getState(modeStatelDesc);
        // Current model descriptor
		ValueStateDescriptor<Model<RECORD, RESULT>> currentModelDesc = new ValueStateDescriptor<>(
			"currentModel",         // state name
			new ModelTypeSerializer()); // type information
        // Create current model
		currentModel = getRuntimeContext().getState(currentModelDesc);
	}

	/**
	 * Process data. Invoked every time when a new data element to be processed arrives.
	 * @param value Data to serve.
	 * @param ctx Flink execution context.
	 * @param out result's collector.
	 */
	@Override
	public void processElement1(DataToServe<RECORD> value, Context ctx, Collector<ServingResult<RESULT>> out) throws Exception {

        // Process data
		if (currentModel.value() != null){
			long start = System.currentTimeMillis();
            // Actually serve
			Object result = currentModel.value().score(value.getRecord());
			long duration = System.currentTimeMillis() - start;
            // Update state
			modelState.update(modelState.value().incrementUsage(duration));
            // Write result
			out.collect(new ServingResult(duration, result));
		}
	}

	/**
	 * Process model. Invoked every time when a new model arrives.
	 * @param model Model to serve.
	 * @param ctx Flink execution context.
	 * @param out result's collector.
	 */
	@Override
	public void processElement2(ModelToServe model, Context ctx, Collector<ServingResult<RESULT>> out) throws Exception {
		System.out.println("New model - " + model);
		Optional<Model<RECORD, RESULT>> m = DataConverter.toModel(model);    // Create a new model
		if (m.isPresent()) {
			// Clean up current model
			if (currentModel.value() != null){
				currentModel.value().cleanup();
			}
			// Update model
			currentModel.update(m.get());
			modelState.update(new ModelToServeStats(model));
		}
		else {
			System.out.println("Model creation for " + model + " failed");
		}
	}
}
