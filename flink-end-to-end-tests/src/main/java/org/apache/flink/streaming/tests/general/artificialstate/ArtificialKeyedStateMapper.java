/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.tests.general.artificialstate;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ArtificialKeyedStateMapper<IN, OUT> extends RichMapFunction<IN, OUT> implements CheckpointedFunction {

	private final MapFunction<IN, OUT> mapFunction;
	private final List<ArtificialKeyedStateBuilder<IN>> artificialKeyedStateBuilder;

	public ArtificialKeyedStateMapper(
		MapFunction<IN, OUT> mapFunction,
		ArtificialKeyedStateBuilder<IN> artificialKeyedStateBuilder) {
		this(mapFunction, Collections.singletonList(artificialKeyedStateBuilder));
	}

	public ArtificialKeyedStateMapper(
		MapFunction<IN, OUT> mapFunction,
		List<ArtificialKeyedStateBuilder<IN>> artificialKeyedStateBuilder) {

		this.mapFunction = mapFunction;
		this.artificialKeyedStateBuilder = artificialKeyedStateBuilder;
		Set<String> stateNames = new HashSet<>(this.artificialKeyedStateBuilder.size());
		for (ArtificialKeyedStateBuilder<IN> stateBuilder : this.artificialKeyedStateBuilder) {
			if (!stateNames.add(stateBuilder.getStateName())) {
				throw new IllegalArgumentException("Duplicated state name: " + stateBuilder.getStateName());
			}
		}
	}

	@Override
	public OUT map(IN value) throws Exception {
		for (ArtificialKeyedStateBuilder<IN> stateBuilder : artificialKeyedStateBuilder) {
			stateBuilder.artificialStateForElement(value);
		}

		return mapFunction.map(value);
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		for (ArtificialKeyedStateBuilder<IN> stateBuilder : artificialKeyedStateBuilder) {
			stateBuilder.initialize(context);
		}
	}
}
