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

package org.apache.flink.streaming.tests.artificialstate;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.tests.artificialstate.builder.ArtificialStateBuilder;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A generic, stateful {@link MapFunction} that allows specifying what states to maintain
 * based on a provided list of {@link ArtificialStateBuilder}s.
 */
public class ArtificialKeyedStateMapper<IN, OUT> extends RichMapFunction<IN, OUT> implements CheckpointedFunction {

	private static final long serialVersionUID = 513012258173556604L;

	private final MapFunction<IN, OUT> mapFunction;
	private final List<ArtificialStateBuilder<IN>> artificialStateBuilders;

	public ArtificialKeyedStateMapper(
		MapFunction<IN, OUT> mapFunction,
		ArtificialStateBuilder<IN> artificialStateBuilders) {
		this(mapFunction, Collections.singletonList(artificialStateBuilders));
	}

	public ArtificialKeyedStateMapper(
		MapFunction<IN, OUT> mapFunction,
		List<ArtificialStateBuilder<IN>> artificialStateBuilders) {

		this.mapFunction = mapFunction;
		this.artificialStateBuilders = artificialStateBuilders;
		Set<String> stateNames = new HashSet<>(this.artificialStateBuilders.size());
		for (ArtificialStateBuilder<IN> stateBuilder : this.artificialStateBuilders) {
			if (!stateNames.add(stateBuilder.getStateName())) {
				throw new IllegalArgumentException("Duplicated state name: " + stateBuilder.getStateName());
			}
		}
	}

	@Override
	public OUT map(IN value) throws Exception {
		for (ArtificialStateBuilder<IN> stateBuilder : artificialStateBuilders) {
			stateBuilder.artificialStateForElement(value);
		}

		return mapFunction.map(value);
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		for (ArtificialStateBuilder<IN> stateBuilder : artificialStateBuilders) {
			stateBuilder.initialize(context);
		}
	}
}
