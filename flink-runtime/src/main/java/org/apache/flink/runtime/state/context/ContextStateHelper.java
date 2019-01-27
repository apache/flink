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

package org.apache.flink.runtime.state.context;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.FoldingState;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.SortedMapState;
import org.apache.flink.api.common.state.SortedMapStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateBinder;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractInternalStateBackend;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.heap.KeyContextImpl;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.keyed.KeyedListState;
import org.apache.flink.runtime.state.keyed.KeyedListStateDescriptor;
import org.apache.flink.runtime.state.keyed.KeyedMapState;
import org.apache.flink.runtime.state.keyed.KeyedMapStateDescriptor;
import org.apache.flink.runtime.state.keyed.KeyedSortedMapState;
import org.apache.flink.runtime.state.keyed.KeyedSortedMapStateDescriptor;
import org.apache.flink.runtime.state.keyed.KeyedValueState;
import org.apache.flink.runtime.state.keyed.KeyedValueStateDescriptor;
import org.apache.flink.runtime.state.subkeyed.SubKeyedListState;
import org.apache.flink.runtime.state.subkeyed.SubKeyedListStateDescriptor;
import org.apache.flink.runtime.state.subkeyed.SubKeyedMapState;
import org.apache.flink.runtime.state.subkeyed.SubKeyedMapStateDescriptor;
import org.apache.flink.runtime.state.subkeyed.SubKeyedValueState;
import org.apache.flink.runtime.state.subkeyed.SubKeyedValueStateDescriptor;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A helper to create {@link ContextKeyedState} and {@link ContextSubKeyedState}.
 */
public class ContextStateHelper implements StateBinder {

	/** All {@link State}s created by this Heper. */
	private final Map<String, State> states;

	private final KeyContextImpl keyContext;

	private final ExecutionConfig executionConfig;

	private final AbstractInternalStateBackend internalStateBackend;

	/** For caching the last accessed state. */
	private String lastStateName;

	private InternalKvState lastState;

	private final TaskKvStateRegistry kvStateRegistry;

	public ContextStateHelper(
		KeyContextImpl keyContext,
		ExecutionConfig executionConfig,
		AbstractInternalStateBackend internalStateBackend) {

		this.keyContext = Preconditions.checkNotNull(keyContext);
		this.executionConfig = Preconditions.checkNotNull(executionConfig);
		this.internalStateBackend = Preconditions.checkNotNull(internalStateBackend);
		this.kvStateRegistry = internalStateBackend.getKvStateRegistry();
		this.states = new HashMap<>();
	}

	public String getLastStateName() {
		return lastStateName;
	}

	public InternalKvState getLastState() {
		return lastState;
	}

	@Override
	public <T> ValueState<T> createValueState(ValueStateDescriptor<T> stateDesc) throws Exception {
		Preconditions.checkNotNull(stateDesc, "stateDesc cannot be null.");

		String stateName = stateDesc.getName();

		State state = states.get(stateName);

		if (state == null) {
			stateDesc.initializeSerializerUnlessSet(executionConfig);

			KeyedValueStateDescriptor<Object, T> keyedStateDescriptor =
				new KeyedValueStateDescriptor<Object, T>(
					stateDesc.getName(),
					keyContext.getKeySerializer(),
					stateDesc.getSerializer()
				);

			KeyedValueState<Object, T> keyedState = internalStateBackend.getKeyedState(keyedStateDescriptor);

			state = new ContextValueState<>(keyContext, keyedState, stateDesc);
			registerAsQueryableState(stateDesc, state);
			states.put(stateName, state);
		}
		return (ValueState) state;
	}

	public <N, T> ValueState<T> createValueState(
		TypeSerializer<N> namespaceSerializer,
		ValueStateDescriptor<T> stateDesc) throws Exception {

		Preconditions.checkNotNull(namespaceSerializer, "namespaceSerializer cannot be null.");

		State state;

		if (VoidNamespaceSerializer.INSTANCE.equals(namespaceSerializer)) {
			state = createValueState(stateDesc);
		} else {
			Preconditions.checkNotNull(stateDesc, "stateDesc cannot be null.");

			String stateName = stateDesc.getName();

			state = states.get(stateName);
			if (state == null) {
				stateDesc.initializeSerializerUnlessSet(executionConfig);

				SubKeyedValueStateDescriptor<Object, N, T> subKeyedValueStateDescriptor =
					new SubKeyedValueStateDescriptor<Object, N, T>(
						stateDesc.getName(),
						keyContext.getKeySerializer(),
						namespaceSerializer,
						stateDesc.getSerializer()
					);

				SubKeyedValueState<Object, N, T> subKeyedValueState = internalStateBackend.getSubKeyedState(subKeyedValueStateDescriptor);
				state = new ContextSubKeyedValueState<>(this.keyContext, subKeyedValueState, stateDesc.getDefaultValue());
				registerAsQueryableState(stateDesc, state);

				states.put(stateName, state);
			}

		}

		return (ValueState) state;
	}

	@Override
	public <T> ListState<T> createListState(ListStateDescriptor<T> stateDesc) throws Exception {
		Preconditions.checkNotNull(stateDesc, "stateDesc cannot be null.");

		String stateName = stateDesc.getName();

		State state = states.get(stateName);

		if (state == null) {
			stateDesc.initializeSerializerUnlessSet(executionConfig);

			KeyedListStateDescriptor<Object, T> keyedStateDescriptor =
				new KeyedListStateDescriptor<>(
					stateDesc.getName(),
					keyContext.getKeySerializer(),
					stateDesc.getElementSerializer()
				);

			KeyedListState<Object, T> keyedState = internalStateBackend.getKeyedState(keyedStateDescriptor);

			state = new ContextListState<>(keyContext, keyedState);
			registerAsQueryableState(stateDesc, state);
			states.put(stateName, state);
		}
		return (ListState) state;
	}

	public <N, T> ListState<T> createListState(
		TypeSerializer<N> namespaceSerializer,
		ListStateDescriptor<T> stateDesc) throws Exception {

		Preconditions.checkNotNull(namespaceSerializer, "namespaceSerializer cannot be null.");

		State state;
		if (VoidNamespaceSerializer.INSTANCE.equals(namespaceSerializer)) {
			state = createListState(stateDesc);
		} else {
			Preconditions.checkNotNull(stateDesc, "stateDesc cannot be null.");

			String stateName = stateDesc.getName();

			state = states.get(stateName);
			if (state == null) {
				stateDesc.initializeSerializerUnlessSet(executionConfig);

				SubKeyedListStateDescriptor<Object, N, T> subKeyedListStateDescriptor =
					new SubKeyedListStateDescriptor<>(
						stateDesc.getName(),
						keyContext.getKeySerializer(),
						namespaceSerializer,
						stateDesc.getElementSerializer()
					);

				SubKeyedListState<Object, N, T> subKeyedListState = internalStateBackend.getSubKeyedState(subKeyedListStateDescriptor);
				state = new ContextSubKeyedListState<>(keyContext, subKeyedListState);
				registerAsQueryableState(stateDesc, state);
				states.put(stateName, state);
			}
		}

		return (ListState) state;
	}

	@Override
	public <T> ReducingState<T> createReducingState(ReducingStateDescriptor<T> stateDesc) throws Exception {
		Preconditions.checkNotNull(stateDesc, "stateDesc cannot be null.");

		String stateName = stateDesc.getName();

		State state = states.get(stateName);

		if (state == null) {
			stateDesc.initializeSerializerUnlessSet(executionConfig);

			KeyedValueStateDescriptor<Object, T> keyedStateDescriptor =
				new KeyedValueStateDescriptor<>(
					stateDesc.getName(),
					keyContext.getKeySerializer(),
					stateDesc.getSerializer()
				);

			KeyedValueState<Object, T> keyedState = internalStateBackend.getKeyedState(keyedStateDescriptor);

			state = new ContextReducingState<>(keyContext, keyedState, stateDesc.getReduceFunction());
			registerAsQueryableState(stateDesc, state);
			states.put(stateName, state);
		}
		return (ReducingState) state;
	}

	public <N, T> ReducingState<T> createReducingState(
		TypeSerializer<N> namespaceSerializer,
		ReducingStateDescriptor<T> stateDesc) throws Exception {

		Preconditions.checkNotNull(namespaceSerializer, "namespaceSerializer cannot be null.");

		State state;
		if (VoidNamespaceSerializer.INSTANCE.equals(namespaceSerializer)) {
			state = createReducingState(stateDesc);
		} else {
			Preconditions.checkNotNull(stateDesc, "stateDesc cannot be null.");

			String stateName = stateDesc.getName();

			state = states.get(stateName);
			if (state == null) {
				stateDesc.initializeSerializerUnlessSet(executionConfig);

				SubKeyedValueStateDescriptor<Object, N, T> subKeyedValueStateDescriptor =
					new SubKeyedValueStateDescriptor<>(
						stateDesc.getName(),
						keyContext.getKeySerializer(),
						namespaceSerializer,
						stateDesc.getSerializer()
					);

				SubKeyedValueState<Object, N, T> subKeyedValueState = internalStateBackend.getSubKeyedState(subKeyedValueStateDescriptor);
				state = new ContextSubKeyedReducingState<>(
					keyContext,
					subKeyedValueState,
					stateDesc.getReduceFunction()
				);
				registerAsQueryableState(stateDesc, state);
				states.put(stateName, state);
			}
		}

		return (ReducingState) state;
	}

	@Override
	public <IN, ACC, OUT> AggregatingState<IN, OUT> createAggregatingState(AggregatingStateDescriptor<IN, ACC, OUT> stateDesc) throws Exception {
		Preconditions.checkNotNull(stateDesc, "stateDesc cannot be null.");

		String stateName = stateDesc.getName();

		State state = states.get(stateName);

		if (state == null) {
			stateDesc.initializeSerializerUnlessSet(executionConfig);

			KeyedValueStateDescriptor<Object, ACC> keyedStateDescriptor =
				new KeyedValueStateDescriptor<>(
					stateDesc.getName(),
					keyContext.getKeySerializer(),
					stateDesc.getSerializer());

			KeyedValueState<Object, ACC> keyedState = internalStateBackend.getKeyedState(keyedStateDescriptor);

			state = new ContextAggregatingState<>(keyContext, keyedState, stateDesc.getAggregateFunction());
			registerAsQueryableState(stateDesc, state);
			states.put(stateName, state);
		}
		return (AggregatingState) state;
	}

	public <N, IN, ACC, OUT> AggregatingState<IN, OUT> createAggregatingState(
		TypeSerializer<N> namespaceSerializer,
		AggregatingStateDescriptor<IN, ACC, OUT> stateDesc) throws Exception {

		Preconditions.checkNotNull(namespaceSerializer, "namespaceSerializer cannot be null.");

		State state;
		if (VoidNamespaceSerializer.INSTANCE.equals(namespaceSerializer)) {
			state = createAggregatingState(stateDesc);
		} else {
			Preconditions.checkNotNull(stateDesc, "stateDesc cannot be null.");

			String stateName = stateDesc.getName();

			state = states.get(stateName);
			if (state == null) {
				stateDesc.initializeSerializerUnlessSet(executionConfig);

				SubKeyedValueStateDescriptor<Object, N, ACC> subKeyedValueStateDescriptor =
					new SubKeyedValueStateDescriptor<>(
						stateDesc.getName(),
						keyContext.getKeySerializer(),
						namespaceSerializer,
						stateDesc.getSerializer()
					);
				SubKeyedValueState<Object, N, ACC> subKeyedValueState = internalStateBackend.getSubKeyedState(subKeyedValueStateDescriptor);
				state =
					new ContextSubKeyedAggregatingState<>(
						keyContext,
						subKeyedValueState,
						stateDesc.getAggregateFunction()
					);
				registerAsQueryableState(stateDesc, state);
				states.put(stateName, state);
			}
		}

		return (AggregatingState) state;
	}

	@Override
	public <T, ACC> FoldingState<T, ACC> createFoldingState(FoldingStateDescriptor<T, ACC> stateDesc) throws Exception {
		Preconditions.checkNotNull(stateDesc, "stateDesc cannot be null.");

		String stateName = stateDesc.getName();

		State state = states.get(stateName);

		if (state == null) {
			stateDesc.initializeSerializerUnlessSet(executionConfig);

			KeyedValueStateDescriptor<Object, ACC> keyedStateDescriptor =
				new KeyedValueStateDescriptor<>(
					stateDesc.getName(),
					keyContext.getKeySerializer(),
					stateDesc.getSerializer()
				);

			KeyedValueState<Object, ACC> keyedState = internalStateBackend.getKeyedState(keyedStateDescriptor);

			state = new ContextFoldingState<>(keyContext, keyedState, stateDesc);
			registerAsQueryableState(stateDesc, state);
			states.put(stateName, state);
		}
		return (FoldingState) state;
	}

	public <N, IN, ACC> FoldingState<IN, ACC> createFoldingState(
		TypeSerializer<N> namespaceSerializer,
		FoldingStateDescriptor<IN, ACC> stateDesc) throws Exception {

		Preconditions.checkNotNull(namespaceSerializer, "namespaceSerializer cannot be null.");

		State state;
		if (VoidNamespaceSerializer.INSTANCE.equals(namespaceSerializer)) {
			state = createFoldingState(stateDesc);
		} else {
			Preconditions.checkNotNull(stateDesc, "stateDesc cannot be null.");

			String stateName = stateDesc.getName();

			state = states.get(stateName);
			if (state == null) {
				stateDesc.initializeSerializerUnlessSet(executionConfig);

				SubKeyedValueStateDescriptor<Object, N, ACC> subKeyedValueStateDescriptor =
					new SubKeyedValueStateDescriptor<>(
						stateDesc.getName(),
						keyContext.getKeySerializer(),
						namespaceSerializer,
						stateDesc.getSerializer()
					);

				SubKeyedValueState<Object, N, ACC> subKeyedValueState = internalStateBackend.getSubKeyedState(subKeyedValueStateDescriptor);

				state = new ContextSubKeyedFoldingState<>(
					keyContext,
					subKeyedValueState,
					stateDesc
				);
				registerAsQueryableState(stateDesc, state);
				states.put(stateName, state);
			}
		}

		return (FoldingState) state;
	}

	@Override
	public <MK, MV> MapState<MK, MV> createMapState(MapStateDescriptor<MK, MV> stateDesc) throws Exception {
		Preconditions.checkNotNull(stateDesc, "stateDesc cannot be null.");

		String stateName = stateDesc.getName();

		State state = states.get(stateName);

		if (state == null) {
			stateDesc.initializeSerializerUnlessSet(executionConfig);

			KeyedMapStateDescriptor<Object, MK, MV> keyedStateDescriptor =
				new KeyedMapStateDescriptor<>(
					stateDesc.getName(),
					keyContext.getKeySerializer(),
					stateDesc.getKeySerializer(),
					stateDesc.getValueSerializer()
				);

			KeyedMapState<Object, MK, MV> keyedState = internalStateBackend.getKeyedState(keyedStateDescriptor);

			state = new ContextMapState<>(keyContext, keyedState);
			registerAsQueryableState(stateDesc, state);

			states.put(stateName, state);
		}
		return (MapState) state;
	}

	public <N, MK, MV> MapState<MK, MV> createMapState(
		TypeSerializer<N> namespaceSerializer,
		MapStateDescriptor<MK, MV> stateDesc) throws Exception {

		Preconditions.checkNotNull(namespaceSerializer, "namespaceSerializer cannot be null.");

		State state;
		if (VoidNamespaceSerializer.INSTANCE.equals(namespaceSerializer)) {
			state = createMapState(stateDesc);
		} else {
			Preconditions.checkNotNull(stateDesc, "stateDesc cannot be null.");

			String stateName = stateDesc.getName();

			state = states.get(stateName);
			if (state == null) {
				stateDesc.initializeSerializerUnlessSet(executionConfig);

				SubKeyedMapStateDescriptor<Object, N, MK, MV> subKeyedMapStateDescriptor = new SubKeyedMapStateDescriptor(
					stateDesc.getName(),
					keyContext.getKeySerializer(),
					namespaceSerializer,
					stateDesc.getKeySerializer(),
					stateDesc.getValueSerializer()
				);

				SubKeyedMapState<Object, N, MK, MV> subKeyedMapState = internalStateBackend.getSubKeyedState(subKeyedMapStateDescriptor);

				state = new ContextSubKeyedMapState<Object, N, MK, MV>(
					keyContext,
					subKeyedMapState);

				registerAsQueryableState(stateDesc, state);

				states.put(stateName, state);
			}
		}

		return (MapState) state;
	}

	@Override
	public <MK, MV> SortedMapState<MK, MV> createSortedMapState(SortedMapStateDescriptor<MK, MV> stateDesc) throws Exception {
		Preconditions.checkNotNull(stateDesc, "stateDesc cannot be null.");

		String stateName = stateDesc.getName();

		State state = states.get(stateName);

		if (state == null) {
			stateDesc.initializeSerializerUnlessSet(executionConfig);

			KeyedSortedMapStateDescriptor<Object, MK, MV> keyedStateDescriptor =
				new KeyedSortedMapStateDescriptor<>(
					stateDesc.getName(),
					keyContext.getKeySerializer(),
					stateDesc.getSerializer()
				);

			KeyedSortedMapState<Object, MK, MV> keyedState = internalStateBackend.getKeyedState(keyedStateDescriptor);

			state = new ContextSortedMapState<>(keyContext, keyedState);
			registerAsQueryableState(stateDesc, state);

			states.put(stateName, state);
		}
		return (SortedMapState) state;
	}

	public <N, MK, MV> SortedMapState<MK, MV> createSortedMapState(
		TypeSerializer<N> namespaceSerializer,
		SortedMapStateDescriptor<MK, MV> stateDesc) throws Exception {
		Preconditions.checkNotNull(namespaceSerializer, "namespaceSerializer cannot be null.");
		Preconditions.checkNotNull(stateDesc, "stateDesc cannot be null.");

		String stateName = stateDesc.getName();

		State state = states.get(stateName);
		if (state == null) {
			if (VoidNamespaceSerializer.INSTANCE.equals(namespaceSerializer)) {
				return createSortedMapState(stateDesc);
			}
		}
		throw new UnsupportedOperationException("Not supported to create sorted map state with namespace.");
	}

	public <N, S extends State> S getOrCreateKeyedState(
		TypeSerializer<N> namespaceSerializer,
		StateDescriptor<S, ?> stateDescriptor) throws Exception {

		Preconditions.checkNotNull(stateDescriptor);
		Preconditions.checkNotNull(namespaceSerializer, "namespaceSerializer cannot be null.");

		String stateName = stateDescriptor.getName();

		if (lastStateName != null && lastStateName.equals(stateName)) {
			return (S) lastState;
		}

		State state = null;

		switch (stateDescriptor.getType()) {
			case VALUE:
				state = createValueState(
					namespaceSerializer, (ValueStateDescriptor<?>) stateDescriptor);
				break;
			case LIST:
				state = createListState(
					namespaceSerializer, (ListStateDescriptor<?>) stateDescriptor);
				break;
			case MAP:
				state = createMapState(
					namespaceSerializer, (MapStateDescriptor<?, ?>) stateDescriptor);
				break;
			case FOLDING:
				state = createFoldingState(
					namespaceSerializer, (FoldingStateDescriptor<?, ?>) stateDescriptor);
				break;
			case REDUCING:
				state = createReducingState(
					namespaceSerializer, (ReducingStateDescriptor<?>) stateDescriptor);
				break;
			case AGGREGATING:
				state = createAggregatingState(
					namespaceSerializer, (AggregatingStateDescriptor<?, ?, ?>) stateDescriptor);
				break;
			case SORTEDMAP:
				state = createSortedMapState(
					namespaceSerializer, (SortedMapStateDescriptor<?, ?>) stateDescriptor);
				break;
			default:
				throw new RuntimeException("Not a supported State: " + stateDescriptor.getType());
		}

		lastStateName = stateName;
		lastState = (InternalKvState) state;

		return (S) state;
	}

	public <N, S extends State> S getPartitionedState(
		final N namespace,
		final TypeSerializer<N> namespaceSerializer,
		final StateDescriptor<S, ?> stateDescriptor) throws Exception {

		checkNotNull(namespace, "Namespace cannot be null.");

		String stateName = stateDescriptor.getName();

		if (lastStateName != null && lastStateName.equals(stateName)) {
			lastState.setCurrentNamespace(namespace);
			return (S) lastState;
		}

		State previous = states.get(stateName);
		if (previous != null) {
			lastState = (InternalKvState) previous;
			lastState.setCurrentNamespace(namespace);
			lastStateName = stateName;
			return (S) previous;
		}

		final S state = getOrCreateKeyedState(namespaceSerializer, stateDescriptor);
		InternalKvState kvState = (InternalKvState) state;
		lastStateName = stateName;
		lastState = kvState;
		kvState.setCurrentNamespace(namespace);

		return state;
	}

	public KeyContextImpl getKeyContext() {
		return keyContext;
	}

	public AbstractInternalStateBackend getInternalStateBackend() {
		return internalStateBackend;
	}

	public ExecutionConfig getExecutionConfig() {
		return executionConfig;
	}

	public void dispose() {
		if (kvStateRegistry != null) {
			kvStateRegistry.unregisterAll();
		}
		states.clear();
		lastState = null;
		lastStateName = null;
	}

	private void registerAsQueryableState(StateDescriptor stateDesc, State state) {
		if (stateDesc.isQueryable()) {
			Preconditions.checkNotNull(kvStateRegistry, "Can not register queryable state, because the registry is null.");
			kvStateRegistry.registerKvState(internalStateBackend.getKeyGroupRange(), stateDesc.getQueryableStateName(), (InternalKvState) state);
		}
	}
}
