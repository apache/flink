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

package org.apache.flink.runtime.state.ttl.mock;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyExtractorFunction;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.PriorityComparator;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.StateSnapshotTransformer.StateSnapshotTransformFactory;
import org.apache.flink.runtime.state.StateSnapshotTransformers;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSet;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.ttl.TtlStateFactory;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** State backend which produces in memory mock state objects. */
public class MockKeyedStateBackend<K> extends AbstractKeyedStateBackend<K> {

	private interface StateFactory {
		<N, SV, S extends State, IS extends S> IS createInternalState(
			TypeSerializer<N> namespaceSerializer,
			StateDescriptor<S, SV> stateDesc) throws Exception;
	}

	private static final Map<Class<? extends StateDescriptor>, StateFactory> STATE_FACTORIES =
		Stream.of(
			Tuple2.of(ValueStateDescriptor.class, (StateFactory) MockInternalValueState::createState),
			Tuple2.of(ListStateDescriptor.class, (StateFactory) MockInternalListState::createState),
			Tuple2.of(MapStateDescriptor.class, (StateFactory) MockInternalMapState::createState),
			Tuple2.of(ReducingStateDescriptor.class, (StateFactory) MockInternalReducingState::createState),
			Tuple2.of(AggregatingStateDescriptor.class, (StateFactory) MockInternalAggregatingState::createState)
		).collect(Collectors.toMap(t -> t.f0, t -> t.f1));

	private final Map<String, Map<K, Map<Object, Object>>> stateValues;

	private final Map<String, StateSnapshotTransformer<Object>> stateSnapshotFilters;

	MockKeyedStateBackend(
		TaskKvStateRegistry kvStateRegistry,
		TypeSerializer<K> keySerializer,
		ClassLoader userCodeClassLoader,
		ExecutionConfig executionConfig,
		TtlTimeProvider ttlTimeProvider,
		Map<String, Map<K, Map<Object, Object>>> stateValues,
		Map<String, StateSnapshotTransformer<Object>> stateSnapshotFilters,
		CloseableRegistry cancelStreamRegistry,
		InternalKeyContext<K> keyContext) {
		super(kvStateRegistry, keySerializer, userCodeClassLoader,
			executionConfig, ttlTimeProvider, cancelStreamRegistry, keyContext);
		this.stateValues = stateValues;
		this.stateSnapshotFilters = stateSnapshotFilters;
	}

	@Override
	@SuppressWarnings("unchecked")
	@Nonnull
	public <N, SV, SEV, S extends State, IS extends S> IS createInternalState(
		@Nonnull TypeSerializer<N> namespaceSerializer,
		@Nonnull StateDescriptor<S, SV> stateDesc,
		@Nonnull StateSnapshotTransformFactory<SEV> snapshotTransformFactory) throws Exception {
		StateFactory stateFactory = STATE_FACTORIES.get(stateDesc.getClass());
		if (stateFactory == null) {
			String message = String.format("State %s is not supported by %s",
				stateDesc.getClass(), TtlStateFactory.class);
			throw new FlinkRuntimeException(message);
		}
		IS state = stateFactory.createInternalState(namespaceSerializer, stateDesc);
		stateSnapshotFilters.put(stateDesc.getName(),
			(StateSnapshotTransformer<Object>) getStateSnapshotTransformer(stateDesc, snapshotTransformFactory));
		((MockInternalKvState<K, N, SV>) state).values = () -> stateValues
			.computeIfAbsent(stateDesc.getName(), n -> new HashMap<>())
			.computeIfAbsent(getCurrentKey(), k -> new HashMap<>());
		return state;
	}

	@SuppressWarnings("unchecked")
	private <SV, SEV> StateSnapshotTransformer<SV> getStateSnapshotTransformer(
		StateDescriptor<?, SV> stateDesc,
		StateSnapshotTransformFactory<SEV> snapshotTransformFactory) {
		Optional<StateSnapshotTransformer<SEV>> original = snapshotTransformFactory.createForDeserializedState();
		if (original.isPresent()) {
			if (stateDesc instanceof ListStateDescriptor) {
				return (StateSnapshotTransformer<SV>) new StateSnapshotTransformers.ListStateSnapshotTransformer<>(original.get());
			} else if (stateDesc instanceof MapStateDescriptor) {
				return (StateSnapshotTransformer<SV>) new StateSnapshotTransformers.MapStateSnapshotTransformer<>(original.get());
			} else {
				return (StateSnapshotTransformer<SV>) original.get();
			}
		} else {
			return null;
		}
	}

	@Override
	public int numKeyValueStateEntries() {
		int count = 0;
		for (String state : stateValues.keySet()) {
			for (K key : stateValues.get(state).keySet()) {
				count += stateValues.get(state).get(key).size();
			}
		}
		return count;
	}

	@Override
	public boolean requiresLegacySynchronousTimerSnapshots() {
		return false;
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) {
		// noop
	}

	@Override
	public void notifyCheckpointAborted(long checkpointId) {
		// noop
	}

	@Override
	public <N> Stream<K> getKeys(String state, N namespace) {
		return stateValues.get(state).entrySet().stream()
			.filter(e -> e.getValue().containsKey(namespace))
			.map(Map.Entry::getKey);
	}

	@Override
	@SuppressWarnings("unchecked")
	public <N> Stream<Tuple2<K, N>> getKeysAndNamespaces(String state) {
		return stateValues.get(state).entrySet().stream()
			.flatMap(entry ->
				entry.getValue().entrySet().stream()
					.map(namespace ->
						Tuple2.of(entry.getKey(), (N) namespace.getKey())));
	}

	@Nonnull
	@Override
	public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
		long checkpointId,
		long timestamp,
		@Nonnull CheckpointStreamFactory streamFactory,
		@Nonnull CheckpointOptions checkpointOptions) {
		return new FutureTask<>(() ->
			SnapshotResult.of(new MockKeyedStateHandle<>(copy(stateValues, stateSnapshotFilters))));
	}

	static <K> Map<String, Map<K, Map<Object, Object>>> copy(
		Map<String, Map<K, Map<Object, Object>>> stateValues, Map<String, StateSnapshotTransformer<Object>> stateSnapshotFilters) {
		Map<String, Map<K, Map<Object, Object>>> snapshotStates = new HashMap<>();
		for (String stateName : stateValues.keySet()) {
			StateSnapshotTransformer<Object> stateSnapshotTransformer = stateSnapshotFilters.getOrDefault(stateName, null);
			Map<K, Map<Object, Object>> keyedValues = snapshotStates.computeIfAbsent(stateName, s -> new HashMap<>());
			for (K key : stateValues.get(stateName).keySet()) {
				Map<Object, Object> snapshotedValues = keyedValues.computeIfAbsent(key, s -> new HashMap<>());
				for (Object namespace : stateValues.get(stateName).get(key).keySet()) {
					copyEntry(stateValues, snapshotedValues, stateName, key, namespace, stateSnapshotTransformer);
				}
			}
		}
		return snapshotStates;
	}

	@SuppressWarnings("unchecked")
	private static <K> void copyEntry(
		Map<String, Map<K, Map<Object, Object>>> stateValues,
		Map<Object, Object> snapshotedValues,
		String stateName,
		K key,
		Object namespace,
		StateSnapshotTransformer<Object> stateSnapshotTransformer) {
		Object value = stateValues.get(stateName).get(key).get(namespace);
		value = value instanceof List ? new ArrayList<>((List) value) : value;
		value = value instanceof Map ? new HashMap<>((Map) value) : value;
		Object filteredValue = stateSnapshotTransformer == null ? value : stateSnapshotTransformer.filterOrTransform(value);
		if (filteredValue != null) {
			snapshotedValues.put(namespace, filteredValue);
		}
	}

	@Nonnull
	@Override
	public <T extends HeapPriorityQueueElement & PriorityComparable & Keyed> KeyGroupedInternalPriorityQueue<T>
	create(
		@Nonnull String stateName,
		@Nonnull TypeSerializer<T> byteOrderedElementSerializer) {
		return new HeapPriorityQueueSet<>(
			PriorityComparator.forPriorityComparableObjects(),
			KeyExtractorFunction.forKeyedObjects(),
			0,
			keyGroupRange,
			0);
	}

	static class MockKeyedStateHandle<K> implements KeyedStateHandle {
		private static final long serialVersionUID = 1L;

		final Map<String, Map<K, Map<Object, Object>>> snapshotStates;

		MockKeyedStateHandle(Map<String, Map<K, Map<Object, Object>>> snapshotStates) {
			this.snapshotStates = snapshotStates;
		}

		@Override
		public void discardState() {
			snapshotStates.clear();
		}

		@Override
		public long getStateSize() {
			throw new UnsupportedOperationException();
		}

		@Override
		public void registerSharedStates(SharedStateRegistry stateRegistry) {

		}

		@Override
		public KeyGroupRange getKeyGroupRange() {
			throw new UnsupportedOperationException();
		}

		@Override
		public KeyedStateHandle getIntersection(KeyGroupRange keyGroupRange) {
			throw new UnsupportedOperationException();
		}
	}
}
