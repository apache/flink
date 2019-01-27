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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.context.ContextStateHelper;
import org.apache.flink.runtime.state.heap.HeapInternalStateBackend;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;
import org.apache.flink.runtime.state.internal.InternalFoldingState;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.internal.InternalReducingState;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.runtime.state.keyed.KeyedState;
import org.apache.flink.runtime.state.subkeyed.SubKeyedState;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.RunnableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Wrapper of {@link InternalStateBackend} to support backward compatibility for {@link AbstractKeyedStateBackend}.
 * @param <K>
 */
public class KeyedStateBackendWrapper<K> extends AbstractKeyedStateBackend<K> {

	private final AbstractInternalStateBackend internalStateBackend;

	/** The binder to create user-facing states. */
	protected transient ContextStateHelper contextStateHelper;

	public KeyedStateBackendWrapper(
		ContextStateHelper contextStateHelper) {

		super(contextStateHelper.getInternalStateBackend().getKvStateRegistry(),
			contextStateHelper.getKeyContext().getKeySerializer(),
			contextStateHelper.getInternalStateBackend().getUserClassLoader(),
			contextStateHelper.getInternalStateBackend().getNumGroups(),
			contextStateHelper.getInternalStateBackend().getKeyGroupRange(),
			contextStateHelper.getExecutionConfig());

		this.contextStateHelper = contextStateHelper;
		this.internalStateBackend = contextStateHelper.getInternalStateBackend();
	}

	@Override
	protected <N, T> InternalValueState<K, N, T> createValueState(
		TypeSerializer<N> namespaceSerializer,
		ValueStateDescriptor<T> stateDesc) throws Exception {

		return (InternalValueState<K, N, T>) contextStateHelper.createValueState(namespaceSerializer, stateDesc);
	}

	@Override
	protected <N, T> InternalListState<K, N, T> createListState(
		TypeSerializer<N> namespaceSerializer,
		ListStateDescriptor<T> stateDesc) throws Exception {

		return (InternalListState<K, N, T>) contextStateHelper.createListState(namespaceSerializer, stateDesc);
	}

	@Override
	protected <N, T> InternalReducingState<K, N, T> createReducingState(
		TypeSerializer<N> namespaceSerializer,
		ReducingStateDescriptor<T> stateDesc) throws Exception {

		return (InternalReducingState<K, N, T>) contextStateHelper.createReducingState(namespaceSerializer, stateDesc);
	}

	@Override
	protected <N, T, ACC, R> InternalAggregatingState<K, N, T, ACC, R> createAggregatingState(
		TypeSerializer<N> namespaceSerializer,
		AggregatingStateDescriptor<T, ACC, R> stateDesc) throws Exception {

		return (InternalAggregatingState<K, N, T, ACC, R>) contextStateHelper.createAggregatingState(namespaceSerializer, stateDesc);
	}

	@Override
	protected <N, T, ACC> InternalFoldingState<K, N, T, ACC> createFoldingState(
		TypeSerializer<N> namespaceSerializer,
		FoldingStateDescriptor<T, ACC> stateDesc) throws Exception {

		return (InternalFoldingState<K, N, T, ACC>) contextStateHelper.createFoldingState(namespaceSerializer, stateDesc);
	}

	@Override
	protected <N, UK, UV> InternalMapState<K, N, UK, UV> createMapState(
		TypeSerializer<N> namespaceSerializer,
		MapStateDescriptor<UK, UV> stateDesc) throws Exception {

		return (InternalMapState<K, N, UK, UV>) contextStateHelper.createMapState(namespaceSerializer, stateDesc);
	}

	@Override
	public int numStateEntries() {
		return internalStateBackend.numStateEntries();
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		internalStateBackend.notifyCheckpointComplete(checkpointId);
	}

	@Override
	public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
		long checkpointId,
		long timestamp,
		CheckpointStreamFactory streamFactory,
		CheckpointOptions checkpointOptions) throws Exception {

		return internalStateBackend.snapshot(checkpointId, timestamp, streamFactory, checkpointOptions);
	}

	@Override
	public void restore(Collection<KeyedStateHandle> state) throws Exception {
		internalStateBackend.restore(state);
	}

	@Override
	public void setCurrentKey(K newKey) {
		contextStateHelper.getKeyContext().setCurrentKey(newKey);
	}

	@Override
	public <N, S extends State, T> void applyToAllKeys(
		N namespace,
		TypeSerializer<N> namespaceSerializer,
		StateDescriptor<S, T> stateDescriptor,
		KeyedStateFunction<K, S> function) throws Exception {

		try (Stream<K> keyStream = getKeys(stateDescriptor.getName(), namespace)) {

			final S state = getPartitionedState(
				namespace,
				namespaceSerializer,
				stateDescriptor);

			if (internalStateBackend instanceof HeapInternalStateBackend) {
				final List<K> keys = keyStream.collect(Collectors.toList());
				for (K key : keys) {
					setCurrentKey(key);
					function.process(key, state);
				}
			} else {
				keyStream.forEach((K key) -> {
					setCurrentKey(key);
					try {
						function.process(key, state);
					} catch (Throwable e) {
						// we wrap the checked exception in an unchecked
						// one and catch it (and re-throw it) later.
						throw new RuntimeException(e);
					}
				});
			}
		}
	}

	@Override
	public <N, S extends State, T> S getOrCreateKeyedState(TypeSerializer<N> namespaceSerializer, StateDescriptor<S, T> stateDescriptor) throws Exception {
		return contextStateHelper.getOrCreateKeyedState(namespaceSerializer, stateDescriptor);
	}

	@Override
	public <N, S extends State> S getPartitionedState(
		N namespace,
		TypeSerializer<N> namespaceSerializer,
		StateDescriptor<S, ?> stateDescriptor) throws Exception {

		return contextStateHelper.getPartitionedState(namespace, namespaceSerializer, stateDescriptor);
	}

	@Override
	public void dispose() {
		internalStateBackend.dispose();
		contextStateHelper.dispose();
	}

	@Override
	public void close() throws IOException {
		internalStateBackend.close();
	}

	@Override
	public K getCurrentKey() {
		return (K) contextStateHelper.getKeyContext().getCurrentKey();
	}

	@Override
	public int getCurrentKeyGroupIndex() {
		return contextStateHelper.getKeyContext().getCurrentKeyGroupIndex();
	}

	@Override
	public int getNumberOfKeyGroups() {
		return internalStateBackend.getNumGroups();
	}

	@Override
	public KeyGroupRange getKeyGroupRange() {
		return internalStateBackend.getKeyGroupRange();
	}

	@Override
	public TypeSerializer<K> getKeySerializer() {
		return contextStateHelper.getKeyContext().getKeySerializer();
	}

	@Override
	public <N> Stream<K> getKeys(String state, N namespace) {
		KeyedState keyedState = internalStateBackend.getKeyedStates().get(state);
		if (keyedState != null) {
			Preconditions.checkState(VoidNamespace.get().equals(namespace),
				"Expected VoidNamespace when getKeys over keyedState.");
			Iterable<K> iterable = keyedState.keys();
			return StreamSupport.stream(iterable.spliterator(), false);
		}

		SubKeyedState subKeyedState = internalStateBackend.getSubKeyedStates().get(state);
		if (subKeyedState != null) {
			return StreamSupport.stream(subKeyedState.keys(namespace).spliterator(), false);
		}

		return Stream.empty();
	}

	@Override
	public StreamCompressionDecorator getKeyGroupCompressionDecorator() {
		return UncompressedStreamCompressionDecorator.INSTANCE;
	}
}
