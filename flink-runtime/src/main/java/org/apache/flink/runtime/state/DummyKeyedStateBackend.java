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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;
import org.apache.flink.runtime.state.internal.InternalFoldingState;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.internal.InternalReducingState;
import org.apache.flink.runtime.state.internal.InternalValueState;

import java.util.Collection;
import java.util.concurrent.RunnableFuture;

import java.util.stream.Stream;

/**
 * Dummy Keyed StateBackend.
 */
public class DummyKeyedStateBackend<K> extends AbstractKeyedStateBackend<K> {

	public DummyKeyedStateBackend(TaskKvStateRegistry kvStateRegistry, TypeSerializer<K> keySerializer, ClassLoader userCodeClassLoader, int numberOfKeyGroups, KeyGroupRange keyGroupRange, ExecutionConfig executionConfig) {
		super(kvStateRegistry, keySerializer, userCodeClassLoader, numberOfKeyGroups, keyGroupRange, executionConfig);
	}

	@Override
	protected <N, T> InternalValueState<K, N, T> createValueState(TypeSerializer<N> namespaceSerializer, ValueStateDescriptor<T> stateDesc) throws Exception {
		return null;
	}

	@Override
	protected <N, T> InternalListState<K, N, T> createListState(TypeSerializer<N> namespaceSerializer, ListStateDescriptor<T> stateDesc) throws Exception {
		return null;
	}

	@Override
	protected <N, T> InternalReducingState<K, N, T> createReducingState(TypeSerializer<N> namespaceSerializer, ReducingStateDescriptor<T> stateDesc) throws Exception {
		return null;
	}

	@Override
	protected <N, T, ACC, R> InternalAggregatingState<K, N, T, ACC, R> createAggregatingState(TypeSerializer<N> namespaceSerializer, AggregatingStateDescriptor<T, ACC, R> stateDesc) throws Exception {
		return null;
	}

	@Override
	protected <N, T, ACC> InternalFoldingState<K, N, T, ACC> createFoldingState(TypeSerializer<N> namespaceSerializer, FoldingStateDescriptor<T, ACC> stateDesc) throws Exception {
		return null;
	}

	@Override
	protected <N, UK, UV> InternalMapState<K, N, UK, UV> createMapState(TypeSerializer<N> namespaceSerializer, MapStateDescriptor<UK, UV> stateDesc) throws Exception {
		return null;
	}

	@Override
	public int numStateEntries() {
		return 0;
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {

	}

	@Override
	public <N> Stream<K> getKeys(String state, N namespace) {
		return null;
	}

	@Override
	public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(long checkpointId, long timestamp, CheckpointStreamFactory streamFactory, CheckpointOptions checkpointOptions) throws Exception {
		return DoneFuture.of(SnapshotResult.empty());
	}

	@Override
	public void restore(Collection<KeyedStateHandle> state) throws Exception {

	}
}
