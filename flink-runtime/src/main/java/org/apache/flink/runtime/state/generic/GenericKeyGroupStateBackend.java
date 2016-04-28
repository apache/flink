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

package org.apache.flink.runtime.state.generic;

import org.apache.flink.api.common.state.FoldingState;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MergingState;
import org.apache.flink.api.common.state.PartitionedState;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.StateBackend;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.KeyGroupAssigner;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.KeyGroupStateBackend;
import org.apache.flink.runtime.state.PartitionedStateBackend;
import org.apache.flink.runtime.state.PartitionedStateSnapshot;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class GenericKeyGroupStateBackend<KEY> implements KeyGroupStateBackend<KEY> {

	private final AbstractStateBackend stateBackend;

	private final TypeSerializer<KEY> keySerializer;

	private final KeyGroupAssigner<KEY> keyGroupAssigner;

	private final Map<Integer, PartitionedStateBackend<KEY>> partitionedStateBackends;

	private final Map<String, GenericKeyGroupKVState<KEY, ?, ?, ?>> kvStates;

	private KEY currentKey = null;

	private int currentKeyGroupIndex = -1;

	private PartitionedStateBackend<KEY> currentPartitionedStateBackend = null;

	public GenericKeyGroupStateBackend(
		AbstractStateBackend abstractStateBackend,
		TypeSerializer<KEY> keySerializer,
		KeyGroupAssigner<KEY> keyGroupAssigner) {
		this.stateBackend = Preconditions.checkNotNull(abstractStateBackend);
		this.keySerializer = Preconditions.checkNotNull(keySerializer);
		this.keyGroupAssigner = Preconditions.checkNotNull(keyGroupAssigner);

		partitionedStateBackends = new HashMap<>();
		kvStates = new HashMap<>();
	}


	PartitionedStateBackend<KEY> getBackend(int keyGroupIndex) {
		if (partitionedStateBackends.containsKey(keyGroupIndex)) {
			return partitionedStateBackends.get(keyGroupIndex);
		} else {
			PartitionedStateBackend partitionedStateBackend = null;
			try {
				partitionedStateBackend = stateBackend.createPartitionedStateBackend(keySerializer);
			} catch (Exception e) {
				throw new RuntimeException("Could not create the partitioned state backend for " +
					"key group index " + keyGroupIndex + ".", e);
			}

			partitionedStateBackends.put(keyGroupIndex, partitionedStateBackend);

			return partitionedStateBackend;
		}
	}

	@Override
	public void close() throws Exception {
		for (PartitionedStateBackend<KEY> partitionedStateBackend: partitionedStateBackends.values()) {
			partitionedStateBackend.close();
		}
	}

	@Override
	public <S extends PartitionedState> void dispose(StateDescriptor<S, ?> stateDescriptor) {
		for (PartitionedStateBackend<KEY> partitionedStateBackend: partitionedStateBackends.values()) {
			partitionedStateBackend.dispose(stateDescriptor);
		}
	}

	@Override
	public void setCurrentKey(KEY key) throws Exception {
		this.currentKey = key;

		int keyGroupIndex = keyGroupAssigner.getKeyGroupIndex(key);

		if (keyGroupIndex != currentKeyGroupIndex) {
			this.currentKeyGroupIndex = keyGroupIndex;
			this.currentPartitionedStateBackend = getBackend(currentKeyGroupIndex);

			currentPartitionedStateBackend.setCurrentKey(key);

			for (GenericKeyGroupKVState<KEY, ?, ?, ?> kvState: kvStates.values()) {
				kvState.setPartitionedStateBackend(currentPartitionedStateBackend);
			}
		}
	}

	@Override
	public KEY getCurrentKey() {
		return this.currentKey;
	}

	@Override
	public <N, S extends PartitionedState> S getPartitionedState(N namespace, final TypeSerializer<N> namespaceSerializer, StateDescriptor<S, ?> stateDescriptor) throws Exception {
		Preconditions.checkNotNull(namespace);
		Preconditions.checkNotNull(namespaceSerializer);
		Preconditions.checkNotNull(stateDescriptor);

		if (kvStates.containsKey(stateDescriptor.getName())) {
			GenericKeyGroupKVState<KEY, ?, N, ?> kvState = (GenericKeyGroupKVState<KEY, ?, N, ?>)kvStates.get(stateDescriptor.getName());

			kvState.setNamespace(namespace);

			return (S) kvState;
		} else {
			S state = stateDescriptor.bind(new StateBackend() {
				@Override
				public <T> ValueState<T> createValueState(ValueStateDescriptor<T> stateDesc) throws Exception {
					return new GenericKeyGroupValueState<KEY, T, N>(stateDesc, namespaceSerializer);
				}

				@Override
				public <T> ListState<T> createListState(ListStateDescriptor<T> stateDesc) throws Exception {
					return new GenericKeyGroupListState<KEY, T, N>(stateDesc, namespaceSerializer);
				}

				@Override
				public <T> ReducingState<T> createReducingState(ReducingStateDescriptor<T> stateDesc) throws Exception {
					return new GenericKeyGroupReducingState<KEY, T, N>(stateDesc, namespaceSerializer);
				}

				@Override
				public <T, ACC> FoldingState<T, ACC> createFoldingState(FoldingStateDescriptor<T, ACC> stateDesc) throws Exception {
					return new GenericKeyGroupFoldingState<KEY, T, ACC, N>(stateDesc, namespaceSerializer);
				}
			});

			GenericKeyGroupKVState<KEY, ?, N, ?> kvState = (GenericKeyGroupKVState<KEY, ?, N, ?>)state;

			kvStates.put(stateDescriptor.getName(), kvState);

			kvState.setNamespace(namespace);

			if (currentPartitionedStateBackend != null) {
				kvState.setPartitionedStateBackend(currentPartitionedStateBackend);
			}

			return (S) kvState;
		}
	}

	@Override
	public <N, S extends MergingState<?, ?>> void mergePartitionedStates(N target, Collection<N> sources, TypeSerializer<N> namespaceSerializer, StateDescriptor<S, ?> stateDescriptor) throws Exception {
		if (currentPartitionedStateBackend != null) {
			currentPartitionedStateBackend.mergePartitionedStates(target, sources, namespaceSerializer, stateDescriptor);
		} else {
			throw new RuntimeException("No PartitionedStateBackend has been set. This indicates that no key has been specified.");
		}
	}

	@Override
	public Map<Integer, PartitionedStateSnapshot> snapshotPartitionedState(long checkpointId, long timestamp) throws Exception {
		Map<Integer, PartitionedStateSnapshot> partitionedStateSnapshots = new HashMap<>(partitionedStateBackends.size());

		for (Map.Entry<Integer, PartitionedStateBackend<KEY>> entry: partitionedStateBackends.entrySet()) {
			PartitionedStateSnapshot partitionedStateSnapshot = entry.getValue().snapshotPartitionedState(checkpointId, timestamp);
			partitionedStateSnapshots.put(entry.getKey(), partitionedStateSnapshot);
		}

		return partitionedStateSnapshots;
	}

	@Override
	public void restorePartitionedState(Map<Integer, PartitionedStateSnapshot> partitionedStateSnapshots, long recoveryTimestamp) throws Exception {
		for (Map.Entry<Integer, PartitionedStateSnapshot> entry: partitionedStateSnapshots.entrySet()) {
			PartitionedStateBackend<KEY> partitionedStateBackend = getBackend(entry.getKey());
			partitionedStateBackend.restorePartitionedState(entry.getValue(), recoveryTimestamp);
		}
	}

	@Override
	public void notifyCompletedCheckpoint(long checkpointId) throws Exception {
		for (PartitionedStateBackend<KEY> partitionedStateBackend: partitionedStateBackends.values()) {
			partitionedStateBackend.notifyCompletedCheckpoint(checkpointId);
		}
	}
}
