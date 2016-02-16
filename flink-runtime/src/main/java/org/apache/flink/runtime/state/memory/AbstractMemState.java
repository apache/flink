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

package org.apache.flink.runtime.state.memory;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.AbstractHeapState;
import org.apache.flink.runtime.state.KvStateSnapshot;
import org.apache.flink.runtime.util.DataOutputSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Base class for partitioned {@link ListState} implementations that are backed by a regular
 * heap hash map. The concrete implementations define how the state is checkpointed.
 * 
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <SV> The type of the values in the state.
 * @param <S> The type of State
 * @param <SD> The type of StateDescriptor for the State S
 */
public abstract class AbstractMemState<K, N, SV, S extends State, SD extends StateDescriptor<S, ?>>
		extends AbstractHeapState<K, N, SV, S, SD, MemoryStateBackend> {

	public AbstractMemState(TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		TypeSerializer<SV> stateSerializer,
		SD stateDesc) {
		super(keySerializer, namespaceSerializer, stateSerializer, stateDesc);
	}

	public AbstractMemState(TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		TypeSerializer<SV> stateSerializer,
		SD stateDesc,
		HashMap<N, Map<K, SV>> state) {
		super(keySerializer, namespaceSerializer, stateSerializer, stateDesc, state);
	}

	public abstract KvStateSnapshot<K, N, S, SD, MemoryStateBackend> createHeapSnapshot(byte[] bytes);

	@Override
	public KvStateSnapshot<K, N, S, SD, MemoryStateBackend> snapshot(long checkpointId, long timestamp) throws Exception {

		DataOutputSerializer out = new DataOutputSerializer(Math.max(size() * 16, 16));

		out.writeInt(state.size());
		for (Map.Entry<N, Map<K, SV>> namespaceState: state.entrySet()) {
			N namespace = namespaceState.getKey();
			namespaceSerializer.serialize(namespace, out);
			out.writeInt(namespaceState.getValue().size());
			for (Map.Entry<K, SV> entry: namespaceState.getValue().entrySet()) {
				keySerializer.serialize(entry.getKey(), out);
				stateSerializer.serialize(entry.getValue(), out);
			}
		}

		byte[] bytes = out.getCopyOfBuffer();

		return createHeapSnapshot(bytes);
	}
}
