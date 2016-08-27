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

package org.apache.flink.runtime.state.filesystem;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.state.FoldingState;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.query.netty.message.KvStateRequestSerializer;
import org.apache.flink.runtime.state.KvState;
import org.apache.flink.runtime.state.KvStateSnapshot;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Heap-backed partitioned {@link FoldingState} that is
 * snapshotted into files.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <T> The type of the values that can be folded into the state.
 * @param <ACC> The type of the value in the folding state.
 */
public class FsFoldingState<K, N, T, ACC>
	extends AbstractFsState<K, N, ACC, FoldingState<T, ACC>, FoldingStateDescriptor<T, ACC>>
	implements FoldingState<T, ACC> {

	private final FoldFunction<T, ACC> foldFunction;

	/**
	 * Creates a new and empty partitioned state.
	 *
	 * @param backend The file system state backend backing snapshots of this state
	 * @param keySerializer The serializer for the key.
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param stateDesc The state identifier for the state. This contains name
	 *                           and can create a default state value.
	 */
	public FsFoldingState(FsStateBackend backend,
		TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		FoldingStateDescriptor<T, ACC> stateDesc) {
		super(backend, keySerializer, namespaceSerializer, stateDesc.getSerializer(), stateDesc);
		this.foldFunction = stateDesc.getFoldFunction();
	}

	/**
	 * Creates a new key/value state with the given state contents.
	 * This method is used to re-create key/value state with existing data, for example from
	 * a snapshot.
	 *
	 * @param backend The file system state backend backing snapshots of this state
	 * @param keySerializer The serializer for the key.
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param stateDesc The state identifier for the state. This contains name
	 * and can create a default state value.
	 * @param state The map of key/value pairs to initialize the state with.
	 */
	public FsFoldingState(FsStateBackend backend,
		TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		FoldingStateDescriptor<T, ACC> stateDesc,
		HashMap<N, Map<K, ACC>> state) {
		super(backend, keySerializer, namespaceSerializer, stateDesc.getSerializer(), stateDesc, state);
		this.foldFunction = stateDesc.getFoldFunction();
	}

	@Override
	public ACC get() {
		if (currentNSState == null) {
			Preconditions.checkState(currentNamespace != null, "No namespace set");
			currentNSState = state.get(currentNamespace);
		}
		if (currentNSState != null) {
			Preconditions.checkState(currentKey != null, "No key set");
			return currentNSState.get(currentKey);
		} else {
			return null;
		}
	}

	@Override
	public void add(T value) throws IOException {
		Preconditions.checkState(currentKey != null, "No key set");

		if (currentNSState == null) {
			Preconditions.checkState(currentNamespace != null, "No namespace set");
			currentNSState = createNewNamespaceMap();
			state.put(currentNamespace, currentNSState);
		}

		ACC currentValue = currentNSState.get(currentKey);
		try {
			if (currentValue == null) {
				currentNSState.put(currentKey, foldFunction.fold(stateDesc.getDefaultValue(), value));
			} else {
				currentNSState.put(currentKey, foldFunction.fold(currentValue, value));

			}
		} catch (Exception e) {
			throw new RuntimeException("Could not add value to folding state.", e);
		}
	}

	@Override
	public KvStateSnapshot<K, N, FoldingState<T, ACC>, FoldingStateDescriptor<T, ACC>, FsStateBackend> createHeapSnapshot(Path filePath) {
		return new Snapshot<>(getKeySerializer(), getNamespaceSerializer(), stateSerializer, stateDesc, filePath);
	}

	@Override
	public byte[] getSerializedValue(K key, N namespace) throws Exception {
		Preconditions.checkNotNull(key, "Key");
		Preconditions.checkNotNull(namespace, "Namespace");

		Map<K, ACC> stateByKey = state.get(namespace);

		if (stateByKey != null) {
			return KvStateRequestSerializer.serializeValue(stateByKey.get(key), stateDesc.getSerializer());
		} else {
			return null;
		}
	}

	public static class Snapshot<K, N, T, ACC> extends AbstractFsStateSnapshot<K, N, ACC, FoldingState<T, ACC>, FoldingStateDescriptor<T, ACC>> {
		private static final long serialVersionUID = 1L;

		public Snapshot(TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer,
			TypeSerializer<ACC> stateSerializer,
			FoldingStateDescriptor<T, ACC> stateDescs,
			Path filePath) {
			super(keySerializer, namespaceSerializer, stateSerializer, stateDescs, filePath);
		}

		@Override
		public KvState<K, N, FoldingState<T, ACC>, FoldingStateDescriptor<T, ACC>, FsStateBackend> createFsState(FsStateBackend backend, HashMap<N, Map<K, ACC>> stateMap) {
			return new FsFoldingState<>(backend, keySerializer, namespaceSerializer, stateDesc, stateMap);
		}
	}

}
