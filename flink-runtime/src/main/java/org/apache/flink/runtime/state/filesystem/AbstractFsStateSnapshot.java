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

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.state.KvState;
import org.apache.flink.runtime.state.KvStateSnapshot;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * A snapshot of a heap key/value state stored in a file.
 * 
 * @param <K> The type of the key in the snapshot state.
 * @param <N> The type of the namespace in the snapshot state.
 * @param <SV> The type of the state value.
 */
public abstract class AbstractFsStateSnapshot<K, N, SV, S extends State, SD extends StateDescriptor<S>> extends AbstractFileStateHandle implements KvStateSnapshot<K, N, S, SD, FsStateBackend> {

	private static final long serialVersionUID = 1L;

	/** Key Serializer */
	protected final TypeSerializer<K> keySerializer;

	/** Namespace Serializer */
	protected final TypeSerializer<N> namespaceSerializer;

	/** Serializer for the state value */
	protected final TypeSerializer<SV> stateSerializer;

	/** StateDescriptor, for sanity checks */
	protected final SD stateDesc;

	/**
	 * Creates a new state snapshot with data in the file system.
	 *
	 * @param keySerializer The serializer for the keys.
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param stateSerializer The serializer for the elements in the state HashMap
	 * @param stateDesc The state identifier
	 * @param filePath The path where the snapshot data is stored.
	 */
	public AbstractFsStateSnapshot(TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		TypeSerializer<SV> stateSerializer,
		SD stateDesc,
		Path filePath) {
		super(filePath);
		this.stateDesc = stateDesc;
		this.keySerializer = keySerializer;
		this.stateSerializer = stateSerializer;
		this.namespaceSerializer = namespaceSerializer;

	}

	public abstract KvState<K, N, S, SD, FsStateBackend> createFsState(FsStateBackend backend, HashMap<N, Map<K, SV>> stateMap);

	@Override
	public KvState<K, N, S, SD, FsStateBackend> restoreState(
		FsStateBackend stateBackend,
		final TypeSerializer<K> keySerializer,
		ClassLoader classLoader,
		long recoveryTimestamp) throws Exception {

		// validity checks
		if (!this.keySerializer.equals(keySerializer)) {
			throw new IllegalArgumentException(
				"Cannot restore the state from the snapshot with the given serializers. " +
					"State (K/V) was serialized with " +
					"(" + this.keySerializer + ") " +
					"now is (" + keySerializer + ")");
		}

		// state restore
		try (FSDataInputStream inStream = stateBackend.getFileSystem().open(getFilePath())) {
			DataInputViewStreamWrapper inView = new DataInputViewStreamWrapper(new DataInputStream(inStream));


			final int numKeys = inView.readInt();
			HashMap<N, Map<K, SV>> stateMap = new HashMap<>(numKeys);

			for (int i = 0; i < numKeys; i++) {
				N namespace = namespaceSerializer.deserialize(inView);
				final int numValues = inView.readInt();
				Map<K, SV> namespaceMap = new HashMap<>(numValues);
				stateMap.put(namespace, namespaceMap);
				for (int j = 0; j < numValues; j++) {
					K key = keySerializer.deserialize(inView);
					SV value = stateSerializer.deserialize(inView);
					namespaceMap.put(key, value);
				}
			}

//			return new FsHeapValueState<>(stateBackend, keySerializer, namespaceSerializer, stateDesc, stateMap);
			return createFsState(stateBackend, stateMap);
		}
		catch (Exception e) {
			throw new Exception("Failed to restore state from file system", e);
		}
	}

	/**
	 * Returns the file size in bytes.
	 *
	 * @return The file size in bytes.
	 * @throws IOException Thrown if the file system cannot be accessed.
	 */
	@Override
	public long getStateSize() throws IOException {
		return getFileSize();
	}
}
