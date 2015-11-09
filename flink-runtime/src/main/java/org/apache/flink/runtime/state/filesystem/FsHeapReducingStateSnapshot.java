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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateIdentifier;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.InputViewDataInputStreamWrapper;
import org.apache.flink.runtime.state.KvStateSnapshot;

import java.io.DataInputStream;
import java.util.HashMap;

/**
 * A snapshot of a heap partitioned {@link ListState} stored in a
 * file.
 *
 * @param <K> The type of the key in the snapshot state.
 * @param <V> The type of the values.
 */
public class FsHeapReducingStateSnapshot<K, V> extends AbstractFileState implements KvStateSnapshot<K, ReducingState<V>, ReducingStateIdentifier<V>, FsStateBackend> {

	private static final long serialVersionUID = 1L;

	/** Name of the key serializer class */
	private final String keySerializerClassName;

	/** Hash of the StateIdentifier, for sanity checks */
	int stateIdentifierHash;

	/**
	 * Creates a new state snapshot with data in the file system.
	 *
	 * @param keySerializer The serializer for the keys.
	 * @param filePath The path where the snapshot data is stored.
	 */
	public FsHeapReducingStateSnapshot(TypeSerializer<K> keySerializer, ReducingStateIdentifier<V> stateIdentifiers, Path filePath) {
		super(filePath);
		this.keySerializerClassName = keySerializer.getClass().getName();
		this.stateIdentifierHash = stateIdentifiers.hashCode();
	}

	@Override
	public FsHeapReducingState<K, V> restoreState(
			FsStateBackend stateBackend,
			final TypeSerializer<K> keySerializer,
			ReducingStateIdentifier<V> stateIdentifier,
			ClassLoader classLoader) throws Exception {

		// validity checks
		if (!keySerializer.getClass().getName().equals(keySerializerClassName)
				|| stateIdentifierHash != stateIdentifier.hashCode()) {
			throw new IllegalArgumentException(
					"Cannot restore the state from the snapshot with the given serializers. " +
							"State (K/V) was serialized with " +
							"(" + keySerializerClassName + "/" + stateIdentifierHash + ") " +
							"now is (" + keySerializer.getClass().getName() + "/" + stateIdentifier.hashCode() + ")");
		}

		// state restore
		try (FSDataInputStream inStream = stateBackend.getFileSystem().open(getFilePath())) {
			InputViewDataInputStreamWrapper inView = new InputViewDataInputStreamWrapper(new DataInputStream(inStream));

			final int numEntries = inView.readInt();
			HashMap<K, V> stateMap = new HashMap<>(numEntries);

			TypeSerializer<V> valueSerializer = stateIdentifier.getSerializer();
			for (int i = 0; i < numEntries; i++) {
				K key = keySerializer.deserialize(inView);
				V value = valueSerializer.deserialize(inView);
				stateMap.put(key, value);
			}

			return new FsHeapReducingState<>(keySerializer, stateIdentifier, stateMap, stateBackend);
		}
		catch (Exception e) {
			throw new Exception("Failed to restore state from file system", e);
		}
	}
}
