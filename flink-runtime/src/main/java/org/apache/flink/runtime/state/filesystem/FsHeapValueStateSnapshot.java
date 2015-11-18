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

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateIdentifier;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.InputViewDataInputStreamWrapper;
import org.apache.flink.runtime.state.KvStateSnapshot;

import java.io.DataInputStream;
import java.util.HashMap;

/**
 * A snapshot of a heap key/value state stored in a file.
 * 
 * @param <K> The type of the key in the snapshot state.
 * @param <V> The type of the value.
 */
public class FsHeapValueStateSnapshot<K, V> extends AbstractFileState implements KvStateSnapshot<K, ValueState<V>, ValueStateIdentifier<V>, FsStateBackend> {
	
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
	public FsHeapValueStateSnapshot(TypeSerializer<K> keySerializer, ValueStateIdentifier<V> stateIdentifiers, Path filePath) {
		super(filePath);
		this.stateIdentifierHash = stateIdentifiers.hashCode();
		this.keySerializerClassName = keySerializer.getClass().getName();
	}

	@Override
	public FsHeapValueState<K, V> restoreState(
			FsStateBackend stateBackend,
			final TypeSerializer<K> keySerializer,
			ValueStateIdentifier<V> stateIdentifier,
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
				V initialState = valueSerializer.deserialize(inView);
				stateMap.put(key, initialState);
			}

			return new FsHeapValueState<>(keySerializer, stateIdentifier, stateMap, stateBackend);
		}
		catch (Exception e) {
			throw new Exception("Failed to restore state from file system", e);
		}
	}
}
