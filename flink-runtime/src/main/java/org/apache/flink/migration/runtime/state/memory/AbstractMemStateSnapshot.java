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

package org.apache.flink.migration.runtime.state.memory;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.migration.runtime.state.KvStateSnapshot;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackend;
import org.apache.flink.runtime.state.heap.StateTable;
import org.apache.flink.runtime.util.DataInputDeserializer;

import java.io.IOException;

/**
 * @deprecated Internal class for savepoint backwards compatibility. Don't use for other purposes.
 */
@Deprecated
@SuppressWarnings("deprecation")
public abstract class AbstractMemStateSnapshot<K, N, SV, S extends State, SD extends StateDescriptor<S, ?>> 
		implements KvStateSnapshot<K, N, S, SD>, MigrationRestoreSnapshot<K, N, SV> {

	private static final long serialVersionUID = 1L;

	/** Key Serializer */
	protected final TypeSerializer<K> keySerializer;

	/** Namespace Serializer */
	protected final TypeSerializer<N> namespaceSerializer;

	/** Serializer for the state value */
	protected final TypeSerializer<SV> stateSerializer;

	/** StateDescriptor, for sanity checks */
	protected final SD stateDesc;

	/** The serialized data of the state key/value pairs */
	private final byte[] data;
	
	private transient boolean closed;

	/**
	 * Creates a new heap memory state snapshot.
	 *
	 * @param keySerializer The serializer for the keys.
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param stateSerializer The serializer for the elements in the state HashMap
	 * @param stateDesc The state identifier
	 * @param data The serialized data of the state key/value pairs
	 */
	public AbstractMemStateSnapshot(TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		TypeSerializer<SV> stateSerializer,
		SD stateDesc,
		byte[] data) {
		this.keySerializer = keySerializer;
		this.namespaceSerializer = namespaceSerializer;
		this.stateSerializer = stateSerializer;
		this.stateDesc = stateDesc;
		this.data = data;
	}

	@Override
	@SuppressWarnings("unchecked")
	public StateTable<K, N, SV> deserialize(
			String stateName,
			HeapKeyedStateBackend<K> stateBackend) throws IOException {

		final DataInputDeserializer inView = new DataInputDeserializer(data, 0, data.length);
		AbstractMigrationRestoreStrategy<K, N, SV> restoreStrategy =
				new AbstractMigrationRestoreStrategy<K, N, SV>(keySerializer, namespaceSerializer, stateSerializer) {
					@Override
					protected DataInputView openDataInputView() throws IOException {
						return inView;
					}
				};
		return restoreStrategy.deserialize(stateName, stateBackend);
	}

	/**
	 * Discarding the heap state is a no-op.
	 */
	@Override
	public void discardState() {}

	@Override
	public long getStateSize() {
		return data.length;
	}

	@Override
	public void close() {
		closed = true;
	}

	public TypeSerializer<K> getKeySerializer() {
		return keySerializer;
	}

	public TypeSerializer<N> getNamespaceSerializer() {
		return namespaceSerializer;
	}

	public TypeSerializer<SV> getStateSerializer() {
		return stateSerializer;
	}

	public byte[] getData() {
		return data;
	}

	@Override
	public String toString() {
		return "AbstractMemStateSnapshot{" +
				"keySerializer=" + keySerializer +
				", namespaceSerializer=" + namespaceSerializer +
				", stateSerializer=" + stateSerializer +
				", stateDesc=" + stateDesc +
				'}';
	}
}
