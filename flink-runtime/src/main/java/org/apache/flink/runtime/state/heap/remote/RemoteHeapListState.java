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

package org.apache.flink.runtime.state.heap.remote;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Heap-backed partitioned {@link ListState} that is snapshotted into files.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of the value.
 */
class RemoteHeapListState<K, N, V>
	extends AbstractRemoteHeapState<K, N, List<V>>
	implements InternalListState<K, N, V> {

	/** Serializer for the values. */
	private final TypeSerializer<V> elementSerializer;

	/**
	 * Separator of StringAppendTestOperator in RocksDB.
	 */
	private static final byte DELIMITER = ',';

	/**
	 * Creates a new key/value state for the given hash map of key/value pairs.
	 *
	 * @param keySerializer The serializer for the keys.
	 * @param valueSerializer The serializer for the state.
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param kvStateInfo StateInfo containing descriptors
	 * @param defaultValue The default value for the state.
	 * @param backend KeyBackend
	 */
	private RemoteHeapListState(
		TypeSerializer<K> keySerializer,
		TypeSerializer<List<V>> valueSerializer,
		TypeSerializer<N> namespaceSerializer,
		RemoteHeapKeyedStateBackend.RemoteHeapKvStateInfo kvStateInfo,
		List<V> defaultValue,
		RemoteHeapKeyedStateBackend backend) {
		super(
			keySerializer,
			valueSerializer,
			namespaceSerializer,
			kvStateInfo,
			defaultValue,
			backend);

		ListSerializer<V> castedListSerializer = (ListSerializer<V>) valueSerializer;
		this.elementSerializer = castedListSerializer.getElementSerializer();
	}

	@Override
	public TypeSerializer<K> getKeySerializer() {
		return keySerializer;
	}

	@Override
	public TypeSerializer<N> getNamespaceSerializer() {
		return namespaceSerializer;
	}

	@Override
	public TypeSerializer<List<V>> getValueSerializer() {
		return valueSerializer;
	}

	// ------------------------------------------------------------------------
	//  state access
	// ------------------------------------------------------------------------

	/**
	 * Returns the current value for the state. When the state is not
	 * partitioned the returned value is the same for all inputs in a given
	 * operator instance. If state partitioning is applied, the value returned
	 * depends on the current operator input, as the operator maintains an
	 * independent state for each partition.
	 *
	 * <p><b>NOTE TO IMPLEMENTERS:</b> if the state is empty, then this method
	 * should return {@code null}.
	 *
	 * @return The operator state value corresponding to the current input or {@code null}
	 * 	if the state is empty.
	 *
	 * @throws Exception Thrown if the system cannot access the state.
	 */
	@Override
	public Iterable<V> get() throws Exception {
		return getInternal();
	}

	/**
	 * Get internally stored value.
	 *
	 * @return internally stored value.
	 *
	 */
	@Override
	public List<V> getInternal() {
		try {
			byte[] key = serializeCurrentKeyWithGroupAndNamespaceDesc(kvStateInfo.nameBytes);
			byte[] valueBytes = backend.syncRemClient.get(key);
			return deserializeList(valueBytes);
		} catch (Exception e) {
			throw new FlinkRuntimeException("Error while retrieving data from remote heap", e);
		}
	}

	private List<V> deserializeList(
		byte[] valueBytes) {
		if (valueBytes == null) {
			return null;
		}

		dataInputView.setBuffer(valueBytes);

		List<V> result = new ArrayList<>();
		V next;
		while ((next = deserializeNextElement(dataInputView, elementSerializer)) != null) {
			result.add(next);
		}
		return result;
	}

	private static <V> V deserializeNextElement(
		DataInputDeserializer in,
		TypeSerializer<V> elementSerializer) {
		try {
			if (in.available() > 0) {
				V element = elementSerializer.deserialize(in);
				if (in.available() > 0) {
					in.readByte();
				}
				return element;
			}
		} catch (IOException e) {
			throw new FlinkRuntimeException("Unexpected list element deserialization failure", e);
		}
		return null;
	}

	@Override
	public void add(V value) {
		Preconditions.checkNotNull(value, "You cannot add null to a ListState.");

		try {
			backend.syncRemClient.rpush(
				serializeCurrentKeyWithGroupAndNamespaceDesc(kvStateInfo.nameBytes),
				serializeValue(value, elementSerializer)
			);
		} catch (Exception e) {
			throw new FlinkRuntimeException("Error while adding data to remote heap", e);
		}
	}

	// ------------------------------------------------------------------------
	//  state merging
	// ------------------------------------------------------------------------

	/**
	 * Merges the state of the current key for the given source namespaces into the state of
	 * the target namespace.
	 *
	 * @param target The target namespace where the merged state should be stored.
	 * @param sources The source namespaces whose state should be merged.
	 *
	 * @throws Exception The method may forward exception thrown internally (by I/O or functions).
	 */
	@Override
	public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
		if (sources == null || sources.isEmpty()) {
			return;
		}

		try {
			// create the target full-binary-key
			setCurrentNamespace(target);
			final byte[] targetKey = serializeCurrentKeyWithGroupAndNamespaceDesc(kvStateInfo.nameBytes);

			// merge the sources to the target
			for (N source : sources) {
				if (source != null) {
					setCurrentNamespace(source);
					final byte[] sourceKey = serializeCurrentKeyWithGroupAndNamespaceDesc(
						kvStateInfo.nameBytes);

					byte[] valueBytes = backend.syncRemClient.get(sourceKey);

					if (valueBytes != null) {
						backend.syncRemClient.del(sourceKey);
						backend.syncRemClient.rpush(targetKey, valueBytes);
					}
				}
			}
		} catch (Exception e) {
			throw new FlinkRuntimeException("Error while merging state in remote heap", e);
		}
	}

	@Override
	public void update(List<V> values) throws Exception {
		updateInternal(values);
	}

	/**
	 * Update internally stored value.
	 *
	 * @param valueToStore new value to store.
	 *
	 * @throws Exception The method may forward exception thrown internally (by I/O or functions).
	 */
	@Override
	public void updateInternal(List<V> valueToStore) throws Exception {
		Preconditions.checkNotNull(valueToStore, "List of values to add cannot be null.");

		if (!valueToStore.isEmpty()) {
			try {
				backend.syncRemClient.lpush(
					serializeCurrentKeyWithGroupAndNamespaceDesc(kvStateInfo.nameBytes),
					serializeValueList(valueToStore, elementSerializer, DELIMITER));
			} catch (Exception e) {
				throw new FlinkRuntimeException("Error while updating data to REM", e);
			}
		} else {
			clear();
		}
	}

	@Override
	public void addAll(List<V> values) throws Exception {
		Preconditions.checkNotNull(values, "List of values to add cannot be null.");

		if (!values.isEmpty()) {
			try {
				backend.syncRemClient.rpush(
					serializeCurrentKeyWithGroupAndNamespaceDesc(kvStateInfo.nameBytes),
					serializeValueList(values, elementSerializer, DELIMITER));
			} catch (Exception e) {
				throw new FlinkRuntimeException("Error while updating data to remote heap", e);
			}
		}
	}

	@SuppressWarnings("unchecked")
	static <E, K, N, SV, S extends State, IS extends S> IS create(
		StateDescriptor<S, SV> stateDesc,
		RegisteredKeyValueStateBackendMetaInfo<N, SV> metaInfo,
		TypeSerializer<K> keySerializer,
		RemoteHeapKeyedStateBackend backend) {
		RemoteHeapKeyedStateBackend.RemoteHeapKvStateInfo kvState = backend.getRemoteHeapKvStateInfo(
			stateDesc.getName());
		return (IS) new RemoteHeapListState<>(
			keySerializer,
			(TypeSerializer<List<E>>) metaInfo.getStateSerializer(),
			metaInfo.getNamespaceSerializer(),
			kvState,
			(List<E>) stateDesc.getDefaultValue(),
			backend);
	}
}
