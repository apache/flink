/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

public class SpillableMapState<K, N, UK, UV>
	extends AbstractHeapState<K, N, Map<UK, UV>>
	implements InternalMapState<K, N, UK, UV> {

	/**
	 * Creates a new key/value state for the given hash map of key/value pairs.
	 *
	 * @param stateTable          The state table for which this state is associated to.
	 * @param keySerializer       The serializer for the keys.
	 * @param valueSerializer     The serializer for the state.
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param defaultValue        The default value for the state.
	 */
	private SpillableMapState(
		StateTable<K, N, Map<UK, UV>> stateTable,
		TypeSerializer<K> keySerializer,
		TypeSerializer<Map<UK, UV>> valueSerializer,
		TypeSerializer<N> namespaceSerializer,
		Map<UK, UV> defaultValue) {
		super(stateTable, keySerializer, valueSerializer, namespaceSerializer, defaultValue);

		Preconditions.checkState(valueSerializer instanceof MapSerializer, "Unexpected serializer type.");
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
	public TypeSerializer<Map<UK, UV>> getValueSerializer() {
		return valueSerializer;
	}

	@Override
	public UV get(UK userKey) {

		Map<UK, UV> userMap = stateTable.get(currentNamespace);

		if (userMap == null) {
			return null;
		}

		return userMap.get(userKey);
	}

	@Override
	public void put(UK userKey, UV userValue) {

		Map<UK, UV> userMap = stateTable.get(currentNamespace);
		if (userMap == null) {
			userMap = new HashMap<>();
		}

		userMap.put(userKey, userValue);
		stateTable.put(currentNamespace, userMap);
	}

	@Override
	public void putAll(Map<UK, UV> value) {

		Map<UK, UV> userMap = stateTable.get(currentNamespace);

		if (userMap == null) {
			userMap = new HashMap<>();
		}

		userMap.putAll(value);
		stateTable.put(currentNamespace, userMap);
	}

	@Override
	public void remove(UK userKey) {

		Map<UK, UV> userMap = stateTable.get(currentNamespace);
		if (userMap == null) {
			return;
		}

		userMap.remove(userKey);

		if (userMap.isEmpty()) {
			clear();
		} else {
			stateTable.put(currentNamespace, userMap);
		}
	}

	@Override
	public boolean contains(UK userKey) {
		Map<UK, UV> userMap = stateTable.get(currentNamespace);
		return userMap != null && userMap.containsKey(userKey);
	}

	@Override
	public Iterable<Map.Entry<UK, UV>> entries() {
		Map<UK, UV> userMap = stateTable.get(currentNamespace);

		if (userMap == null) {
			return null;
		}

		return new Iterable<Map.Entry<UK, UV>>() {
			@Override
			public Iterator<Map.Entry<UK, UV>> iterator() {
				return new IteratorWrapper(currentNamespace, userMap);
			}
		};
	}

	@Override
	public Iterable<UK> keys() {
		Map<UK, UV> userMap = stateTable.get(currentNamespace);

		if (userMap == null) {
			return null;
		}

		return new Iterable<UK>() {
			@Override
			public Iterator<UK> iterator() {
				IteratorWrapper iteratorWrapper = new IteratorWrapper(currentNamespace, userMap);
				return new Iterator<UK>() {
					@Override
					public boolean hasNext() {
						return iteratorWrapper.hasNext();
					}

					@Override
					public UK next() {
						return iteratorWrapper.next().getKey();
					}

					@Override
					public void remove() {
						iteratorWrapper.remove();
					}
				};
			}
		};
	}

	@Override
	public Iterable<UV> values() {
		Map<UK, UV> userMap = stateTable.get(currentNamespace);

		if (userMap == null) {
			return null;
		}

		return new Iterable<UV>() {
			@Override
			public Iterator<UV> iterator() {
				IteratorWrapper iteratorWrapper = new IteratorWrapper(currentNamespace, userMap);
				return new Iterator<UV>() {
					@Override
					public boolean hasNext() {
						return iteratorWrapper.hasNext();
					}

					@Override
					public UV next() {
						return iteratorWrapper.next().getValue();
					}

					@Override
					public void remove() {
						iteratorWrapper.remove();
					}
				};
			}
		};
	}

	@Override
	public Iterator<Map.Entry<UK, UV>> iterator() {
		Map<UK, UV> userMap = stateTable.get(currentNamespace);
		return userMap == null ? null : new IteratorWrapper(currentNamespace, userMap);
	}

	@Override
	public byte[] getSerializedValue(
		final byte[] serializedKeyAndNamespace,
		final TypeSerializer<K> safeKeySerializer,
		final TypeSerializer<N> safeNamespaceSerializer,
		final TypeSerializer<Map<UK, UV>> safeValueSerializer) throws Exception {

		Preconditions.checkNotNull(serializedKeyAndNamespace);
		Preconditions.checkNotNull(safeKeySerializer);
		Preconditions.checkNotNull(safeNamespaceSerializer);
		Preconditions.checkNotNull(safeValueSerializer);

		Tuple2<K, N> keyAndNamespace = KvStateSerializer.deserializeKeyAndNamespace(serializedKeyAndNamespace,
			safeKeySerializer,
			safeNamespaceSerializer);

		Map<UK, UV> result = stateTable.get(keyAndNamespace.f0, keyAndNamespace.f1);

		if (result == null) {
			return null;
		}

		final MapSerializer<UK, UV> serializer = (MapSerializer<UK, UV>) safeValueSerializer;

		final TypeSerializer<UK> dupUserKeySerializer = serializer.getKeySerializer();
		final TypeSerializer<UV> dupUserValueSerializer = serializer.getValueSerializer();

		return KvStateSerializer.serializeMap(result.entrySet(), dupUserKeySerializer, dupUserValueSerializer);
	}

	@SuppressWarnings("unchecked")
	static <UK, UV, K, N, SV, S extends State, IS extends S> IS create(
		StateDescriptor<S, SV> stateDesc, StateTable<K, N, SV> stateTable, TypeSerializer<K> keySerializer) {
		return (IS) new SpillableMapState<>((StateTable<K, N, Map<UK, UV>>) stateTable,
			keySerializer,
			(TypeSerializer<Map<UK, UV>>) stateTable.getStateSerializer(),
			stateTable.getNamespaceSerializer(),
			(Map<UK, UV>) stateDesc.getDefaultValue());
	}

	class IteratorWrapper implements Iterator<Map.Entry<UK, UV>> {

		private final N namespace;

		private final Map<UK, UV> userMap;

		private final Iterator<Map.Entry<UK, UV>> entryIterator;

		private EntryWrapper currentEntry;

		IteratorWrapper(N namespace, Map<UK, UV> userMap) {
			this.namespace = namespace;
			this.userMap = userMap;
			this.entryIterator = userMap.entrySet().iterator();
		}

		@Override
		public boolean hasNext() {
			return entryIterator.hasNext();
		}

		@Override
		public Map.Entry<UK, UV> next() {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}

			currentEntry = new EntryWrapper(namespace, userMap, entryIterator.next());
			return currentEntry;
		}

		@Override
		public void remove() {
			entryIterator.remove();
			currentEntry.setDeleted();
			// TODO add StateTable#put(K, N, S)
			stateTable.put(namespace, userMap);
		}
	}

	class EntryWrapper implements Map.Entry<UK, UV> {

		private final N namespace;

		private Map<UK, UV> userMap;

		private final Map.Entry<UK, UV> userEntry;

		private boolean deleted;

		EntryWrapper(N namespace, Map<UK, UV> userMap, Map.Entry<UK, UV> entry) {
			this.namespace = namespace;
			this.userMap = userMap;
			this.userEntry = entry;
			this.deleted = false;
		}

		@Override
		public UK getKey() {
			return userEntry.getKey();
		}

		@Override
		public UV getValue() {
			return deleted ? null : userEntry.getValue();
		}

		@Override
		public UV setValue(UV value) {
			if (deleted) {
				throw new IllegalStateException("The value has already been deleted.");
			}

			UV oldUserValue = userEntry.setValue(value);
			// TODO add StateTable#put(K, N, S)
			stateTable.put(namespace, userMap);

			return oldUserValue;
		}

		public void setDeleted() {
			deleted = true;
		}
	}
}
