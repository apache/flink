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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Heap-backed partitioned {@link MapState} that is snapshotted into files.
 *
 * @param <K>  The type of the key.
 * @param <N>  The type of the namespace.
 * @param <UK> The type of the keys in the state.
 * @param <UV> The type of the values in the state.
 */
public class HeapMapState<K, N, UK, UV>
		extends AbstractHeapState<K, N, HashMap<UK, UV>, MapState<UK, UV>, MapStateDescriptor<UK, UV>>
		implements InternalMapState<N, UK, UV> {

	/**
	 * Creates a new key/value state for the given hash map of key/value pairs.
	 *
	 * @param stateDesc  The state identifier for the state. This contains name
	 *                   and can create a default state value.
	 * @param stateTable The state tab;e to use in this kev/value state. May contain initial state.
	 */
	public HeapMapState(
			MapStateDescriptor<UK, UV> stateDesc,
			StateTable<K, N, HashMap<UK, UV>> stateTable,
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer) {
		super(stateDesc, stateTable, keySerializer, namespaceSerializer);
	}

	@Override
	public UV get(UK userKey) {

		HashMap<UK, UV> userMap = stateTable.get(currentNamespace);

		if (userMap == null) {
			return null;
		}
		
		return userMap.get(userKey);
	}

	@Override
	public void put(UK userKey, UV userValue) {

		HashMap<UK, UV> userMap = stateTable.get(currentNamespace);
		if (userMap == null) {
			userMap = new HashMap<>();
			stateTable.put(currentNamespace, userMap);
		}

		userMap.put(userKey, userValue);
	}

	@Override
	public void putAll(Map<UK, UV> value) {

		HashMap<UK, UV> userMap = stateTable.get(currentNamespace);

		if (userMap == null) {
			userMap = new HashMap<>();
			stateTable.put(currentNamespace, userMap);
		}

		userMap.putAll(value);
	}

	@Override
	public void remove(UK userKey) {

		HashMap<UK, UV> userMap = stateTable.get(currentNamespace);
		if (userMap == null) {
			return;
		}

		userMap.remove(userKey);

		if (userMap.isEmpty()) {
			clear();
		}
	}

	@Override
	public boolean contains(UK userKey) {
		HashMap<UK, UV> userMap = stateTable.get(currentNamespace);
		return userMap != null && userMap.containsKey(userKey);
	}

	@Override
	public Iterable<Map.Entry<UK, UV>> entries() {
		HashMap<UK, UV> userMap = stateTable.get(currentNamespace);
		return userMap == null ? null : userMap.entrySet();
	}
	
	@Override
	public Iterable<UK> keys() {
		HashMap<UK, UV> userMap = stateTable.get(currentNamespace);
		return userMap == null ? null : userMap.keySet();
	}

	@Override
	public Iterable<UV> values() {
		HashMap<UK, UV> userMap = stateTable.get(currentNamespace);
		return userMap == null ? null : userMap.values();
	}

	@Override
	public Iterator<Map.Entry<UK, UV>> iterator() {
		HashMap<UK, UV> userMap = stateTable.get(currentNamespace);
		return userMap == null ? null : userMap.entrySet().iterator();
	}

	@Override
	public byte[] getSerializedValue(K key, N namespace) throws IOException {
		Preconditions.checkState(namespace != null, "No namespace given.");
		Preconditions.checkState(key != null, "No key given.");

		HashMap<UK, UV> result = stateTable.get(key, namespace);

		if (null == result) {
			return null;
		}

		TypeSerializer<UK> userKeySerializer = stateDesc.getKeySerializer();
		TypeSerializer<UV> userValueSerializer = stateDesc.getValueSerializer();

		return KvStateSerializer.serializeMap(result.entrySet(), userKeySerializer, userValueSerializer);
	}
}
