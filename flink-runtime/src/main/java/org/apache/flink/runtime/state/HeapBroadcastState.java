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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * A {@link BroadcastState Broadcast State} backed a heap-based {@link Map}.
 *
 * @param <K> The key type of the elements in the {@link BroadcastState Broadcast State}.
 * @param <V> The value type of the elements in the {@link BroadcastState Broadcast State}.
 */
public class HeapBroadcastState<K, V> implements BackendWritableBroadcastState<K, V> {

	/**
	 * Meta information of the state, including state name, assignment mode, and serializer.
	 */
	private RegisteredBroadcastBackendStateMetaInfo<K, V> stateMetaInfo;

	/**
	 * The internal map the holds the elements of the state.
	 */
	private final Map<K, V> backingMap;

	/**
	 * A serializer that allows to perform deep copies of internal map state.
	 */
	private final MapSerializer<K, V> internalMapCopySerializer;

	HeapBroadcastState(RegisteredBroadcastBackendStateMetaInfo<K, V> stateMetaInfo) {
		this(stateMetaInfo, new HashMap<>());
	}

	private HeapBroadcastState(final RegisteredBroadcastBackendStateMetaInfo<K, V> stateMetaInfo, final Map<K, V> internalMap) {

		this.stateMetaInfo = Preconditions.checkNotNull(stateMetaInfo);
		this.backingMap = Preconditions.checkNotNull(internalMap);
		this.internalMapCopySerializer = new MapSerializer<>(stateMetaInfo.getKeySerializer(), stateMetaInfo.getValueSerializer());
	}

	private HeapBroadcastState(HeapBroadcastState<K, V> toCopy) {
		this(toCopy.stateMetaInfo.deepCopy(), toCopy.internalMapCopySerializer.copy(toCopy.backingMap));
	}

	@Override
	public void setStateMetaInfo(RegisteredBroadcastBackendStateMetaInfo<K, V> stateMetaInfo) {
		this.stateMetaInfo = stateMetaInfo;
	}

	@Override
	public RegisteredBroadcastBackendStateMetaInfo<K, V> getStateMetaInfo() {
		return stateMetaInfo;
	}

	@Override
	public HeapBroadcastState<K, V> deepCopy() {
		return new HeapBroadcastState<>(this);
	}

	@Override
	public void clear() {
		backingMap.clear();
	}

	@Override
	public String toString() {
		return "HeapBroadcastState{" +
				"stateMetaInfo=" + stateMetaInfo +
				", backingMap=" + backingMap +
				", internalMapCopySerializer=" + internalMapCopySerializer +
				'}';
	}

	@Override
	public long write(FSDataOutputStream out) throws IOException {
		long partitionOffset = out.getPos();

		DataOutputView dov = new DataOutputViewStreamWrapper(out);
		dov.writeInt(backingMap.size());
		for (Map.Entry<K, V> entry: backingMap.entrySet()) {
			getStateMetaInfo().getKeySerializer().serialize(entry.getKey(), dov);
			getStateMetaInfo().getValueSerializer().serialize(entry.getValue(), dov);
		}

		return partitionOffset;
	}

	@Override
	public V get(K key) {
		return backingMap.get(key);
	}

	@Override
	public void put(K key, V value) {
		backingMap.put(key, value);
	}

	@Override
	public void putAll(Map<K, V> map) {
		backingMap.putAll(map);
	}

	@Override
	public void remove(K key) {
		backingMap.remove(key);
	}

	@Override
	public boolean contains(K key) {
		return backingMap.containsKey(key);
	}

	@Override
	public Iterator<Map.Entry<K, V>> iterator() {
		return backingMap.entrySet().iterator();
	}

	@Override
	public Iterable<Map.Entry<K, V>> entries() {
		return backingMap.entrySet();
	}

	@Override
	public Iterable<Map.Entry<K, V>> immutableEntries() {
		return Collections.unmodifiableSet(backingMap.entrySet());
	}
}
