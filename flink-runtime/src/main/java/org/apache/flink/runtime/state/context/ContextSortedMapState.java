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

package org.apache.flink.runtime.state.context;

import org.apache.flink.api.common.state.SortedMapState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.heap.KeyContextImpl;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.keyed.KeyedSortedMapState;
import org.apache.flink.runtime.state.keyed.KeyedState;

import java.util.Iterator;
import java.util.Map;

/**
 * An implementation of {@link SortedMapState} which is backed by a
 * {@link KeyedSortedMapState}. The values of the states depend on the current key
 * of the keyContext. That is, when the current key of the keyContext changes, the
 * values accessed will be changed as well.
 *
 * @param <UK> The type of the keys in the state.
 * @param <UV> The type of the values in the state.
 */
public class ContextSortedMapState<K, UK, UV>
	implements ContextKeyedState<K, Map<UK, UV>>, SortedMapState<UK, UV>, InternalMapState<K, VoidNamespace, UK, UV> {

	/** The keyContext to which the state belongs. */
	private final KeyContextImpl<K> keyContext;

	/** The keyed state backing the state. */
	private final KeyedSortedMapState<Object, UK, UV> keyedState;

	public ContextSortedMapState(
		final KeyContextImpl<K> keyContext,
		KeyedSortedMapState<Object, UK, UV> keyedState
	) {
		this.keyContext = keyContext;
		this.keyedState = keyedState;
	}

	@Override
	public Map.Entry<UK, UV> firstEntry() {
		return keyedState.firstEntry(keyContext.getCurrentKey());
	}

	@Override
	public Map.Entry<UK, UV> lastEntry() {
		return keyedState.lastEntry(keyContext.getCurrentKey());
	}

	@Override
	public Iterator<Map.Entry<UK, UV>> headIterator(UK endKey) {
		return keyedState.headIterator(keyContext.getCurrentKey(), endKey);
	}

	@Override
	public Iterator<Map.Entry<UK, UV>> tailIterator(UK startKey) {
		return keyedState.tailIterator(keyContext.getCurrentKey(), startKey);
	}

	@Override
	public Iterator<Map.Entry<UK, UV>> subIterator(UK startKey, UK endKey) {
		return keyedState.subIterator(keyContext.getCurrentKey(), startKey, endKey);
	}

	@Override
	public boolean contains(UK key) {
		return keyedState.contains(keyContext.getCurrentKey(), key);
	}

	@Override
	public UV get(UK key) {
		return keyedState.get(keyContext.getCurrentKey(), key);
	}

	@Override
	public void put(UK key, UV value) {
		keyedState.add(keyContext.getCurrentKey(), key, value);
	}

	@Override
	public void putAll(Map<UK, UV> map) {
		keyedState.addAll(keyContext.getCurrentKey(), map);
	}

	@Override
	public void remove(UK key) {
		keyedState.remove(keyContext.getCurrentKey(), key);
	}

	@Override
	public Iterable<Map.Entry<UK, UV>> entries() {
		return ContextSortedMapState.this::iterator;
	}

	@Override
	public Iterable<UK> keys() {
		return () -> {
			Iterator<Map.Entry<UK, UV>> entryIterator = ContextSortedMapState.this.iterator();

			return new Iterator<UK>() {
				@Override
				public boolean hasNext() {
					return entryIterator.hasNext();
				}

				@Override
				public UK next() {
					Map.Entry<UK, UV> entry = entryIterator.next();
					return entry.getKey();
				}
			};
		};
	}

	@Override
	public Iterable<UV> values() {
		return () -> {
			Iterator<Map.Entry<UK, UV>> entryIterator = ContextSortedMapState.this.iterator();

			return new Iterator<UV>() {
				@Override
				public boolean hasNext() {
					return entryIterator.hasNext();
				}

				@Override
				public UV next() {
					Map.Entry<UK, UV> entry = entryIterator.next();
					return entry.getValue();
				}
			};
		};
	}

	@Override
	public Iterator<Map.Entry<UK, UV>> iterator() {
		return keyedState.iterator(keyContext.getCurrentKey());
	}

	@Override
	public void clear() {
		keyedState.remove(keyContext.getCurrentKey());
	}

	@Override
	public KeyedState getKeyedState() {
		return keyedState;
	}

	@Override
	public TypeSerializer<K> getKeySerializer() {
		return keyContext.getKeySerializer();
	}

	@Override
	public TypeSerializer<Map<UK, UV>> getValueSerializer() {
		return keyedState.getDescriptor().getValueSerializer();
	}

	@Override
	public byte[] getSerializedValue(
		byte[] serializedKeyAndNamespace,
		TypeSerializer safeKeySerializer,
		TypeSerializer safeNamespaceSerializer,
		TypeSerializer safeValueSerializer) throws Exception {

		return keyedState.getSerializedValue(serializedKeyAndNamespace, safeKeySerializer, safeValueSerializer);
	}
}
