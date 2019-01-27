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

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.heap.KeyContextImpl;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.keyed.KeyedMapState;
import org.apache.flink.runtime.state.keyed.KeyedState;

import java.util.Iterator;
import java.util.Map;

/**
 * An implementation of {@link MapState} which is backed by a
 * {@link KeyedMapState}. The values of the states depend on the current key
 * of the keyContext. That is, when the current key of the keyContext changes, the
 * values accessed will be changed as well.
 *
 * @param <UV> The type of the keys in the state.
 * @param <UV> The type of the values in the state.
 */
public class ContextMapState<K, UK, UV>
	implements ContextKeyedState<K, Map<UK, UV>>, InternalMapState<K, VoidNamespace, UK, UV> {

	/** The keyContext to which the state belongs. */
	private final KeyContextImpl<K> keyContext;

	/** The keyed state backing the state. */
	private final KeyedMapState<Object, UK, UV> keyedState;

	public ContextMapState(
		final KeyContextImpl<K> keyContext,
		KeyedMapState<Object, UK, UV> keyedState
	) {
		this.keyContext = keyContext;
		this.keyedState = keyedState;
	}

	@Override
	public void clear() {
		keyedState.remove(keyContext.getCurrentKey());
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
	public boolean contains(UK key) {
		return keyedState.contains(keyContext.getCurrentKey(), key);
	}

	@Override
	public Iterable<Map.Entry<UK, UV>> entries() {
		final Iterator<Map.Entry<UK, UV>> iterator = keyedState.iterator(keyContext.getCurrentKey());
		if (!iterator.hasNext()) {
			return null;
		} else {
			return () -> iterator;
		}
	}

	@Override
	public Iterable<UK> keys() {
		final Iterator<Map.Entry<UK, UV>> entryIterator = keyedState.iterator(keyContext.getCurrentKey());

		final Iterator<UK> keyIterator = new Iterator<UK>() {
			@Override
			public boolean hasNext() {
				return entryIterator.hasNext();
			}

			@Override
			public UK next() {
				return entryIterator.next().getKey();
			}

			@Override
			public void remove() {
				entryIterator.remove();
			}
		};

		return () -> keyIterator;
	}

	@Override
	public Iterable<UV> values() {
		final Iterator<Map.Entry<UK, UV>> entryIterator = keyedState.iterator(keyContext.getCurrentKey());

		final Iterator<UV> valueIterator = new Iterator<UV>() {
			@Override
			public boolean hasNext() {
				return entryIterator.hasNext();
			}

			@Override
			public UV next() {
				return entryIterator.next().getValue();
			}
		};

		return () -> valueIterator;
	}

	@Override
	public Iterator<Map.Entry<UK, UV>> iterator() {
		return keyedState.iterator(keyContext.getCurrentKey());
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
