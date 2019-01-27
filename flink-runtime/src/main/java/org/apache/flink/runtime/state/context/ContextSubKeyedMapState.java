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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.heap.KeyContextImpl;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.subkeyed.SubKeyedMapState;
import org.apache.flink.runtime.state.subkeyed.SubKeyedState;
import org.apache.flink.util.Preconditions;

import java.util.Iterator;
import java.util.Map;

/**
 * used for map state.
 *
 * @param <K> The type of key the state is associated to.
 * @param <N> The type of the namespace.
 * @param <UK> Type of the values folded into the state.
 * @param <UV> Type of the value in the state.
 */
public class ContextSubKeyedMapState<K, N, UK, UV>
	implements ContextSubKeyedState<K, N, Map<UK, UV>>, InternalMapState<K, N, UK, UV> {

	private N namespace;

	private final KeyContextImpl<K> operator;

	private final SubKeyedMapState<Object, N, UK, UV> subKeyedMapState;

	public ContextSubKeyedMapState(
		KeyContextImpl<K> operator,
		SubKeyedMapState<Object, N, UK, UV> subKeyedMapState) {
		Preconditions.checkNotNull(operator);
		Preconditions.checkNotNull(subKeyedMapState);
		this.operator = operator;
		this.subKeyedMapState = subKeyedMapState;
	}

	@Override
	public UV get(UK key) throws Exception {
		return subKeyedMapState.get(operator.getCurrentKey(), namespace, key);
	}

	@Override
	public void put(UK key, UV value) throws Exception {
		subKeyedMapState.add(operator.getCurrentKey(), namespace, key, value);
	}

	@Override
	public void putAll(Map<UK, UV> map) throws Exception {
		subKeyedMapState.addAll(operator.getCurrentKey(), namespace, map);
	}

	@Override
	public void remove(UK key) throws Exception {
		subKeyedMapState.remove(operator.getCurrentKey(), namespace, key);
	}

	@Override
	public boolean contains(UK key) throws Exception {
		return subKeyedMapState.contains(operator.getCurrentKey(), namespace, key);
	}

	@Override
	public Iterable<Map.Entry<UK, UV>> entries() throws Exception {
		final Iterator<Map.Entry<UK, UV>> iterator = subKeyedMapState.iterator(operator.getCurrentKey(), namespace);
		return () -> iterator;
	}

	@Override
	public Iterable<UK> keys() throws Exception {
		final Iterator<Map.Entry<UK, UV>> entryIterator = subKeyedMapState.iterator(operator.getCurrentKey(), namespace);

		final Iterator<UK> keyIterator = new Iterator<UK>() {
			@Override
			public boolean hasNext() {
				return entryIterator.hasNext();
			}

			@Override
			public UK next() {
				return entryIterator.next().getKey();
			}
		};

		return () -> keyIterator;
	}

	@Override
	public Iterable<UV> values() throws Exception {
		final Iterator<Map.Entry<UK, UV>> entryIterator = subKeyedMapState.iterator(operator.getCurrentKey(), namespace);

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
	public Iterator<Map.Entry<UK, UV>> iterator() throws Exception {
		return subKeyedMapState.iterator(operator.getCurrentKey(), namespace);
	}

	@Override
	public void clear() {
		subKeyedMapState.remove(operator.getCurrentKey(), namespace);
	}

	@Override
	public SubKeyedState getSubKeyedState() {
		return subKeyedMapState;
	}

	@Override
	public TypeSerializer<K> getKeySerializer() {
		return operator.getKeySerializer();
	}

	@Override
	public TypeSerializer<N> getNamespaceSerializer() {
		return subKeyedMapState.getDescriptor().getNamespaceSerializer();
	}

	@Override
	public TypeSerializer<Map<UK, UV>> getValueSerializer() {
		return subKeyedMapState.getDescriptor().getValueSerializer();
	}

	@Override
	public void setCurrentNamespace(N namespace) {
		this.namespace = namespace;
	}

	@Override
	public byte[] getSerializedValue(byte[] serializedKeyAndNamespace, TypeSerializer<K> safeKeySerializer, TypeSerializer<N> safeNamespaceSerializer, TypeSerializer<Map<UK, UV>> safeValueSerializer) throws Exception {
		return new byte[0];
	}

}
