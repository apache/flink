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
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.subkeyed.SubKeyedListState;
import org.apache.flink.runtime.state.subkeyed.SubKeyedState;

import java.util.Collection;
import java.util.List;

/**
 * used for ListState.
 *
 * @param <K> The type of key the state is associated to.
 * @param <N> The type of the namespace.
 * @param <E> The type of elements in the list.
 */
public class ContextSubKeyedListState<K, N, E>
	implements ContextSubKeyedAppendingState<K, N, E, List<E>, Iterable<E>>, InternalListState<K, N, E> {

	private N namespace;

	private final KeyContextImpl<K> keyContext;

	private final SubKeyedListState<Object, N, E> subKeyedListState;

	public ContextSubKeyedListState(KeyContextImpl<K> keyContext, SubKeyedListState<Object, N, E> subKeyedListState) {
		this.keyContext = keyContext;
		this.subKeyedListState = subKeyedListState;
	}

	@Override
	public Iterable<E> get() throws Exception {
		return subKeyedListState.get(getCurrentKey(), namespace);
	}

	@Override
	public void add(E value) throws Exception {
		subKeyedListState.add(getCurrentKey(), namespace, value);
	}

	@Override
	public void clear() {
		subKeyedListState.remove(getCurrentKey(), namespace);
	}

	private Object getCurrentKey() {
		return keyContext.getCurrentKey();
	}

	@Override
	public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
		if (sources != null) {
			for (N source : sources) {
				List<E> list = subKeyedListState.get(getCurrentKey(), source);
				if (list != null) {
					subKeyedListState.addAll(getCurrentKey(), target, list);
					subKeyedListState.remove(getCurrentKey(), source);
				}
			}
		}
	}

	@Override
	public void update(List<E> values) {
		if (values == null || values.isEmpty()) {
			subKeyedListState.remove(getCurrentKey(), namespace);
		} else {
			subKeyedListState.putAll(getCurrentKey(), namespace, values);
		}
	}

	@Override
	public void addAll(List<E> values) {
		if (values != null && !values.isEmpty()) {
			subKeyedListState.addAll(getCurrentKey(), namespace, values);
		}
	}

	@Override
	public TypeSerializer<K> getKeySerializer() {
		return keyContext.getKeySerializer();
	}

	@Override
	public TypeSerializer<N> getNamespaceSerializer() {
		return subKeyedListState.getDescriptor().getNamespaceSerializer();
	}

	@Override
	public TypeSerializer<List<E>> getValueSerializer() {
		return subKeyedListState.getDescriptor().getValueSerializer();
	}

	@Override
	public void setCurrentNamespace(N namespace) {
		this.namespace = namespace;
	}

	@Override
	public byte[] getSerializedValue(byte[] serializedKeyAndNamespace, TypeSerializer<K> safeKeySerializer, TypeSerializer<N> safeNamespaceSerializer, TypeSerializer<List<E>> safeValueSerializer) throws Exception {
		return new byte[0];
	}

	@Override
	public SubKeyedState getSubKeyedState() {
		return subKeyedListState;
	}
}
