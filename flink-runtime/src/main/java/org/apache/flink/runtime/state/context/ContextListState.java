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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.heap.KeyContextImpl;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.keyed.KeyedListState;
import org.apache.flink.runtime.state.keyed.KeyedState;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.List;

/**
 * An implementation of {@link ListState} which is backed by a
 * {@link KeyedListState}. The values of the states depend on the current key
 * of the keyContext. That is, when the current key of the keyContext changes, the
 * values accessed will be changed as well.
 *
 * <p> This class exposes empty state properly as an empty list.
 *
 * @param <E> The type of the elements in the state.
 */
public class ContextListState<K, E>
	implements ContextKeyedState<K, List<E>>, InternalListState<K, VoidNamespace, E> {

	/** The keyContext to which the state belongs. */
	private final KeyContextImpl<K> keyContext;

	/** The keyed state backing the state. */
	private final KeyedListState<Object, E> keyedState;

	public ContextListState(
		final KeyContextImpl<K> keyContext,
		final KeyedListState<Object, E> keyedState
	) {
		Preconditions.checkNotNull(keyContext);
		Preconditions.checkNotNull(keyedState);

		this.keyContext = keyContext;
		this.keyedState = keyedState;
	}

	@Override
	public Iterable<E> get() {
		return keyedState.get(keyContext.getCurrentKey());
	}

	@Override
	public void add(E value) {
		keyedState.add(keyContext.getCurrentKey(), value);
	}

	@Override
	public void clear() {
		keyedState.remove(keyContext.getCurrentKey());
	}

	@Override
	public void update(List<E> values) {
		Preconditions.checkNotNull(values, "List of values to add cannot be null.");

		if (values.isEmpty()) {
			keyedState.remove(keyContext.getCurrentKey());
		} else {
			keyedState.putAll(keyContext.getCurrentKey(), values);
		}
	}

	@Override
	public void addAll(List<E> values) {
		Preconditions.checkNotNull(values, "List of values to add cannot be null.");

		if (!values.isEmpty()) {
			keyedState.addAll(keyContext.getCurrentKey(), values);
		}
	}

	@Override
	public KeyedState<Object, List<E>> getKeyedState() {
		return keyedState;
	}

	@Override
	public void mergeNamespaces(VoidNamespace target, Collection<VoidNamespace> sources) throws Exception {
		throw new UnsupportedOperationException("mergeNamespaces should not be called.");
	}

	@Override
	public TypeSerializer<K> getKeySerializer() {
		return keyContext.getKeySerializer();
	}

	@Override
	public TypeSerializer<VoidNamespace> getNamespaceSerializer() {
		return VoidNamespaceSerializer.INSTANCE;
	}

	@Override
	public TypeSerializer<List<E>> getValueSerializer() {
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

