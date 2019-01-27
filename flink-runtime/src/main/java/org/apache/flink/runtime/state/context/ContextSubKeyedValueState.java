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
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.runtime.state.subkeyed.SubKeyedState;
import org.apache.flink.runtime.state.subkeyed.SubKeyedValueState;
import org.apache.flink.runtime.state.heap.KeyContextImpl;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * A helper class to get SubKeyedValueState.
 *
 * @param <K> The type of key the state is associated to.
 * @param <N> The type of the namespace.
 * @param <V> The type of values kept internally in state.
 */
public class ContextSubKeyedValueState<K, N, V>
	implements ContextSubKeyedState<K, N, V>, InternalValueState<K, N, V> {

	private N namespace;

	private final KeyContextImpl<K> keyContext;

	private final SubKeyedValueState<K, N, V> subKeyedValueState;

	private final V defaultValue;

	public ContextSubKeyedValueState(
		KeyContextImpl<K> keyContext,
		SubKeyedValueState<K, N, V> subKeyedValueState,
		V defaultValue) {
		Preconditions.checkNotNull(keyContext);
		Preconditions.checkNotNull(subKeyedValueState);
		this.keyContext = keyContext;
		this.subKeyedValueState = subKeyedValueState;
		this.defaultValue = defaultValue;
	}

	@Override
	public V value() throws IOException {
		V value = subKeyedValueState.get(keyContext.getCurrentKey(), namespace);
		return value == null ? defaultValue : value;
	}

	@Override
	public void update(V value) throws IOException {
		subKeyedValueState.put(keyContext.getCurrentKey(), namespace, value);
	}

	@Override
	public void clear() {
		subKeyedValueState.remove(keyContext.getCurrentKey(), namespace);
	}

	@Override
	public TypeSerializer<K> getKeySerializer() {
		return keyContext.getKeySerializer();
	}

	@Override
	public TypeSerializer<N> getNamespaceSerializer() {
		return subKeyedValueState.getDescriptor().getNamespaceSerializer();
	}

	@Override
	public TypeSerializer<V> getValueSerializer() {
		return subKeyedValueState.getDescriptor().getValueSerializer();
	}

	@Override
	public void setCurrentNamespace(N namespace) {
		this.namespace = namespace;
	}

	@Override
	public byte[] getSerializedValue(byte[] serializedKeyAndNamespace, TypeSerializer<K> safeKeySerializer, TypeSerializer<N> safeNamespaceSerializer, TypeSerializer<V> safeValueSerializer) throws Exception {
		return subKeyedValueState.getSerializedValue(serializedKeyAndNamespace, safeKeySerializer, safeNamespaceSerializer, safeValueSerializer);
	}

	@Override
	public SubKeyedState getSubKeyedState() {
		return subKeyedValueState;
	}
}
