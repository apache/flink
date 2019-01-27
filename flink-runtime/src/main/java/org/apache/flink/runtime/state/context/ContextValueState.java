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

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.runtime.state.keyed.KeyedState;
import org.apache.flink.runtime.state.keyed.KeyedValueState;
import org.apache.flink.runtime.state.heap.KeyContextImpl;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * An implementation of {@link ValueState} which is backed by a
 * {@link KeyedValueState}. The values of the states depend on the current key
 * of the keyContext. That is, when the current key of the keyContext changes, the
 * values accessed will be changed as well.
 *
 * @param <V> The type of the values in the state.
 */
public class ContextValueState<K, V> implements ContextKeyedState<K, V>, InternalValueState<K, VoidNamespace, V> {

	/** The keyContext to which the state belongs. */
	private final KeyContextImpl<K> keyContext;

	/** The keyed state backing the state. */
	private final KeyedValueState<Object, V> keyedState;

	/** The descriptor of the state. */
	private final ValueStateDescriptor<V> stateDescriptor;

	public ContextValueState(
		final KeyContextImpl<K> operator,
		final KeyedValueState<Object, V> keyedState,
		final ValueStateDescriptor<V> stateDescriptor
	) {
		Preconditions.checkNotNull(operator);
		Preconditions.checkNotNull(keyedState);
		Preconditions.checkNotNull(stateDescriptor);

		this.keyContext = operator;
		this.keyedState = keyedState;
		this.stateDescriptor = stateDescriptor;
	}

	@Override
	public V value() throws IOException {
		Object key = keyContext.getCurrentKey();
		Preconditions.checkNotNull(key, "No key set. This method should not be called outside of a keyed context.");

		V value = keyedState.get(key);
		return value == null ? stateDescriptor.getDefaultValue() : value;
	}

	@Override
	public void update(V value) throws IOException {
		if (value == null) {
			clear();
		} else {
			keyedState.put(keyContext.getCurrentKey(), value);
		}
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
	public TypeSerializer<V> getValueSerializer() {
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
