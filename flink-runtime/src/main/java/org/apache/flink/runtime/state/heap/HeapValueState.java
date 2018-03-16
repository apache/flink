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

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.InternalValueState;

/**
 * Heap-backed partitioned {@link org.apache.flink.api.common.state.ValueState} that is snapshotted
 * into files.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of the value.
 */
public class HeapValueState<K, N, V>
		extends AbstractHeapState<K, N, V, ValueState<V>, ValueStateDescriptor<V>>
		implements InternalValueState<N, V> {

	/**
	 * Creates a new key/value state for the given hash map of key/value pairs.
	 *
	 * @param stateDesc The state identifier for the state. This contains name
	 *                           and can create a default state value.
	 * @param stateTable The state tab;e to use in this kev/value state. May contain initial state.
	 */
	public HeapValueState(
			ValueStateDescriptor<V> stateDesc,
			StateTable<K, N, V> stateTable,
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer) {
		super(stateDesc, stateTable, keySerializer, namespaceSerializer);
	}

	@Override
	public V value() {
		final V result = stateTable.get(currentNamespace);

		if (result == null) {
			return stateDesc.getDefaultValue();
		}

		return result;
	}

	@Override
	public void update(V value) {

		if (value == null) {
			clear();
			return;
		}

		stateTable.put(currentNamespace, value);
	}
}
