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

import org.apache.flink.api.common.state.AppendingState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.InternalAppendingState;

/**
 * Base class for {@link AppendingState} ({@link InternalAppendingState}) that is stored on the heap.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <IN> The type of the input elements.
 * @param <SV> The type of the values in the state.
 * @param <OUT> The type of the output elements.
 */
abstract class AbstractHeapAppendingState<K, N, IN, SV, OUT>
	extends AbstractHeapState<K, N, SV>
	implements InternalAppendingState<K, N, IN, SV, OUT> {
	/**
	 * Creates a new key/value state for the given hash map of key/value pairs.
	 *
	 * @param stateTable          The state table for which this state is associated to.
	 * @param keySerializer       The serializer for the keys.
	 * @param valueSerializer     The serializer for the state.
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param defaultValue        The default value for the state.
	 */
	AbstractHeapAppendingState(
		StateTable<K, N, SV> stateTable,
		TypeSerializer<K> keySerializer,
		TypeSerializer<SV> valueSerializer,
		TypeSerializer<N> namespaceSerializer,
		SV defaultValue) {
		super(stateTable, keySerializer, valueSerializer, namespaceSerializer, defaultValue);
	}

	@Override
	public SV getInternal() {
		return stateTable.get(currentNamespace);
	}

	@Override
	public void updateInternal(SV valueToStore) {
		stateTable.put(currentNamespace, valueToStore);
	}
}
