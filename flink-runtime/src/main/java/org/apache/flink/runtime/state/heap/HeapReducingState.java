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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.internal.InternalReducingState;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * Heap-backed partitioned {@link ReducingState} that is snapshotted into files.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of the value.
 */
public class HeapReducingState<K, N, V>
		extends AbstractHeapMergingState<K, N, V, V, V, ReducingState<V>>
		implements InternalReducingState<K, N, V> {

	private final ReduceTransformation<V> reduceTransformation;

	/**
	 * Creates a new key/value state for the given hash map of key/value pairs.
	 *
	 * @param stateTable The state table for which this state is associated to.
	 * @param keySerializer The serializer for the keys.
	 * @param valueSerializer The serializer for the state.
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param defaultValue The default value for the state.
	 * @param reduceFunction The reduce function used for reducing state.
	 */
	public HeapReducingState(
			StateTable<K, N, V> stateTable,
			TypeSerializer<K> keySerializer,
			TypeSerializer<V> valueSerializer,
			TypeSerializer<N> namespaceSerializer,
			V defaultValue,
			ReduceFunction<V> reduceFunction) {

		super(stateTable, keySerializer, valueSerializer, namespaceSerializer, defaultValue);
		this.reduceTransformation = new ReduceTransformation<>(reduceFunction);
	}

	@Override
	public TypeSerializer<K> getKeySerializer() {
		return keySerializer;
	}

	@Override
	public TypeSerializer<N> getNamespaceSerializer() {
		return namespaceSerializer;
	}

	@Override
	public TypeSerializer<V> getValueSerializer() {
		return valueSerializer;
	}

	// ------------------------------------------------------------------------
	//  state access
	// ------------------------------------------------------------------------

	@Override
	public V get() {
		return stateTable.get(currentNamespace);
	}

	@Override
	public void add(V value) throws IOException {

		if (value == null) {
			clear();
			return;
		}

		try {
			stateTable.transform(currentNamespace, value, reduceTransformation);
		} catch (Exception e) {
			throw new IOException("Exception while applying ReduceFunction in reducing state", e);
		}
	}

	// ------------------------------------------------------------------------
	//  state merging
	// ------------------------------------------------------------------------

	@Override
	protected V mergeState(V a, V b) throws Exception {
		return reduceTransformation.apply(a, b);
	}

	static final class ReduceTransformation<V> implements StateTransformationFunction<V, V> {

		private final ReduceFunction<V> reduceFunction;

		ReduceTransformation(ReduceFunction<V> reduceFunction) {
			this.reduceFunction = Preconditions.checkNotNull(reduceFunction);
		}

		@Override
		public V apply(V previousState, V value) throws Exception {
			return previousState != null ? reduceFunction.reduce(previousState, value) : value;
		}
	}
}
