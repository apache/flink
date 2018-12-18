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

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.state.FoldingState;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.internal.InternalFoldingState;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * Heap-backed partitioned {@link FoldingState} that is snapshotted into files.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <T> The type of the values that can be folded into the state.
 * @param <ACC> The type of the value in the folding state.
 *
 * @deprecated will be removed in a future version
 */
@Deprecated
class HeapFoldingState<K, N, T, ACC>
	extends AbstractHeapAppendingState<K, N, T, ACC, ACC>
	implements InternalFoldingState<K, N, T, ACC> {
	/** The function used to fold the state. */
	private final FoldTransformation foldTransformation;

	/**
	 * Creates a new key/value state for the given hash map of key/value pairs.
	 *
	 * @param stateTable The state table for which this state is associated to.
	 * @param keySerializer The serializer for the keys.
	 * @param valueSerializer The serializer for the state.
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param defaultValue The default value for the state.
	 * @param foldFunction The fold function used for folding state.
	 */
	private HeapFoldingState(
		StateTable<K, N, ACC> stateTable,
		TypeSerializer<K> keySerializer,
		TypeSerializer<ACC> valueSerializer,
		TypeSerializer<N> namespaceSerializer,
		ACC defaultValue,
		FoldFunction<T, ACC> foldFunction) {
		super(stateTable, keySerializer, valueSerializer, namespaceSerializer, defaultValue);
		this.foldTransformation = new FoldTransformation(foldFunction);
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
	public TypeSerializer<ACC> getValueSerializer() {
		return valueSerializer;
	}

	// ------------------------------------------------------------------------
	//  state access
	// ------------------------------------------------------------------------

	@Override
	public ACC get() {
		return getInternal();
	}

	@Override
	public void add(T value) throws IOException {

		if (value == null) {
			clear();
			return;
		}

		try {
			stateTable.transform(currentNamespace, value, foldTransformation);
		} catch (Exception e) {
			throw new IOException("Could not add value to folding state.", e);
		}
	}

	private final class FoldTransformation implements StateTransformationFunction<ACC, T> {

		private final FoldFunction<T, ACC> foldFunction;

		FoldTransformation(FoldFunction<T, ACC> foldFunction) {
			this.foldFunction = Preconditions.checkNotNull(foldFunction);
		}

		@Override
		public ACC apply(ACC previousState, T value) throws Exception {
			return foldFunction.fold((previousState != null) ? previousState : getDefaultValue(), value);
		}
	}

	@SuppressWarnings("unchecked")
	static <K, N, SV, S extends State, IS extends S> IS create(
		StateDescriptor<S, SV> stateDesc,
		StateTable<K, N, SV> stateTable,
		TypeSerializer<K> keySerializer) {
		return (IS) new HeapFoldingState<>(
			stateTable,
			keySerializer,
			stateTable.getStateSerializer(),
			stateTable.getNamespaceSerializer(),
			stateDesc.getDefaultValue(),
			((FoldingStateDescriptor<SV, SV>) stateDesc).getFoldFunction());
	}
}
