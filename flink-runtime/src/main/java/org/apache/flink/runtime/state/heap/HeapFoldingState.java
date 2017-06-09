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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.internal.InternalFoldingState;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * Heap-backed partitioned {@link FoldingState} that is
 * snapshotted into files.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <T> The type of the values that can be folded into the state.
 * @param <ACC> The type of the value in the folding state.
 *
 * @deprecated will be removed in a future version
 */
@Deprecated
public class HeapFoldingState<K, N, T, ACC>
		extends AbstractHeapState<K, N, ACC, FoldingState<T, ACC>, FoldingStateDescriptor<T, ACC>>
		implements InternalFoldingState<N, T, ACC> {

	/** The function used to fold the state */
	private final FoldTransformation<T, ACC> foldTransformation;

	/**
	 * Creates a new key/value state for the given hash map of key/value pairs.
	 *
	 * @param stateDesc The state identifier for the state. This contains name
	 *                           and can create a default state value.
	 * @param stateTable The state tab;e to use in this kev/value state. May contain initial state.
	 */
	public HeapFoldingState(
			FoldingStateDescriptor<T, ACC> stateDesc,
			StateTable<K, N, ACC> stateTable,
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer) {
		super(stateDesc, stateTable, keySerializer, namespaceSerializer);
		this.foldTransformation = new FoldTransformation<>(stateDesc);
	}

	// ------------------------------------------------------------------------
	//  state access
	// ------------------------------------------------------------------------

	@Override
	public ACC get() {
		return stateTable.get(currentNamespace);
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

	private static final class FoldTransformation<T, ACC> implements StateTransformationFunction<ACC, T> {

		private final FoldingStateDescriptor<T, ACC> stateDescriptor;
		private final FoldFunction<T, ACC> foldFunction;

		FoldTransformation(FoldingStateDescriptor<T, ACC> stateDesc) {
			this.stateDescriptor = Preconditions.checkNotNull(stateDesc);
			this.foldFunction = Preconditions.checkNotNull(stateDesc.getFoldFunction());
		}

		@Override
		public ACC apply(ACC previousState, T value) throws Exception {
			return foldFunction.fold((previousState != null) ? previousState : stateDescriptor.getDefaultValue(), value);
		}
	}
}
