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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.state.FoldingState;
import org.apache.flink.api.common.state.ValueState;

/**
 * Generic implementation of {@link FoldingState} based on a wrapped {@link ValueState}.
 *
 * @param <N> The type of the namespace.
 * @param <T> The type of the values that can be folded into the state.
 * @param <ACC> The type of the value in the folding state.
 * @param <W> Generic type that extends both the underlying {@code ValueState} and {@code KvState}.
 */
public class GenericFoldingState<N, T, ACC, W extends ValueState<ACC> & KvState<N>>
	implements FoldingState<T, ACC>, KvState<N> {

	private final W wrappedState;
	private final FoldFunction<T, ACC> foldFunction;

	/**
	 * Creates a new {@code FoldingState} that wraps the given {@link ValueState}. The
	 * {@code ValueState} must have the initial value of the fold as default value.
	 *
	 * @param wrappedState The wrapped {@code ValueState}
	 * @param foldFunction The {@code FoldFunction} to use for folding values into the state
	 */
	@SuppressWarnings("unchecked")
	public GenericFoldingState(ValueState<ACC> wrappedState, FoldFunction<T, ACC> foldFunction) {
		if (!(wrappedState instanceof KvState)) {
			throw new IllegalArgumentException("Wrapped state must be a KvState.");
		}
		this.wrappedState = (W) wrappedState;
		this.foldFunction = foldFunction;
	}

	@Override
	public void setCurrentNamespace(N namespace) {
		wrappedState.setCurrentNamespace(namespace);
	}

	@Override
	public byte[] getSerializedValue(byte[] serializedKeyAndNamespace) throws Exception {
		return wrappedState.getSerializedValue(serializedKeyAndNamespace);
	}

	@Override
	public ACC get() throws Exception {
		return wrappedState.value();
	}

	@Override
	public void add(T value) throws Exception {
		ACC currentValue = wrappedState.value();
		wrappedState.update(foldFunction.fold(currentValue, value));
	}

	@Override
	public void clear() {
		wrappedState.clear();
	}
}
