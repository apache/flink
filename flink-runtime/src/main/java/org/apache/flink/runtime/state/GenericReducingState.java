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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ValueState;

/**
 * Generic implementation of {@link ReducingState} based on a wrapped {@link ValueState}.
 *
 * @param <N> The type of the namespace.
 * @param <T> The type of the values stored in this {@code ReducingState}.
 * @param <W> Generic type that extends both the underlying {@code ValueState} and {@code KvState}.
 */
public class GenericReducingState<N, T, W extends ValueState<T> & KvState<N>>
	implements ReducingState<T>, KvState<N> {

	private final W wrappedState;
	private final ReduceFunction<T> reduceFunction;

	/**
	 * Creates a new {@code ReducingState} that wraps the given {@link ValueState}. The
	 * {@code ValueState} must have a default value of {@code null}.
	 *
	 * @param wrappedState The wrapped {@code ValueState}
	 * @param reduceFunction The {@code ReduceFunction} to use for combining values.
	 */
	@SuppressWarnings("unchecked")
	public GenericReducingState(ValueState<T> wrappedState, ReduceFunction<T> reduceFunction) {
		if (!(wrappedState instanceof KvState)) {
			throw new IllegalArgumentException("Wrapped state must be a KvState.");
		}
		this.wrappedState = (W) wrappedState;
		this.reduceFunction = reduceFunction;
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
	public T get() throws Exception {
		return wrappedState.value();
	}

	@Override
	public void add(T value) throws Exception {
		T currentValue = wrappedState.value();
		if (currentValue == null) {
			wrappedState.update(value);
		} else {
			wrappedState.update(reduceFunction.reduce(currentValue, value));
		}
	}

	@Override
	public void clear() {
		wrappedState.clear();
	}
}
