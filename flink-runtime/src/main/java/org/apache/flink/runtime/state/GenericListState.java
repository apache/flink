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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ValueState;

import java.util.ArrayList;

/**
 * Generic implementation of {@link ListState} based on a wrapped {@link ValueState}.
 *
 * @param <N> The type of the namespace.
 * @param <T> The type of the values stored in this {@code ListState}.
 * @param <W> Generic type that extends both the underlying {@code ValueState} and {@code KvState}.
 */
public class GenericListState<N, T, W extends ValueState<ArrayList<T>> & KvState<N>>
	implements ListState<T>, KvState<N> {

	private final W wrappedState;

	/**
	 * Creates a new {@code ListState} that wraps the given {@link ValueState}. The
	 * {@code ValueState} must have a default value of {@code null}.
	 *
	 * @param wrappedState The wrapped {@code ValueState}
	 */
	@SuppressWarnings("unchecked")
	public GenericListState(ValueState<ArrayList<T>> wrappedState) {
		if (!(wrappedState instanceof KvState)) {
			throw new IllegalArgumentException("Wrapped state must be a KvState.");
		}
		this.wrappedState = (W) wrappedState;
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
	public Iterable<T> get() throws Exception {
		return wrappedState.value();
	}

	@Override
	public void add(T value) throws Exception {
		ArrayList<T> currentValue = wrappedState.value();
		if (currentValue == null) {
			currentValue = new ArrayList<>();
			currentValue.add(value);
			wrappedState.update(currentValue);
		} else {
			currentValue.add(value);
			wrappedState.update(currentValue);
		}
	}

	@Override
	public void clear() {
		wrappedState.clear();
	}
}
