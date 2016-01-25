/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.runtime.state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.ArrayList;
import java.util.Collections;

/**
 * Generic implementation of {@link ListState} based on a wrapped {@link ValueState}.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <T> The type of the values stored in this {@code ListState}.
 * @param <Backend> The type of {@link AbstractStateBackend} that manages this {@code KvState}.
 * @param <W> Generic type that extends both the underlying {@code ValueState} and {@code KvState}.
 */
public class GenericListState<K, N, T, Backend extends AbstractStateBackend, W extends ValueState<ArrayList<T>> & KvState<K, N, ValueState<ArrayList<T>>, ValueStateDescriptor<ArrayList<T>>, Backend>>
	implements ListState<T>, KvState<K, N, ListState<T>, ListStateDescriptor<T>, Backend> {

	private final W wrappedState;

	@SuppressWarnings("unchecked")
	public GenericListState(ValueState<ArrayList<T>> wrappedState) {
		if (!(wrappedState instanceof KvState)) {
			throw new IllegalArgumentException("Wrapped state must be a KvState.");
		}
		this.wrappedState = (W) wrappedState;
	}

	@Override
	public void setCurrentKey(K key) {
		wrappedState.setCurrentKey(key);
	}

	@Override
	public void setCurrentNamespace(N namespace) {
		wrappedState.setCurrentNamespace(namespace);
	}

	@Override
	public KvStateSnapshot<K, N, ListState<T>, ListStateDescriptor<T>, Backend> snapshot(
		long checkpointId,
		long timestamp) throws Exception {
		KvStateSnapshot<K, N, ValueState<ArrayList<T>>, ValueStateDescriptor<ArrayList<T>>, Backend> wrappedSnapshot = wrappedState.snapshot(
			checkpointId,
			timestamp);
		return new Snapshot<>(wrappedSnapshot);
	}

	@Override
	public void dispose() {
		wrappedState.dispose();
	}

	@Override
	public Iterable<T> get() throws Exception {
		ArrayList<T> result = wrappedState.value();
		if (result == null) {
			return Collections.emptyList();
		}
		return result;
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

	private static class Snapshot<K, N, T, Backend extends AbstractStateBackend> implements KvStateSnapshot<K, N, ListState<T>, ListStateDescriptor<T>, Backend> {
		private static final long serialVersionUID = 1L;

		private final KvStateSnapshot<K, N, ValueState<ArrayList<T>>, ValueStateDescriptor<ArrayList<T>>, Backend> wrappedSnapshot;

		public Snapshot(KvStateSnapshot<K, N, ValueState<ArrayList<T>>, ValueStateDescriptor<ArrayList<T>>, Backend> wrappedSnapshot) {
			this.wrappedSnapshot = wrappedSnapshot;
		}

		@Override
		@SuppressWarnings("unchecked")
		public KvState<K, N, ListState<T>, ListStateDescriptor<T>, Backend> restoreState(
			Backend stateBackend,
			TypeSerializer<K> keySerializer,
			ClassLoader classLoader,
			long recoveryTimestamp) throws Exception {
			return new GenericListState((ValueState<T>) wrappedSnapshot.restoreState(stateBackend, keySerializer, classLoader, recoveryTimestamp));
		}

		@Override
		public void discardState() throws Exception {
			wrappedSnapshot.discardState();
		}

		@Override
		public long getStateSize() throws Exception {
			return wrappedSnapshot.getStateSize();
		}
	}
}
