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

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.state.FoldingState;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/**
 * Generic implementation of {@link FoldingState} based on a wrapped {@link ValueState}.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <T> The type of the values that can be folded into the state.
 * @param <ACC> The type of the value in the folding state.
 * @param <Backend> The type of {@link AbstractStateBackend} that manages this {@code KvState}.
 * @param <W> Generic type that extends both the underlying {@code ValueState} and {@code KvState}.
 */
public class GenericFoldingState<K, N, T, ACC, Backend extends AbstractStateBackend, W extends ValueState<ACC> & KvState<K, N, ValueState<ACC>, ValueStateDescriptor<ACC>, Backend>>
	implements FoldingState<T, ACC>, KvState<K, N, FoldingState<T, ACC>, FoldingStateDescriptor<T, ACC>, Backend> {

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
	public void setCurrentKey(K key) {
		wrappedState.setCurrentKey(key);
	}

	@Override
	public void setCurrentNamespace(N namespace) {
		wrappedState.setCurrentNamespace(namespace);
	}

	@Override
	public KvStateSnapshot<K, N, FoldingState<T, ACC>, FoldingStateDescriptor<T, ACC>, Backend> snapshot(
		long checkpointId,
		long timestamp) throws Exception {
		KvStateSnapshot<K, N, ValueState<ACC>, ValueStateDescriptor<ACC>, Backend> wrappedSnapshot = wrappedState.snapshot(
			checkpointId,
			timestamp);
		return new Snapshot<>(wrappedSnapshot, foldFunction);
	}

	@Override
	public void dispose() {
		wrappedState.dispose();
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

	private static class Snapshot<K, N, T, ACC, Backend extends AbstractStateBackend> implements KvStateSnapshot<K, N, FoldingState<T, ACC>, FoldingStateDescriptor<T, ACC>, Backend> {
		private static final long serialVersionUID = 1L;

		private final KvStateSnapshot<K, N, ValueState<ACC>, ValueStateDescriptor<ACC>, Backend> wrappedSnapshot;

		private final FoldFunction<T, ACC> foldFunction;

		public Snapshot(KvStateSnapshot<K, N, ValueState<ACC>, ValueStateDescriptor<ACC>, Backend> wrappedSnapshot,
			FoldFunction<T, ACC> foldFunction) {
			this.wrappedSnapshot = wrappedSnapshot;
			this.foldFunction = foldFunction;
		}

		@Override
		@SuppressWarnings("unchecked")
		public KvState<K, N, FoldingState<T, ACC>, FoldingStateDescriptor<T, ACC>, Backend> restoreState(
			Backend stateBackend,
			TypeSerializer<K> keySerializer,
			ClassLoader classLoader,
			long recoveryTimestamp) throws Exception {
			return new GenericFoldingState((ValueState<ACC>) wrappedSnapshot.restoreState(stateBackend, keySerializer, classLoader, recoveryTimestamp), foldFunction);
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
