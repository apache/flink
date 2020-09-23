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

package org.apache.flink.streaming.api.operators.sorted.state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.InternalListState;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link ListState} which keeps value for a single key at a time.
 */
class BatchExecutionKeyListState<K, N, T>
		extends MergingAbstractBatchExecutionKeyState<K, N, List<T>, T, Iterable<T>>
		implements InternalListState<K, N, T> {

	protected BatchExecutionKeyListState(
			List<T> defaultValue,
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer,
			TypeSerializer<List<T>> stateTypeSerializer) {
		super(defaultValue, keySerializer, namespaceSerializer, stateTypeSerializer);
	}

	@Override
	public void update(List<T> values) {
		checkNotNull(values);
		clear();
		for (T value : values) {
			add(value);
		}
	}

	@Override
	public void addAll(List<T> values) {
		if (checkNotNull(values).isEmpty()) {
			return;
		}
		for (T value : values) {
			add(value);
		}
	}

	@Override
	public void add(T value) {
		checkNotNull(value);
		initIfNull();
		getCurrentNamespaceValue().add(value);
	}

	private void initIfNull() {
		if (getCurrentNamespaceValue() == null) {
			setCurrentNamespaceValue(new ArrayList<>());
		}
	}

	@Override
	public Iterable<T> get() throws Exception {
		return getCurrentNamespaceValue();
	}

	@SuppressWarnings("unchecked")
	static <T, K, N, SV, S extends State, IS extends S> IS create(
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer,
			StateDescriptor<S, SV> stateDesc) {
		return (IS) new BatchExecutionKeyListState<>(
			(List<T>) stateDesc.getDefaultValue(),
			keySerializer,
			namespaceSerializer,
			(TypeSerializer<List<T>>) stateDesc.getSerializer());
	}

	@Override
	protected List<T> merge(List<T> target, List<T> source) {
		target.addAll(source);
		return target;
	}
}
