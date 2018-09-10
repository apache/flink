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

package org.apache.flink.runtime.state.ttl;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.InternalReducingState;

import java.util.Collection;

/**
 * This class wraps reducing state with TTL logic.
 *
 * @param <K> The type of key the state is associated to
 * @param <N> The type of the namespace
 * @param <T> Type of the user value of state with TTL
 */
class TtlReducingState<K, N, T>
	extends AbstractTtlState<K, N, T, TtlValue<T>, InternalReducingState<K, N, TtlValue<T>>>
	implements InternalReducingState<K, N, T> {
	TtlReducingState(
		InternalReducingState<K, N, TtlValue<T>> originalState,
		StateTtlConfig config,
		TtlTimeProvider timeProvider,
		TypeSerializer<T> valueSerializer) {
		super(originalState, config, timeProvider, valueSerializer);
	}

	@Override
	public T get() throws Exception {
		return getInternal();
	}

	@Override
	public void add(T value) throws Exception {
		original.add(wrapWithTs(value));
	}

	@Override
	public void clear() {
		original.clear();
	}

	@Override
	public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
		original.mergeNamespaces(target, sources);
	}

	@Override
	public T getInternal() throws Exception {
		return getWithTtlCheckAndUpdate(original::getInternal, original::updateInternal);
	}

	@Override
	public void updateInternal(T valueToStore) throws Exception {
		original.updateInternal(wrapWithTs(valueToStore));
	}
}
