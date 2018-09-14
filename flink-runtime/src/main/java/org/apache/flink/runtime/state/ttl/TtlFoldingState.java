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

import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.InternalFoldingState;

/**
 * This class wraps folding state with TTL logic.
 *
 * @param <T> Type of the values folded into the state
 * @param <ACC> Type of the value in the state
 *
 * @deprecated use {@link AggregatingState} instead
 */
@Deprecated
class TtlFoldingState<K, N, T, ACC>
	extends AbstractTtlState<K, N, ACC, TtlValue<ACC>, InternalFoldingState<K, N, T, TtlValue<ACC>>>
	implements InternalFoldingState<K, N, T, ACC> {
	TtlFoldingState(
		InternalFoldingState<K, N, T, TtlValue<ACC>> originalState,
		StateTtlConfig config,
		TtlTimeProvider timeProvider,
		TypeSerializer<ACC> valueSerializer) {
		super(originalState, config, timeProvider, valueSerializer);
	}

	@Override
	public ACC get() throws Exception {
		return getInternal();
	}

	@Override
	public void add(T value) throws Exception {
		original.add(value);
	}

	@Override
	public void clear() {
		original.clear();
	}

	@Override
	public ACC getInternal() throws Exception {
		return getWithTtlCheckAndUpdate(original::getInternal, original::updateInternal);
	}

	@Override
	public void updateInternal(ACC valueToStore) throws Exception {
		original.updateInternal(wrapWithTs(valueToStore));
	}
}
