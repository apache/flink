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

package org.apache.flink.runtime.state.internal;

import org.apache.flink.api.common.state.AppendingState;

/**
 * The peer to the {@link AppendingState} in the internal state type hierarchy.
 *
 * <p>See {@link InternalKvState} for a description of the internal state hierarchy.
 *
 * @param <K> The type of key the state is associated to
 * @param <N> The type of the namespace
 * @param <IN> The type of elements added to the state
 * @param <SV> The type of elements in the state
 * @param <OUT> The type of the resulting element in the state
 */
public interface InternalAppendingState<K, N, IN, SV, OUT> extends InternalKvState<K, N, SV>, AppendingState<IN, OUT> {
	/**
	 * Get internally stored value.
	 *
	 * @return internally stored value.
	 *
	 * @throws Exception The method may forward exception thrown internally (by I/O or functions).
	 */
	SV getInternal() throws Exception;

	/**
	 * Update internally stored value.
	 *
	 * @param valueToStore new value to store.
	 *
	 * @throws Exception The method may forward exception thrown internally (by I/O or functions).
	 */
	void updateInternal(SV valueToStore) throws Exception;
}
