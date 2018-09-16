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

package org.apache.flink.queryablestate.client.state;

import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * A read-only {@link ReducingState} that does not allow for modifications.
 *
 * <p>This is the result returned when querying Flink's keyed state using the
 * {@link org.apache.flink.queryablestate.client.QueryableStateClient Queryable State Client} and
 * providing an {@link ReducingStateDescriptor}.
 */
public final class ImmutableReducingState<V> extends ImmutableState implements ReducingState<V> {

	private final V value;

	private ImmutableReducingState(V value) {
		this.value = Preconditions.checkNotNull(value);
	}

	@Override
	public V get() {
		return value;
	}

	@Override
	public void add(V newValue) {
		throw MODIFICATION_ATTEMPT_ERROR;
	}

	@Override
	public void clear() {
		throw MODIFICATION_ATTEMPT_ERROR;
	}

	@SuppressWarnings("unchecked")
	public static <V, S extends State> S createState(
		StateDescriptor<S, V> stateDescriptor,
		byte[] serializedState) throws IOException {
		final V state = KvStateSerializer.deserializeValue(
			serializedState,
			stateDescriptor.getSerializer());
		return (S) new ImmutableReducingState<>(state);
	}
}
