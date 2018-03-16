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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.List;

/**
 * A read-only {@link ListState} that does not allow for modifications.
 *
 * <p>This is the result returned when querying Flink's keyed state using the
 * {@link org.apache.flink.queryablestate.client.QueryableStateClient Queryable State Client} and
 * providing an {@link ListStateDescriptor}.
 */
@PublicEvolving
public final class ImmutableListState<V> extends ImmutableState implements ListState<V> {

	private final List<V> listState;

	private ImmutableListState(final List<V> state) {
		this.listState = Preconditions.checkNotNull(state);
	}

	@Override
	public Iterable<V> get() {
		return listState;
	}

	@Override
	public void add(V value) {
		throw MODIFICATION_ATTEMPT_ERROR;
	}

	@Override
	public void clear() {
		throw MODIFICATION_ATTEMPT_ERROR;
	}

	public static <V> ImmutableListState<V> createState(
			final ListStateDescriptor<V> stateDescriptor,
			final byte[] serializedState) throws IOException {

		final List<V> state = KvStateSerializer.deserializeList(
				serializedState,
				stateDescriptor.getElementSerializer());
		return new ImmutableListState<>(state);
	}

	@Override
	public void update(List<V> values) throws Exception {
		throw MODIFICATION_ATTEMPT_ERROR;
	}

	@Override
	public void addAll(List<V> values) throws Exception {
		throw MODIFICATION_ATTEMPT_ERROR;
	}
}
