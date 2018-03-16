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
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * A read-only {@link AggregatingState} that <b>does not</b> allow for modifications.
 *
 * <p>This is the type of the result returned when querying Flink's keyed state using the
 * {@link org.apache.flink.queryablestate.client.QueryableStateClient Queryable State Client} and
 * providing an {@link AggregatingStateDescriptor}.
 */
@PublicEvolving
public final class ImmutableAggregatingState<IN, OUT> extends ImmutableState implements AggregatingState<IN, OUT> {

	private final OUT value;

	private ImmutableAggregatingState(OUT value) {
		this.value = Preconditions.checkNotNull(value);
	}

	@Override
	public OUT get() {
		return value;
	}

	@Override
	public void add(Object newValue) {
		throw MODIFICATION_ATTEMPT_ERROR;
	}

	@Override
	public void clear() {
		throw MODIFICATION_ATTEMPT_ERROR;
	}

	public static <IN, ACC, OUT> ImmutableAggregatingState<IN, OUT> createState(
			final AggregatingStateDescriptor<IN, ACC, OUT> stateDescriptor,
			final byte[] serializedValue) throws IOException {

		final ACC accumulator = KvStateSerializer.deserializeValue(
				serializedValue,
				stateDescriptor.getSerializer());

		final OUT state = stateDescriptor.getAggregateFunction().getResult(accumulator);
		return new ImmutableAggregatingState<>(state);
	}
}
