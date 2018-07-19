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

package org.apache.flink.streaming.tests.verify;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;

import javax.annotation.Nonnull;

import java.util.List;

class TtlAggregatingStateVerifier extends AbstractTtlStateVerifier<
	AggregatingStateDescriptor<Integer, Long, String>, AggregatingState<Integer, String>, Long, Integer, String> {
	TtlAggregatingStateVerifier() {
		super(new AggregatingStateDescriptor<>("TtlAggregatingStateVerifier", AGG_FUNC, LongSerializer.INSTANCE));
	}

	@Override
	@Nonnull
	State createState(@Nonnull FunctionInitializationContext context) {
		return context.getKeyedStateStore().getAggregatingState(stateDesc);
	}

	@Override
	@Nonnull
	public TypeSerializer<Integer> getUpdateSerializer() {
		return IntSerializer.INSTANCE;
	}

	@Override
	@Nonnull
	public Integer generateRandomUpdate() {
		return RANDOM.nextInt(100);
	}

	@Override
	String getInternal(@Nonnull AggregatingState<Integer, String> state) throws Exception {
		return state.get();
	}

	@Override
	void updateInternal(@Nonnull AggregatingState<Integer, String> state, Integer update) throws Exception {
		state.add(update);
	}

	@Override
	String expected(@Nonnull List<ValueWithTs<Integer>> updates, long currentTimestamp) {
		if (updates.isEmpty()) {
			return null;
		}
		long acc = AGG_FUNC.createAccumulator();
		long lastTs = updates.get(0).getTimestampAfterUpdate();
		for (ValueWithTs<Integer> update : updates) {
			if (expired(lastTs, update.getTimestampAfterUpdate())) {
				acc = AGG_FUNC.createAccumulator();
			}
			acc = AGG_FUNC.add(update.getValue(), acc);
			lastTs = update.getTimestampAfterUpdate();
		}
		return expired(lastTs, currentTimestamp) ? null : AGG_FUNC.getResult(acc);
	}

	private static final AggregateFunction<Integer, Long, String> AGG_FUNC =
		new AggregateFunction<Integer, Long, String>() {
			@Override
			public Long createAccumulator() {
				return 3L;
			}

			@Override
			public Long add(Integer value, Long accumulator) {
				return accumulator + value;
			}

			@Override
			public String getResult(Long accumulator) {
				return Long.toString(accumulator);
			}

			@Override
			public Long merge(Long a, Long b) {
				return a + b;
			}
		};
}
