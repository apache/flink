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

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Test suite for {@link TtlAggregatingState}. */
public class TtlAggregatingStateTest
	extends TtlMergingStateBase.TtlIntegerMergingStateBase<TtlAggregatingState<?, String, Integer, Long, String>, Integer, String> {
	private static final long DEFAULT_ACCUMULATOR = 3L;

	@Override
	TtlAggregatingState<?, String, Integer, Long, String> createState() {
		TtlAggregateFunction<Integer, Long, String> ttlAggregateFunction =
			new TtlAggregateFunction<>(AGGREGATE, ttlConfig, timeProvider);
		return new TtlAggregatingState<>(
			new MockInternalTtlAggregatingState<>(ttlAggregateFunction),
			ttlConfig, timeProvider, null, ttlAggregateFunction);
	}

	@Override
	void initTestValues() {
		updater = v -> ttlState.add(v);
		getter = () -> ttlState.get();
		originalGetter = () -> ttlState.original.get();

		updateEmpty = 5;
		updateUnexpired = 7;
		updateExpired = 6;

		getUpdateEmpty = "8";
		getUnexpired = "15";
		getUpdateExpired = "9";
	}

	@Override
	String getMergeResult(
		List<Tuple2<String, Integer>> unexpiredUpdatesToMerge,
		List<Tuple2<String, Integer>> finalUpdatesToMerge) {
		Set<String> namespaces = new HashSet<>();
		unexpiredUpdatesToMerge.forEach(t -> namespaces.add(t.f0));
		finalUpdatesToMerge.forEach(t -> namespaces.add(t.f0));
		return Integer.toString(getIntegerMergeResult(unexpiredUpdatesToMerge, finalUpdatesToMerge) +
			namespaces.size() * (int) DEFAULT_ACCUMULATOR);
	}

	private static class MockInternalTtlAggregatingState<K, N, IN, ACC, OUT>
		extends MockInternalMergingState<K, N, IN, ACC, OUT> implements InternalAggregatingState<K, N, IN, ACC, OUT> {
		private final AggregateFunction<IN, ACC, OUT> aggregateFunction;

		private MockInternalTtlAggregatingState(AggregateFunction<IN, ACC, OUT> aggregateFunction) {
			this.aggregateFunction = aggregateFunction;
		}

		@Override
		public OUT get() {
			return aggregateFunction.getResult(getInternal());
		}

		@Override
		public void add(IN value) {
			updateInternal(aggregateFunction.add(value,  getInternal()));
		}

		@Override
		ACC mergeState(ACC acc, ACC nAcc) {
			return aggregateFunction.merge(acc, nAcc);
		}
	}

	private static final AggregateFunction<Integer, Long, String> AGGREGATE =
		new AggregateFunction<Integer, Long, String>() {
			@Override
			public Long createAccumulator() {
				return DEFAULT_ACCUMULATOR;
			}

			@Override
			public Long add(Integer value, Long accumulator) {
				return accumulator + value;
			}

			@Override
			public String getResult(Long accumulator) {
				return accumulator.toString();
			}

			@Override
			public Long merge(Long a, Long b) {
				return a + b;
			}
		};
}
