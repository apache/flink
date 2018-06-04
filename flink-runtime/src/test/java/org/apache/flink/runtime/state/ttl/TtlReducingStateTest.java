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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.internal.InternalReducingState;

import java.util.List;

/** Test suite for {@link TtlReducingState}. */
public class TtlReducingStateTest
	extends TtlMergingStateBase.TtlIntegerMergingStateBase<TtlReducingState<?, String, Integer>, Integer, Integer> {
	@Override
	TtlReducingState<?, String, Integer> createState() {
		ReduceFunction<TtlValue<Integer>> ttlReduceFunction = new TtlReduceFunction<>(REDUCE, ttlConfig, timeProvider);
		return new TtlReducingState<>(
			new MockInternalReducingState<>(ttlReduceFunction), ttlConfig, timeProvider, null);
	}

	@Override
	void initTestValues() {
		updater = v -> ttlState.add(v);
		getter = () -> ttlState.get();
		originalGetter = () -> ttlState.original.get();

		updateEmpty = 5;
		updateUnexpired = 7;
		updateExpired = 6;

		getUpdateEmpty = 5;
		getUnexpired = 12;
		getUpdateExpired = 6;
	}

	@Override
	Integer getMergeResult(
		List<Tuple2<String, Integer>> unexpiredUpdatesToMerge,
		List<Tuple2<String, Integer>> finalUpdatesToMerge) {
		return getIntegerMergeResult(unexpiredUpdatesToMerge, finalUpdatesToMerge);
	}

	private static class MockInternalReducingState<K, N, T>
		extends MockInternalMergingState<K, N, T, T, T> implements InternalReducingState<K, N, T> {
		private final ReduceFunction<T> reduceFunction;

		private MockInternalReducingState(ReduceFunction<T> reduceFunction) {
			this.reduceFunction = reduceFunction;
		}

		@Override
		public T get() {
			return getInternal();
		}

		@Override
		public void add(T value) throws Exception {
			updateInternal(reduceFunction.reduce(get(), value));
		}

		@Override
		T mergeState(T t, T nAcc) throws Exception {
			return reduceFunction.reduce(t, nAcc);
		}
	}

	private static final ReduceFunction<Integer> REDUCE = (v1, v2) -> {
		if (v1 == null && v2 == null) {
			return null;
		} else if (v1 == null) {
			return v2;
		} else if (v2 == null) {
			return v1;
		} else {
			return v1 + v2;
		}
	};
}
